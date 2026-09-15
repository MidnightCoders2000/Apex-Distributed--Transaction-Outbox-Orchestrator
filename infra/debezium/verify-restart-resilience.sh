#!/usr/bin/env bash
# Proves that a Postgres restart does not lose outbox events: the logical
# replication slot holds WAL server-side until Debezium acks past it, and
# Debezium's own offsets live in the apex_connect_offsets Kafka topic, not
# in connector memory. See docs/runbooks/cdc-restart-verification.md.
set -euo pipefail
cd "$(dirname "$0")/../.."

set -a
source .env
set +a

COMPOSE="docker-compose --profile cdc-test"
CONNECTOR_URL="http://localhost:8083/connectors/apex-outbox-connector"
TOPIC="apex.public.outbox_event"
TS="$(date +%s)"
N=20

echo "== verify-restart-resilience: run ${TS} =="

wait_pg_ready() {
  local tries=0
  until $COMPOSE exec -T postgres pg_isready -U apex -d apex >/dev/null 2>&1; do
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
      echo "postgres did not become ready in time" >&2
      exit 1
    fi
    sleep 1
  done
}

wait_connector_running() {
  local tries=0
  local status
  while true; do
    status="$(curl -s "${CONNECTOR_URL}/status" || true)"
    if echo "$status" | grep -q '"connector":{"state":"RUNNING"' \
      && ! echo "$status" | grep -q '"state":"FAILED"'; then
      return 0
    fi
    tries=$((tries + 1))
    if [ "$tries" -ge 60 ]; then
      echo "connector did not reach RUNNING (last status: $status)" >&2
      return 1
    fi
    sleep 2
  done
}

get_slot_info() {
  $COMPOSE exec -T postgres psql -U apex -d apex -t -A -c \
    "SELECT slot_name || '|' || active || '|' || confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'apex_outbox_slot';" \
    | tr -d '\r'
}

wait_slot_exists() {
  local tries=0
  while [ -z "$(get_slot_info)" ]; do
    tries=$((tries + 1))
    if [ "$tries" -ge 30 ]; then
      echo "replication slot apex_outbox_slot never appeared" >&2
      return 1
    fi
    sleep 1
  done
}

# Waits for confirmed_flush_lsn to move past $1 (a pg_lsn string). Bounds
# the wait since heartbeat.interval.ms=10000 is what drives flush progress
# during otherwise-quiet periods.
wait_lsn_advance() {
  local base="$1" tries=0 lsn cmp
  while true; do
    lsn="$(get_slot_info | cut -d'|' -f3)"
    if [ -n "$lsn" ]; then
      cmp="$($COMPOSE exec -T postgres psql -U apex -d apex -t -A -c \
        "SELECT ('${lsn}'::pg_lsn > '${base}'::pg_lsn);" | tr -d '\r ')"
      [ "$cmp" = "t" ] && return 0
    fi
    tries=$((tries + 1))
    if [ "$tries" -ge 40 ]; then
      return 1
    fi
    sleep 2
  done
}

insert_markers() {
  local suffix="$1"
  $COMPOSE exec -T postgres psql -U apex -d apex -v ON_ERROR_STOP=1 -q -c "
    INSERT INTO outbox_event (aggregate_type, aggregate_id, event_type, payload, correlation_id)
    SELECT 'order', gen_random_uuid()::text, 'OrderCreated', jsonb_build_object('seq', n), 'restart-test-${TS}-${suffix}'
    FROM generate_series(1, ${N}) AS n;
  " >/dev/null
}

echo "-- resetting any prior connector state --"
# The connector name and topic.prefix are shared with the Neon-targeted
# registration (register-connector.sh). Kafka Connect keys offsets by
# {server: topic.prefix}, not by target host, so a stale offset from a
# previous run (against Neon or an earlier local container) would make
# Debezium resume from an LSN that doesn't exist in the fresh local DB
# and silently skip the events this test just inserted. Stop + reset
# offsets + delete before every run so registration always starts clean.
curl -s -o /dev/null -X PUT "${CONNECTOR_URL}/stop" 2>&1 || true
curl -s -o /dev/null -X DELETE "${CONNECTOR_URL}/offsets" 2>&1 || true
curl -s -o /dev/null -X DELETE "${CONNECTOR_URL}" 2>&1 || true

echo "-- fresh cdc-test postgres --"
$COMPOSE rm -fsv postgres >/dev/null 2>&1 || true
$COMPOSE up -d postgres
wait_pg_ready

echo "-- applying bootstrap DDL --"
$COMPOSE exec -T postgres psql -U apex -d apex -v ON_ERROR_STOP=1 < infra/db/outbox_event.sql >/dev/null

echo "-- registering connector against local postgres --"
export DIRECT_DB_HOST=postgres
export DB_PORT=5432
export DB_SSLMODE=disable
export DB_USERNAME=apex
export DB_PASSWORD=apex
export DB_NAME=apex
envsubst < infra/debezium/outbox-connector.json \
  | curl -s -X POST -H "Content-Type: application/json" --data @- http://localhost:8083/connectors >/dev/null

INITIAL_RUNNING=false
if wait_connector_running; then INITIAL_RUNNING=true; fi
wait_slot_exists || true

BASELINE="$(get_slot_info)"
IFS='|' read -r BASE_SLOT BASE_ACTIVE BASE_LSN <<< "$BASELINE"
echo "baseline slot: ${BASELINE:-<none>}"

echo "-- inserting ${N} pre-restart markers --"
insert_markers "pre"

# Wait for Kafka Connect's periodic offset commit to durably flush past the
# pre-markers before restarting. Debezium is at-least-once: restarting
# while a commit is still in flight would legitimately redeliver events
# already sent but not yet checkpointed. Waiting here isolates what this
# test is actually about (does the slot/offsets survive a restart) from
# that separate, already-documented at-least-once behavior (see the
# idempotency-key note in PLAN.md's Day-0 contract).
if [ -n "$BASE_LSN" ]; then
  wait_lsn_advance "$BASE_LSN" || true
fi
MID="$(get_slot_info)"
MID_LSN="$(printf '%s' "$MID" | cut -d'|' -f3)"

echo "-- restarting postgres --"
$COMPOSE restart postgres
wait_pg_ready

echo "-- inserting ${N} post-restart markers (no re-registration) --"
insert_markers "post"

AFTER_RUNNING=false
if wait_connector_running; then AFTER_RUNNING=true; fi
if [ -n "$MID_LSN" ]; then
  wait_lsn_advance "$MID_LSN" || true
fi

AFTER="$(get_slot_info)"
IFS='|' read -r AFTER_SLOT AFTER_ACTIVE AFTER_LSN <<< "$AFTER"
echo "after-restart slot: ${AFTER:-<none>}"

LSN_ADVANCED=false
if [ -n "$BASE_LSN" ] && [ -n "$AFTER_LSN" ]; then
  CMP="$($COMPOSE exec -T postgres psql -U apex -d apex -t -A -c \
    "SELECT ('${AFTER_LSN}'::pg_lsn > '${BASE_LSN}'::pg_lsn);" | tr -d '\r ')"
  [ "$CMP" = "t" ] && LSN_ADVANCED=true
fi

echo "-- consuming ${TOPIC} from Kafka --"
CONSUMED="$(docker-compose exec -T kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic "${TOPIC}" \
  --from-beginning \
  --timeout-ms 30000 2>/dev/null || true)"

PRE_COUNT="$(printf '%s' "$CONSUMED" | grep -c "restart-test-${TS}-pre" || true)"
POST_COUNT="$(printf '%s' "$CONSUMED" | grep -c "restart-test-${TS}-post" || true)"

# Criterion a: all post-restart markers reached Kafka without re-registering the connector.
PASS_A=false
[ "$POST_COUNT" -eq "$N" ] && PASS_A=true

# Criterion b: same slot object survived the restart and confirmed_flush_lsn advanced.
PASS_B=false
if [ "$BASE_SLOT" = "apex_outbox_slot" ] && [ "$AFTER_SLOT" = "apex_outbox_slot" ] && [ "$LSN_ADVANCED" = "true" ]; then
  PASS_B=true
fi

# Criterion c: no gap, no duplicates across the restart boundary.
PASS_C=false
[ "$PRE_COUNT" -eq "$N" ] && [ "$POST_COUNT" -eq "$N" ] && PASS_C=true

# Criterion d: connector returned to RUNNING on its own, no task FAILED.
PASS_D=false
[ "$INITIAL_RUNNING" = "true" ] && [ "$AFTER_RUNNING" = "true" ] && PASS_D=true

fmt() { [ "$1" = "true" ] && echo "PASS" || echo "FAIL"; }

echo ""
echo "== results (run ${TS}) =="
printf '%-4s %-60s %-6s\n' "a)" "post-restart markers reached Kafka (${POST_COUNT}/${N}), no re-register" "$(fmt $PASS_A)"
printf '%-4s %-60s %-6s\n' "b)" "same slot survived, confirmed_flush_lsn advanced" "$(fmt $PASS_B)"
printf '%-4s %-60s %-6s\n' "c)" "pre=${PRE_COUNT}/${N} post=${POST_COUNT}/${N}, no gap/dup" "$(fmt $PASS_C)"
printf '%-4s %-60s %-6s\n' "d)" "connector back to RUNNING, no FAILED task" "$(fmt $PASS_D)"
echo ""

if [ "$PASS_A" = "true" ] && [ "$PASS_B" = "true" ] && [ "$PASS_C" = "true" ] && [ "$PASS_D" = "true" ]; then
  echo "OVERALL: PASS"
  exit 0
else
  echo "OVERALL: FAIL"
  exit 1
fi
