#!/usr/bin/env bash
# Proves that a Postgres restart does not lose outbox events: the logical
# replication slot holds WAL server-side until Debezium acks past it, and
# Debezium's own offsets live in the apex_connect_offsets Kafka topic, not
# in connector memory. See docs/runbooks/cdc-restart-verification.md.
#
# This test registers its OWN connector -- name, topic.prefix, slot and
# publication are all suffixed for the test -- so it never touches the real
# Neon-targeted registration from register-connector.sh, that connector's
# offsets, or the apex.public.outbox_event topic real consumers read.
set -euo pipefail
cd "$(dirname "$0")/../.."

COMPOSE="docker-compose --profile cdc-test"

CONNECTOR_NAME="apex-outbox-connector-cdctest"
TOPIC_PREFIX="apextest"
SLOT_NAME="apex_outbox_slot_cdctest"
PUBLICATION_NAME="apex_outbox_pub_cdctest"
export CONNECTOR_NAME TOPIC_PREFIX SLOT_NAME PUBLICATION_NAME

CONNECTOR_URL="http://localhost:8083/connectors/${CONNECTOR_NAME}"
TOPIC="${TOPIC_PREFIX}.public.outbox_event"
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

# concat_ws, not ||: confirmed_flush_lsn is NULL on a freshly created slot
# that has not confirmed anything yet, and `a || NULL` is NULL for the
# whole row -- which would make wait_slot_exists spin to its timeout on a
# slot that does in fact exist.
get_slot_info() {
  $COMPOSE exec -T postgres psql -U apex -d apex -t -A -c \
    "SELECT concat_ws('|', slot_name, active, confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name = '${SLOT_NAME}';" \
    | tr -d '\r'
}

wait_slot_exists() {
  local tries=0
  while [ -z "$(get_slot_info)" ]; do
    tries=$((tries + 1))
    if [ "$tries" -ge 30 ]; then
      echo "replication slot ${SLOT_NAME} never appeared" >&2
      return 1
    fi
    sleep 1
  done
}

# Polls until the slot reports active = t. A single read of
# pg_replication_slots is a point-in-time sample: catching the connector
# between reconnect retries would report `f` and fail criterion (b) for a
# slot that is about to come back. Every other wait in this script is a
# bounded poll; this one has to be too.
wait_slot_active() {
  local tries=0
  while [ "$(get_slot_info | cut -d'|' -f2)" != "t" ]; do
    tries=$((tries + 1))
    if [ "$tries" -ge 30 ]; then
      return 1
    fi
    sleep 2
  done
  return 0
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

# The test connector is disposable and scoped to this run, so drop it and
# let the fresh local Postgres below get a fresh registration. Nothing here
# can reach apex-outbox-connector (the Neon one) -- different name, different
# topic.prefix, so different offsets too.
echo "-- dropping any prior ${CONNECTOR_NAME} --"
curl -s -o /dev/null -X DELETE "${CONNECTOR_URL}" || true

echo "-- fresh cdc-test postgres --"
$COMPOSE rm -fsv postgres >/dev/null 2>&1 || true
$COMPOSE up -d postgres
wait_pg_ready

echo "-- applying bootstrap DDL --"
$COMPOSE exec -T postgres psql -U apex -d apex -v ON_ERROR_STOP=1 < infra/db/outbox_event.sql >/dev/null

echo "-- registering ${CONNECTOR_NAME} against local postgres --"
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
IFS='|' read -r _ BASE_ACTIVE BASE_LSN <<< "$BASELINE"
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

SLOT_REACTIVATED=false
if wait_slot_active; then SLOT_REACTIVATED=true; fi
# Read `active` at the moment (b) is decided, not after the LSN wait below,
# which can run for 80s. Reporting the later value could print (t -> f)
# next to a PASS verdict that was taken from SLOT_REACTIVATED.
ACTIVE_AT_CHECK="$(get_slot_info | cut -d'|' -f2)"

if [ -n "$MID_LSN" ]; then
  wait_lsn_advance "$MID_LSN" || true
fi

AFTER="$(get_slot_info)"
IFS='|' read -r _ _ AFTER_LSN <<< "$AFTER"
echo "after-restart slot: ${AFTER:-<none>}"

# Compare against MID_LSN (taken immediately before the restart), not
# BASE_LSN (taken before the 20 pre-restart inserts). Those inserts advance
# the LSN on their own, so an AFTER > BASE comparison would hold even if
# the restart broke replication outright and nothing moved afterwards --
# the same class of can't-fail check as last round's slot_name comparison.
# This also gives the wait_lsn_advance "$MID_LSN" call above a result
# instead of discarding it.
LSN_ADVANCED=false
if [ -n "$MID_LSN" ] && [ -n "$AFTER_LSN" ]; then
  CMP="$($COMPOSE exec -T postgres psql -U apex -d apex -t -A -c \
    "SELECT ('${AFTER_LSN}'::pg_lsn > '${MID_LSN}'::pg_lsn);" | tr -d '\r ')"
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

# Criterion a: no gap, no duplicates across the restart boundary. The
# post-restart batch was inserted without re-registering the connector, but
# that is a property of this script's flow, not a separately asserted
# check -- nothing below re-POSTs the config.
PASS_A=false
[ "$PRE_COUNT" -eq "$N" ] && [ "$POST_COUNT" -eq "$N" ] && PASS_A=true

# Criterion b: the connector re-attached to the slot after the restart
# (active = t) and its confirmed_flush_lsn advanced past the baseline.
# Comparing slot_name before/after would be a tautology: slot.name is fixed
# in the config, so a dropped-and-recreated slot has the same name and a
# higher LSN too. `active` is the signal that a consumer is actually
# attached; the no-duplicate half of (a) is what rules out a resnapshot.
# wait_slot_active polls rather than sampling once, so a reconnect still
# in progress is a slower PASS, not a false FAIL.
PASS_B=false
if [ "$SLOT_REACTIVATED" = "true" ] && [ "$LSN_ADVANCED" = "true" ]; then
  PASS_B=true
fi

# Criterion c: connector returned to RUNNING on its own, no task FAILED.
PASS_C=false
[ "$INITIAL_RUNNING" = "true" ] && [ "$AFTER_RUNNING" = "true" ] && PASS_C=true

fmt() { [ "$1" = "true" ] && echo "PASS" || echo "FAIL"; }

echo ""
echo "== results (run ${TS}) =="
printf '%-4s %-66s %-6s\n' "a)" "pre=${PRE_COUNT}/${N} post=${POST_COUNT}/${N}, no gap/dup across restart" "$(fmt $PASS_A)"
printf '%-4s %-66s %-6s\n' "b)" "slot re-activated (${BASE_ACTIVE:-?} -> ${ACTIVE_AT_CHECK:-?}), LSN advanced past restart" "$(fmt $PASS_B)"
printf '%-4s %-66s %-6s\n' "c)" "connector back to RUNNING, no FAILED task" "$(fmt $PASS_C)"
echo ""

if [ "$PASS_A" = "true" ] && [ "$PASS_B" = "true" ] && [ "$PASS_C" = "true" ]; then
  echo "OVERALL: PASS"
  exit 0
else
  echo "OVERALL: FAIL"
  exit 1
fi
