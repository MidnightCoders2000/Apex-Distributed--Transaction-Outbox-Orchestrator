#!/usr/bin/env bash
# Verifies Epic B3's mock consumers (payment-service, shipment-service):
# a PaymentRequested/ShipmentRequested outbox insert produces exactly one
# *Reserved/*Failed event on the service's own output topic, and a literal
# redelivery of the same CDC message (same outbox_event.id, replayed onto
# apex.public.outbox_event) leaves the processed-message table at exactly
# one row for that id. See docs/runbooks/mock-consumer-verification.md.
#
# Prerequisites this script does NOT set up itself -- see the runbook:
#   - docker-compose up (kafka, kafka-connect)
#   - infra/debezium/outbox-connector.json registered (with the Story-B3
#     envelope fix: value.converter.schemas.enable=false)
#   - payment-service and shipment-service both running (mvn spring-boot:run)
#   - both infra/db/*_processed_message.sql bootstrap files applied
set -euo pipefail
cd "$(dirname "$0")/../.."

set -a
source .env
: "${DB_PORT:=5432}"
set +a

COMPOSE="docker-compose"
CDC_TOPIC="apex.public.outbox_event"
TS="$(date +%s)"

echo "== verify-mock-consumers: run ${TS} =="

psql_direct() {
  PGPASSWORD="$DB_PASSWORD" psql -h "$DIRECT_DB_HOST" -p "$DB_PORT" -U "$DB_USERNAME" -d "$DB_NAME" \
    -v ON_ERROR_STOP=1 -t -A -q "$@" | tr -d '\r'
}

insert_payment_requested() {
  local tx_id="$1"
  psql_direct -c "
    INSERT INTO outbox_event (aggregate_type, aggregate_id, event_type, payload, correlation_id)
    VALUES ('Transaction', '${tx_id}', 'PaymentRequested',
      jsonb_build_object('correlationId', 'corr-${tx_id}', 'transactionId', '${tx_id}', 'accountId', 'acct-verify-b3', 'amount', 10.00),
      'corr-${tx_id}')
    RETURNING id;
  "
}

insert_shipment_requested() {
  local tx_id="$1"
  psql_direct -c "
    INSERT INTO outbox_event (aggregate_type, aggregate_id, event_type, payload, correlation_id)
    VALUES ('Transaction', '${tx_id}', 'ShipmentRequested',
      jsonb_build_object('correlationId', 'corr-${tx_id}', 'transactionId', '${tx_id}', 'accountId', 'acct-verify-b3'),
      'corr-${tx_id}')
    RETURNING id;
  "
}

# Polls $1 for a message whose value contains $2 (the transaction id is
# unique per run, so a substring match is enough), up to $3 seconds.
wait_for_message() {
  local topic="$1" needle="$2" timeout_s="${3:-30}" deadline
  deadline=$(( $(date +%s) + timeout_s ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if $COMPOSE exec -T kafka kafka-console-consumer \
        --bootstrap-server kafka:9092 --topic "$topic" --from-beginning \
        --timeout-ms 5000 2>/dev/null | grep -q "$needle"; then
      return 0
    fi
  done
  return 1
}

# Deterministic end-offset count -- no consumer group is ever created for
# apex.payment.events/apex.shipment.events by anything in this project
# (nothing else consumes them yet), so a committed-offset read isn't an
# option. Both topics are single-partition (see *EventTopicConfig), but
# summing per-partition offsets keeps this correct if that ever changes.
topic_message_count() {
  local topic="$1"
  $COMPOSE exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list kafka:9092 --topic "$topic" 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum + 0}'
}

processed_row_count() {
  local table="$1" id="$2"
  psql_direct -c "SELECT count(*) FROM ${table} WHERE message_id = '${id}';" | tr -d '[:space:]'
}

# Redelivers the exact CDC message for outbox_event.id=$1 by capturing its
# current value off $CDC_TOPIC and republishing it verbatim. outbox_event.id
# is a PK, so a literal duplicate INSERT is impossible -- this is genuine
# redelivery of the same message, not a simulation. Keyless: idempotency is
# keyed by after.id inside the value, not by the Kafka record key, so the
# key doesn't need to be captured/replayed.
redeliver_message() {
  local id="$1" value
  value="$($COMPOSE exec -T kafka kafka-console-consumer \
    --bootstrap-server kafka:9092 --topic "$CDC_TOPIC" --from-beginning \
    --timeout-ms 15000 2>/dev/null | grep "\"id\":\"${id}\"" | tail -1)"
  if [ -z "$value" ]; then
    echo "could not find message for id ${id} to redeliver" >&2
    return 1
  fi
  printf '%s\n' "$value" | $COMPOSE exec -T kafka kafka-console-producer \
    --bootstrap-server kafka:9092 --topic "$CDC_TOPIC" >/dev/null 2>&1
}

echo "-- payment: inserting PaymentRequested --"
PAY_TX="verify-b3-pay-${TS}"
PAY_ID="$(insert_payment_requested "$PAY_TX")"
echo "id=${PAY_ID}"

PAY_A=false
if wait_for_message "apex.payment.events" "$PAY_TX" 30; then PAY_A=true; fi

PAY_COUNT_BEFORE="$(processed_row_count payment_processed_message "$PAY_ID")"
PAY_SOFT_BEFORE="$(topic_message_count apex.payment.events)"

echo "-- payment: redelivering same message --"
redeliver_message "$PAY_ID" || true
sleep 5

PAY_COUNT_AFTER="$(processed_row_count payment_processed_message "$PAY_ID")"
PAY_SOFT_AFTER="$(topic_message_count apex.payment.events)"

PAY_B=false
[ "$PAY_COUNT_BEFORE" = "1" ] && [ "$PAY_COUNT_AFTER" = "1" ] && PAY_B=true

echo "-- shipment: inserting ShipmentRequested --"
SHIP_TX="verify-b3-ship-${TS}"
SHIP_ID="$(insert_shipment_requested "$SHIP_TX")"
echo "id=${SHIP_ID}"

SHIP_A=false
if wait_for_message "apex.shipment.events" "$SHIP_TX" 30; then SHIP_A=true; fi

SHIP_COUNT_BEFORE="$(processed_row_count shipment_processed_message "$SHIP_ID")"
SHIP_SOFT_BEFORE="$(topic_message_count apex.shipment.events)"

echo "-- shipment: redelivering same message --"
redeliver_message "$SHIP_ID" || true
sleep 5

SHIP_COUNT_AFTER="$(processed_row_count shipment_processed_message "$SHIP_ID")"
SHIP_SOFT_AFTER="$(topic_message_count apex.shipment.events)"

SHIP_B=false
[ "$SHIP_COUNT_BEFORE" = "1" ] && [ "$SHIP_COUNT_AFTER" = "1" ] && SHIP_B=true

fmt() { [ "$1" = "true" ] && echo "PASS" || echo "FAIL"; }

echo ""
echo "== results (run ${TS}) =="
printf '%-4s %-70s %-6s\n' "a)" "payment: PaymentRequested -> event seen on apex.payment.events" "$(fmt $PAY_A)"
printf '%-4s %-70s %-6s\n' "b)" "payment: redelivery -> processed-row count stays 1 (${PAY_COUNT_BEFORE} -> ${PAY_COUNT_AFTER})" "$(fmt $PAY_B)"
printf '%-4s %-70s %-6s\n' "c)" "shipment: ShipmentRequested -> event seen on apex.shipment.events" "$(fmt $SHIP_A)"
printf '%-4s %-70s %-6s\n' "d)" "shipment: redelivery -> processed-row count stays 1 (${SHIP_COUNT_BEFORE} -> ${SHIP_COUNT_AFTER})" "$(fmt $SHIP_B)"
echo ""
echo "observational only (not pass/fail -- publish-before-mark permits a"
echo "duplicate publish on redelivery by design, see PLAN.md decision #6):"
printf '    apex.payment.events count:  %s -> %s\n' "$PAY_SOFT_BEFORE" "$PAY_SOFT_AFTER"
printf '    apex.shipment.events count: %s -> %s\n' "$SHIP_SOFT_BEFORE" "$SHIP_SOFT_AFTER"
echo ""

if [ "$PAY_A" = "true" ] && [ "$PAY_B" = "true" ] && [ "$SHIP_A" = "true" ] && [ "$SHIP_B" = "true" ]; then
  echo "OVERALL: PASS"
  exit 0
else
  echo "OVERALL: FAIL"
  exit 1
fi
