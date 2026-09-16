# Runbook: Mock consumer verification

Epic B3, stories 1, 2, 4. Proves the mock payment/shipment consumers process
real outbox rows end-to-end: a `PaymentRequested`/`ShipmentRequested` insert
produces exactly one `*Reserved`/`*Failed` event on the service's own topic,
and idempotency holds under a literal redelivery of the same CDC message.

## Prerequisites

- `docker-compose up -d` running (redis, zookeeper, kafka, kafka-connect).
- `infra/debezium/outbox-connector.json` registered via
  `./infra/debezium/register-connector.sh` — **after** the Epic B3 envelope
  fix (`value.converter.schemas.enable: false`) was added to that file; if
  it was registered before that change, re-run the script to pick it up.
- `infra/db/payment_processed_message.sql` and
  `infra/db/shipment_processed_message.sql` applied to the target Postgres.
- `payment-service` and `shipment-service` both running locally
  (`mvn spring-boot:run`), each picking up `.env` from the repo root.
- `docker`, `docker-compose`, `psql` available on the host.

## Running it

```bash
./infra/debezium/verify-mock-consumers.sh
```

Fully automated — no prompts. It:

1. Inserts a `PaymentRequested` outbox row for a run-unique transaction id.
2. Polls `apex.payment.events` for the resulting `PaymentReserved`/`PaymentFailed`.
3. Records that id's `payment_processed_message` row count and the topic's
   end offset.
4. Redelivers the **exact same** CDC message — captured off
   `apex.public.outbox_event`, republished verbatim — and re-checks both.
   `outbox_event.id` is a PK, so a duplicate `INSERT` isn't possible; this
   is a literal redelivery, not a simulated one.
5. Repeats 1-4 for `ShipmentRequested` / `shipment-service` /
   `apex.shipment.events`.

## The PASS criteria — hard vs. observational

| # | Criterion | Strength | What it rules out |
|---|-----------|----------|--------------------|
| a | `PaymentRequested` insert → a matching event appears on `apex.payment.events` within 30s | Hard | Consumer not wired, envelope parsing broken, event-type filter wrong |
| b | Redelivery → `payment_processed_message` row count for that id stays exactly 1 | Hard | The `message_id` PK constraint is what this actually guarantees (PLAN.md decision #6) |
| c | Same as (a), shipment side | Hard | Shipment consumer parity with payment |
| d | Same as (b), shipment side | Hard | Shipment idempotency parity |
| — | `apex.payment.events` / `apex.shipment.events` end-offset count, before vs. after redelivery | Observational only | Nothing — logged, not asserted |

The observational row is deliberately not pass/fail. Decision #6 is
publish-before-mark: the best-effort `existsById` check usually catches a
same-process redelivery before it re-publishes (as it did in every run
logged below), but nothing in the design *guarantees* that under a race —
the guarantee is only "at most one processed-message row," not "at most one
publish." Asserting "no duplicate publish" here would assert a property the
code doesn't promise; the first time a real race hit it, this script would
report a regression that isn't one.

## Why the redelivery here is real, not simulated

`outbox_event.id` is the table's primary key, so a second `INSERT` with the
same id is impossible — there's no way to make Postgres/Debezium emit the
same CDC message twice by inserting again. Instead, the script captures the
exact JSON value Debezium already put on `apex.public.outbox_event` for that
id and republishes it verbatim via `kafka-console-producer`. The consumer
reads it exactly as it would a real redelivery (Kafka is at-least-once; a
rebalance or an unacked-offset restart produces the same duplicate from the
broker's point of view) — genuine redelivery, not a proxy for it. The
republish is keyless: idempotency is keyed by `after.id` inside the message
value, not by the Kafka record key, so the key never needs to be captured or
replayed.

## Failure causes

- **Envelope still schema-wrapped** — `infra/debezium/outbox-connector.json`
  was edited but the connector was never re-registered; re-run
  `register-connector.sh`. `OutboxEventEnvelopeParser` logs a parse warning
  per message when this happens, visible in the service's own log.
- **`ddl-auto: validate` startup failure** — one or both
  `infra/db/*_processed_message.sql` files weren't applied yet.
- **Test (a) or (c) times out** — the relevant service isn't actually
  running, or is pointed at the wrong Kafka listener
  (`localhost:29092`, the host-reachable listener added for Epic B3 — see
  `docker-compose.yml` — not the container-internal `kafka:9092` that
  `kafka-connect` uses).
- **Test (b) or (d) fails** — the `existsById` best-effort check regressed,
  or (less likely) the `message_id` unique constraint was dropped from the
  bootstrap table.

## Results log

| Date | payment a | payment b | shipment c | shipment d | payment.events count | shipment.events count | Overall |
|------|-----------|-----------|-------------|-------------|----------------------|------------------------|---------|
| 2026-09-16 | PASS | PASS (1→1) | PASS | PASS (1→1) | 5→5 | 3→3 | **PASS** |

Run against the real Neon-backed `outbox_event` table and the local
docker-compose Kafka/Debezium, with both services running via `mvn
spring-boot:run`. The nonzero starting topic counts (5 and 3) are from prior
manual end-to-end checks made while building this epic (success path,
failure-injection trigger path, etc.), not a sign of anything wrong — the
script only checks the delta across its own redelivery step, not the
absolute count.
