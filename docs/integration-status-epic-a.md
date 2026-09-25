# Integration status: Track B vs. landed Epic A1–A3

Written after Epic A1–A3 (Saga Orchestrator, Transactional Outbox, Flyway
migrations — commit `a6fabe6`) landed on `main`, since Track B (payment/shipment
mock consumers) was built and merged *before* that work existed, against the
Day 0 contract in [PLAN.md](../PLAN.md) rather than against real orchestrator
code.

## Confirmed compatible (no action needed)

- `outbox_event` table schema (columns `id`, `aggregate_type`, `aggregate_id`,
  `event_type`, `payload` jsonb, `correlation_id`, `created_at`) matches
  exactly what `common-messaging`'s `OutboxEnvelope`/`OutboxEventEnvelopeParser`
  and the Debezium connector (`infra/debezium/outbox-connector.json`) expect.
- Topic naming (`apex.payment.events`, `apex.shipment.events`) and the
  `apex.public.outbox_event` CDC topic match the Day 0 contract on both sides.

## Fixed in this branch

- **`orchestrator-service`'s `OutboxEvent` entity was mapped to table
  `outbox_events` (plural)** while the Flyway migration
  (`V1__init_saga_tables.sql`), the Debezium connector's
  `table.include.list`, and every other reference in the repo use singular
  `outbox_event`. With `spring.jpa.hibernate.ddl-auto: validate`, this fails
  orchestrator-service's startup outright (schema validation error) — nothing
  downstream can be exercised against a real orchestrator until this is
  fixed. One-line fix: `@Table(name = "outbox_event")`.

## NOT fixed here — needs Track A owner, this is a design gap, not a typo

The current `SagaOrchestrator`/`TransactionService` do not yet implement the
event flow the Day 0 contract describes, so **no real saga can complete
end-to-end today**, independent of the table-name bug above:

1. **No outbox row is ever published with `event_type = "PaymentRequested"`
   or `"ShipmentRequested"`.** `TransactionService.processTransaction` only
   emits `TransactionStartedEvent`; `SagaOrchestrator` only emits
   `ReservedShipmentCommand` (on payment-reserved) and `CancelPaymentCommand`
   (on shipment-failed). payment-service/shipment-service are configured
   (correctly, per the Day 0 contract) to filter on `PaymentRequested`/
   `ShipmentRequested` — those event types are simply never produced, so the
   mock consumers will never see a transaction that came from a real API call.
2. **`orchestrator-service` has no Kafka consumer at all.** `SagaOrchestrator`
   reacts to `PaymentReservedEvent`/`ShipmentFailedEvent`, but those are local
   Spring `@EventListener` records with no publisher anywhere — nothing
   bridges `apex.payment.events`/`apex.shipment.events` back into the
   orchestrator. The saga state machine is currently unreachable code.
3. **`apex.saga.commands` (the Day 0 contract's compensation-command topic)
   is never produced to.** Track B's runbook already flags consuming a
   compensation command as out of scope pending this; it still is.

None of the above is guessed at or implemented here — it's Track A's design
call how the orchestrator should bridge Kafka replies back into the saga
(a `@KafkaListener` per reply topic translating into the existing
`PaymentReservedEvent`/`ShipmentFailedEvent`, presumably, plus emitting
`PaymentRequested`/`ShipmentRequested` outbox rows at the right saga steps).
Flagging it here so it's tracked instead of silently discovered later at
integration time.
