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
   is never produced to.** `SagaOrchestrator` instead routes
   `CancelPaymentCommand` through the same `outbox_event`/CDC path as every
   other command (`event_type = "CancelPaymentCommand"` on the shared
   `apex.public.outbox_event` topic) rather than a dedicated Kafka topic —
   confirmed compatible with how Track B's consumers already read that
   topic, so this one is resolved, just not the way the Day 0 doc originally
   sketched it.

None of the above (1-2) is guessed at or implemented here — it's Track A's
design call how the orchestrator should bridge Kafka replies back into the
saga (a `@KafkaListener` per reply topic translating into the existing
`PaymentReservedEvent`/`ShipmentFailedEvent`, presumably, plus emitting
`PaymentRequested`/`ShipmentRequested` outbox rows at the right saga steps).
Flagging it here so it's tracked instead of silently discovered later at
integration time.

## Fixed on the Track B side (this branch)

- **`payment-service` now consumes `CancelPaymentCommand`** off
  `apex.public.outbox_event` (own consumer group
  `apex-payment-service-compensation`, same idempotency table as
  `PaymentOutboxConsumer`) and publishes `PaymentCompensated` on
  `apex.payment.events`. See `PaymentCompensationConsumer`. The command's
  payload currently only carries `transactionId` (no `correlationId`), so
  the published `PaymentCompensated.correlationId()` is `null` — needs a
  Track A fix if `correlationId` should be populated end-to-end.
- **`PaymentReserved`/`PaymentFailed`/`ShipmentReserved`/`ShipmentFailed`/
  `PaymentCompensated` (`common-events`) now self-report an `"eventType"`
  JSON field** (via `@JsonGetter`), since `SagaReplyListener` reads
  `root.get("eventType")` off the raw message body but these records were
  never publishing one — every reply was silently dropped (caught, logged,
  discarded) before this. This alone does not make `SagaReplyListener` work:
  it also expects a nested `payload.transactionId`, but these events publish
  `transactionId` at the top level with no `payload` wrapper. **Track A
  still needs to update `SagaReplyListener` to read `transactionId` from the
  message root**, not from a `payload` sub-object, since Track B's message
  shape is what's actually on the wire and is shared by all consumers.
