# Apex — Distributed Transaction & Outbox Orchestrator

## Business problem

A client places an order. Service A reserves payment. Service B tries to ship
the product and fails. Without a reliable mechanism to detect the failure and
reverse the payment, the system ends up in a corrupted, inconsistent state.

Apex is an event-driven transaction engine that guarantees data consistency
across independent services using:

- **Transactional Outbox** — the business write and the "event to publish"
  write happen in one local DB transaction, so no dual-write split-brain.
- **Change Data Capture (Debezium)** — tails the Postgres WAL and reliably
  streams outbox rows to Kafka, without the app ever calling Kafka directly.
- **Saga Orchestration** — a central orchestrator reacts to success/failure
  events from downstream services and issues compensating commands to roll
  back already-completed steps.

## Non-goals

- No LLMs, RAG, vector search, or AI gateway concepts anywhere in this
  project. This is a deliberate pivot away from that domain.
- Not aiming for a generic CRUD API — the point is failure handling, not
  feature count.

## Tech stack

Java 21, Spring Boot 3.3, PostgreSQL, Apache Kafka, Debezium, Redis,
Testcontainers.

---

## Day 0 — Contract both developers must agree on first

Before splitting up, agree on and write down:

1. **`outbox_event` table schema** — columns: `id`, `aggregate_type`,
   `aggregate_id`, `event_type`, `payload` (JSON), `correlation_id`,
   `created_at`.
2. **Event/command catalog for the saga**, e.g.:
   `PaymentRequested → PaymentReserved | PaymentFailed`
   `ShipmentRequested → ShipmentReserved | ShipmentFailed`
   `PaymentCompensated`
3. **Kafka topic naming convention**, e.g. `apex.payment.events`,
   `apex.shipment.events`, `apex.saga.commands`.
4. **Idempotency key format** for the REST API and for downstream consumers
   (consumers must dedupe by message id — Kafka is at-least-once, not
   exactly-once).

Once this contract is fixed, the two tracks below can be built largely
independently against fakes/stubs and integrated at the end.

---

## Ownership

- **Colleague → Track A.** Develops against the in-memory/fake event bus, so
  no local Docker is required until the integration checkpoint.
- **You → Track B + Docker/infra owner.** Runs docker-compose locally, owns
  the final integration checkpoint (wiring Track A's outbox into real
  Kafka), and owns deployment (planned target: Render — see Open questions).

---

## Track A — Transaction Core & Orchestrator (colleague)

Owns the "brain": the API, the outbox write path, and the saga state
machine. Can be built against an in-memory/fake event bus first, swapped for
real Kafka at integration time.

### Epic A1 — Transaction API & Idempotency
- Story: REST endpoint accepting a transaction request payload.
- Story: Extract `Idempotency-Key` header, check/set it in Redis.
- Story: Return `409 Conflict` on a duplicate key within the TTL window.
- Story: Input validation + structured error responses.

### Epic A2 — Transactional Outbox
- Story: JPA entities for `Transaction` and `OutboxEvent`.
- Story: Single `@Transactional` method that writes both rows atomically.
- Story: Verify via test that a forced exception rolls back both writes.
- Story: (integration point) swap direct outbox insert for whatever
  Track B's CDC pipeline expects, once agreed in the Day 0 contract.

### Epic A3 — Saga Orchestrator
- Story: Orchestrator state machine per transaction (states: `STARTED`,
  `PAYMENT_RESERVED`, `SHIPMENT_RESERVED`, `COMPLETED`, `COMPENSATING`,
  `REVERSED`).
- Story: Consume success/failure events, transition state.
- Story: On downstream failure, publish compensating commands for every
  already-completed prior step.
- Story: Persist saga state so a crash mid-flow can resume correctly.

---

## Track B — Event Infrastructure & Downstream Services (you)

Owns the "body": the plumbing that makes events flow reliably, and the
services that react to commands. Can be built and demoed independently using
manually-inserted outbox rows before Track A's API exists.

### Epic B1 — Platform Infra
- Story: `docker-compose.yml` with Postgres, Redis, Kafka (or Redpanda),
  Debezium connect.
- Story: Debezium connector config tailing the `outbox_event` table.
- Story: Verify manually inserted rows appear on the Kafka topic (via
  Redpanda Console / kafka-console-consumer).

### Epic B2 — CDC Pipeline correctness
- Story: Confirm WAL-based delivery survives a Postgres restart (no lost
  events).
- Story: Document outbox table cleanup/retention strategy (avoid unbounded
  growth).

### Epic B3 — Mock Downstream Services
- Story: Mock "Service A" (payment) Kafka consumer — idempotent by message
  id, persists processed-ids to avoid double-processing.
- Story: Mock "Service B" (shipment) Kafka consumer, same idempotency
  guarantee.
- Story: Configurable failure injection (e.g. shipment fails N% of the time
  or for specific input) to trigger saga rollback paths on demand.
- Story: Consumers publish their own success/failure events back for the
  Orchestrator to react to.

---

## Shared — Integration & Delivery

### Epic S1 — Integration Testing
- Story: Testcontainers spin-up of real Postgres + Kafka + Redis in CI
  (no mocks/H2).
- Story: End-to-end happy-path test (request → outbox → Kafka → both
  services succeed → `COMPLETED`).
- Story: End-to-end rollback test (shipment fails → payment compensated →
  `REVERSED`).
- Story: CI pipeline (GitHub Actions) running `mvn verify` with
  Testcontainers on a clean runner.

### Epic S2 — Docs & Interview Demo
- Story: Architecture diagram (request flow + failure flow).
- Story: README explaining the dual-write problem and why Outbox+CDC solves
  it.
- Story: Short scripted demo: trigger a doomed transaction live, show the
  compensating rollback in the DB/logs.

---

## Suggested sequencing

1. Day 0 contract (both, together).
2. A1 + B1 in parallel (API/idempotency vs. docker-compose/Debezium infra).
3. A2 + B2 in parallel (outbox atomicity vs. CDC correctness).
4. Integration checkpoint: wire Track A's outbox writes to actually flow
   through Track B's real Kafka topics (drop the fake event bus).
5. A3 + B3 in parallel (orchestrator logic vs. mock consumers + failure
   injection) — these two talk to each other constantly, so keep this phase
   tightly synced (daily check-in).
6. S1 + S2 together at the end.

## Open questions (fill in as you decide)

- [ ] Repo hosting / access for both developers (not set up yet).
- [ ] Branching strategy / PR review expectations.
- [ ] Choreography fallback considered and rejected — confirm both agree on
      pure orchestration (per the original design rationale: centralized,
      easier to reason about and demo).
- [x] Deployment target for the demo: Render (you own this — Render builds
      the containers in the cloud, so colleague needs no local Docker for
      it either).
