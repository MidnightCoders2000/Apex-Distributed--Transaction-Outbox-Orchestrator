# Apex — Distributed Transaction & Outbox Orchestrator

See [PLAN.md](PLAN.md) for the epic/story breakdown, the Day-0 contract, and
the two-track work split.

## Modules

- `common-events` — shared event/command contracts (the Day-0 contract, in code).
- `orchestrator-service` — REST API, idempotency, transactional outbox, saga orchestrator.
- `payment-service` — mock downstream payment consumer.
- `shipment-service` — mock downstream shipment consumer.

## Database

Postgres is hosted on [Neon](https://neon.tech) (not run locally). Two
connection modes are used:

- **Pooled** (`DB_URL`) — used by the Spring services for regular queries.
- **Direct / non-pooler** (`DIRECT_DB_HOST`) — required by Debezium, since
  PgBouncer pooled connections don't support the logical replication CDC
  depends on. Logical replication must be enabled on the Neon project
  (Project Settings → Logical Replication).

## Environment variables

Each service reads `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`, `DB_NAME` from a
`.env` file at the repo root (see `.env.example`). `.env` is git-ignored —
never commit real credentials.

- Running via `mvn spring-boot:run` from the repo root: the `.env` file is
  picked up automatically.
- Running via IntelliJ: set the working directory of the run configuration
  to the repo root, or set the variables directly as environment variables
  on the run configuration.

## Local infrastructure

```bash
docker-compose up -d
```

Brings up Redis, Kafka, and Kafka Connect (with Debezium) — Postgres is Neon,
not part of this compose file. Register the outbox connector once Kafka
Connect is up:

```bash
./infra/debezium/register-connector.sh
```

This substitutes `DIRECT_DB_HOST`/`DB_USERNAME`/`DB_PASSWORD`/`DB_NAME` from
`.env` into `infra/debezium/outbox-connector.json` and registers it — the
real values never get written to a file in the repo.
