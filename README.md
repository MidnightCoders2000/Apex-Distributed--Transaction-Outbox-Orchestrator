# Apex — Distributed Transaction & Outbox Orchestrator

See [PLAN.md](PLAN.md) for the epic/story breakdown, the Day-0 contract, and
the two-track work split.

## Modules

- `common-events` — shared event/command contracts (the Day-0 contract, in code).
- `orchestrator-service` — REST API, idempotency, transactional outbox, saga orchestrator.
- `payment-service` — mock downstream payment consumer.
- `shipment-service` — mock downstream shipment consumer.

## Environment variables

Each service reads `DB_URL`, `DB_USERNAME`, `DB_PASSWORD` from a `.env` file
at the repo root (see `.env.example`). `.env` is git-ignored — never commit
real credentials.

- Running via `mvn spring-boot:run` from the repo root: the `.env` file is
  picked up automatically.
- Running via IntelliJ: set the working directory of the run configuration
  to the repo root, or set `DB_URL`/`DB_USERNAME`/`DB_PASSWORD` directly as
  environment variables on the run configuration.

## Local infrastructure

```bash
docker-compose up -d
```

Brings up Postgres, Redis, Kafka, and Kafka Connect (with Debezium). Register
the outbox connector once Kafka Connect is up:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @infra/debezium/outbox-connector.json \
  http://localhost:8083/connectors
```
