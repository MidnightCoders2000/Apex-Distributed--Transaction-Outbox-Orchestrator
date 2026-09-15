# Apex — Distributed Transaction & Outbox Orchestrator

See [PLAN.md](PLAN.md) for the epic/story breakdown, the Day-0 contract, and
the two-track work split.

## Modules

- `common-events` — shared event/command contracts (the Day-0 contract, in code).
- `orchestrator-service` — REST API, idempotency, transactional outbox, saga orchestrator.
- `payment-service` — mock downstream payment consumer.
- `shipment-service` — mock downstream shipment consumer.

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
