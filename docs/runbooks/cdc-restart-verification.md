# Runbook: CDC restart-survival verification

Epic B2, story 1. Proves that a Postgres restart does not lose outbox
events flowing through Debezium into Kafka.

## Prerequisites

- `docker-compose up -d` already running (redis, zookeeper, kafka,
  kafka-connect).
- `.env` populated (see `.env.example`) — the script only reads it for the
  preamble; the actual DB target for this test is local, not Neon.
- Nothing else bound to host port `5433`.
- `docker`, `docker-compose`, `curl`, `envsubst`, `psql` (inside the
  postgres container, not required on the host) available.

## Running it

```bash
./infra/debezium/verify-restart-resilience.sh
```

This is fully automated — no prompts, no manual restart. It:

1. Starts a throwaway `postgres:16` container on the `cdc-test`
   compose profile (port 5433, `wal_level=logical`), recreated fresh on
   every run so results don't accumulate across invocations.
2. Applies `infra/db/outbox_event.sql`.
3. Registers `infra/debezium/outbox-connector.json` against that local
   instance (same template, same `envsubst` path as
   `register-connector.sh`, with `DIRECT_DB_HOST=postgres`,
   `DB_PORT=5432`, `DB_SSLMODE=disable` overridden for the local run).
4. Records the replication slot's baseline `confirmed_flush_lsn`.
5. Inserts 20 pre-restart marker rows.
6. Runs `docker-compose restart postgres` and waits for `pg_isready`.
7. Inserts 20 post-restart marker rows — **without re-registering the
   connector**. This is the assertion that actually matters: the
   connector has to reconnect and resume on its own.
8. Consumes `apex.public.outbox_event` from Kafka and counts markers per
   batch.

Expected output ends with a PASS/FAIL table and an `OVERALL: PASS` line;
exit code is non-zero if anything failed.

## The four PASS criteria

A naive check — "grep both batches out of `--from-beginning`" — passes in
scenarios that have nothing to do with resilience (e.g. a full resnapshot
after the restart would also produce both batches). These four criteria
are checked independently so a false pass isn't possible:

| # | Criterion | What it rules out |
|---|-----------|--------------------|
| a | All 20 post-restart markers reached Kafka with the connector **never re-registered** | The connector silently dying and a human/script papering over it by re-POSTing the config |
| b | `pg_replication_slots` shows the **same** `slot_name` before and after, and `confirmed_flush_lsn` **advanced** past the baseline | A dropped-and-recreated slot (which would also "work" but proves nothing about WAL retention) |
| c | Pre-marker count == 20 **and** post-marker count == 20, no more, no less | A gap (lost events) or duplicates (double-delivery) straddling the restart boundary |
| d | Connector state returns to `RUNNING` on its own, with no task in `FAILED`, both before and after the restart | A connector that's technically "up" but stuck retrying with a failed task |

## Why this proves resilience

A logical replication slot pins WAL server-side until the consumer's
`confirmed_flush_lsn` advances past it — Postgres will not discard WAL the
slot still needs, restart or not. Separately, Debezium doesn't keep its
own read position in memory: it stores connector offsets in the
`apex_connect_offsets` Kafka topic (see `docker-compose.yml`), which
survives a Postgres restart untouched because it lives in Kafka, not
Postgres. Together, those two facts mean a Postgres restart should lose
nothing as long as the slot survives and Debezium reconnects — which is
exactly what criteria (a)-(d) check independently, instead of just
asserting the end state looks fine.

## Failure causes

- **Slot dropped or renamed** — check `max_replication_slots` on the
  target Postgres; if it's exhausted, Postgres may fail to preserve the
  slot across restart. `SELECT * FROM pg_replication_slots;` to inspect.
- **`apex_connect_offsets` topic missing or recreated** — Kafka Connect
  won't know where it left off; look for `kafka-connect` logs mentioning
  offset topic creation. If Kafka's own storage was wiped between runs,
  Debezium restarts from a fresh snapshot instead of resuming.
- **Connector task `FAILED`** — `curl -s
  localhost:8083/connectors/apex-outbox-connector/status | python -m
  json.tool` and read the task's `trace` field; usually a connection
  error to the (now-restarted) Postgres, which should self-heal on
  Debezium's retry loop within the script's polling window.
- **`wal_level` not `logical`** — the connector will fail at
  registration, not after restart; the `cdc-test` postgres service in
  `docker-compose.yml` sets this explicitly via `command:`, so this
  should only appear if that service definition changes.

## Neon vs. local

This test runs against a local, disposable `postgres:16` container (opt-in
via the `cdc-test` compose profile), not the Neon instance the rest of
the app uses. Reasoning:

- Neon's free tier gives no repeatable, scriptable way to restart compute
  — it's a manual action in the console.
- Neon compute auto-suspends on idle, which would confound "did the slot
  survive a restart" with "did the slot survive an idle suspend."
- Replication-slot behavior on the free plan isn't something we control
  or can rely on for a resilience proof.

The local container makes the experiment deterministic, repeatable, free,
and scriptable — a controlled experiment instead of a one-off click in
someone else's UI. It is the **authoritative** result for this story.
Neon continues to be the database for normal development; it plays no
part in this test.

If a Neon run is ever performed (manual console restart), record it as a
**separate row** in the results log below — it does not replace the local
result.

## Results log

| Date | Target | Pre count | Post count | a | b | c | d | Overall |
|------|--------|-----------|------------|---|---|---|---|---------|
| 2026-09-15 | local (cdc-test) | 20/20 | 20/20 | PASS | PASS | PASS | PASS | **PASS** |
| 2026-09-15 | local (cdc-test), rerun #2 | 20/20 | 20/20 | PASS | PASS | PASS | PASS | **PASS** |
| 2026-09-15 | local (cdc-test), rerun #3 | 20/20 | 20/20 | PASS | PASS | PASS | PASS | **PASS** |

Ran three times back to back (`./infra/debezium/verify-restart-resilience.sh`) to
confirm repeatability: each run tore down and recreated the `cdc-test`
Postgres container, and each landed on the same slot state
(`apex_outbox_slot`, one row in `pg_replication_slots` — no duplication) and
the same marker counts.

Two things had to be fixed to get a genuine pass, both left in place:

- Kafka Connect keys offsets by `{server: topic.prefix}`, and this
  connector shares its name/prefix with the Neon-targeted registration.
  A stale offset from an earlier run made Debezium resume from an LSN
  that didn't exist in the fresh local DB and silently skip events. The
  script now does `PUT .../stop` → `DELETE .../offsets` → `DELETE
  .../connectors/apex-outbox-connector` before every run.
- `offset.flush.interval.ms` defaults to 60000 on the Kafka Connect
  worker. With the default, restarting Postgres shortly after inserting
  the pre-markers restarted the connector before that checkpoint had
  flushed, and Debezium's at-least-once resume redelivered the
  pre-markers (confirmed via duplicate `id`s in the consumed topic — not
  data loss, but not the clean boundary this test needs either). Set to
  `OFFSET_FLUSH_INTERVAL_MS: 5000` on the `kafka-connect` service in
  `docker-compose.yml`, and the script now waits for
  `confirmed_flush_lsn` to advance past each batch before moving on.

