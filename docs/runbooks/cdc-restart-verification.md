# Runbook: CDC restart-survival verification

Epic B2, story 1. Proves that a Postgres restart does not lose outbox
events flowing through Debezium into Kafka.

## Prerequisites

- `docker-compose up -d` already running (redis, zookeeper, kafka,
  kafka-connect).
- No `.env` needed: the test targets a local throwaway Postgres and sets
  all six connection variables itself.
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
   `DB_PORT=5432`, `DB_SSLMODE=disable` overridden for the local run)
   under its **own identity** — see "Isolation from the real connector"
   below.
4. Records the replication slot's baseline `confirmed_flush_lsn`.
5. Inserts 20 pre-restart marker rows.
6. Runs `docker-compose restart postgres` and waits for `pg_isready`.
7. Inserts 20 post-restart marker rows — **without re-registering the
   connector**. This is the assertion that actually matters: the
   connector has to reconnect and resume on its own.
8. Consumes `apextest.public.outbox_event` from Kafka and counts markers
   per batch.

Expected output ends with a PASS/FAIL table and an `OVERALL: PASS` line;
exit code is non-zero if anything failed.

## Isolation from the real connector

The test registers a connector with its own name, topic prefix, slot and
publication:

| | Real (`register-connector.sh`) | Test |
|---|---|---|
| `name` | `apex-outbox-connector` | `apex-outbox-connector-cdctest` |
| `topic.prefix` | `apex` | `apextest` |
| `slot.name` | `apex_outbox_slot` | `apex_outbox_slot_cdctest` |
| `publication.name` | `apex_outbox_pub` | `apex_outbox_pub_cdctest` |

All four are `envsubst` variables in `outbox-connector.json`, defaulted to
the real values in `register-connector.sh` and overridden by the test.

This matters because Kafka Connect keys connector offsets by
`{server: topic.prefix}`, not by target host. When the test shared the
real name and prefix, two things went wrong at once: a stale offset from
an earlier run made Debezium resume from an LSN that did not exist in the
fresh local database and silently skip the test's events, and the test's
marker rows landed in `apex.public.outbox_event` alongside real events.
The workaround was to stop the connector, delete its offsets and delete
the connector before every run — which also destroyed the Neon-targeted
registration, leaving it pointed at a one-shot local container with no
step to re-register it.

The publication is also created narrowly. `publication.autocreate.mode`
defaults to `all_tables`, which makes Debezium issue `CREATE PUBLICATION
... FOR ALL TABLES` even though `table.include.list` is only
`public.outbox_event`. That happened to work locally (the `apex` user is a
superuser there), but on Neon it needs both a privilege the app role does
not necessarily have and, worse, it would decode WAL for every one of
Track A's tables. The connector sets
`"publication.autocreate.mode": "filtered"`, which publishes only the
included tables.

Separate identities remove all of that: there are no shared offsets to
reset, no shared topic to pollute, and running this test never touches the
Neon registration. **Do not** re-introduce a reset of
`apex-outbox-connector` here.

## The three PASS criteria

A naive check — "grep both batches out of `--from-beginning`" — passes in
scenarios that have nothing to do with resilience (e.g. a full resnapshot
after the restart would also produce both batches). These three criteria
are independent signals:

| # | Criterion | What it rules out |
|---|-----------|--------------------|
| a | Pre-marker count == 20 **and** post-marker count == 20, no more, no less | A gap (lost events) or duplicates (double-delivery) straddling the restart boundary — and, since a resnapshot would redeliver the pre-batch, a resnapshot masquerading as a clean resume |
| b | After the restart the slot polls to `active = t` (bounded wait, not a single sample) **and** `confirmed_flush_lsn` **advanced** past the baseline | A slot that exists but has no consumer attached to it, or one that is attached but making no progress |
| c | Connector state returns to `RUNNING` on its own, with no task in `FAILED`, both before and after the restart | A connector that is technically "up" but stuck retrying with a failed task |

Two checks that were here previously were dropped as non-signals:

- **"post markers arrived, connector never re-registered."** The count
  half is already criterion (a); the "never re-registered" half is not an
  asserted condition at all, just a property of how the script is
  written — nothing in it re-POSTs the config after the restart.
- **"same `slot_name` before and after."** `slot.name` is fixed in the
  connector config, so `BASE_SLOT == AFTER_SLOT` holds whenever the slot
  exists at all, including for a dropped-and-recreated slot (same name,
  higher LSN). It ruled out nothing. Its intended target — a slot that is
  not the one the connector is really consuming from — is covered by
  `active = t` in (b) plus the no-duplicate half of (a).

## Why this proves resilience

A logical replication slot pins WAL server-side until the consumer's
`confirmed_flush_lsn` advances past it — Postgres will not discard WAL the
slot still needs, restart or not. Separately, Debezium doesn't keep its
own read position in memory: it stores connector offsets in the
`apex_connect_offsets` Kafka topic (see `docker-compose.yml`), which
survives a Postgres restart untouched because it lives in Kafka, not
Postgres. Together, those two facts mean a Postgres restart should lose
nothing as long as the slot survives and Debezium reconnects — which is
exactly what criteria (a)-(c) check, instead of just asserting the end
state looks fine.

## Failure causes

- **Slot dropped or renamed** — check `max_replication_slots` on the
  target Postgres; if it's exhausted, Postgres may fail to preserve the
  slot across restart. `SELECT * FROM pg_replication_slots;` to inspect.
- **`apex_connect_offsets` topic missing or recreated** — Kafka Connect
  won't know where it left off; look for `kafka-connect` logs mentioning
  offset topic creation. If Kafka's own storage was wiped between runs,
  Debezium restarts from a fresh snapshot instead of resuming.
- **Connector task `FAILED`** — `curl -s
  localhost:8083/connectors/apex-outbox-connector-cdctest/status | python -m
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

Criteria (a)-(d) below use the original four-criterion numbering, recorded
as they were run. Read against the current three criteria:

- Old (c) is today's **(a)**, and old (d) is today's **(c)** — same checks,
  renumbered. These three runs are evidence for both.
- Today's **(b)** is *not* covered. Old (b) compared slot names; `active =
  t` was never asserted in these runs, so they say nothing about it.
  **(b) is unverified until the next run**, which should be logged under
  the three-criterion numbering.

| Date | Target | Pre count | Post count | a | b | c | d | Overall |
|------|--------|-----------|------------|---|---|---|---|---------|
| 2026-09-15 | local (cdc-test) | 20/20 | 20/20 | PASS | PASS | PASS | PASS | **PASS** |
| 2026-09-15 | local (cdc-test), rerun #2 | 20/20 | 20/20 | PASS | PASS | PASS | PASS | **PASS** |
| 2026-09-15 | local (cdc-test), rerun #3 | 20/20 | 20/20 | PASS | PASS | PASS | PASS | **PASS** |

Ran three times back to back (`./infra/debezium/verify-restart-resilience.sh`) to
confirm repeatability: each run tore down and recreated the `cdc-test`
Postgres container, and each landed on the same slot state (one row in
`pg_replication_slots`, no duplication) and the same marker counts. The
slot was named `apex_outbox_slot` in those runs, since the test still
shared the real connector's identity then; it is `apex_outbox_slot_cdctest`
now.

Two things had to be fixed to get a genuine pass:

- Kafka Connect keys offsets by `{server: topic.prefix}`, and the test
  connector originally shared its name/prefix with the Neon-targeted
  registration. A stale offset from an earlier run made Debezium resume
  from an LSN that did not exist in the fresh local DB and silently skip
  events. Those runs worked around it by stopping the connector, deleting
  its offsets and deleting the connector before every run; that reset has
  since been replaced by giving the test its own connector identity (see
  "Isolation from the real connector"), which removes the cause rather
  than the symptom.
- `offset.flush.interval.ms` defaults to 60000 on the Kafka Connect
  worker. With the default, restarting Postgres shortly after inserting
  the pre-markers restarted the connector before that checkpoint had
  flushed, and Debezium's at-least-once resume redelivered the
  pre-markers (confirmed via duplicate `id`s in the consumed topic — not
  data loss, but not the clean boundary this test needs either). Set to
  `OFFSET_FLUSH_INTERVAL_MS: 5000` on the `kafka-connect` service in
  `docker-compose.yml`, and the script now waits for
  `confirmed_flush_lsn` to advance past each batch before moving on.

