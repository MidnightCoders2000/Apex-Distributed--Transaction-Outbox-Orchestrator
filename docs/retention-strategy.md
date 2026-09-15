# Outbox retention strategy

Epic B2, story 2. `outbox_event` is append-only from the app's point of
view — nothing currently deletes from it — so without a retention policy
it grows unbounded.

## The strategy

Nightly, delete rows older than 14 days:

```sql
DELETE FROM outbox_event
WHERE created_at < now() - interval '14 days';
```

See `infra/db/retention-manual.sql` (uses
`idx_outbox_event_created_at` from `infra/db/outbox_event.sql`, so this is
an index scan, not a sequential scan).

At demo volume a single unbatched `DELETE` is fine. Past roughly 10^6
rows this should become a `LIMIT`-batched loop (delete N rows, commit,
repeat) to avoid holding a long lock and to avoid a single large WAL
spike. Note that the delete itself produces WAL, which Debezium will
stream like any other change — that's exactly why the connector sets
`"tombstones.on.delete": "false"` (see
`infra/debezium/outbox-connector.json`): without it, every retention run
would emit a Kafka tombstone per deleted row into the event topic, which
downstream consumers have no reason to see.

## Why deleting is safe

Debezium reads committed changes from the Postgres WAL stream, not from
the live table. By the time a row is old enough to be a retention
candidate, Debezium has long since shipped it to Kafka — deleting it
locally has no effect on what already went out.

The one exception is the **initial snapshot**: at connector bootstrap
(`snapshot.mode: initial`, made explicit in
`infra/debezium/outbox-connector.json` in story 1), Debezium reads the
table's live contents once to establish a starting point before it
switches to streaming WAL. Any row deleted *before that snapshot
completes* is never captured. This only matters once, at first setup — a
manual check ("did the snapshot finish before anything old got cleaned
up?") rather than something the retention job needs to automate around.

## Why 14 days

The window has to comfortably exceed the longest realistic period a
downstream consumer could be down and still be able to use the table as
a replay source, plus slack to investigate an incident over a weekend.
Two weeks covers that at this project's demo scale while keeping the
table small.

This is a tunable, not a law. To change it, edit the `interval '14
days'` literal in `infra/db/retention-manual.sql` — there's no other copy
of the value to keep in sync.

## Scheduler

**Primary: scheduled GitHub Actions workflow**
(`.github/workflows/outbox-retention.yml`), running

```
psql "$DB_URL" -f infra/db/retention-manual.sql
```

on `cron: "0 3 * * *"`, plus `workflow_dispatch` so it can be triggered by
hand. Free on a public repo. Requires `DB_URL` as a repository secret
(Settings → Secrets and variables → Actions) — the pooled Neon connection
is fine here since this is a plain `DELETE`, not a replication
connection.

Note the format differs from the app's local `.env`: Spring reads a JDBC
URL (`jdbc:postgresql://...`) plus separate `DB_USERNAME`/`DB_PASSWORD`,
but `psql` needs a single libpq URI. The `DB_URL` **secret** should be
set as `postgresql://<user>:<password>@<pooler-host>/<db>?sslmode=require`
— it's a different value from local `DB_URL` even though the name is the
same, since GitHub secrets and `.env` are entirely separate stores.

Two caveats:

- GitHub may disable scheduled workflows on repositories with no recent
  activity; if the job silently stops running, check
  Actions → the workflow → whether it's been auto-disabled, and
  re-enable or trigger it manually.
- On any always-on box, OS `cron` running the same `psql` command is the
  direct equivalent and doesn't have that disablement risk.

**`pg_cron`, if the plan ever changes:** not available on the Neon Free
plan, so it isn't used here. If the project is ever upgraded to a plan
that supports it, the same delete can be registered directly in Postgres
instead of relying on an external scheduler:

```sql
SELECT cron.schedule(
  'outbox-retention',
  '0 3 * * *',
  $$DELETE FROM outbox_event WHERE created_at < now() - interval '14 days'$$
);
```

If that's ever done, retire the GitHub Actions workflow so retention
isn't running twice.

## Follow-up

When Track A lands the Flyway migration in Epic A2, `idx_outbox_event_created_at`
moves into that migration (replacing the temporary DDL in
`infra/db/outbox_event.sql`), and this cleanup becomes part of normal ops
tooling rather than a standalone script bootstrapped by Track B.
