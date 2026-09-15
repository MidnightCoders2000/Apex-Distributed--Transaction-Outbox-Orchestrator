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

See `infra/db/retention-manual.sql`. It wraps the `DELETE` in a CTE and
selects `count(*)` over `RETURNING`, so each run prints the number of rows
it removed — a scheduled job that silently deletes nothing looks identical
to one that works, otherwise.

`idx_outbox_event_created_at` (from `infra/db/outbox_event.sql`) is
available for the `created_at` predicate, but whether the planner uses it
depends on selectivity: on steady-state runs, where only a small tail of
the table is older than 14 days, an index scan is likely; on the very
first run after a long backlog, when most of the table qualifies, a
sequential scan is the cheaper plan and Postgres will pick it. Both are
correct; only the first is fast.

At demo volume a single unbatched `DELETE` is fine. Past roughly 10^6
rows this should become a `LIMIT`-batched loop (delete N rows, commit,
repeat) to avoid holding a long lock and to avoid a single large WAL
spike. ## Keeping retention deletes out of the event topic

The delete itself produces WAL, which Debezium reads like any other
change. Left alone, every nightly run would push a 14-day batch of
`op: "d"` events into `apex.public.outbox_event`, where the Epic B3 mock
consumers would receive them as ordinary events — housekeeping noise
masquerading as domain events.

`"tombstones.on.delete": "false"` alone does **not** prevent this. That
flag only suppresses the extra tombstone record (a message with a null
value, emitted after a delete for log-compaction purposes); the delete
event itself is still emitted.

So the connector also sets `"skipped.operations": "d"` (see
`infra/debezium/outbox-connector.json`), which drops delete events before
they are written to the topic. Both settings are kept: `skipped.operations`
does the actual work, and `tombstones.on.delete: false` documents that no
tombstone is wanted either.

This is safe for the outbox pattern specifically, because a row's deletion
carries no information — the event's meaning was fully captured by the
insert. If a future connector ever needs to capture real deletes from a
different table, it should be a separate connector rather than a relaxation
of this one.

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
psql -v ON_ERROR_STOP=1 -f infra/db/retention-manual.sql
```

on `cron: "0 3 * * *"` — **GitHub cron is always UTC**, with no repository
timezone setting and no DST adjustment, so this is 03:00 UTC year-round —
plus `workflow_dispatch` so it can be triggered by hand.

Two details in the workflow are deliberate:

- `ON_ERROR_STOP=1`. Without it `psql` exits 0 even when the script hits a
  SQL error (missing table, revoked privilege), so the job would report
  green while deleting nothing.
- The connection is passed as `PG*` environment variables parsed from the
  `DB_URL` secret, not as a `psql "$DB_URL"` argument. An argv connection
  string puts the password in the runner's process list.

The workflow also sets `permissions: {}` (it needs no `GITHUB_TOKEN`
scopes), `timeout-minutes: 10`, and a `concurrency` group so a manual
dispatch can't overlap the scheduled run. Free on a public repo. Requires `DB_URL` as a repository secret
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

**`heartbeat.action.query`.** `outbox_event` is the only captured table,
while the rest of Track A's tables stay busy. Debezium only advances
`confirmed_flush_lsn` when it has something to acknowledge, so during a
quiet period on `outbox_event` the slot can hold WAL generated by every
other table — `heartbeat.interval.ms: 10000` sends heartbeats but has
nothing to commit against. The fix is `heartbeat.action.query` (a tiny
write to a dedicated heartbeat table on each heartbeat, which gives the
connector a captured change to flush past). Worth adding once Track A's
write volume is real; not needed at current demo volume.

When Track A lands the Flyway migration in Epic A2, `idx_outbox_event_created_at`
moves into that migration (replacing the temporary DDL in
`infra/db/outbox_event.sql`), and this cleanup becomes part of normal ops
tooling rather than a standalone script bootstrapped by Track B.
