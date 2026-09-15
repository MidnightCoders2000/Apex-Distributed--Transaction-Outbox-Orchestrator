-- TEMPORARY bootstrap DDL owned by Track B, for Epic B2 verification only.
-- Track A (Epic A2) replaces this with a Flyway migration; when that lands,
-- delete this file and re-point the runbook at the migration.
CREATE TABLE IF NOT EXISTS outbox_event (
    id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type text        NOT NULL,
    aggregate_id   text        NOT NULL,
    event_type     text        NOT NULL,
    payload        jsonb       NOT NULL,
    correlation_id text,
    created_at     timestamptz NOT NULL DEFAULT now()
);

-- Retention deletes scan by age; without this the nightly job is a seq scan.
CREATE INDEX IF NOT EXISTS idx_outbox_event_created_at
    ON outbox_event (created_at);
