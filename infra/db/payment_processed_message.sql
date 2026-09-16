-- TEMPORARY bootstrap DDL owned by Track B, for Epic B3 mock-consumer idempotency.
-- Applied manually the same way infra/db/outbox_event.sql is (ddl-auto: validate
-- means payment-service won't start until this table exists).
CREATE TABLE IF NOT EXISTS payment_processed_message (
    message_id   uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
