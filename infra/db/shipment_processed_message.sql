-- TEMPORARY bootstrap DDL owned by Track B, for Epic B3 mock-consumer idempotency.
-- See infra/db/payment_processed_message.sql for rationale.
CREATE TABLE IF NOT EXISTS shipment_processed_message (
    message_id   uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
