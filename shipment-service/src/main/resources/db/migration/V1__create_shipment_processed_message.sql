-- Epic B3 mock-consumer idempotency table. IF NOT EXISTS: this table was
-- created manually (see infra/db/shipment_processed_message.sql, now
-- superseded) before Flyway was adopted for this service, so this
-- migration must be a no-op against that already-existing table while
-- still being the thing that creates it for anyone starting fresh.
CREATE TABLE IF NOT EXISTS shipment_processed_message (
    message_id   uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
