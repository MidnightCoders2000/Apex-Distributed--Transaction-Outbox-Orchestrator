-- SUPERSEDED: kept only as a record of what was applied by hand before
-- Flyway was adopted for this table. shipment-service now creates and
-- tracks this table itself via
-- shipment-service/src/main/resources/db/migration/V1__create_shipment_processed_message.sql
-- on every startup; don't apply this file directly anymore.
CREATE TABLE IF NOT EXISTS shipment_processed_message (
    message_id   uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);
