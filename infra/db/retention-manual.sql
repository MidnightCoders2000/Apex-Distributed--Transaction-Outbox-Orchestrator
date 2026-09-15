-- Outbox retention: drop events older than the 14-day window.
-- Safe post-bootstrap: Debezium ships events from the WAL at commit time,
-- not from live table state. See docs/retention-strategy.md.
DELETE FROM outbox_event
WHERE created_at < now() - interval '14 days';
