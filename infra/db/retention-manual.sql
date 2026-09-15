-- Outbox retention: drop events older than the 14-day window.
-- Safe post-bootstrap: Debezium ships events from the WAL at commit time,
-- not from live table state. See docs/retention-strategy.md.
--
-- Reports the number of rows removed so a scheduled run leaves something
-- verifiable in its log rather than silent success.
WITH deleted AS (
  DELETE FROM outbox_event
  WHERE created_at < now() - interval '14 days'
  RETURNING 1
)
SELECT count(*) AS outbox_rows_deleted FROM deleted;
