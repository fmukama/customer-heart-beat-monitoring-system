-- Distinguish an open window from a finalized one.
--
-- Aggregates are snapshotted while a window is still open so the
-- current day is queryable. Without this flag there is no way to tell
-- a partial snapshot from a closed window, and the consumer cannot
-- know which windows to rehydrate into memory after a restart.

ALTER TABLE heart_rate_daily
ADD COLUMN IF NOT EXISTS
    is_finalized BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS
    idx_heart_rate_daily_open
ON heart_rate_daily (is_finalized)
WHERE is_finalized = FALSE;
