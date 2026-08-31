-- Align the database backstop with schemas/heart_rate_event.json,
-- which declares heart_rate as 20-250. Previously the CHECK only
-- required a positive value, so out-of-range readings could persist.

ALTER TABLE heart_rate_events
DROP CONSTRAINT IF EXISTS heart_rate_positive;

ALTER TABLE heart_rate_events
DROP CONSTRAINT IF EXISTS heart_rate_in_range;

ALTER TABLE heart_rate_events
ADD CONSTRAINT heart_rate_in_range
    CHECK (heart_rate BETWEEN 20 AND 250);
