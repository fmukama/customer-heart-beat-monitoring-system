CREATE TABLE IF NOT EXISTS heart_rate_events (
    event_id UUID PRIMARY KEY,

    customer_id VARCHAR(100) NOT NULL,

    heart_rate INTEGER NOT NULL,

    event_time TIMESTAMPTZ NOT NULL,

    ingestion_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    status VARCHAR(20) NOT NULL DEFAULT 'NORMAL',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT heart_rate_positive
        CHECK (heart_rate > 0),

    CONSTRAINT valid_status
        CHECK (status IN ('NORMAL', 'ABNORMAL'))
);


ALTER TABLE heart_rate_events
ADD COLUMN IF NOT EXISTS
    is_late BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE heart_rate_events
ADD COLUMN IF NOT EXISTS
    lateness_seconds DOUBLE PRECISION;

-- Queries will frequently filter by customer and time.
CREATE INDEX IF NOT EXISTS idx_heart_rate_customer_time
    ON heart_rate_events (customer_id, event_time);


-- Useful for time-based queries.
CREATE INDEX IF NOT EXISTS idx_heart_rate_event_time
    ON heart_rate_events (event_time);


-- Useful when investigating abnormal readings.
CREATE INDEX IF NOT EXISTS idx_heart_rate_status
    ON heart_rate_events (status);