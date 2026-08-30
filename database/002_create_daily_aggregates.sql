CREATE TABLE IF NOT EXISTS heart_rate_daily (
    customer_id VARCHAR(100) NOT NULL,

    window_start TIMESTAMPTZ NOT NULL,

    window_end TIMESTAMPTZ NOT NULL,

    event_count BIGINT NOT NULL,

    average_heart_rate DOUBLE PRECISION NOT NULL,

    minimum_heart_rate INTEGER NOT NULL,

    maximum_heart_rate INTEGER NOT NULL,

    abnormal_count BIGINT NOT NULL,

    finalized_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (
        customer_id,
        window_start
    )
);


CREATE INDEX IF NOT EXISTS
    idx_heart_rate_daily_window
ON heart_rate_daily (
    window_start,
    window_end
);