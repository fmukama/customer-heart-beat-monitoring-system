-- Alert history, so the system can answer
-- "show me every notification generated in the last 30 days".
--
-- Written by the notifier service from Alertmanager webhooks, not by
-- the consumer: ConsumerDown is one of the alerts, and a recorder that
-- dies with the thing it reports on would never record the most
-- important event.

CREATE TABLE IF NOT EXISTS notifications (
    notification_id BIGSERIAL PRIMARY KEY,

    -- Alertmanager's per-alert hash. Needed to match a resolved
    -- webhook back to the firing row it closes.
    fingerprint VARCHAR(64) NOT NULL,

    alert_type VARCHAR(64) NOT NULL,

    severity VARCHAR(16) NOT NULL,

    message TEXT NOT NULL,

    source VARCHAR(128),

    status VARCHAR(16) NOT NULL DEFAULT 'FIRING',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    resolved_at TIMESTAMPTZ,

    -- Full label set, for debugging an alert after the fact.
    labels JSONB NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT valid_notification_status
        CHECK (status IN ('FIRING', 'RESOLVED')),

    CONSTRAINT valid_notification_severity
        CHECK (severity IN ('CRITICAL', 'WARNING', 'INFO')),

    CONSTRAINT resolved_has_timestamp
        CHECK (
            (status = 'RESOLVED' AND resolved_at IS NOT NULL)
            OR (status = 'FIRING' AND resolved_at IS NULL)
        )
);


-- At most one open row per alert. Alertmanager retries webhook
-- delivery, so a repeated firing notification must not duplicate.
CREATE UNIQUE INDEX IF NOT EXISTS
    idx_notifications_active
ON notifications (fingerprint)
WHERE status = 'FIRING';


-- "Every CONSUMER_DOWN in the last 30 days"
CREATE INDEX IF NOT EXISTS
    idx_notifications_type_time
ON notifications (alert_type, created_at DESC);


-- "Everything in the last 30 days"
CREATE INDEX IF NOT EXISTS
    idx_notifications_created
ON notifications (created_at DESC);
