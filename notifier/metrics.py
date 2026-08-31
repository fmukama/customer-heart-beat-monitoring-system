from prometheus_client import Counter, Gauge

# Scraped by Prometheus and used by the PostgresUnavailable alert rule.
# Lives here rather than in the consumer so the signal survives a
# consumer outage.
postgres_up = Gauge(
    "heartbeat_postgres_up",
    "1 when the notifier can reach PostgreSQL, 0 otherwise.",
)

alerts_received = Counter(
    "notifier_alerts_received_total",
    "Alerts received from Alertmanager.",
    ["status"],
)

alerts_persisted = Counter(
    "notifier_alerts_persisted_total",
    "Alerts written to the notifications table.",
)

duplicates_ignored = Counter(
    "notifier_duplicates_ignored_total",
    "Webhook retries that matched an existing open alert.",
)

write_failures = Counter(
    "notifier_write_failures_total",
    "Failed attempts to persist an alert.",
)

pending_writes = Gauge(
    "notifier_pending_writes",
    "Alerts buffered in memory awaiting a reachable PostgreSQL.",
)
