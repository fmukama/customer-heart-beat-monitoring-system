import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)


messages_processed = Counter(
    "consumer_messages_processed_total",
    "Events validated, stored and acknowledged.",
)

messages_failed = Counter(
    "consumer_messages_failed_total",
    "Events that failed processing, by reason.",
    ["reason"],
)

dlq_messages = Counter(
    "consumer_dlq_messages_total",
    "Events published to the dead letter topic.",
)

abnormal_events = Counter(
    "consumer_abnormal_events_total",
    "Events classified ABNORMAL.",
)

late_events = Counter(
    "consumer_late_events_total",
    "Events arriving behind the watermark, by whether the "
    "aggregate still accepted them.",
    ["aggregated"],
)

processing_seconds = Histogram(
    "consumer_processing_seconds",
    "Time to process a single event end to end.",
    buckets=(
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
    ),
)

# How far the watermark trails wall-clock time. Grows when the stream
# stalls or falls behind, so it doubles as a liveness signal.
watermark_lag_seconds = Gauge(
    "consumer_watermark_lag_seconds",
    "Seconds between now and the current watermark.",
)

windows_open = Gauge(
    "consumer_windows_open",
    "Windows currently held in memory.",
)

# Unconsumed messages per assigned partition. Drives the KafkaLagHigh
# alert. Self-reported rather than taken from a broker exporter: if this
# consumer dies the series goes absent, but ConsumerDown covers that
# case, so nothing is missed.
partition_lag = Gauge(
    "consumer_partition_lag",
    "Messages behind the log end offset, per assigned partition.",
    ["topic", "partition"],
)

windows_finalized = Counter(
    "consumer_windows_finalized_total",
    "Windows closed and persisted.",
)


def serve(port: int) -> None:
    start_http_server(port)

    logger.info(
        "Metrics available on :%d/metrics",
        port,
    )
