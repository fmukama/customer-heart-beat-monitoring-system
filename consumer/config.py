import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumerConfig:

    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    topic: str = os.getenv(
        "KAFKA_HEART_RATE_TOPIC",
        "heart-rate-events",
    )

    dlq_topic: str = os.getenv(
        "KAFKA_HEART_RATE_DLQ_TOPIC",
        "heart-rate-events-dlq",
    )

    group_id: str = os.getenv(
        "KAFKA_CONSUMER_GROUP_ID",
        "heartbeat-consumer-group",
    )

    auto_offset_reset: str = os.getenv(
        "KAFKA_AUTO_OFFSET_RESET",
        "earliest",
    )

    postgres_host: str = os.getenv(
        "POSTGRES_HOST",
        "localhost",
    )

    postgres_port: int = int(
        os.getenv(
            "POSTGRES_PORT",
            "5432",
        )
    )

    postgres_db: str = os.getenv(
        "POSTGRES_DB",
        "heartbeat",
    )

    postgres_user: str = os.getenv(
        "POSTGRES_USER",
        "heartbeat_user",
    )

    postgres_password: str = os.getenv(
        "POSTGRES_PASSWORD",
        "heartbeat_password",
    )

    max_retry_attempts: int = int(
        os.getenv(
            "MAX_RETRY_ATTEMPTS",
            "4",
        )
    )

    # Watermark lags the maximum event time seen by this much, so
    # moderately out-of-order events still land in the right window.
    allowed_out_of_orderness_seconds: int = int(
        os.getenv(
            "ALLOWED_OUT_OF_ORDERNESS_SECONDS",
            "300",
        )
    )

    # Grace period past window_end before a window is finalized.
    # Events arriving after finalization are stored raw but do not
    # change the aggregate.
    allowed_lateness_seconds: int = int(
        os.getenv(
            "ALLOWED_LATENESS_SECONDS",
            "3600",
        )
    )

    window_flush_interval_seconds: float = float(
        os.getenv(
            "WINDOW_FLUSH_INTERVAL_SECONDS",
            "30",
        )
    )

    # Bounds how long the poll loop blocks, so window flushing still
    # ticks when no messages are arriving.
    poll_timeout_ms: int = int(
        os.getenv(
            "KAFKA_POLL_TIMEOUT_MS",
            "1000",
        )
    )

    metrics_port: int = int(
        os.getenv(
            "METRICS_PORT",
            "8000",
        )
    )