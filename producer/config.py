import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaProducerConfig:
    """
    Configuration for the Kafka producer.
    """

    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    topic: str = os.getenv(
        "KAFKA_HEART_RATE_TOPIC",
        "heart-rate-events",
    )

    client_id: str = os.getenv(
        "KAFKA_PRODUCER_CLIENT_ID",
        "heartbeat-producer",
    )

    acks: str = os.getenv(
        "KAFKA_ACKS",
        "all",
    )

    retries: int = int(
        os.getenv(
            "KAFKA_RETRIES",
            "5",
        )
    )

    linger_ms: int = int(
        os.getenv(
            "KAFKA_LINGER_MS",
            "5",
        )
    )

    batch_size: int = int(
        os.getenv(
            "KAFKA_BATCH_SIZE",
            "16384",
        )
    )

    compression_type: str = os.getenv(
        "KAFKA_COMPRESSION_TYPE",
        "gzip",
    )

    # Waiting for each message's acknowledgement costs a round trip per
    # event, which caps throughput at roughly 1/linger_ms. Keep it on
    # for demos and per-event logging; turn it off for load tests.
    sync_send: bool = os.getenv(
        "KAFKA_SYNC_SEND",
        "true",
    ).lower() == "true"

    # Async mode logs a running total instead of every event.
    log_every: int = int(
        os.getenv(
            "KAFKA_PRODUCER_LOG_EVERY",
            "1000",
        )
    )