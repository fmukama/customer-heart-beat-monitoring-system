import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumerConfig:
    """Configuration for the Kafka consumer."""

    bootstrap_servers: str = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    topic: str = os.getenv(
        "KAFKA_HEART_RATE_TOPIC",
        "heart-rate-events",
    )

    group_id: str = os.getenv(
        "KAFKA_CONSUMER_GROUP_ID",
        "heartbeat-consumer-group",
    )

    auto_offset_reset: str = os.getenv(
        "KAFKA_AUTO_OFFSET_RESET",
        "earliest",
    )

    enable_auto_commit: bool = os.getenv(
        "KAFKA_ENABLE_AUTO_COMMIT",
        "false",
    ).lower() == "true"

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