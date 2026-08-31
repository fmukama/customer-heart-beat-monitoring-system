import os
from dataclasses import dataclass


@dataclass(frozen=True)
class NotifierConfig:
    """
    Configuration for the alert notification sink.
    """

    host: str = os.getenv(
        "NOTIFIER_HOST",
        "0.0.0.0",
    )

    port: int = int(
        os.getenv(
            "NOTIFIER_PORT",
            "9091",
        )
    )

    # How often PostgreSQL availability is probed, which also drives
    # draining of writes that failed while it was unreachable.
    probe_interval_seconds: float = float(
        os.getenv(
            "NOTIFIER_PROBE_INTERVAL_SECONDS",
            "15",
        )
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
