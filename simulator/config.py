import os
from dataclasses import dataclass


def _optional_int(name: str) -> int | None:
    raw = os.getenv(name, "").strip()

    return int(raw) if raw else None


@dataclass(frozen=True)
class SimulatorConfig:
    """
    Configuration for the synthetic heart-rate event generator.
    """

    customer_count: int = int(
        os.getenv(
            "SIMULATOR_CUSTOMER_COUNT",
            "10",
        )
    )

    # Target emission rate. Drives pacing between events.
    events_per_second: float = float(
        os.getenv(
            "SIMULATOR_EVENTS_PER_SECOND",
            "1",
        )
    )

    # Normal simulated heart-rate range.
    normal_min: int = int(
        os.getenv(
            "SIMULATOR_NORMAL_MIN",
            "60",
        )
    )

    normal_max: int = int(
        os.getenv(
            "SIMULATOR_NORMAL_MAX",
            "100",
        )
    )

    # Values outside the normal range are used to
    # simulate abnormal events.
    abnormal_min: int = int(
        os.getenv(
            "SIMULATOR_ABNORMAL_MIN",
            "40",
        )
    )

    abnormal_max: int = int(
        os.getenv(
            "SIMULATOR_ABNORMAL_MAX",
            "180",
        )
    )

    # Probability that a generated event is abnormal.
    abnormal_probability: float = float(
        os.getenv(
            "SIMULATOR_ABNORMAL_PROBABILITY",
            "0.05",
        )
    )

    # Probability that an event carries a backdated event_time,
    # making it arrive out of order.
    out_of_order_probability: float = float(
        os.getenv(
            "SIMULATOR_OUT_OF_ORDER_PROBABILITY",
            "0.05",
        )
    )

    # Maximum amount an out-of-order event_time is backdated.
    max_backdate_seconds: float = float(
        os.getenv(
            "SIMULATOR_MAX_BACKDATE_SECONDS",
            "600",
        )
    )

    # Seed the RNG for reproducible runs. Unset means random.
    seed: int | None = _optional_int("SIMULATOR_SEED")
