from dataclasses import dataclass


@dataclass(frozen=True)
class SimulatorConfig:
    """
    Configuration for the synthetic heart-rate event generator.
    """

    customer_count: int = 10
    interval_seconds: float = 1.0

    # Normal simulated heart-rate range.
    normal_min: int = 60
    normal_max: int = 100

    # Values outside the normal range are used to
    # simulate abnormal events.
    abnormal_min: int = 40
    abnormal_max: int = 180

    # Probability that a generated event is abnormal.
    abnormal_probability: float = 0.05

    # Probability that an event is deliberately delayed.
    late_probability: float = 0.05

    # Maximum artificial delay for a late event.
    max_late_delay_seconds: float = 10.0

    # Number of events generated per batch.
    batch_size: int = 1