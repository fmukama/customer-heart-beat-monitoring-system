import json
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Iterator

from .config import SimulatorConfig
from .scenarios import (
    generate_abnormal_heart_rate,
    generate_late_delay,
    generate_normal_heart_rate,
    should_be_abnormal,
    should_be_late,
)


def utc_now() -> datetime:
    """
    Return the current UTC time.
    """

    return datetime.now(timezone.utc)


def generate_event(
    customer_id: str,
    config: SimulatorConfig,
) -> dict:
    """
    Generate a single heart-rate event.
    """

    if should_be_abnormal(config.abnormal_probability):
        heart_rate = generate_abnormal_heart_rate(
            minimum=config.abnormal_min,
            maximum=config.abnormal_max,
        )
    else:
        heart_rate = generate_normal_heart_rate(
            minimum=config.normal_min,
            maximum=config.normal_max,
        )

    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "heart_rate": heart_rate,
        "event_time": utc_now().isoformat(),
    }


def generate_events(
    config: SimulatorConfig,
) -> Iterator[dict]:
    """
    Continuously generate heart-rate events.

    This function produces Python dictionaries.
    Kafka is deliberately not involved here.
    """

    customers = [
        f"customer-{number:04d}"
        for number in range(1, config.customer_count + 1)
    ]

    while True:
        for _ in range(config.batch_size):

            customer_id = random.choice(customers)

            event = generate_event(
                customer_id=customer_id,
                config=config,
            )

            yield event

        time.sleep(config.interval_seconds)


def main() -> None:
    """
    Run the simulator and print events as JSON.
    """

    config = SimulatorConfig()

    print(
        f"Starting simulator with "
        f"{config.customer_count} customers..."
    )

    for event in generate_events(config):

        late = should_be_late(config.late_probability)

        if late:
            delay = generate_late_delay(
                config.max_late_delay_seconds
            )

            print(
                f"[SIMULATING LATE EVENT] "
                f"delay={delay:.2f}s "
                f"event_id={event['event_id']}"
            )

            time.sleep(delay)

        print(json.dumps(event), flush=True)


if __name__ == "__main__":
    main()