import json
import random
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

from .config import SimulatorConfig
from .scenarios import (
    generate_abnormal_heart_rate,
    generate_backdate_seconds,
    generate_normal_heart_rate,
    should_be_abnormal,
    should_be_out_of_order,
)


def utc_now() -> datetime:
    """
    Return the current UTC time.
    """

    return datetime.now(UTC)


def generate_event(
    customer_id: str,
    config: SimulatorConfig,
    now: datetime | None = None,
) -> dict:
    """
    Generate a single heart-rate event.

    With probability out_of_order_probability the event_time is shifted
    into the past, so the event arrives out of order relative to the
    events already emitted.
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

    event_time = now or utc_now()

    if should_be_out_of_order(config.out_of_order_probability):
        backdate = generate_backdate_seconds(
            config.max_backdate_seconds
        )

        event_time = event_time - timedelta(
            seconds=backdate
        )

    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "heart_rate": heart_rate,
        "event_time": event_time.isoformat(),
    }


def generate_events(
    config: SimulatorConfig,
) -> Iterator[dict]:
    """
    Continuously generate heart-rate events at events_per_second.

    This function produces Python dictionaries.
    Kafka is deliberately not involved here.
    """

    customers = [
        f"customer-{number:04d}"
        for number in range(1, config.customer_count + 1)
    ]

    interval = (
        1.0 / config.events_per_second
        if config.events_per_second > 0
        else 0.0
    )

    next_emit = time.monotonic()

    while True:
        yield generate_event(
            customer_id=random.choice(customers),
            config=config,
        )

        next_emit += interval

        # Absolute schedule, so generation cost does not drift the rate.
        sleep_for = next_emit - time.monotonic()

        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_emit = time.monotonic()


def main() -> None:
    """
    Run the simulator and print events as JSON.

    stdout carries only JSON events so it can be piped into the producer.
    Diagnostics go to stderr.
    """

    config = SimulatorConfig()

    if config.seed is not None:
        random.seed(config.seed)

    print(
        f"Starting simulator: {config.customer_count} customers, "
        f"{config.events_per_second} events/sec, "
        f"out_of_order={config.out_of_order_probability}, "
        f"seed={config.seed}",
        file=sys.stderr,
    )

    for event in generate_events(config):
        print(json.dumps(event), flush=True)


if __name__ == "__main__":
    main()
