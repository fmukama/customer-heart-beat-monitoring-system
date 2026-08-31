import json
import random
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

from .config import SimulatorConfig
from .faults import (
    corrupt_event,
    generate_abnormal_heart_rate,
    generate_backdate_seconds,
    generate_normal_heart_rate,
    generate_out_of_range_heart_rate,
    should_happen,
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
    Generate a single heart-rate event, with faults.

    Each fault models something a real sensor fleet does, and each is
    independently tunable so a run can be made clean or hostile:

    * abnormal        valid reading outside 60-100 bpm
    * out of order    event_time backdated, so it arrives late
    * extreme late    backdated days, as if a device had been offline
    * out of range    impossible value, rejected by the schema
    * invalid         malformed payload from a buggy device

    Duplicates are applied one level up, in generate_events, since they
    re-emit a whole event rather than alter one.
    """

    if should_happen(config.out_of_range_probability):
        # Violates the schema range, so this one is rejected outright
        # rather than stored and tagged ABNORMAL.
        heart_rate = generate_out_of_range_heart_rate()

    elif should_happen(config.abnormal_probability):
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

    if should_happen(config.extreme_late_probability):
        # Checked before ordinary out-of-orderness, so an extreme
        # backdate is never overwritten by a smaller one.
        event_time = event_time - timedelta(
            seconds=generate_backdate_seconds(
                config.extreme_backdate_seconds
            )
        )

    elif should_happen(config.out_of_order_probability):
        event_time = event_time - timedelta(
            seconds=generate_backdate_seconds(
                config.max_backdate_seconds
            )
        )

    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "heart_rate": heart_rate,
        "event_time": event_time.isoformat(),
    }

    if should_happen(config.invalid_probability):
        return corrupt_event(event)

    return event


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

    previous: dict | None = None

    while True:
        if (
            previous is not None
            and should_happen(config.duplicate_probability)
        ):
            # A device retrying a send it already made. Kafka's
            # at-least-once behaviour looks the same downstream.
            event = previous
        else:
            event = generate_event(
                customer_id=random.choice(customers),
                config=config,
            )

            previous = event

        yield event

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
        f"{config.events_per_second} events/sec, seed={config.seed}\n"
        f"  faults: abnormal={config.abnormal_probability} "
        f"out_of_order={config.out_of_order_probability} "
        f"extreme_late={config.extreme_late_probability} "
        f"out_of_range={config.out_of_range_probability} "
        f"invalid={config.invalid_probability} "
        f"duplicate={config.duplicate_probability}",
        file=sys.stderr,
    )

    for event in generate_events(config):
        print(json.dumps(event), flush=True)


if __name__ == "__main__":
    main()
