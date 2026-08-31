import random
from dataclasses import replace
from datetime import datetime

from simulator.config import SimulatorConfig
from simulator.generator import generate_event
from simulator.scenarios import (
    generate_backdate_seconds,
    generate_normal_heart_rate,
    should_be_out_of_order,
)


def config(**overrides) -> SimulatorConfig:
    return replace(SimulatorConfig(), **overrides)


def event_time_of(event: dict) -> datetime:
    return datetime.fromisoformat(event["event_time"])


def test_normal_heart_rate_within_range():
    for _ in range(200):
        assert 60 <= generate_normal_heart_rate(60, 100) <= 100


def test_backdate_is_bounded():
    for _ in range(200):
        assert 1.0 <= generate_backdate_seconds(600) <= 600


def test_out_of_order_probability_bounds():
    assert should_be_out_of_order(0.0) is False
    assert should_be_out_of_order(1.0) is True


def test_out_of_order_event_time_is_backdated():
    # This is the behavior the watermark depends on: a late event must
    # carry an event_time in the past, not merely arrive later.
    settings = config(
        out_of_order_probability=1.0,
        max_backdate_seconds=600,
    )

    reference = generate_event("customer-0001", config(
        out_of_order_probability=0.0,
    ))

    backdated = generate_event("customer-0001", settings)

    assert event_time_of(backdated) < event_time_of(reference)


def test_in_order_event_time_is_not_backdated():
    settings = config(out_of_order_probability=0.0)

    before = generate_event("customer-0001", settings)
    after = generate_event("customer-0001", settings)

    assert event_time_of(after) >= event_time_of(before)


def test_seeded_runs_are_reproducible():
    settings = config(
        out_of_order_probability=0.5,
        abnormal_probability=0.5,
    )

    fixed_now = datetime.fromisoformat(
        "2026-08-30T12:00:00+00:00"
    )

    def run() -> list[int]:
        random.seed(1234)

        return [
            generate_event(
                "customer-0001",
                settings,
                now=fixed_now,
            )["heart_rate"]
            for _ in range(20)
        ]

    assert run() == run()
