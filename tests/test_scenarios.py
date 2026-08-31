import random
from dataclasses import replace
from datetime import UTC, datetime
from itertools import islice

import pytest

from consumer.validation import InvalidEventError, validate_event
from simulator.config import SimulatorConfig
from simulator.generator import generate_event, generate_events
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


# ---------------------------------------------------------------
# Automatic fault injection
# ---------------------------------------------------------------


def all_faults_off(**overrides) -> SimulatorConfig:
    settings = {
        "abnormal_probability": 0.0,
        "out_of_order_probability": 0.0,
        "extreme_late_probability": 0.0,
        "out_of_range_probability": 0.0,
        "invalid_probability": 0.0,
        "duplicate_probability": 0.0,
    }

    settings.update(overrides)

    return config(**settings)


def test_clean_config_produces_only_valid_normal_events():
    settings = all_faults_off()

    for _ in range(50):
        event = generate_event("customer-0001", settings)

        validate_event(event)

        assert 60 <= event["heart_rate"] <= 100


def test_out_of_range_events_violate_the_schema():
    settings = all_faults_off(out_of_range_probability=1.0)

    event = generate_event("customer-0001", settings)

    # Not merely abnormal: outside the schema's 20-250 range.
    assert not 20 <= event["heart_rate"] <= 250

    with pytest.raises(InvalidEventError):
        validate_event(event)


def test_invalid_events_are_rejected_by_validation():
    settings = all_faults_off(invalid_probability=1.0)

    for _ in range(40):
        event = generate_event("customer-0001", settings)

        with pytest.raises(InvalidEventError):
            validate_event(event)


def test_corruption_always_preserves_customer_id():
    # The producer uses customer_id as the Kafka key, so losing it
    # would crash the producer rather than exercise validation.
    settings = all_faults_off(invalid_probability=1.0)

    for _ in range(40):
        event = generate_event("customer-0007", settings)

        assert event["customer_id"] == "customer-0007"


def test_corruption_varies():
    settings = all_faults_off(invalid_probability=1.0)

    shapes = {
        tuple(sorted(generate_event("customer-0001", settings)))
        for _ in range(60)
    }

    # Several distinct failure modes, not one repeated shape.
    assert len(shapes) > 1


def test_extreme_late_events_are_dramatically_backdated():
    settings = all_faults_off(
        extreme_late_probability=1.0,
        extreme_backdate_seconds=172800,
    )

    reference = datetime.now(UTC)

    event = generate_event("customer-0001", settings)

    lateness = (reference - event_time_of(event)).total_seconds()

    # Far beyond the ordinary out-of-orderness allowance of 300s.
    assert lateness > 3600


def test_duplicates_reuse_the_same_event_id():
    settings = all_faults_off(duplicate_probability=1.0)

    events = list(islice(generate_events(settings), 3))

    # First is fresh, the rest repeat it verbatim.
    assert events[1]["event_id"] == events[0]["event_id"]
    assert events[2]["event_id"] == events[0]["event_id"]


def test_no_duplicates_when_probability_is_zero():
    settings = all_faults_off()

    events = list(islice(generate_events(settings), 5))

    ids = {event["event_id"] for event in events}

    assert len(ids) == 5


def test_out_of_range_takes_precedence_over_abnormal():
    # Both would set heart_rate; out of range must win so the event is
    # rejected rather than stored as ABNORMAL.
    settings = all_faults_off(
        out_of_range_probability=1.0,
        abnormal_probability=1.0,
    )

    event = generate_event("customer-0001", settings)

    assert not 20 <= event["heart_rate"] <= 250
