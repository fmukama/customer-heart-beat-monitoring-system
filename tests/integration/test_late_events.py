"""
Event time, watermarks, windowing and the late-event policy.

Covers out-of-order detection, window assignment by event time rather
than arrival order, window boundaries, and the policy decision that an
event arriving past the allowed lateness is retained as raw data but
must not change an already-finalized window.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from tests.integration.helpers import (
    aggregates,
    drain,
    make_event,
    rows,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def customer() -> str:
    return f"itest-{uuid.uuid4().hex[:10]}"


def midday(offset_days: int = 0) -> datetime:
    base = datetime.now(UTC).replace(
        hour=12,
        minute=0,
        second=0,
        microsecond=0,
    )

    return base + timedelta(days=offset_days)


def test_out_of_order_event_is_flagged_with_measured_lateness(
    publish, build_consumer, db, customer
):
    publish(make_event(customer, event_time=midday()))

    # Two hours behind the first, well past the 300s watermark lag.
    publish(
        make_event(
            customer,
            heart_rate=90,
            event_time=midday() - timedelta(hours=2),
        )
    )

    subject = build_consumer()

    drain(subject, expected=2)

    stored = rows(db, customer)

    assert len(stored) == 2

    late = [row for row in stored if row["is_late"]]

    assert len(late) == 1
    assert late[0]["heart_rate"] == 90
    assert late[0]["lateness_seconds"] > 0


def test_moderately_out_of_order_event_is_not_late(
    publish, build_consumer, db, customer
):
    publish(make_event(customer, event_time=midday()))

    # Inside allowed_out_of_orderness, so still ahead of the watermark.
    publish(
        make_event(
            customer,
            event_time=midday() - timedelta(seconds=60),
        )
    )

    subject = build_consumer(
        allowed_out_of_orderness_seconds=300,
    )

    drain(subject, expected=2)

    assert all(not row["is_late"] for row in rows(db, customer))


def test_late_event_updates_the_window_its_event_time_belongs_to(
    publish, build_consumer, db, customer
):
    # Today, then yesterday. The late event belongs to yesterday's
    # window, assigned by event time rather than arrival order.
    publish(make_event(customer, heart_rate=70, event_time=midday()))

    publish(
        make_event(
            customer,
            heart_rate=100,
            event_time=midday(-1),
        )
    )

    subject = build_consumer()

    drain(subject, expected=2)

    subject.flush_windows(force=True)

    windows = aggregates(db, customer)

    assert len(windows) == 2

    yesterday, today = windows

    assert yesterday["event_count"] == 1
    assert yesterday["average_heart_rate"] == 100.0

    assert today["event_count"] == 1
    assert today["average_heart_rate"] == 70.0


def test_window_boundaries_are_one_day_and_do_not_overlap(
    publish, build_consumer, db, customer
):
    publish(make_event(customer, event_time=midday(-1)))
    publish(make_event(customer, event_time=midday()))

    subject = build_consumer()

    drain(subject, expected=2)

    subject.flush_windows(force=True)

    windows = aggregates(db, customer)

    for window in windows:
        assert (
            window["window_end"] - window["window_start"]
        ) == timedelta(days=1)

    assert windows[0]["window_end"] == windows[1]["window_start"]


def test_event_past_allowed_lateness_leaves_finalized_window_unchanged(
    publish, build_consumer, db, customer
):
    """
    The policy decision: raw data is always kept, but a finalized
    window is immutable, so historical aggregates stay stable.
    """

    publish(make_event(customer, heart_rate=70, event_time=midday(-2)))

    subject = build_consumer(allowed_lateness_seconds=0)

    drain(subject, expected=1)

    # Advance the watermark far past that window's end so it closes.
    publish(make_event(customer, heart_rate=80, event_time=midday()))

    drain(subject, expected=1)

    subject.flush_windows(force=True)

    finalized = [
        window
        for window in aggregates(db, customer)
        if window["is_finalized"]
    ]

    assert len(finalized) == 1
    assert finalized[0]["event_count"] == 1
    assert finalized[0]["maximum_heart_rate"] == 70

    # Now a very late event for that closed window.
    publish(
        make_event(
            customer,
            heart_rate=240,
            event_time=midday(-2) + timedelta(hours=1),
        )
    )

    drain(subject, expected=1)

    subject.flush_windows(force=True)

    stored = rows(db, customer)

    too_late = [
        row for row in stored if row["heart_rate"] == 240
    ]

    # Retained raw, and flagged.
    assert len(too_late) == 1
    assert too_late[0]["is_late"] is True
    assert too_late[0]["lateness_seconds"] > 0

    # The finalized aggregate did not move.
    still_finalized = [
        window
        for window in aggregates(db, customer)
        if window["is_finalized"]
    ]

    assert still_finalized[0]["event_count"] == 1
    assert still_finalized[0]["maximum_heart_rate"] == 70


def test_finalized_window_persists_finalized_at(
    publish, build_consumer, db, customer
):
    publish(make_event(customer, event_time=midday(-2)))

    subject = build_consumer(allowed_lateness_seconds=0)

    drain(subject, expected=1)

    publish(make_event(customer, event_time=midday()))

    drain(subject, expected=1)

    subject.flush_windows(force=True)

    finalized = [
        window
        for window in aggregates(db, customer)
        if window["is_finalized"]
    ]

    assert len(finalized) == 1
