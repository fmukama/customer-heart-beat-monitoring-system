from datetime import UTC, datetime, timedelta

import pytest

from consumer.aggregator import DailyAggregator


def at(day: int, hour: int = 12, minute: int = 0) -> datetime:
    return datetime(2026, 8, day, hour, minute, tzinfo=UTC)


def aggregator(
    out_of_orderness_minutes: int = 5,
    lateness_hours: int = 1,
) -> DailyAggregator:
    return DailyAggregator(
        allowed_out_of_orderness=timedelta(
            minutes=out_of_orderness_minutes
        ),
        allowed_lateness=timedelta(hours=lateness_hours),
    )


def add(subject, day, hour=12, minute=0, heart_rate=70, abnormal=False):
    return subject.add_event(
        customer_id="customer-0001",
        heart_rate=heart_rate,
        event_time=at(day, hour, minute),
        is_abnormal=abnormal,
    )


def test_first_event_is_never_late():
    outcome = add(aggregator(), day=30)

    assert outcome.is_late is False
    assert outcome.lateness_seconds == 0.0
    assert outcome.aggregated is True


def test_out_of_order_event_is_detected_as_late():
    subject = aggregator()

    add(subject, day=30, hour=12)

    outcome = add(subject, day=30, hour=11)

    assert outcome.is_late is True
    assert outcome.lateness_seconds > 0


def test_moderately_out_of_order_event_is_not_late():
    # Within allowed_out_of_orderness, so still ahead of the watermark.
    subject = aggregator(out_of_orderness_minutes=5)

    add(subject, day=30, hour=12, minute=10)

    outcome = add(subject, day=30, hour=12, minute=8)

    assert outcome.is_late is False


def test_out_of_order_event_updates_its_own_window():
    subject = aggregator()

    add(subject, day=31, hour=1, heart_rate=80)
    add(subject, day=30, hour=23, heart_rate=60)

    windows = {
        aggregate.window_start.day: aggregate
        for aggregate in subject.snapshot_open_windows()
    }

    assert windows[30].event_count == 1
    assert windows[30].average_heart_rate == 60.0
    assert windows[31].event_count == 1
    assert windows[31].average_heart_rate == 80.0


def test_window_finalizes_once_watermark_passes_allowed_lateness():
    subject = aggregator(lateness_hours=1)

    add(subject, day=30, hour=12)

    assert subject.finalize_ready_windows() == []

    # Push the watermark past 2026-08-31 00:00 + 1h.
    add(subject, day=31, hour=6)

    finalized = subject.finalize_ready_windows()

    assert [a.window_start.day for a in finalized] == [30]


def test_too_late_event_does_not_change_finalized_window():
    subject = aggregator(lateness_hours=1)

    add(subject, day=30, hour=12, heart_rate=70)
    add(subject, day=31, hour=6)

    finalized = subject.finalize_ready_windows()

    original = next(
        a for a in finalized if a.window_start.day == 30
    )

    outcome = add(subject, day=30, hour=13, heart_rate=200)

    assert outcome.is_late is True
    assert outcome.aggregated is False

    # The window is gone from open state and was not reopened.
    assert all(
        a.window_start.day != 30
        for a in subject.snapshot_open_windows()
    )

    assert original.event_count == 1
    assert original.average_heart_rate == 70.0


def test_aggregates_are_per_customer():
    subject = aggregator()

    subject.add_event(
        customer_id="customer-0001",
        heart_rate=60,
        event_time=at(30),
        is_abnormal=False,
    )

    subject.add_event(
        customer_id="customer-0002",
        heart_rate=180,
        event_time=at(30),
        is_abnormal=True,
    )

    by_customer = {
        aggregate.customer_id: aggregate
        for aggregate in subject.snapshot_open_windows()
    }

    assert by_customer["customer-0001"].abnormal_count == 0
    assert by_customer["customer-0002"].abnormal_count == 1


def loader_for(row: dict | None):
    """Stand in for repository.load_window."""

    return lambda customer_id, window_start: row


def persisted(
    count=2,
    average=70.0,
    minimum=70,
    maximum=70,
    abnormal=0,
    finalized=False,
) -> dict:
    return {
        "event_count": count,
        "average_heart_rate": average,
        "minimum_heart_rate": minimum,
        "maximum_heart_rate": maximum,
        "abnormal_count": abnormal,
        "is_finalized": finalized,
    }


def test_window_loaded_lazily_on_first_event():
    subject = DailyAggregator(
        allowed_out_of_orderness=timedelta(minutes=5),
        allowed_lateness=timedelta(hours=1),
        window_loader=loader_for(
            persisted(count=4, average=75.0, minimum=60, maximum=90)
        ),
    )

    # Nothing loaded until an event arrives for the window.
    assert subject.snapshot_open_windows() == []

    add(subject, day=30, hour=12, heart_rate=100)

    snapshot = subject.snapshot_open_windows()[0]

    assert snapshot.event_count == 5
    assert snapshot.maximum_heart_rate == 100
    assert snapshot.average_heart_rate == pytest.approx(80.0)


def test_missing_persisted_window_starts_empty():
    subject = DailyAggregator(
        allowed_out_of_orderness=timedelta(minutes=5),
        allowed_lateness=timedelta(hours=1),
        window_loader=loader_for(None),
    )

    add(subject, day=30, heart_rate=90)

    assert subject.snapshot_open_windows()[0].event_count == 1


def test_window_finalized_in_a_previous_run_is_not_reopened():
    # Without consulting is_finalized from storage, a restarted
    # consumer would happily reopen a closed window.
    subject = DailyAggregator(
        allowed_out_of_orderness=timedelta(minutes=5),
        allowed_lateness=timedelta(hours=1),
        window_loader=loader_for(persisted(finalized=True)),
    )

    outcome = add(subject, day=30, heart_rate=200)

    assert outcome.aggregated is False
    assert subject.snapshot_open_windows() == []


def test_loader_consulted_once_per_window():
    calls = []

    def counting_loader(customer_id, window_start):
        calls.append((customer_id, window_start))


    subject = DailyAggregator(
        allowed_out_of_orderness=timedelta(minutes=5),
        allowed_lateness=timedelta(hours=1),
        window_loader=counting_loader,
    )

    for hour in (10, 11, 12):
        add(subject, day=30, hour=hour)

    assert len(calls) == 1


def test_no_loader_still_works():
    subject = aggregator()

    add(subject, day=30, heart_rate=65)

    assert subject.snapshot_open_windows()[0].event_count == 1


def test_observe_does_not_touch_any_window():
    # The split exists so a duplicate delivery can advance the
    # watermark without being folded into the aggregate.
    subject = aggregator()

    lateness = subject.observe(at(30, 12))

    assert lateness.is_late is False
    assert subject.snapshot_open_windows() == []
    assert subject.watermark is not None


def test_observe_still_judges_lateness():
    subject = aggregator()

    subject.observe(at(30, 12))

    lateness = subject.observe(at(30, 10))

    assert lateness.is_late is True
    assert lateness.lateness_seconds > 0


def test_add_to_window_can_be_called_once_per_stored_event():
    # Simulates the consumer: observe every delivery, but only fold in
    # the ones the database actually accepted.
    subject = aggregator()

    for _ in range(3):
        subject.observe(at(30, 12))

    subject.add_to_window(
        customer_id="customer-0001",
        heart_rate=70,
        event_time=at(30, 12),
        is_abnormal=False,
    )

    snapshot = subject.snapshot_open_windows()[0]

    # Three deliveries, one stored row, one counted event.
    assert snapshot.event_count == 1


def test_add_to_window_refuses_a_finalized_window():
    subject = aggregator(lateness_hours=1)

    add(subject, day=30, hour=12)
    add(subject, day=31, hour=6)

    subject.finalize_ready_windows()

    accepted = subject.add_to_window(
        customer_id="customer-0001",
        heart_rate=200,
        event_time=at(30, 13),
        is_abnormal=True,
    )

    assert accepted is False
