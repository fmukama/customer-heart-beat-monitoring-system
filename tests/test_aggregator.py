from datetime import UTC, datetime, timedelta

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


def test_rehydrate_restores_open_window_totals():
    subject = aggregator()

    restored = subject.rehydrate(
        [
            {
                "customer_id": "customer-0001",
                "window_start": at(30, 0, 0),
                "event_count": 4,
                "average_heart_rate": 75.0,
                "minimum_heart_rate": 60,
                "maximum_heart_rate": 90,
                "abnormal_count": 1,
            }
        ]
    )

    assert restored == 1

    snapshot = subject.snapshot_open_windows()[0]

    assert snapshot.event_count == 4
    assert snapshot.average_heart_rate == 75.0
    assert snapshot.abnormal_count == 1


def test_rehydrated_window_keeps_accumulating():
    # The restart case: totals must continue from what was persisted,
    # not restart at zero and overwrite a good aggregate.
    subject = aggregator()

    subject.rehydrate(
        [
            {
                "customer_id": "customer-0001",
                "window_start": at(30, 0, 0),
                "event_count": 2,
                "average_heart_rate": 70.0,
                "minimum_heart_rate": 70,
                "maximum_heart_rate": 70,
                "abnormal_count": 0,
            }
        ]
    )

    add(subject, day=30, hour=12, heart_rate=100)

    snapshot = subject.snapshot_open_windows()[0]

    assert snapshot.event_count == 3
    assert snapshot.average_heart_rate == 80.0
    assert snapshot.maximum_heart_rate == 100


def test_rehydrate_does_not_seed_watermark():
    subject = aggregator()

    subject.rehydrate(
        [
            {
                "customer_id": "customer-0001",
                "window_start": at(30, 0, 0),
                "event_count": 1,
                "average_heart_rate": 70.0,
                "minimum_heart_rate": 70,
                "maximum_heart_rate": 70,
                "abnormal_count": 0,
            }
        ]
    )

    assert subject.watermark is None
