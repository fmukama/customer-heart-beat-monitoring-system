from datetime import UTC, datetime, timedelta

from consumer.watermark import WatermarkTracker


def at(minute: int) -> datetime:
    return datetime(2026, 8, 30, 10, minute, tzinfo=UTC)


def tracker() -> WatermarkTracker:
    return WatermarkTracker(
        allowed_out_of_orderness=timedelta(minutes=5),
    )


def test_watermark_is_none_before_any_event():
    assert tracker().watermark is None


def test_watermark_trails_max_event_time():
    subject = tracker()

    subject.update(at(10))

    assert subject.watermark == at(5)


def test_watermark_moves_forward():
    subject = tracker()

    subject.update(at(0))
    subject.update(at(10))

    assert subject.watermark == at(5)


def test_watermark_never_moves_backward():
    subject = tracker()

    subject.update(at(10))

    before = subject.watermark

    subject.update(at(1))

    assert subject.watermark == before


def test_watermark_independent_of_arrival_order():
    ascending = tracker()
    shuffled = tracker()

    for minute in [0, 3, 1, 7]:
        ascending.update(at(minute))

    for minute in [7, 1, 3, 0]:
        shuffled.update(at(minute))

    assert ascending.watermark == shuffled.watermark
