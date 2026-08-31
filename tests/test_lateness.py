from datetime import UTC, datetime

from consumer.lateness import is_late, lateness_seconds


def at(minute: int, second: int = 0) -> datetime:
    return datetime(2026, 8, 30, 10, minute, second, tzinfo=UTC)


def test_event_before_watermark_is_late():
    assert is_late(at(1), watermark=at(5)) is True


def test_event_after_watermark_is_not_late():
    assert is_late(at(9), watermark=at(5)) is False


def test_event_exactly_at_watermark_is_not_late():
    assert is_late(at(5), watermark=at(5)) is False


def test_lateness_is_measured_in_seconds():
    assert lateness_seconds(at(4, 30), watermark=at(5)) == 30.0


def test_lateness_is_zero_when_not_late():
    assert lateness_seconds(at(9), watermark=at(5)) == 0.0
