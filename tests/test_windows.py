from datetime import UTC, datetime, timedelta, timezone

from consumer.windows import get_day_window


def test_window_starts_at_utc_midnight():
    start, _ = get_day_window(
        datetime(2026, 8, 30, 13, 45, 12, tzinfo=UTC)
    )

    assert start == datetime(2026, 8, 30, 0, 0, tzinfo=UTC)


def test_window_is_exactly_one_day():
    start, end = get_day_window(
        datetime(2026, 8, 30, 13, 45, tzinfo=UTC)
    )

    assert end - start == timedelta(days=1)


def test_consecutive_windows_do_not_overlap():
    _, first_end = get_day_window(
        datetime(2026, 8, 30, 23, 59, 59, tzinfo=UTC)
    )

    second_start, _ = get_day_window(
        datetime(2026, 8, 31, 0, 0, 0, tzinfo=UTC)
    )

    assert first_end == second_start


def test_window_assigned_from_event_time_not_arrival():
    early, _ = get_day_window(
        datetime(2026, 8, 30, 0, 0, 1, tzinfo=UTC)
    )

    late, _ = get_day_window(
        datetime(2026, 8, 30, 23, 59, 59, tzinfo=UTC)
    )

    assert early == late


def test_non_utc_event_time_is_normalized():
    # 01:30 at +03:00 is 22:30 the previous day in UTC.
    start, _ = get_day_window(
        datetime(
            2026,
            8,
            31,
            1,
            30,
            tzinfo=timezone(timedelta(hours=3)),
        )
    )

    assert start == datetime(2026, 8, 30, 0, 0, tzinfo=UTC)
