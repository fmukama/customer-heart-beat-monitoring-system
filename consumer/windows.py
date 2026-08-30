from datetime import datetime, timedelta, timezone


def get_day_window(
    event_time: datetime,
) -> tuple[datetime, datetime]:

    event_time = event_time.astimezone(
        timezone.utc
    )

    window_start = event_time.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    window_end = window_start + timedelta(
        days=1
    )

    return window_start, window_end