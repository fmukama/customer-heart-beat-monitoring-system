from datetime import datetime


def is_late(
    event_time: datetime,
    watermark: datetime,
) -> bool:
    return event_time < watermark


def lateness_seconds(
    event_time: datetime,
    watermark: datetime,
) -> float:
    """
    How far behind the watermark an event is. Zero if not late.
    """

    if event_time >= watermark:
        return 0.0

    return (watermark - event_time).total_seconds()
