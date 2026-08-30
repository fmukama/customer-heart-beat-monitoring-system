from datetime import datetime


def is_late(
    event_time: datetime,
    watermark: datetime,
) -> bool:

    return event_time < watermark