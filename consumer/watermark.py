from datetime import datetime, timedelta


class WatermarkTracker:

    def __init__(
        self,
        allowed_out_of_orderness: timedelta,
    ) -> None:

        self.allowed_out_of_orderness = (
            allowed_out_of_orderness
        )

        self.max_event_time: datetime | None = None

    def update(
        self,
        event_time: datetime,
    ) -> datetime:

        if (
            self.max_event_time is None
            or event_time > self.max_event_time
        ):
            self.max_event_time = event_time

        return self.watermark

    @property
    def watermark(self) -> datetime | None:

        if self.max_event_time is None:
            return None

        return (
            self.max_event_time
            - self.allowed_out_of_orderness
        )