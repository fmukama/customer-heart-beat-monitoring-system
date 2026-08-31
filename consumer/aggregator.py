from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

from .lateness import is_late, lateness_seconds
from .watermark import WatermarkTracker
from .window_state import WindowState
from .windows import get_day_window


@dataclass(frozen=True)
class Lateness:
    """
    How an event sits relative to the watermark.
    """

    is_late: bool

    lateness_seconds: float


@dataclass(frozen=True)
class EventOutcome:
    """
    What happened to one event's contribution to its window.
    """

    is_late: bool

    lateness_seconds: float

    # False when the event's window was already finalized, so the
    # aggregate was left untouched. The raw event is still stored.
    aggregated: bool


@dataclass(frozen=True)
class WindowAggregate:
    customer_id: str

    window_start: datetime

    window_end: datetime

    event_count: int

    average_heart_rate: float

    minimum_heart_rate: int

    maximum_heart_rate: int

    abnormal_count: int


class DailyAggregator:
    """
    Event-time aggregation into 1-day tumbling windows.

    Windows are assigned from event_time, never arrival order. A window
    is finalized once the watermark passes window_end plus the allowed
    lateness; after that its aggregate is immutable and later events are
    recorded raw only.
    """

    def __init__(
        self,
        allowed_out_of_orderness: timedelta,
        allowed_lateness: timedelta,
        window_loader: Callable[
            [str, datetime], dict | None
        ] | None = None,
    ) -> None:

        self.allowed_lateness = allowed_lateness

        # Consulted on first event for a window, so state survives a
        # restart. See repository.load_window for why this is lazy.
        self.window_loader = window_loader

        self.watermark_tracker = WatermarkTracker(
            allowed_out_of_orderness=allowed_out_of_orderness,
        )

        self.windows: dict[
            tuple[str, datetime], WindowState
        ] = {}

        self.finalized: set[tuple[str, datetime]] = set()

    @property
    def watermark(self) -> datetime | None:
        return self.watermark_tracker.watermark

    def _restore(
        self,
        key: tuple[str, datetime],
    ) -> WindowState | None:
        """
        Seed a window from persisted state.

        Returns None when the window was already finalized, meaning it
        must not be reopened. The watermark is deliberately not seeded
        from persisted rows: it is derived from event time actually
        observed, and a stale watermark would misclassify lateness.
        """

        if self.window_loader is None:
            return WindowState()

        row = self.window_loader(*key)

        if row is None:
            return WindowState()

        if row["is_finalized"]:
            self.finalized.add(key)

            return None

        count = row["event_count"]

        return WindowState(
            count=count,
            heart_rate_sum=round(
                row["average_heart_rate"] * count
            ),
            minimum=row["minimum_heart_rate"],
            maximum=row["maximum_heart_rate"],
            abnormal_count=row["abnormal_count"],
        )

    def observe(self, event_time: datetime) -> Lateness:
        """
        Advance the watermark and judge lateness. No window is touched.

        Separate from add_to_window because the consumer must know
        is_late *before* writing the raw row, but must only update
        window state *after* the write proves the event was not a
        duplicate. Doing both in one step double-counts redeliveries.
        """

        previous_watermark = self.watermark_tracker.watermark

        self.watermark_tracker.update(event_time)

        # Lateness is judged against the watermark as it stood before
        # this event, so an event is never late relative to itself.
        late = (
            previous_watermark is not None
            and is_late(event_time, previous_watermark)
        )

        lateness = (
            lateness_seconds(event_time, previous_watermark)
            if previous_watermark is not None
            else 0.0
        )

        return Lateness(
            is_late=late,
            lateness_seconds=lateness,
        )

    def add_to_window(
        self,
        customer_id: str,
        heart_rate: int,
        event_time: datetime,
        is_abnormal: bool,
    ) -> bool:
        """
        Fold one event into its window.

        Returns False when the window is already finalized, meaning the
        aggregate was deliberately left alone.
        """

        window_start, _ = get_day_window(event_time)

        key = (customer_id, window_start)

        if key in self.finalized:
            return False

        state = self.windows.get(key)

        if state is None:
            state = self._restore(key)

            if state is None:
                # Finalized in a previous run of this consumer.
                return False

            self.windows[key] = state

        state.add(
            heart_rate=heart_rate,
            is_abnormal=is_abnormal,
        )

        return True

    def add_event(
        self,
        customer_id: str,
        heart_rate: int,
        event_time: datetime,
        is_abnormal: bool,
    ) -> EventOutcome:
        """
        Observe and aggregate in one step.

        Convenient when there is no database round trip in between, as
        in the unit tests. The consumer uses the two calls separately.
        """

        lateness = self.observe(event_time)

        aggregated = self.add_to_window(
            customer_id=customer_id,
            heart_rate=heart_rate,
            event_time=event_time,
            is_abnormal=is_abnormal,
        )

        return EventOutcome(
            is_late=lateness.is_late,
            lateness_seconds=lateness.lateness_seconds,
            aggregated=aggregated,
        )

    def finalize_ready_windows(self) -> list[WindowAggregate]:
        """
        Release every window the watermark has moved safely past.
        """

        watermark = self.watermark_tracker.watermark

        if watermark is None:
            return []

        ready = []

        for key in list(self.windows):
            customer_id, window_start = key

            _, window_end = get_day_window(window_start)

            if watermark <= window_end + self.allowed_lateness:
                continue

            state = self.windows.pop(key)

            self.finalized.add(key)

            ready.append(
                WindowAggregate(
                    customer_id=customer_id,
                    window_start=window_start,
                    window_end=window_end,
                    event_count=state.count,
                    average_heart_rate=state.average,
                    minimum_heart_rate=state.minimum,
                    maximum_heart_rate=state.maximum,
                    abnormal_count=state.abnormal_count,
                )
            )

        return ready

    def snapshot_open_windows(self) -> list[WindowAggregate]:
        """
        Current value of every window still open.

        Used to persist partial aggregates so today's window is
        queryable before it closes.
        """

        return [
            WindowAggregate(
                customer_id=customer_id,
                window_start=window_start,
                window_end=get_day_window(window_start)[1],
                event_count=state.count,
                average_heart_rate=state.average,
                minimum_heart_rate=state.minimum,
                maximum_heart_rate=state.maximum,
                abnormal_count=state.abnormal_count,
            )
            for (
                customer_id,
                window_start,
            ), state in self.windows.items()
            if state.count > 0
        ]
