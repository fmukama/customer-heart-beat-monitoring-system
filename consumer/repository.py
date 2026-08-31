from datetime import UTC, datetime

from psycopg import Connection

from .aggregator import WindowAggregate

INSERT_EVENT_SQL = """
INSERT INTO heart_rate_events (
    event_id,
    customer_id,
    heart_rate,
    event_time,
    ingestion_time,
    status,
    is_late,
    lateness_seconds
)
VALUES (
    %(event_id)s,
    %(customer_id)s,
    %(heart_rate)s,
    %(event_time)s,
    %(ingestion_time)s,
    %(status)s,
    %(is_late)s,
    %(lateness_seconds)s
)
ON CONFLICT (event_id) DO NOTHING;
"""


UPSERT_DAILY_SQL = """
INSERT INTO heart_rate_daily (
    customer_id,
    window_start,
    window_end,
    event_count,
    average_heart_rate,
    minimum_heart_rate,
    maximum_heart_rate,
    abnormal_count,
    finalized_at,
    is_finalized
)
VALUES (
    %(customer_id)s,
    %(window_start)s,
    %(window_end)s,
    %(event_count)s,
    %(average_heart_rate)s,
    %(minimum_heart_rate)s,
    %(maximum_heart_rate)s,
    %(abnormal_count)s,
    %(finalized_at)s,
    %(is_finalized)s
)
ON CONFLICT (customer_id, window_start) DO UPDATE SET
    window_end         = EXCLUDED.window_end,
    event_count        = EXCLUDED.event_count,
    average_heart_rate = EXCLUDED.average_heart_rate,
    minimum_heart_rate = EXCLUDED.minimum_heart_rate,
    maximum_heart_rate = EXCLUDED.maximum_heart_rate,
    abnormal_count     = EXCLUDED.abnormal_count,
    finalized_at       = EXCLUDED.finalized_at,
    is_finalized       = EXCLUDED.is_finalized;
"""


def insert_event(
    connection: Connection,
    event: dict,
) -> bool:
    """
    Insert a heart-rate event.

    Returns:
        True  -> a new row was inserted.
        False -> event already existed.
    """

    event["ingestion_time"] = datetime.now(UTC)

    event.setdefault("is_late", False)
    event.setdefault("lateness_seconds", None)

    with connection.cursor() as cursor:
        cursor.execute(
            INSERT_EVENT_SQL,
            event,
        )

        inserted = cursor.rowcount == 1

    connection.commit()

    return inserted


LOAD_OPEN_WINDOWS_SQL = """
SELECT
    customer_id,
    window_start,
    event_count,
    average_heart_rate,
    minimum_heart_rate,
    maximum_heart_rate,
    abnormal_count
FROM heart_rate_daily
WHERE is_finalized = FALSE;
"""


def upsert_daily_aggregate(
    connection: Connection,
    aggregate: WindowAggregate,
    is_finalized: bool,
) -> None:
    """
    Persist one window's aggregate.

    Idempotent, so re-finalizing a window after a restart or replay
    overwrites rather than duplicating. finalized_at is only meaningful
    once is_finalized is true.
    """

    with connection.cursor() as cursor:
        cursor.execute(
            UPSERT_DAILY_SQL,
            {
                "customer_id": aggregate.customer_id,
                "window_start": aggregate.window_start,
                "window_end": aggregate.window_end,
                "event_count": aggregate.event_count,
                "average_heart_rate": aggregate.average_heart_rate,
                "minimum_heart_rate": aggregate.minimum_heart_rate,
                "maximum_heart_rate": aggregate.maximum_heart_rate,
                "abnormal_count": aggregate.abnormal_count,
                "finalized_at": datetime.now(UTC),
                "is_finalized": is_finalized,
            },
        )

    connection.commit()


def load_open_windows(connection: Connection) -> list[dict]:
    """
    Every window not yet finalized.

    Offsets are committed as events are processed, so a restarted
    consumer never re-reads them. Without reloading these the in-memory
    state would restart empty and the next snapshot would overwrite a
    good aggregate with a partial one.
    """

    with connection.cursor() as cursor:
        cursor.execute(LOAD_OPEN_WINDOWS_SQL)

        columns = [
            description[0]
            for description in cursor.description
        ]

        return [
            dict(zip(columns, row, strict=True))
            for row in cursor.fetchall()
        ]
