from datetime import UTC, datetime

from psycopg import Connection

INSERT_EVENT_SQL = """
INSERT INTO heart_rate_events (
    event_id,
    customer_id,
    heart_rate,
    event_time,
    ingestion_time,
    status
)
VALUES (
    %(event_id)s,
    %(customer_id)s,
    %(heart_rate)s,
    %(event_time)s,
    %(ingestion_time)s,
    %(status)s
)
ON CONFLICT (event_id) DO NOTHING;
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

    with connection.cursor() as cursor:
        cursor.execute(
            INSERT_EVENT_SQL,
            event,
        )

        inserted = cursor.rowcount == 1

    connection.commit()

    return inserted