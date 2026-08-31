import json
import time
import uuid
from datetime import UTC, datetime, timedelta

from kafka import KafkaConsumer


def make_event(
    customer_id: str,
    heart_rate: int = 75,
    event_time: datetime | None = None,
    **overrides,
) -> dict:
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "heart_rate": heart_rate,
        "event_time": (
            event_time or datetime.now(UTC)
        ).isoformat(),
    }

    event.update(overrides)

    return event


def drain(
    subject,
    expected: int,
    timeout: float = 30.0,
) -> int:
    """
    Drive the real process_message path until `expected` messages have
    been handled, or the timeout expires.

    run() loops forever, so tests poll the same way it does instead.
    """

    deadline = time.monotonic() + timeout

    handled = 0

    while handled < expected and time.monotonic() < deadline:
        batches = subject.consumer.poll(timeout_ms=1000)

        for messages in batches.values():
            for message in messages:
                subject.process_message(message)

                handled += 1

    return handled


def rows(connection, customer_id: str) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT event_id, heart_rate, status, event_time,
                   ingestion_time, is_late, lateness_seconds
            FROM heart_rate_events
            WHERE customer_id = %s
            ORDER BY event_time
            """,
            (customer_id,),
        )

        columns = [d[0] for d in cursor.description]

        return [
            dict(zip(columns, row, strict=True))
            for row in cursor.fetchall()
        ]


def aggregates(connection, customer_id: str) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT window_start, window_end, event_count,
                   average_heart_rate, minimum_heart_rate,
                   maximum_heart_rate, abnormal_count, is_finalized
            FROM heart_rate_daily
            WHERE customer_id = %s
            ORDER BY window_start
            """,
            (customer_id,),
        )

        columns = [d[0] for d in cursor.description]

        return [
            dict(zip(columns, row, strict=True))
            for row in cursor.fetchall()
        ]


def read_dlq(
    bootstrap_servers: str,
    topic: str,
    expected: int = 1,
    timeout_ms: int = 20000,
) -> list[dict]:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=f"itest-dlq-{uuid.uuid4().hex[:8]}",
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    try:
        collected = []

        for message in consumer:
            collected.append(message.value)

            if len(collected) >= expected:
                break

        return collected

    finally:
        consumer.close()


def days_from_now(days: float) -> datetime:
    return datetime.now(UTC) + timedelta(days=days)
