"""
Publish one crafted event to exercise a specific pipeline path.

Runs inside the dev service, so Kafka is reachable by name:

    make inject WHAT=late

Cases:
    normal      valid reading, should land as NORMAL
    abnormal    valid but outside 60-100, should land as ABNORMAL
    invalid     malformed event_id, should reach the DLQ
    outofrange  600 bpm, violates the schema, should reach the DLQ
    late        event_time backdated 2h, should be flagged is_late
    toolate     event_time in a window already finalized
    future      event_time +3d, advances the watermark and closes windows
    duplicate   same event_id sent twice, should store exactly one row
"""

import json
import os
import sys
import uuid
from datetime import UTC, datetime, timedelta

from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_HEART_RATE_TOPIC", "heart-rate-events")

CUSTOMER = os.getenv("INJECT_CUSTOMER", "customer-0001")


def base(**overrides) -> dict:
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": CUSTOMER,
        "heart_rate": 75,
        "event_time": datetime.now(UTC).isoformat(),
    }

    event.update(overrides)

    return event


def now_minus(**delta) -> str:
    return (datetime.now(UTC) - timedelta(**delta)).isoformat()


def now_plus(**delta) -> str:
    return (datetime.now(UTC) + timedelta(**delta)).isoformat()


CASES = {
    "normal": lambda: [base(heart_rate=72)],
    "abnormal": lambda: [base(heart_rate=185)],
    "invalid": lambda: [base(event_id="not-a-uuid")],
    "outofrange": lambda: [base(heart_rate=600)],
    "late": lambda: [
        base(heart_rate=90, event_time=now_minus(hours=2))
    ],
    "toolate": lambda: [
        base(heart_rate=240, event_time=now_minus(days=2))
    ],
    "future": lambda: [
        base(heart_rate=70, event_time=now_plus(days=3))
    ],
}


def duplicate() -> list[dict]:
    event = base(heart_rate=70)

    return [event, dict(event)]


CASES["duplicate"] = duplicate


def main() -> None:
    what = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.getenv("WHAT", "")
    ).strip()

    if what not in CASES:
        print(
            "Usage: python scripts/inject.py <case>\n"
            f"Cases: {', '.join(sorted(CASES))}",
            file=sys.stderr,
        )

        raise SystemExit(2)

    producer = KafkaProducer(
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:29092",
        ),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )

    try:
        for event in CASES[what]():
            producer.send(
                TOPIC,
                key=event["customer_id"],
                value=event,
            ).get(timeout=15)

            print(
                f"[{what}] event_id={event['event_id']} "
                f"heart_rate={event['heart_rate']} "
                f"event_time={event['event_time']}"
            )

        producer.flush()

    finally:
        producer.close()


if __name__ == "__main__":
    main()
