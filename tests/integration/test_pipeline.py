"""
Kafka -> Consumer -> PostgreSQL, against real infrastructure.

Covers the happy path, abnormal readings, dead-lettering of malformed
events, retry on a transient database failure, and idempotency under
at-least-once delivery.
"""

import uuid

import pytest

from consumer import repository
from tests.integration.helpers import (
    aggregates,
    drain,
    make_event,
    read_dlq,
    rows,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def customer() -> str:
    return f"itest-{uuid.uuid4().hex[:10]}"


def test_valid_event_is_stored_with_normal_status(
    publish, build_consumer, db, customer
):
    publish(make_event(customer, heart_rate=72))

    subject = build_consumer()

    assert drain(subject, expected=1) == 1

    stored = rows(db, customer)

    assert len(stored) == 1
    assert stored[0]["heart_rate"] == 72
    assert stored[0]["status"] == "NORMAL"
    assert stored[0]["is_late"] is False


def test_ingestion_time_is_recorded_separately_from_event_time(
    publish, build_consumer, db, customer
):
    event = make_event(customer)

    publish(event)

    subject = build_consumer()

    drain(subject, expected=1)

    stored = rows(db, customer)[0]

    # Deliverable 12: event time is when it happened, ingestion time
    # is when we received it. They are distinct columns.
    assert stored["ingestion_time"] >= stored["event_time"]


@pytest.mark.parametrize("heart_rate", [45, 180])
def test_abnormal_reading_is_stored_and_tagged(
    publish, build_consumer, db, customer, heart_rate
):
    publish(make_event(customer, heart_rate=heart_rate))

    subject = build_consumer()

    drain(subject, expected=1)

    stored = rows(db, customer)

    # Abnormal is not invalid: it must persist, tagged.
    assert len(stored) == 1
    assert stored[0]["status"] == "ABNORMAL"

    subject.flush_windows(force=True)

    assert aggregates(db, customer)[0]["abnormal_count"] == 1


def test_malformed_event_reaches_dlq_and_not_postgres(
    publish, build_consumer, db, customer, topics, bootstrap_servers
):
    _, dlq_topic = topics

    publish(
        make_event(customer, heart_rate=75, event_id="not-a-uuid")
    )

    subject = build_consumer()

    drain(subject, expected=1)

    assert rows(db, customer) == []

    published = read_dlq(bootstrap_servers, dlq_topic)

    assert len(published) == 1

    failure = published[0]

    assert failure["error_type"] == "VALIDATION_ERROR"
    assert failure["original_event"]["customer_id"] == customer
    assert failure["source_topic"] == topics[0]
    assert failure["source_offset"] == 0
    assert failure["failed_at"]


def test_out_of_range_reading_reaches_dlq(
    publish, build_consumer, db, customer, topics, bootstrap_servers
):
    _, dlq_topic = topics

    # 500 bpm is structurally valid JSON but violates the schema range.
    publish(make_event(customer, heart_rate=500))

    subject = build_consumer()

    drain(subject, expected=1)

    assert rows(db, customer) == []

    failure = read_dlq(bootstrap_servers, dlq_topic)[0]

    assert "250" in failure["error_message"]


def test_transient_database_failure_is_retried_until_it_succeeds(
    publish, build_consumer, db, customer, monkeypatch
):
    publish(make_event(customer, heart_rate=88))

    subject = build_consumer()

    real_insert = repository.insert_event

    attempts = {"count": 0}

    def flaky_insert(connection, event):
        attempts["count"] += 1

        if attempts["count"] < 3:
            raise ConnectionError("simulated database outage")

        return real_insert(connection, event)

    monkeypatch.setattr(
        "consumer.consumer.insert_event",
        flaky_insert,
    )

    drain(subject, expected=1)

    # Retried, then landed. No silent loss.
    assert attempts["count"] == 3

    stored = rows(db, customer)

    assert len(stored) == 1
    assert stored[0]["heart_rate"] == 88


def test_idempotency_duplicate_event_id_stored_once(
    publish, build_consumer, db, customer
):
    event = make_event(customer, heart_rate=70)

    publish(event)
    publish(event)

    subject = build_consumer()

    drain(subject, expected=2)

    # Deliverable 09: at-least-once delivery must not duplicate rows.
    assert len(rows(db, customer)) == 1


def test_aggregate_reflects_every_stored_event(
    publish, build_consumer, db, customer
):
    for heart_rate in (60, 80, 100, 190):
        publish(make_event(customer, heart_rate=heart_rate))

    subject = build_consumer()

    drain(subject, expected=4)

    subject.flush_windows(force=True)

    aggregate = aggregates(db, customer)[0]

    assert aggregate["event_count"] == 4
    assert aggregate["minimum_heart_rate"] == 60
    assert aggregate["maximum_heart_rate"] == 190
    assert aggregate["average_heart_rate"] == pytest.approx(107.5)
    assert aggregate["abnormal_count"] == 1
    assert aggregate["is_finalized"] is False


def test_duplicate_does_not_inflate_the_aggregate(
    publish, build_consumer, db, customer
):
    """
    A redelivery is refused by ON CONFLICT, so it must not be counted
    in the window either. Otherwise the aggregate drifts above the raw
    count and reconciliation fails.
    """

    event = make_event(customer, heart_rate=80)

    publish(event)
    publish(event)
    publish(make_event(customer, heart_rate=60))

    subject = build_consumer()

    drain(subject, expected=3)

    subject.flush_windows(force=True)

    stored = rows(db, customer)

    aggregate = aggregates(db, customer)[0]

    # Two distinct readings from three deliveries.
    assert len(stored) == 2
    assert aggregate["event_count"] == 2
    assert aggregate["average_heart_rate"] == pytest.approx(70.0)


def test_aggregate_matches_raw_count_under_redelivery(
    publish, build_consumer, db, customer
):
    events = [
        make_event(customer, heart_rate=70 + index)
        for index in range(5)
    ]

    for event in events:
        publish(event)

    # Redeliver two of them.
    publish(events[0])
    publish(events[3])

    subject = build_consumer()

    drain(subject, expected=7)

    subject.flush_windows(force=True)

    assert aggregates(db, customer)[0]["event_count"] == len(
        rows(db, customer)
    )
