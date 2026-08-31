import json

import pytest

from consumer.dlq import DeadLetterProducer
from consumer.errors import DLQPublishError


class FakeFuture:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def get(self, timeout=None):
        if self.error:
            raise self.error

        return object()


class FakeKafkaProducer:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent = []
        self.closed = False

    def send(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

        return FakeFuture(self.error)

    def close(self):
        self.closed = True


@pytest.fixture
def producer(monkeypatch):
    def build(error=None):
        fake = FakeKafkaProducer(error)

        monkeypatch.setattr(
            "consumer.dlq.KafkaProducer",
            lambda **kwargs: fake,
        )

        subject = DeadLetterProducer(
            bootstrap_servers="localhost:9092",
            topic="heart-rate-events-dlq",
        )

        return subject, fake

    return build


def send(subject, event=None):
    subject.send(
        event=event
        if event is not None
        else {"customer_id": "customer-0001", "heart_rate": 70},
        error_type="VALIDATION_ERROR",
        error_message="heart_rate: 500 is greater than the maximum of 250",
        source_topic="heart-rate-events",
        source_partition=2,
        source_offset=123,
    )


def test_dlq_message_preserves_original_event(producer):
    subject, fake = producer()

    original = {"customer_id": "customer-0001", "heart_rate": 500}

    send(subject, original)

    _, _, payload = fake.sent[0]

    assert payload["original_event"] == original


def test_dlq_message_records_error_details(producer):
    subject, fake = producer()

    send(subject)

    _, _, payload = fake.sent[0]

    assert payload["error_type"] == "VALIDATION_ERROR"
    assert "250" in payload["error_message"]


def test_dlq_message_records_kafka_source(producer):
    subject, fake = producer()

    send(subject)

    _, _, payload = fake.sent[0]

    assert payload["source_topic"] == "heart-rate-events"
    assert payload["source_partition"] == 2
    assert payload["source_offset"] == 123
    assert payload["failed_at"]


def test_dlq_is_keyed_by_customer(producer):
    subject, fake = producer()

    send(subject)

    _, key, _ = fake.sent[0]

    assert key == "customer-0001"


def test_dlq_key_falls_back_when_customer_missing(producer):
    subject, fake = producer()

    send(subject, {"heart_rate": 70})

    _, key, _ = fake.sent[0]

    assert key == "unknown"


def test_dlq_payload_is_json_serializable(producer):
    subject, fake = producer()

    send(subject)

    _, _, payload = fake.sent[0]

    assert json.loads(json.dumps(payload)) == payload


def test_publish_failure_raises_dlq_publish_error(producer):
    subject, _ = producer(error=RuntimeError("broker down"))

    # The consumer relies on this to withhold the offset commit.
    with pytest.raises(DLQPublishError):
        send(subject)
