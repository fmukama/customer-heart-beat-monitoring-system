import uuid

import pytest

from consumer.validation import (
    InvalidEventError,
    validate_event,
)


def valid_event() -> dict:

    return {
        "event_id": str(uuid.uuid4()),
        "customer_id": "customer-001",
        "heart_rate": 75,
        "event_time": "2026-08-30T10:00:00+00:00",
    }


def test_valid_event():

    event = valid_event()

    validate_event(event)


def test_missing_customer_id():

    event = valid_event()

    del event["customer_id"]

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_invalid_heart_rate():

    event = valid_event()

    event["heart_rate"] = -10

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_invalid_event_id():

    event = valid_event()

    event["event_id"] = "not-a-uuid"

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_invalid_event_time():

    event = valid_event()

    event["event_time"] = "not-a-date"

    with pytest.raises(InvalidEventError):

        validate_event(event)