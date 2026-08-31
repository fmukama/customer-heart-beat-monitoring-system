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


def test_heart_rate_below_schema_minimum():

    event = valid_event()

    event["heart_rate"] = 5

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_heart_rate_above_schema_maximum():

    event = valid_event()

    event["heart_rate"] = 500

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_heart_rate_at_schema_bounds_is_valid():

    for heart_rate in (20, 250):

        event = valid_event()

        event["heart_rate"] = heart_rate

        validate_event(event)


def test_non_integer_heart_rate():

    event = valid_event()

    event["heart_rate"] = 75.5

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_unexpected_field_is_rejected():

    event = valid_event()

    event["injected"] = "unexpected"

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_empty_customer_id_is_rejected():

    event = valid_event()

    event["customer_id"] = ""

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_naive_event_time_is_rejected():

    event = valid_event()

    event["event_time"] = "2026-08-30T10:00:00"

    with pytest.raises(InvalidEventError):

        validate_event(event)


def test_zulu_event_time_is_accepted():

    event = valid_event()

    event["event_time"] = "2026-08-30T10:00:00Z"

    validate_event(event)