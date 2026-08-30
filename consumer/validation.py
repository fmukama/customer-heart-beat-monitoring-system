from datetime import datetime
from uuid import UUID

from .errors import PermanentEventError


class InvalidEventError(PermanentEventError):
    """Raised when a heart-rate event is invalid."""


def validate_event(event: dict) -> None:
    """
    Validate the minimum requirements of a heart-rate event.
    """

    required_fields = {
        "event_id",
        "customer_id",
        "heart_rate",
        "event_time",
    }

    missing_fields = required_fields - event.keys()

    if missing_fields:
        raise InvalidEventError(
            f"Missing fields: {sorted(missing_fields)}"
        )

    try:
        UUID(event["event_id"])
    except (ValueError, AttributeError, TypeError) as exc:
        raise InvalidEventError(
            "Invalid event_id"
        ) from exc

    if not isinstance(event["customer_id"], str):
        raise InvalidEventError(
            "customer_id must be a string"
        )

    if not isinstance(event["heart_rate"], int):
        raise InvalidEventError(
            "heart_rate must be an integer"
        )

    if event["heart_rate"] <= 0:
        raise InvalidEventError(
            "heart_rate must be greater than zero"
        )

    try:
        datetime.fromisoformat(
            event["event_time"].replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise InvalidEventError(
            "Invalid event_time"
        ) from exc