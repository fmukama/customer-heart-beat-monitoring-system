import json
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from uuid import UUID

import jsonschema

from .errors import PermanentEventError

SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent
    / "schemas"
    / "heart_rate_event.json"
)


class InvalidEventError(PermanentEventError):
    """Raised when a heart-rate event is invalid."""


@lru_cache(maxsize=1)
def _validator() -> jsonschema.Draft202012Validator:
    schema = json.loads(
        SCHEMA_PATH.read_text(encoding="utf-8")
    )

    return jsonschema.Draft202012Validator(schema)


def validate_event(event: dict) -> None:
    """
    Validate a heart-rate event against schemas/heart_rate_event.json.

    The schema is the single source of truth for structure, types and
    ranges. Formats the schema declares but cannot enforce on its own
    (uuid, date-time) are checked afterwards.
    """

    errors = sorted(
        _validator().iter_errors(event),
        key=lambda error: list(error.path),
    )

    if errors:
        raise InvalidEventError(
            "; ".join(
                f"{'.'.join(str(part) for part in error.path) or 'event'}: "
                f"{error.message}"
                for error in errors
            )
        )

    try:
        UUID(event["event_id"])
    except (ValueError, AttributeError, TypeError) as exc:
        raise InvalidEventError(
            "Invalid event_id"
        ) from exc

    try:
        parse_event_time(event["event_time"])
    except ValueError as exc:
        raise InvalidEventError(
            "Invalid event_time"
        ) from exc


def parse_event_time(raw: str) -> datetime:
    """
    Parse an ISO-8601 event_time into an aware UTC datetime.
    """

    parsed = datetime.fromisoformat(raw)

    if parsed.tzinfo is None:
        raise ValueError(
            "event_time must include a timezone offset"
        )

    return parsed
