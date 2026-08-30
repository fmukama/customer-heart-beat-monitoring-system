import json


def serialize_event(event: dict) -> bytes:
    """
    Convert a Python event dictionary into UTF-8 encoded JSON.
    """

    return json.dumps(
        event,
        separators=(",", ":"),
    ).encode("utf-8")