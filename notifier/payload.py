"""
Turning Alertmanager webhook bodies into notification records.

Pure functions only, so this unit-tests without a server or a database.
"""

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime

VALID_SEVERITIES = {"CRITICAL", "WARNING", "INFO"}


@dataclass(frozen=True)
class Notification:
    fingerprint: str

    alert_type: str

    severity: str

    message: str

    source: str | None

    status: str

    created_at: datetime

    labels: dict


def to_alert_type(alertname: str) -> str:
    """
    ConsumerDown -> CONSUMER_DOWN, DLQRateHigh -> DLQ_RATE_HIGH.

    Runs of capitals stay together, so DLQ does not become D_L_Q.
    """

    if not alertname:
        return "UNKNOWN"

    # Split before a capital that starts a word: DLQRateHigh -> DLQ_RateHigh
    spaced = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", alertname)

    # Then split any remaining lower-to-upper boundary.
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", spaced)

    return re.sub(r"_+", "_", spaced).upper()


def parse_timestamp(raw: str | None) -> datetime:
    """
    Alertmanager sends RFC-3339. Fall back to now if it is missing or
    unparseable — a slightly wrong timestamp beats a dropped alert.
    """

    if not raw:
        return datetime.now(UTC)

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return datetime.now(UTC)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)

    return parsed


def normalise_severity(raw: str | None) -> str:
    candidate = (raw or "").strip().upper()

    return candidate if candidate in VALID_SEVERITIES else "INFO"


def build_message(alert: dict) -> str:
    annotations = alert.get("annotations") or {}

    for key in ("summary", "description", "message"):
        value = (annotations.get(key) or "").strip()

        if value:
            return value

    labels = alert.get("labels") or {}

    return f"{labels.get('alertname', 'Alert')} fired"


def build_source(alert: dict) -> str | None:
    labels = alert.get("labels") or {}

    for key in ("instance", "job", "service"):
        value = (labels.get(key) or "").strip()

        if value:
            return value

    return None


def parse_alerts(body: dict) -> list[Notification]:
    """
    Extract every alert in one webhook body.

    Alertmanager groups alerts, so a single POST can carry several.
    """

    alerts = body.get("alerts") or []

    if not isinstance(alerts, list):
        return []

    notifications = []

    for alert in alerts:
        if not isinstance(alert, dict):
            continue

        labels = alert.get("labels") or {}

        status = (
            "RESOLVED"
            if (alert.get("status") or "").lower() == "resolved"
            else "FIRING"
        )

        notifications.append(
            Notification(
                fingerprint=(
                    alert.get("fingerprint")
                    or _synthetic_fingerprint(labels)
                ),
                alert_type=to_alert_type(
                    labels.get("alertname", "")
                ),
                severity=normalise_severity(
                    labels.get("severity")
                ),
                message=build_message(alert),
                source=build_source(alert),
                status=status,
                created_at=parse_timestamp(alert.get("startsAt")),
                labels=labels,
            )
        )

    return notifications


def _synthetic_fingerprint(labels: dict) -> str:
    """
    Alertmanager always sends a fingerprint. This only covers
    hand-crafted payloads, so correlation still works in a demo.

    A real digest rather than hash(), which is salted per process and
    would stop a resolved webhook matching its firing row after the
    notifier restarts.
    """

    joined = ",".join(
        f"{key}={labels[key]}" for key in sorted(labels)
    )

    return hashlib.sha256(
        joined.encode("utf-8")
    ).hexdigest()[:16]
