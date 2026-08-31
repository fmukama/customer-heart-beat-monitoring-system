from datetime import UTC, datetime

import pytest

from notifier.payload import (
    build_message,
    build_source,
    normalise_severity,
    parse_alerts,
    parse_timestamp,
    to_alert_type,
)


def alert(**overrides) -> dict:
    body = {
        "status": "firing",
        "fingerprint": "abc123",
        "labels": {
            "alertname": "ConsumerDown",
            "severity": "CRITICAL",
            "job": "heartbeat-consumer",
            "instance": "172.18.0.7:8000",
        },
        "annotations": {
            "summary": "Heartbeat consumer is down",
            "description": "No healthy instance for over a minute.",
        },
        "startsAt": "2026-08-31T14:32:10.000Z",
    }

    body.update(overrides)

    return body


def webhook(*alerts) -> dict:
    return {"version": "4", "status": "firing", "alerts": list(alerts)}


@pytest.mark.parametrize(
    ("alertname", "expected"),
    [
        ("ConsumerDown", "CONSUMER_DOWN"),
        ("KafkaLagHigh", "KAFKA_LAG_HIGH"),
        ("PostgresUnavailable", "POSTGRES_UNAVAILABLE"),
        ("ProcessingLatencyHigh", "PROCESSING_LATENCY_HIGH"),
        ("WatermarkStuck", "WATERMARK_STUCK"),
        # Runs of capitals must stay together, not become D_L_Q.
        ("DLQRateHigh", "DLQ_RATE_HIGH"),
        ("", "UNKNOWN"),
    ],
)
def test_alert_type_conversion(alertname, expected):
    assert to_alert_type(alertname) == expected


def test_parses_a_firing_alert():
    [notification] = parse_alerts(webhook(alert()))

    assert notification.alert_type == "CONSUMER_DOWN"
    assert notification.severity == "CRITICAL"
    assert notification.status == "FIRING"
    assert notification.fingerprint == "abc123"
    assert notification.message == "Heartbeat consumer is down"
    assert notification.source == "172.18.0.7:8000"


def test_parses_a_resolved_alert():
    [notification] = parse_alerts(
        webhook(alert(status="resolved"))
    )

    assert notification.status == "RESOLVED"
    assert notification.fingerprint == "abc123"


def test_parses_several_alerts_in_one_payload():
    # Alertmanager groups alerts, so one POST can carry many.
    notifications = parse_alerts(
        webhook(
            alert(),
            alert(
                fingerprint="def456",
                labels={
                    "alertname": "DLQRateHigh",
                    "severity": "WARNING",
                },
            ),
        )
    )

    assert [n.alert_type for n in notifications] == [
        "CONSUMER_DOWN",
        "DLQ_RATE_HIGH",
    ]


def test_empty_payload_yields_nothing():
    assert parse_alerts({}) == []
    assert parse_alerts({"alerts": []}) == []
    assert parse_alerts({"alerts": "not-a-list"}) == []


def test_non_dict_alerts_are_skipped():
    notifications = parse_alerts(
        {"alerts": ["nonsense", alert()]}
    )

    assert len(notifications) == 1


def test_unknown_severity_falls_back_to_info():
    assert normalise_severity("bogus") == "INFO"
    assert normalise_severity(None) == "INFO"
    assert normalise_severity("critical") == "CRITICAL"


def test_message_falls_back_through_annotations():
    assert build_message(alert()) == "Heartbeat consumer is down"

    assert (
        build_message(alert(annotations={"description": "only this"}))
        == "only this"
    )

    # No annotations at all: still produce something useful.
    assert "ConsumerDown" in build_message(alert(annotations={}))


def test_source_falls_back_to_job():
    assert (
        build_source(
            alert(
                labels={
                    "alertname": "X",
                    "job": "heartbeat-consumer",
                }
            )
        )
        == "heartbeat-consumer"
    )

    assert build_source(alert(labels={"alertname": "X"})) is None


def test_timestamp_parsing():
    parsed = parse_timestamp("2026-08-31T14:32:10+00:00")

    assert parsed == datetime(2026, 8, 31, 14, 32, 10, tzinfo=UTC)


def test_unparseable_timestamp_falls_back_to_now():
    # A slightly wrong timestamp beats dropping the alert.
    assert parse_timestamp("not-a-date").tzinfo is not None
    assert parse_timestamp(None).tzinfo is not None


def test_missing_fingerprint_is_derived_deterministically():
    payload = alert()

    del payload["fingerprint"]

    first = parse_alerts(webhook(payload))[0].fingerprint
    second = parse_alerts(webhook(payload))[0].fingerprint

    # Must be stable, or a resolved webhook could not close its
    # firing row after the notifier restarts.
    assert first == second
    assert first
