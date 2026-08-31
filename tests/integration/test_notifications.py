"""
Alert notification path against real infrastructure.

Two layers are covered separately:

* the persistence layer runs in-process against the isolated test
  schema, so idempotency and the firing/resolved lifecycle are asserted
  without touching live data
* the HTTP layer posts at the running notifier, which necessarily
  writes to the public schema. Those tests use unique fingerprints and
  delete their own rows afterwards.
"""

import json
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime

import pytest

from notifier.payload import parse_alerts
from notifier.repository import record

pytestmark = pytest.mark.integration

NOTIFIER = "http://notifier:9091"


def webhook(
    fingerprint: str,
    alertname: str = "ConsumerDown",
    severity: str = "CRITICAL",
    status: str = "firing",
) -> dict:
    return {
        "version": "4",
        "status": status,
        "alerts": [
            {
                "status": status,
                "fingerprint": fingerprint,
                "labels": {
                    "alertname": alertname,
                    "severity": severity,
                    "job": "heartbeat-consumer",
                },
                "annotations": {
                    "summary": f"{alertname} test alert",
                },
                "startsAt": datetime.now(UTC).isoformat(),
            }
        ],
    }


def rows_for(connection, fingerprint: str) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT alert_type, severity, message, source,
                   status, resolved_at, labels
            FROM notifications
            WHERE fingerprint = %s
            """,
            (fingerprint,),
        )

        columns = [d[0] for d in cursor.description]

        return [
            dict(zip(columns, row, strict=True))
            for row in cursor.fetchall()
        ]


# ---------------------------------------------------------------
# Persistence layer, isolated schema
# ---------------------------------------------------------------


@pytest.fixture
def fingerprint() -> str:
    return uuid.uuid4().hex[:16]


def test_firing_alert_is_recorded(db, fingerprint):
    [notification] = parse_alerts(webhook(fingerprint))

    assert record(db, notification) is True

    [row] = rows_for(db, fingerprint)

    assert row["alert_type"] == "CONSUMER_DOWN"
    assert row["severity"] == "CRITICAL"
    assert row["status"] == "FIRING"
    assert row["resolved_at"] is None
    assert row["source"] == "heartbeat-consumer"
    assert row["labels"]["alertname"] == "ConsumerDown"


def test_repeated_firing_webhook_does_not_duplicate(db, fingerprint):
    # Alertmanager retries delivery, so this must be idempotent.
    [notification] = parse_alerts(webhook(fingerprint))

    assert record(db, notification) is True
    assert record(db, notification) is False

    assert len(rows_for(db, fingerprint)) == 1


def test_resolved_webhook_closes_the_open_row(db, fingerprint):
    [firing] = parse_alerts(webhook(fingerprint))

    record(db, firing)

    [resolved] = parse_alerts(
        webhook(fingerprint, status="resolved")
    )

    assert record(db, resolved) is True

    [row] = rows_for(db, fingerprint)

    assert row["status"] == "RESOLVED"
    assert row["resolved_at"] is not None


def test_resolving_an_unknown_alert_is_harmless(db, fingerprint):
    [resolved] = parse_alerts(
        webhook(fingerprint, status="resolved")
    )

    assert record(db, resolved) is False
    assert rows_for(db, fingerprint) == []


def test_same_alert_can_fire_again_after_resolving(db, fingerprint):
    # The partial index only constrains rows still FIRING, so a
    # recurring incident becomes a second row rather than being lost.
    [firing] = parse_alerts(webhook(fingerprint))
    [resolved] = parse_alerts(
        webhook(fingerprint, status="resolved")
    )

    record(db, firing)
    record(db, resolved)
    record(db, firing)

    rows = rows_for(db, fingerprint)

    assert len(rows) == 2
    assert {row["status"] for row in rows} == {
        "FIRING",
        "RESOLVED",
    }


def test_severity_and_type_conversion_for_each_alert(db):
    expected = {
        "ConsumerDown": "CONSUMER_DOWN",
        "KafkaLagHigh": "KAFKA_LAG_HIGH",
        "PostgresUnavailable": "POSTGRES_UNAVAILABLE",
        "DLQRateHigh": "DLQ_RATE_HIGH",
        "ProcessingLatencyHigh": "PROCESSING_LATENCY_HIGH",
        "WatermarkStuck": "WATERMARK_STUCK",
    }

    for alertname, alert_type in expected.items():
        key = uuid.uuid4().hex[:16]

        [notification] = parse_alerts(
            webhook(key, alertname=alertname, severity="WARNING")
        )

        record(db, notification)

        assert rows_for(db, key)[0]["alert_type"] == alert_type


# ---------------------------------------------------------------
# HTTP layer, against the running notifier
# ---------------------------------------------------------------


def post(path: str, body: dict | None) -> tuple[int, dict]:
    data = (
        json.dumps(body).encode("utf-8")
        if body is not None
        else b""
    )

    request = urllib.request.Request(
        f"{NOTIFIER}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


@pytest.fixture
def live_fingerprint(base_config):
    """
    Unique key for a row in the public schema, removed afterwards.
    """

    key = f"itest-{uuid.uuid4().hex[:12]}"

    yield key

    import psycopg

    with psycopg.connect(
        host=base_config.postgres_host,
        port=base_config.postgres_port,
        dbname=base_config.postgres_db,
        user=base_config.postgres_user,
        password=base_config.postgres_password,
        autocommit=True,
    ) as connection:
        connection.execute(
            "DELETE FROM notifications WHERE fingerprint = %s",
            (key,),
        )


@pytest.fixture
def public_db(base_config):
    import psycopg

    with psycopg.connect(
        host=base_config.postgres_host,
        port=base_config.postgres_port,
        dbname=base_config.postgres_db,
        user=base_config.postgres_user,
        password=base_config.postgres_password,
        autocommit=True,
    ) as connection:
        yield connection


def test_notifier_health_endpoint():
    with urllib.request.urlopen(
        f"{NOTIFIER}/health", timeout=10
    ) as response:
        assert response.status == 200
        assert json.load(response)["status"] == "ok"


def test_notifier_exposes_postgres_health_metric():
    with urllib.request.urlopen(
        f"{NOTIFIER}/metrics", timeout=10
    ) as response:
        body = response.read().decode()

    # This gauge is what the PostgresUnavailable rule alerts on.
    assert "heartbeat_postgres_up 1.0" in body


def test_webhook_persists_and_resolves_end_to_end(
    live_fingerprint, public_db
):
    status, payload = post(
        "/alerts", webhook(live_fingerprint, alertname="DLQRateHigh")
    )

    assert status == 200
    assert payload["received"] == 1

    [row] = rows_for(public_db, live_fingerprint)

    assert row["alert_type"] == "DLQ_RATE_HIGH"
    assert row["status"] == "FIRING"

    status, _ = post(
        "/alerts",
        webhook(
            live_fingerprint,
            alertname="DLQRateHigh",
            status="resolved",
        ),
    )

    assert status == 200

    [row] = rows_for(public_db, live_fingerprint)

    assert row["status"] == "RESOLVED"
    assert row["resolved_at"] is not None


def test_webhook_rejects_malformed_body():
    request = urllib.request.Request(
        f"{NOTIFIER}/alerts",
        data=b"not json",
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10):
            pytest.fail("expected a 400")
    except urllib.error.HTTPError as error:
        assert error.code == 400


def test_webhook_accepts_a_payload_with_no_alerts():
    # Acknowledged rather than rejected, so Alertmanager does not retry
    # something it will never be able to deliver.
    status, payload = post("/alerts", {"alerts": []})

    assert status == 200
    assert payload["received"] == 0


def test_unknown_path_is_not_found():
    status, _ = post("/nope", {})

    assert status == 404
