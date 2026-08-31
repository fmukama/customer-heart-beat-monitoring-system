import json

from psycopg import Connection

from .payload import Notification

INSERT_FIRING_SQL = """
INSERT INTO notifications (
    fingerprint,
    alert_type,
    severity,
    message,
    source,
    status,
    created_at,
    labels
)
VALUES (
    %(fingerprint)s,
    %(alert_type)s,
    %(severity)s,
    %(message)s,
    %(source)s,
    'FIRING',
    %(created_at)s,
    %(labels)s
)
ON CONFLICT (fingerprint) WHERE status = 'FIRING' DO NOTHING;
"""


RESOLVE_SQL = """
UPDATE notifications
SET status      = 'RESOLVED',
    resolved_at = NOW()
WHERE fingerprint = %(fingerprint)s
  AND status = 'FIRING';
"""


def record(
    connection: Connection,
    notification: Notification,
) -> bool:
    """
    Persist one alert transition.

    Returns True when a row was written or closed, False when the
    webhook was a duplicate Alertmanager retry.
    """

    if notification.status == "RESOLVED":
        return _resolve(connection, notification)

    return _open(connection, notification)


def _open(
    connection: Connection,
    notification: Notification,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            INSERT_FIRING_SQL,
            {
                "fingerprint": notification.fingerprint,
                "alert_type": notification.alert_type,
                "severity": notification.severity,
                "message": notification.message,
                "source": notification.source,
                "created_at": notification.created_at,
                "labels": json.dumps(notification.labels),
            },
        )

        written = cursor.rowcount == 1

    connection.commit()

    return written


def _resolve(
    connection: Connection,
    notification: Notification,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            RESOLVE_SQL,
            {"fingerprint": notification.fingerprint},
        )

        closed = cursor.rowcount >= 1

    connection.commit()

    return closed
