"""
Persistence with a buffer for the case that matters most.

If PostgreSQL is down, the notifier cannot write the
PostgresUnavailable alert — the recorder needs the thing that is
broken. So every alert is logged unconditionally, and failed writes are
held in memory and retried on the probe tick. The row then appears late
rather than never.
"""

import logging
import threading
from collections import deque

import psycopg

from . import metrics
from .config import NotifierConfig
from .payload import Notification
from .repository import record

logger = logging.getLogger(__name__)

# Bounded, so a long outage cannot exhaust memory. Oldest is dropped
# first, and the drop is logged.
MAX_PENDING = 1000


class NotificationStore:

    def __init__(self, config: NotifierConfig) -> None:
        self.config = config

        self.pending: deque[Notification] = deque(
            maxlen=MAX_PENDING
        )

        self.lock = threading.Lock()

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(
            host=self.config.postgres_host,
            port=self.config.postgres_port,
            dbname=self.config.postgres_db,
            user=self.config.postgres_user,
            password=self.config.postgres_password,
            connect_timeout=5,
        )

    def save(self, notification: Notification) -> None:
        """
        Persist now, or buffer for the probe loop to retry.
        """

        # Logged before any database attempt, so the record exists even
        # if every later step fails.
        logger.warning(
            "ALERT %s %s [%s] %s",
            notification.status,
            notification.severity,
            notification.alert_type,
            notification.message,
        )

        try:
            with self.connect() as connection:
                written = record(connection, notification)

            if written:
                metrics.alerts_persisted.inc()
            else:
                metrics.duplicates_ignored.inc()

        except Exception:
            metrics.write_failures.inc()

            logger.exception(
                "Could not persist alert %s; buffering.",
                notification.alert_type,
            )

            self._buffer(notification)

    def _buffer(self, notification: Notification) -> None:
        with self.lock:
            if len(self.pending) == self.pending.maxlen:
                logger.error(
                    "Pending buffer full; dropping oldest alert."
                )

            self.pending.append(notification)

            metrics.pending_writes.set(len(self.pending))

    def drain(self) -> int:
        """
        Retry buffered writes. Called from the probe loop once
        PostgreSQL is reachable again.
        """

        with self.lock:
            queued = list(self.pending)

            self.pending.clear()

        if not queued:
            return 0

        logger.info(
            "Draining %d buffered alert(s).",
            len(queued),
        )

        drained = 0

        for index, notification in enumerate(queued):
            try:
                with self.connect() as connection:
                    if record(connection, notification):
                        metrics.alerts_persisted.inc()
                    else:
                        metrics.duplicates_ignored.inc()

                drained += 1

            except Exception:
                metrics.write_failures.inc()

                logger.warning(
                    "Drain still failing; re-buffering.",
                    exc_info=True,
                )

                # Still unreachable. Re-buffer this one and everything
                # after it, otherwise the rest of the batch is lost.
                for remaining in queued[index:]:
                    self._buffer(remaining)

                break

        with self.lock:
            metrics.pending_writes.set(len(self.pending))

        return drained
