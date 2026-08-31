import logging
import threading

from . import metrics
from .config import NotifierConfig
from .store import NotificationStore

logger = logging.getLogger(__name__)


class PostgresProbe(threading.Thread):
    """
    Independent PostgreSQL health signal.

    Deliberately not sourced from the consumer: if the consumer is down
    its metrics vanish, and a real database outage would be masked by
    ConsumerDown instead of reported on its own.
    """

    def __init__(
        self,
        config: NotifierConfig,
        store: NotificationStore,
    ) -> None:
        super().__init__(daemon=True, name="postgres-probe")

        self.config = config

        self.store = store

        self.stopped = threading.Event()

    def check(self) -> bool:
        try:
            with self.store.connect() as connection:
                connection.execute("SELECT 1")

            return True

        except Exception:
            logger.debug(
                "PostgreSQL probe failed.",
                exc_info=True,
            )

            return False

    def run(self) -> None:
        healthy_before = None

        while not self.stopped.wait(0):
            healthy = self.check()

            metrics.postgres_up.set(1 if healthy else 0)

            if healthy != healthy_before:
                logger.warning(
                    "PostgreSQL is %s.",
                    "reachable" if healthy else "UNREACHABLE",
                )

                healthy_before = healthy

            if healthy:
                self.store.drain()

            if self.stopped.wait(
                self.config.probe_interval_seconds
            ):
                return

    def stop(self) -> None:
        self.stopped.set()
