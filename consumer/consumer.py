import json
import logging
import time
from datetime import timedelta

from kafka import KafkaConsumer

from .aggregator import DailyAggregator
from .anomaly import classify_heart_rate
from .config import ConsumerConfig
from .database import create_connection
from .dlq import DeadLetterProducer
from .errors import (
    DLQPublishError,
    PermanentEventError,
)
from .repository import (
    insert_event,
    load_open_windows,
    upsert_daily_aggregate,
)
from .retry import retry_with_backoff
from .validation import parse_event_time, validate_event

logger = logging.getLogger(__name__)


class HeartRateConsumer:

    def __init__(
        self,
        config: ConsumerConfig,
    ) -> None:

        self.config = config

        self.consumer = KafkaConsumer(
            config.topic,

            bootstrap_servers=config.bootstrap_servers,

            group_id=config.group_id,

            auto_offset_reset=config.auto_offset_reset,

            enable_auto_commit=False,

            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
        )

        self.connection = create_connection(
            config
        )

        self.dlq_producer = DeadLetterProducer(
            bootstrap_servers=config.bootstrap_servers,
            topic=config.dlq_topic,
        )

        self.aggregator = DailyAggregator(
            allowed_out_of_orderness=timedelta(
                seconds=config.allowed_out_of_orderness_seconds,
            ),
            allowed_lateness=timedelta(
                seconds=config.allowed_lateness_seconds,
            ),
        )

        restored = self.aggregator.rehydrate(
            load_open_windows(self.connection)
        )

        if restored:
            logger.info(
                "Restored %d open window(s) from PostgreSQL.",
                restored,
            )

        self.last_flush = time.monotonic()

    def process_message(self, message) -> None:

        event = message.value

        try:
            validate_event(event)

            event_time = parse_event_time(
                event["event_time"]
            )

            status = classify_heart_rate(
                event["heart_rate"]
            )

            event["status"] = status

            outcome = self.aggregator.add_event(
                customer_id=event["customer_id"],
                heart_rate=event["heart_rate"],
                event_time=event_time,
                is_abnormal=status == "ABNORMAL",
            )

            event["is_late"] = outcome.is_late

            event["lateness_seconds"] = (
                outcome.lateness_seconds
                if outcome.is_late
                else None
            )

            retry_with_backoff(
                lambda: insert_event(
                    self.connection,
                    event,
                ),
                max_attempts=self.config.max_retry_attempts,
            )

            self.consumer.commit()

            if outcome.is_late:
                logger.info(
                    "Late event: event=%s lateness=%.1fs aggregated=%s",
                    event["event_id"],
                    outcome.lateness_seconds,
                    outcome.aggregated,
                )

        except PermanentEventError as exc:

            logger.warning(
                "Permanent event error: event=%s error=%s",
                event.get("event_id"),
                exc,
            )

            # Send to DLQ.
            #
            # If DLQ publication fails,
            # the offset is NOT committed.
            self.dlq_producer.send(
                event=event,
                error_type="VALIDATION_ERROR",
                error_message=str(exc),
                source_topic=message.topic,
                source_partition=message.partition,
                source_offset=message.offset,
            )

            # Only acknowledge Kafka after
            # the DLQ successfully receives it.
            self.consumer.commit()

        except DLQPublishError:

            logger.exception(
                "DLQ publication failed. "
                "Offset will not be committed."
            )

            raise

        except Exception:

            logger.exception(
                "Temporary processing failure. "
                "Offset will not be committed."
            )

            raise

    def flush_windows(self, force: bool = False) -> None:
        """
        Persist finalized windows, plus a snapshot of open ones so the
        current day is queryable before it closes.
        """

        elapsed = time.monotonic() - self.last_flush

        if (
            not force
            and elapsed < self.config.window_flush_interval_seconds
        ):
            return

        self.last_flush = time.monotonic()

        finalized = self.aggregator.finalize_ready_windows()

        for aggregate in finalized:
            retry_with_backoff(
                lambda aggregate=aggregate: upsert_daily_aggregate(
                    self.connection,
                    aggregate,
                    is_finalized=True,
                ),
                max_attempts=self.config.max_retry_attempts,
            )

            logger.info(
                "Window finalized: customer=%s window_start=%s events=%d",
                aggregate.customer_id,
                aggregate.window_start.isoformat(),
                aggregate.event_count,
            )

        for aggregate in self.aggregator.snapshot_open_windows():
            retry_with_backoff(
                lambda aggregate=aggregate: upsert_daily_aggregate(
                    self.connection,
                    aggregate,
                    is_finalized=False,
                ),
                max_attempts=self.config.max_retry_attempts,
            )

    def run(self) -> None:

        logger.info(
            "Starting consumer: topic=%s group=%s "
            "out_of_orderness=%ds lateness=%ds",
            self.config.topic,
            self.config.group_id,
            self.config.allowed_out_of_orderness_seconds,
            self.config.allowed_lateness_seconds,
        )

        try:

            # Polled rather than iterated: the blocking iterator only
            # returns on message arrival, so a quiet stream would never
            # flush and windows would never finalize.
            while True:

                batches = self.consumer.poll(
                    timeout_ms=self.config.poll_timeout_ms,
                )

                for messages in batches.values():
                    for message in messages:
                        self.process_message(message)

                self.flush_windows()

        finally:

            self.close()

    def close(self) -> None:

        try:
            self.flush_windows(force=True)
        except Exception:
            logger.exception(
                "Failed to flush windows during shutdown."
            )

        self.consumer.close()

        self.dlq_producer.close()

        self.connection.close()


def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(name)s - "
            "%(message)s"
        ),
    )

    config = ConsumerConfig()

    consumer = HeartRateConsumer(config)

    try:

        consumer.run()

    except KeyboardInterrupt:

        logger.info(
            "Consumer interrupted."
        )


if __name__ == "__main__":
    main()
