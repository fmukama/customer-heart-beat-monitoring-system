import json
import logging
import time
from datetime import UTC, datetime, timedelta

from kafka import KafkaConsumer

from . import metrics
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
    load_window,
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
            window_loader=self.load_window_state,
        )

        self.last_flush = time.monotonic()

    def load_window_state(self, customer_id, window_start):
        return load_window(
            self.connection,
            customer_id,
            window_start,
        )

    def process_message(self, message) -> None:

        event = message.value

        started = time.perf_counter()

        try:
            validate_event(event)

            event_time = parse_event_time(
                event["event_time"]
            )

            status = classify_heart_rate(
                event["heart_rate"]
            )

            event["status"] = status

            # Watermark and lateness first: is_late is a column on the
            # raw row, so it must be known before the insert.
            lateness = self.aggregator.observe(event_time)

            event["is_late"] = lateness.is_late

            event["lateness_seconds"] = (
                lateness.lateness_seconds
                if lateness.is_late
                else None
            )

            inserted = retry_with_backoff(
                lambda: insert_event(
                    self.connection,
                    event,
                ),
                max_attempts=self.config.max_retry_attempts,
            )

            self.consumer.commit()

            metrics.messages_processed.inc()

            # Only fold genuinely new events into the window. A
            # redelivery is refused by ON CONFLICT, and counting it
            # anyway would inflate the aggregate above the raw count.
            aggregated = False

            if inserted:
                aggregated = self.aggregator.add_to_window(
                    customer_id=event["customer_id"],
                    heart_rate=event["heart_rate"],
                    event_time=event_time,
                    is_abnormal=status == "ABNORMAL",
                )

                if status == "ABNORMAL":
                    metrics.abnormal_events.inc()
            else:
                metrics.duplicates_ignored.inc()

            if lateness.is_late:
                metrics.late_events.labels(
                    aggregated=str(aggregated).lower(),
                ).inc()

                logger.info(
                    "Late event: event=%s lateness=%.1fs aggregated=%s",
                    event["event_id"],
                    lateness.lateness_seconds,
                    aggregated,
                )

            self.observe_watermark()

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

            metrics.dlq_messages.inc()

            metrics.messages_failed.labels(
                reason="validation",
            ).inc()

        except DLQPublishError:

            logger.exception(
                "DLQ publication failed. "
                "Offset will not be committed."
            )

            metrics.messages_failed.labels(
                reason="dlq_publish",
            ).inc()

            raise

        except Exception:

            logger.exception(
                "Temporary processing failure. "
                "Offset will not be committed."
            )

            metrics.messages_failed.labels(
                reason="temporary",
            ).inc()

            raise

        finally:

            metrics.processing_seconds.observe(
                time.perf_counter() - started
            )

    def observe_watermark(self) -> None:
        watermark = self.aggregator.watermark

        if watermark is not None:
            metrics.watermark_lag_seconds.set(
                (datetime.now(UTC) - watermark).total_seconds()
            )

        metrics.windows_open.set(
            len(self.aggregator.windows)
        )

    def observe_lag(self) -> None:
        """
        Report unconsumed messages per assigned partition.

        Costs one broker round trip, so it is called on the flush tick
        rather than per message.
        """

        assignment = self.consumer.assignment()

        if not assignment:
            return

        try:
            end_offsets = self.consumer.end_offsets(
                list(assignment)
            )
        except Exception:
            logger.debug(
                "Could not fetch end offsets for lag.",
                exc_info=True,
            )

            return

        for partition in assignment:
            try:
                position = self.consumer.position(partition)
            except Exception:
                # A partition with no position yet, e.g. mid-rebalance.
                logger.debug(
                    "No position for %s.",
                    partition,
                    exc_info=True,
                )

                continue

            if position is None:
                continue

            end = end_offsets.get(partition)

            if end is None:
                continue

            metrics.partition_lag.labels(
                topic=partition.topic,
                partition=str(partition.partition),
            ).set(max(0, end - position))

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

        self.observe_lag()

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

            metrics.windows_finalized.inc()

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

        metrics.serve(self.config.metrics_port)

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

                self.observe_watermark()

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
