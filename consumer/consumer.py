import json
import logging

from kafka import KafkaConsumer

from .anomaly import classify_heart_rate
from .config import ConsumerConfig
from .database import create_connection
from .dlq import DeadLetterProducer
from .errors import (
    DLQPublishError,
    PermanentEventError,
)
from .repository import insert_event
from .retry import retry_with_backoff
from .validation import validate_event

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

    def process_message(self, message) -> None:

        event = message.value

        try:

            # --------------------------------
            # 1. Validate
            # --------------------------------

            validate_event(event)

            # --------------------------------
            # 2. Classify
            # --------------------------------

            event["status"] = classify_heart_rate(
                event["heart_rate"]
            )

            # --------------------------------
            # 3. Write to PostgreSQL
            # --------------------------------

            retry_with_backoff(
                lambda: insert_event(
                    self.connection,
                    event,
                ),
                max_attempts=self.config.max_retry_attempts,
            )

            # --------------------------------
            # 4. Commit Kafka offset
            # --------------------------------

            self.consumer.commit()

            logger.info(
                "Processed event=%s",
                event["event_id"],
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

    def run(self) -> None:

        logger.info(
            "Starting consumer: topic=%s group=%s",
            self.config.topic,
            self.config.group_id,
        )

        try:

            for message in self.consumer:

                self.process_message(message)

        finally:

            self.close()

    def close(self) -> None:

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