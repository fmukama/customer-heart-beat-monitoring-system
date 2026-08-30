import json
import logging

from kafka import KafkaConsumer

from .anomaly import classify_heart_rate
from .config import ConsumerConfig
from .database import create_connection
from .repository import insert_event
from .validation import InvalidEventError, validate_event


logger = logging.getLogger(__name__)


class HeartRateConsumer:
    """
    Consume heart-rate events from Kafka
    and persist them into PostgreSQL.
    """

    def __init__(self, config: ConsumerConfig) -> None:
        self.config = config

        self.consumer = KafkaConsumer(
            config.topic,

            bootstrap_servers=config.bootstrap_servers,

            group_id=config.group_id,

            auto_offset_reset=config.auto_offset_reset,

            enable_auto_commit=config.enable_auto_commit,

            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
        )

        self.connection = create_connection(config)

    def process_message(self, message) -> None:
        """
        Process a single Kafka message.
        """

        event = message.value

        try:
            validate_event(event)

            event["status"] = classify_heart_rate(
                event["heart_rate"]
            )

            inserted = insert_event(
                self.connection,
                event,
            )

            if inserted:
                logger.info(
                    "Stored event_id=%s customer=%s "
                    "heart_rate=%s status=%s",
                    event["event_id"],
                    event["customer_id"],
                    event["heart_rate"],
                    event["status"],
                )
            else:
                logger.warning(
                    "Duplicate event ignored: event_id=%s",
                    event["event_id"],
                )

            # Commit only after PostgreSQL processing succeeds.
            self.consumer.commit()

        except InvalidEventError as exc:
            logger.error(
                "Invalid event: %s",
                exc,
            )

            # For now, we commit invalid messages so that
            # one bad event does not block the partition forever.
            #
            # Later we will introduce a Dead Letter Topic.
            self.consumer.commit()

        except Exception:
            logger.exception(
                "Failed to process Kafka message."
            )

            # Do NOT commit the offset.
            #
            # Kafka can redeliver the event after restart.
            raise

    def run(self) -> None:
        """
        Continuously consume Kafka messages.
        """

        logger.info(
            "Starting consumer. topic=%s group=%s",
            self.config.topic,
            self.config.group_id,
        )

        try:
            for message in self.consumer:
                self.process_message(message)

        finally:
            self.close()

    def close(self) -> None:
        """
        Cleanly close Kafka and PostgreSQL connections.
        """

        self.consumer.close()
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
        logger.info("Consumer interrupted.")


if __name__ == "__main__":
    main()