import logging

from kafka import KafkaProducer

from .config import KafkaProducerConfig
from .serializer import serialize_event

logger = logging.getLogger(__name__)


class HeartRateProducer:
    """
    Kafka producer responsible for sending heart-rate events.
    """

    def __init__(
        self,
        config: KafkaProducerConfig,
    ) -> None:

        self.config = config

        self.sent = 0

        self.failed = 0

        self.producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,

            client_id=config.client_id,

            # Convert the event into bytes.
            value_serializer=serialize_event,

            # Use customer_id as the Kafka key.
            key_serializer=lambda key: key.encode("utf-8"),

            # Production-oriented reliability settings.
            acks=config.acks,
            retries=config.retries,

            # Throughput/batching settings.
            linger_ms=config.linger_ms,
            batch_size=config.batch_size,
            compression_type=config.compression_type,
        )

    def send(self, event: dict) -> None:
        """
        Send a single heart-rate event to Kafka.

        customer_id is used as the Kafka key, so a customer's events
        always land on the same partition and stay ordered.
        """

        customer_id = event["customer_id"]

        future = self.producer.send(
            self.config.topic,
            key=customer_id,
            value=event,
        )

        self.sent += 1

        if not self.config.sync_send:
            # Delivery failures surface through the callback rather than
            # a blocking wait, which is what makes high rates reachable.
            future.add_errback(self._on_delivery_error)

            if self.sent % self.config.log_every == 0:
                logger.info(
                    "Sent %d events (async)",
                    self.sent,
                )

            return

        metadata = future.get(timeout=10)

        logger.info(
            "Event sent: event_id=%s customer_id=%s "
            "topic=%s partition=%s offset=%s",
            event["event_id"],
            customer_id,
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )

    def _on_delivery_error(self, exception: Exception) -> None:
        self.failed += 1

        logger.error(
            "Delivery failed (%d total): %s",
            self.failed,
            exception,
        )

    def flush(self) -> None:
        """
        Wait until buffered records have been sent.
        """

        self.producer.flush()

    def close(self) -> None:
        """
        Close the Kafka producer cleanly.
        """

        self.producer.close()

if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    config = KafkaProducerConfig()
    producer = HeartRateProducer(config)

    try:
        for line in sys.stdin:

            line = line.strip()

            if not line:
                continue

            event = json.loads(line)

            producer.send(event)

    except KeyboardInterrupt:
        logger.info("Producer interrupted.")

    finally:
        producer.close()