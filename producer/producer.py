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

        self.producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,

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

        customer_id is used as the Kafka key.
        """

        customer_id = event["customer_id"]

        future = self.producer.send(
            self.config.topic,
            key=customer_id,
            value=event,
        )

        # For now, wait for Kafka's response.
        # Later, we'll explore asynchronous delivery
        # when performing high-throughput experiments.
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