import json
from datetime import datetime, timezone

from kafka import KafkaProducer


DLQ_TOPIC = "heart-rate-events-dlq"


class DeadLetterProducer:

    def __init__(self, bootstrap_servers: str) -> None:

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,

            value_serializer=lambda value: json.dumps(
                value
            ).encode("utf-8"),

            key_serializer=lambda key: key.encode("utf-8"),
        )

    def send(
        self,
        event: dict,
        error_type: str,
        error_message: str,
        source_topic: str,
        source_partition: int,
        source_offset: int,
    ) -> None:

        dlq_event = {
            "original_event": event,

            "error_type": error_type,

            "error_message": error_message,

            "source_topic": source_topic,

            "source_partition": source_partition,

            "source_offset": source_offset,

            "failed_at": datetime.now(
                timezone.utc
            ).isoformat(),
        }

        customer_id = event.get(
            "customer_id",
            "unknown",
        )

        self.producer.send(
            DLQ_TOPIC,
            key=customer_id,
            value=dlq_event,
        )

        self.producer.flush()

    def close(self) -> None:
        self.producer.close()