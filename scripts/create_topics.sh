#!/usr/bin/env bash

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-heartbeat-kafka}"
TOPIC="${KAFKA_HEART_RATE_TOPIC:-heart-rate-events}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"

echo "Creating Kafka topic..."
echo "  Topic:              ${TOPIC}"
echo "  Partitions:         ${PARTITIONS}"
echo "  Replication factor: ${REPLICATION_FACTOR}"

docker exec "${KAFKA_CONTAINER}" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"

echo
echo "Topic created successfully."

docker exec "${KAFKA_CONTAINER}" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --topic "${TOPIC}"