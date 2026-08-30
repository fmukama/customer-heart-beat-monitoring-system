#!/usr/bin/env bash

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-heartbeat-kafka}"

echo "Creating heart-rate-events..."

docker exec "${KAFKA_CONTAINER}" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic heart-rate-events \
    --partitions 3 \
    --replication-factor 1

echo "Creating heart-rate-events-dlq..."

docker exec "${KAFKA_CONTAINER}" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic heart-rate-events-dlq \
    --partitions 3 \
    --replication-factor 1

echo
echo "Kafka topics:"
docker exec "${KAFKA_CONTAINER}" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list