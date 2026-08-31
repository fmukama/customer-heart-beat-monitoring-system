"""
Integration fixtures.

These tests run inside the `dev` service, which shares
heartbeat_network, so Kafka and PostgreSQL are reachable by service
name. They exercise the real consumer against real infrastructure.

Isolation matters here: the compose stack's own consumer is already
reading heart-rate-events and writing to the public schema. Tests
therefore get their own topics and their own PostgreSQL schema, so a
test run can never corrupt the running pipeline's aggregates and the
running pipeline can never skew a test assertion.
"""

import json
import logging
import os
import uuid
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import UnknownTopicOrPartitionError

from consumer.config import ConsumerConfig
from consumer.consumer import HeartRateConsumer
from consumer.repository import load_open_windows

DATABASE_DIR = Path(__file__).resolve().parents[2] / "database"

TEST_SCHEMA = "integration_test"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: requires a running docker compose stack",
    )


@pytest.fixture(scope="session")
def bootstrap_servers() -> str:
    return os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:29092",
    )


@pytest.fixture(scope="session")
def base_config() -> ConsumerConfig:
    return replace(
        ConsumerConfig(),
        bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:29092",
        ),
        postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
    )


def connect(config: ConsumerConfig) -> psycopg.Connection:
    connection = psycopg.connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )

    connection.autocommit = True

    return connection


@pytest.fixture(scope="session", autouse=True)
def test_schema(base_config):
    """
    Build the production DDL inside a throwaway schema.
    """

    with connect(base_config) as connection:
        connection.execute(
            f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE"
        )

        connection.execute(
            f"CREATE SCHEMA {TEST_SCHEMA}"
        )

        connection.execute(
            f"SET search_path TO {TEST_SCHEMA}"
        )

        for path in sorted(DATABASE_DIR.glob("*.sql")):
            connection.execute(
                path.read_text(encoding="utf-8")
            )

    yield TEST_SCHEMA

    with connect(base_config) as connection:
        connection.execute(
            f"DROP SCHEMA IF EXISTS {TEST_SCHEMA} CASCADE"
        )


@pytest.fixture
def db(base_config, test_schema):
    """
    Connection scoped to the test schema, for assertions.
    """

    with connect(base_config) as connection:
        connection.execute(
            f"SET search_path TO {test_schema}"
        )

        yield connection


@pytest.fixture(scope="session")
def admin(bootstrap_servers):
    client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
    )

    yield client

    client.close()


@pytest.fixture
def topics(admin, bootstrap_servers):
    """
    Dedicated topics per test, so the running consumer never competes
    for these messages.
    """

    suffix = uuid.uuid4().hex[:8]

    main = f"itest-events-{suffix}"
    dlq = f"itest-events-dlq-{suffix}"

    admin.create_topics(
        [
            NewTopic(main, num_partitions=1, replication_factor=1),
            NewTopic(dlq, num_partitions=1, replication_factor=1),
        ]
    )

    yield main, dlq

    try:
        admin.delete_topics([main, dlq])
    except UnknownTopicOrPartitionError:
        pass


@pytest.fixture
def publish(bootstrap_servers, topics):
    main, _ = topics

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode(
            "utf-8"
        ),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
    )

    def send(event: dict) -> None:
        producer.send(
            main,
            key=event.get("customer_id", "unknown"),
            value=event,
        ).get(timeout=15)

    yield send

    producer.flush()
    producer.close()


@pytest.fixture
def build_consumer(base_config, topics, test_schema):
    """
    A real HeartRateConsumer bound to the test topics and schema.
    """

    created = []

    def build(**overrides) -> HeartRateConsumer:
        main, dlq = topics

        config = replace(
            base_config,
            topic=main,
            dlq_topic=dlq,
            group_id=f"itest-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
            window_flush_interval_seconds=0,
            **overrides,
        )

        subject = HeartRateConsumer(config)

        # Redirect every write away from the live pipeline's tables.
        subject.connection.execute(
            f"SET search_path TO {test_schema}"
        )

        # __init__ rehydrated against the default search_path, i.e. the
        # live pipeline's windows. Discard those and reload from the
        # test schema instead.
        subject.aggregator.windows.clear()

        subject.aggregator.rehydrate(
            load_open_windows(subject.connection)
        )

        created.append(subject)

        return subject

    yield build

    for subject in created:
        # Teardown must never mask the test's own result, but a failure
        # here still matters when diagnosing a flaky run.
        try:
            subject.close()
        except Exception:
            logging.getLogger(__name__).warning(
                "Consumer teardown failed.",
                exc_info=True,
            )
