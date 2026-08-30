from psycopg import Connection, connect

from .config import ConsumerConfig


def create_connection(config: ConsumerConfig) -> Connection:
    """
    Create a PostgreSQL connection.
    """

    return connect(
        host=config.postgres_host,
        port=config.postgres_port,
        dbname=config.postgres_db,
        user=config.postgres_user,
        password=config.postgres_password,
    )