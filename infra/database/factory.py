"""Factory for creating database handlers."""

from infra.database.base import BaseDBHandler
from infra.database.postgres import PostgresDBHandler
from infra.database.sqlite import SQLiteDBHandler


def create_db_handler(connection_id: str, db_type: str = "postgres") -> BaseDBHandler:
    """Create a database handler based on connection type.

    Args:
        connection_id: Airflow connection ID or database path for sqlite
        db_type: Type of database ('postgres', etc.)

    Returns:
        A database handler instance

    Raises:
        ValueError: If db_type is not supported
    """
    if db_type == "postgres":
        return PostgresDBHandler(connection_id)
    elif db_type == "sqlite":
        return SQLiteDBHandler(connection_id)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
