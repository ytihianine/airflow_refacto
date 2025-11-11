"""Factory for creating database handlers."""

from infra.database.base import BaseDBHandler
from infra.database.postgres import PostgresDBHandler
from infra.database.sqlite import SQLiteDBHandler

from utils.config.types import DatabaseType


def create_db_handler(
    connection_id: str, db_type: DatabaseType = DatabaseType.POSTGRES
) -> BaseDBHandler:
    """Create a database handler based on connection type.

    Args:
        connection_id: Airflow connection ID or database path for sqlite
        db_type: Type of database ('postgres', etc.)

    Returns:
        A database handler instance

    Raises:
        ValueError: If db_type is not supported
    """
    if db_type == DatabaseType.POSTGRES:
        return PostgresDBHandler(connection_id)
    elif db_type == DatabaseType.SQLITE:
        return SQLiteDBHandler(connection_id)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
