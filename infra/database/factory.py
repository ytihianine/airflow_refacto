"""Factory for creating database handlers."""

from typing import Optional

from airflow.providers.postgres.hooks.postgres import PostgresHook

from .base import BaseDBHandler
from .postgres import PostgresDBHandler


def create_db_handler(connection_id: str, db_type: str = "postgres") -> BaseDBHandler:
    """Create a database handler based on connection type.

    Args:
        connection_id: Airflow connection ID
        db_type: Type of database ('postgres', etc.)

    Returns:
        A database handler instance

    Raises:
        ValueError: If db_type is not supported
    """
    if db_type == "postgres":
        return PostgresDBHandler(connection_id)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def get_default_handler(db_type: str = "postgres") -> BaseDBHandler:
    """Get a database handler with default connection ID.

    Args:
        db_type: Type of database ('postgres', etc.)

    Returns:
        A database handler instance
    """
    default_connections = {"postgres": "db_depose_fichier"}

    connection_id = default_connections.get(db_type)
    if not connection_id:
        raise ValueError(f"No default connection defined for database type: {db_type}")

    return create_db_handler(connection_id, db_type)
