"""Factory for creating database handlers."""

from dataclasses import dataclass
from pathlib import Path

from modules.constants import DEFAULT_PG_DATA_CONN_ID
from modules.enums.database import DatabaseType
from modules.infra.database.base import DBInterface
from modules.infra.database.postgres import PgAdapter
from modules.infra.database.sqlite import SQLiteAdapter
from modules.infra.database.trino import TrinoAdapter


@dataclass(frozen=True)
class DbConfig:
    """Dataclass to hold database configuration parameters."""

    # SQLite specific
    db_path: str | None = None
    # PostgreSQL specific
    connection_id: str = DEFAULT_PG_DATA_CONN_ID
    # Trino specific
    host: str = ""
    user: str = ""
    catalog: str = ""
    port: int = 443
    schema: str = ""
    http_scheme: str = "https"
    verify: bool = True


def _create_sqlite_adapter(db_config: DbConfig) -> SQLiteAdapter:
    """Create a SQLite adapter."""
    if db_config.db_path is None:
        raise ValueError("db_path must be provided for SQLiteAdapter.")
    return SQLiteAdapter(db_path=Path(db_config.db_path))


def _create_pg_adapter(db_config: DbConfig) -> PgAdapter:
    """Create a PostgreSQL adapter."""
    return PgAdapter(connection_id=db_config.connection_id)


def _create_trino_adapter(db_config: DbConfig) -> TrinoAdapter:
    """Create a Trino adapter."""
    return TrinoAdapter(
        host=db_config.host,
        user=db_config.user,
        catalog=db_config.catalog,
        port=db_config.port,
        schema=db_config.schema,
        http_scheme=db_config.http_scheme,
        verify=db_config.verify,
    )


def create_db_handler(db_type: DatabaseType, db_config: DbConfig) -> DBInterface:
    """Create a database handler based on connection type.

    Args:
        db_config: Database configuration
        db_type: Type of database (e.g., 'postgres', 'trino', 'sqlite')

    Returns:
        A database handler instance

    Raises:
        ValueError: If db_type is not supported
    """
    _registry = {
        DatabaseType.POSTGRES: _create_pg_adapter(db_config),
        DatabaseType.TRINO: _create_trino_adapter(db_config),
        DatabaseType.SQLITE: _create_sqlite_adapter(db_config),
    }

    handler = _registry.get(db_type)
    if handler is None:
        raise ValueError(f"Unsupported database type: {db_type}")
    return handler
