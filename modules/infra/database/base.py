"""Base database handler interface and types."""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd


class DBInterface(ABC):
    """Base class for database operations."""

    @abstractmethod
    def get_uri(self) -> str:
        """Get the database URI."""
        pass

    @abstractmethod
    def get_conn(self) -> Any: ...

    @abstractmethod
    def execute(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> None:
        """Execute a query without returning results."""
        pass

    @abstractmethod
    def fetch_one(
        self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Fetch a single row as a dictionary."""
        pass

    @abstractmethod
    def fetch_all(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Fetch all rows as a list of dictionaries."""
        pass

    @abstractmethod
    def fetch_df(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> pd.DataFrame:
        """Fetch results as a pandas DataFrame."""
        pass

    @abstractmethod
    def insert(self, table: str, data: dict[str, Any]) -> None:
        """Insert a single row into a table."""
        pass

    @abstractmethod
    def bulk_insert(self, table: str, data: list[dict[str, Any]]) -> None:
        """Insert multiple rows into a table."""
        pass

    @abstractmethod
    def update(self, table: str, data: dict[str, Any], where: dict[str, Any]) -> None:
        """Update rows in a table."""
        pass

    @abstractmethod
    def delete(self, table: str, where: dict[str, Any]) -> None:
        """Delete rows from a table."""
        pass

    @abstractmethod
    def begin(self) -> None:
        """Begin a transaction."""
        pass

    @abstractmethod
    def commit(self) -> None:
        """Commit the current transaction."""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Rollback the current transaction."""
        pass

    @abstractmethod
    def copy_expert(
        self,
        sql: str,
        filepath: str,
    ) -> None:
        """
        Execute COPY command for efficient bulk data transfer.

        Args:
            sql: COPY command to execute
            filepath: Path to the file to bulk load
        """
        pass
