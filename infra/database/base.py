"""Base database handler interface and types."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Generic, cast

import pandas as pd
from airflow.hooks.base import BaseHook

T = TypeVar("T")


@dataclass
class QueryResult(Generic[T]):
    """Represents the result of a database query."""

    data: T
    affected_rows: Optional[int] = None
    query_time: Optional[float] = None


class BaseDBHandler(ABC):
    """Base class for database operations."""

    @abstractmethod
    def execute(self, query: str, parameters: Optional[Tuple[Any, ...]] = None) -> None:
        """Execute a query without returning results."""
        pass

    @abstractmethod
    def fetch_one(
        self, query: str, parameters: Optional[Tuple[Any, ...]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single row as a dictionary."""
        pass

    @abstractmethod
    def fetch_all(
        self, query: str, parameters: Optional[Tuple[Any, ...]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all rows as a list of dictionaries."""
        pass

    @abstractmethod
    def fetch_df(
        self, query: str, parameters: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """Fetch results as a pandas DataFrame."""
        pass

    @abstractmethod
    def insert(self, table: str, data: Dict[str, Any]) -> None:
        """Insert a single row into a table."""
        pass

    @abstractmethod
    def bulk_insert(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Insert multiple rows into a table."""
        pass

    @abstractmethod
    def update(self, table: str, data: Dict[str, Any], where: Dict[str, Any]) -> None:
        """Update rows in a table."""
        pass

    @abstractmethod
    def delete(self, table: str, where: Dict[str, Any]) -> None:
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
