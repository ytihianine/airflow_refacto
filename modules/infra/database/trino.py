"""Trino database handler implementation (read-only)."""

import logging
import time
from typing import Any

import pandas as pd
import trino.dbapi

from .base import DBInterface
from .exceptions import DatabaseError


class TrinoAdapter(DBInterface):
    """Read-only handler for Trino database operations using trino.dbapi."""

    def __init__(
        self,
        host: str,
        user: str,
        catalog: str,
        port: int = 443,
        schema: str | None = None,
        http_scheme: str = "https",
        verify: bool = True,
    ) -> None:
        """
        Initialize Trino handler.

        Args:
            host: Trino coordinator host.
            user: User used for authentication.
            catalog: Default catalog to query.
            port: Trino coordinator port. Defaults to 443.
            schema: Default schema within the catalog.
            http_scheme: HTTP scheme ('http' or 'https'). Defaults to 'https'.
            verify: Whether to verify SSL certificates. Defaults to True.
        """
        self.host = host
        self.user = user
        self.catalog = catalog
        self.port = port
        self.schema = schema
        self.http_scheme = http_scheme
        self.verify = verify
        self._conn: trino.dbapi.Connection | None = None

    @property
    def conn(self) -> trino.dbapi.Connection:
        """Lazy initialization of Trino connection."""
        if self._conn is None:
            try:
                self._conn = trino.dbapi.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    catalog=self.catalog,
                    schema=self.schema,
                    http_scheme=self.http_scheme,
                    verify=self.verify,
                )
            except Exception as e:
                raise DatabaseError(f"Error connecting to Trino: {e!s}") from e
        return self._conn

    @property
    def connection(self) -> dict[str, Any]:
        return {"host": self.conn.host, "port": self.conn.port}

    def get_uri(self) -> str:
        """Return a Trino URI."""
        return f"{self.http_scheme}://{self.user}@{self.host}:{self.port}/{self.catalog}"

    def get_conn(self) -> Any:
        return self.conn

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def fetch_one(
        self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None
    ) -> dict[str, Any] | None:
        """Fetch a single row as a dictionary."""
        try:
            start_time = time.time()
            cur = self.conn.cursor()
            cur.execute(query, parameters)
            row = cur.fetchone()
            logging.debug(msg=f"Query executed in {time.time() - start_time:.2f}s")
            if row is None:
                return None
            columns = [desc[0] for desc in cur.description]
            return dict(zip(columns, row, strict=False))
        except Exception as e:
            raise DatabaseError(f"Error fetching row: {e!s}") from e

    def fetch_all(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Fetch all rows as a list of dictionaries."""
        try:
            start_time = time.time()
            cur = self.conn.cursor()
            cur.execute(query, parameters)
            rows = cur.fetchall()
            logging.debug(msg=f"Query executed in {time.time() - start_time:.2f}s")
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row, strict=False)) for row in rows]
        except Exception as err:
            raise DatabaseError(f"Error fetching rows: {err!s}") from err

    def fetch_df(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> pd.DataFrame:
        """Fetch results as a pandas DataFrame."""
        try:
            start_time = time.time()
            logging.info(msg=f"Running statement:\n {query}")
            rows = self.fetch_all(query, parameters)
            df = pd.DataFrame(data=rows)
            logging.debug(msg=f"Query executed in {time.time() - start_time:.2f}s")
            return df
        except Exception as e:
            raise DatabaseError(f"Error fetching DataFrame: {e!s}") from e

    # ------------------------------------------------------------------
    # Unsupported write operations
    # ------------------------------------------------------------------

    def execute(self, query: str, parameters: tuple[Any, ...] | dict[str, Any] | None = None) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support execute().")

    def insert(self, table: str, data: dict[str, Any]) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support insert().")

    def bulk_insert(self, table: str, data: list[dict[str, Any]]) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support bulk_insert().")

    def update(self, table: str, data: dict[str, Any], where: dict[str, Any]) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support update().")

    def delete(self, table: str, where: dict[str, Any]) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support delete().")

    def begin(self) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support begin().")

    def commit(self) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support commit().")

    def rollback(self) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support rollback().")

    def copy_expert(self, sql: str, filepath: str) -> None:
        raise NotImplementedError("TrinoAdapter is read-only and does not support copy_expert().")
