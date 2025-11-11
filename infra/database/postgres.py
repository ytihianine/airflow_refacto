"""PostgreSQL database handler implementation."""

import logging
import time
from typing import Any, Dict, List, Optional, Tuple, cast

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.engine.base import Engine

from .base import BaseDBHandler
from .exceptions import DatabaseError


class PostgresDBHandler(BaseDBHandler):
    """Handler for PostgreSQL database operations using Airflow's PostgresHook."""

    def __init__(self, connection_id: str):
        """Initialize with Airflow connection ID."""
        self.connection_id = connection_id
        self._hook: Optional[PostgresHook] = None
        self._engine: Optional[Engine] = None

    @property
    def hook(self) -> PostgresHook:
        """Lazy initialization of PostgresHook."""
        if self._hook is None:
            self._hook = PostgresHook(postgres_conn_id=self.connection_id)
        return self._hook

    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine for DataFrame operations."""
        if self._engine is None:
            self._engine = cast(Engine, self.hook.get_sqlalchemy_engine())
        return self._engine

    def get_uri(self) -> str:
        """Get the database URI."""
        return self.hook.get_uri()

    def execute(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> None:
        """Execute a query without returning results."""
        try:
            start_time = time.time()
            self.hook.run(query, parameters=parameters)
            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
        except Exception as e:
            raise DatabaseError(f"Error executing query: {str(e)}") from e

    def fetch_one(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single row as a dictionary."""
        try:
            start_time = time.time()
            result = self.hook.get_first(query, parameters=parameters)
            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")

            if result is None:
                return None

            # Convert tuple to dict using cursor description
            with self.hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, parameters)
                    if cur.description is None:
                        raise DatabaseError("Query did not return a result set")
                    columns = [str(desc[0]) for desc in cur.description]
                    return dict(zip(columns, result))

        except Exception as e:
            raise DatabaseError(f"Error fetching row: {str(e)}") from e

    def fetch_all(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all rows as a list of dictionaries."""
        try:
            start_time = time.time()
            with self.hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, parameters)
                    if cur.description is None:
                        raise DatabaseError("Query did not return a result set")
                    columns = [str(desc[0]) for desc in cur.description]
                    results = cur.fetchall()

            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
            return [dict(zip(columns, row)) for row in results]

        except Exception as e:
            raise DatabaseError(f"Error fetching rows: {str(e)}") from e

    def fetch_df(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Fetch results as a pandas DataFrame."""
        try:
            start_time = time.time()
            df = self.hook.get_pandas_df(query, parameters=parameters)
            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
            return df
        except Exception as e:
            raise DatabaseError(f"Error fetching DataFrame: {str(e)}") from e

    def insert(self, table: str, data: Dict[str, Any]) -> None:
        """Insert a single row into a table."""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ["%s"] * len(columns)

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        self.execute(query, tuple(values))

    def bulk_insert(self, table: str, data: List[Dict[str, Any]]) -> None:
        """Insert multiple rows into a table."""
        if not data:
            return

        columns = list(data[0].keys())
        values = [[row[col] for col in columns] for row in data]
        placeholders = ["%s"] * len(columns)

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        with self.hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, values)

    def update(self, table: str, data: Dict[str, Any], where: Dict[str, Any]) -> None:
        """Update rows in a table."""
        set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
        where_clause = " AND ".join([f"{k} = %s" for k in where.keys()])

        query = f"""
            UPDATE {table}
            SET {set_clause}
            WHERE {where_clause}
        """

        parameters = tuple(list(data.values()) + list(where.values()))
        self.execute(query, parameters)

    def delete(self, table: str, where: Dict[str, Any]) -> None:
        """Delete rows from a table."""
        where_clause = " AND ".join([f"{k} = %s" for k in where.keys()])

        query = f"""
            DELETE FROM {table}
            WHERE {where_clause}
        """

        self.execute(query, tuple(where.values()))

    def begin(self) -> None:
        """Begin a transaction."""
        self.execute("BEGIN")

    def commit(self) -> None:
        """Commit the current transaction."""
        self.execute("COMMIT")

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.execute("ROLLBACK")

    def copy_expert(self, sql: str, filepath: str) -> None:
        """
        Execute PostgreSQL COPY command using file-like object for efficient bulk data transfer.

        Args:
            sql: COPY command to execute
            filepath: Path to the file to bulk load
            size: Size hint for the driver
            **kwargs: Additional arguments for the copy operation
        """

        try:
            start_time = time.time()
            self.hook.copy_expert(sql, filepath)
            logging.debug(f"COPY operation executed in {time.time() - start_time:.2f}s")
        except Exception as e:
            raise DatabaseError(f"Error during COPY operation: {str(e)}") from e
