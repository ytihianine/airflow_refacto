"""SQLite database handler implementation."""

import logging
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from infra.database.base import BaseDBHandler
from infra.database.exceptions import DatabaseError


class SQLiteDBHandler(BaseDBHandler):
    """Handler for SQLite database operations using sqlite3."""

    def __init__(self, connection_id: str) -> None:
        """
        Initialize SQLite handler.

        Args:
            connection_id: Path to SQLite database file. Defaults to in-memory.
        """
        self.db_path = connection_id
        self._conn: Optional[sqlite3.Connection] = None

    @property
    def conn(self) -> sqlite3.Connection:
        """Lazy initialization of SQLite connection."""
        if self._conn is None:
            try:
                self._conn = sqlite3.connect(self.db_path)
                self._conn.row_factory = sqlite3.Row  # enables dict-like access
            except Exception as e:
                raise DatabaseError(f"Error connecting to SQLite DB: {str(e)}") from e
        return self._conn

    def get_uri(self) -> str:
        """Return a SQLite URI."""
        return f"sqlite:///{self.db_path}"

    def execute(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> None:
        """Execute a query without returning results."""
        try:
            start_time = time.time()
            cur = self.conn.cursor()
            cur.execute(query, parameters or ())
            self.conn.commit()
            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
        except Exception as e:
            raise DatabaseError(f"Error executing query: {str(e)}") from e

    def fetch_one(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single row as a dictionary."""
        try:
            start_time = time.time()
            cur = self.conn.cursor()
            cur.execute(query, parameters or ())
            row = cur.fetchone()
            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
            return dict(row) if row else None
        except Exception as e:
            raise DatabaseError(f"Error fetching row: {str(e)}") from e

    def fetch_all(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all rows as a list of dictionaries."""
        try:
            start_time = time.time()
            cur = self.conn.cursor()
            cur.execute(query, parameters or ())
            rows = cur.fetchall()
            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
            return [dict(row) for row in rows]
        except Exception as e:
            raise DatabaseError(f"Error fetching rows: {str(e)}") from e

    def fetch_df(
        self, query: str, parameters: Optional[Tuple[Any, ...] | dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Fetch results as a pandas DataFrame."""
        try:
            start_time = time.time()
            df = pd.read_sql_query(query, self.conn, params=parameters)
            logging.debug(f"Query executed in {time.time() - start_time:.2f}s")
            return df
        except Exception as e:
            raise DatabaseError(f"Error fetching DataFrame: {str(e)}") from e

    def insert(self, table: str, data: Dict[str, Any]) -> None:
        """Insert a single row into a table."""
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ["?"] * len(columns)

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
        placeholders = ["?"] * len(columns)

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        values = [tuple(row[col] for col in columns) for row in data]
        try:
            start_time = time.time()
            cur = self.conn.cursor()
            cur.executemany(query, values)
            self.conn.commit()
            logging.debug(f"Bulk insert executed in {time.time() - start_time:.2f}s")
        except Exception as e:
            raise DatabaseError(f"Error during bulk insert: {str(e)}") from e

    def update(self, table: str, data: Dict[str, Any], where: Dict[str, Any]) -> None:
        """Update rows in a table."""
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        where_clause = " AND ".join([f"{k} = ?" for k in where.keys()])

        query = f"""
            UPDATE {table}
            SET {set_clause}
            WHERE {where_clause}
        """

        parameters = tuple(data.values()) + tuple(where.values())
        self.execute(query, parameters)

    def delete(self, table: str, where: Dict[str, Any]) -> None:
        """Delete rows from a table."""
        where_clause = " AND ".join([f"{k} = ?" for k in where.keys()])

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
        self.conn.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.conn.rollback()

    def copy_expert(self, sql: str, filepath: str) -> None:
        """
        Emulate PostgreSQL COPY with SQLite by reading from a file and executing inserts.
        Only supports very basic use cases.

        Args:
            sql: Not used (kept for API compatibility).
            filepath: Path to the file to bulk load.
        """
        try:
            start_time = time.time()
            with open(filepath, "r", encoding="utf-8") as f:
                for line in f:
                    self.conn.execute(line.strip())
            self.conn.commit()
            logging.debug(f"File load executed in {time.time() - start_time:.2f}s")
        except Exception as e:
            raise DatabaseError(f"Error during file load: {str(e)}") from e
