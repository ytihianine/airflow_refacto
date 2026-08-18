"""Tests for TrinoAdapter."""

from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from modules.infra.database.exceptions import DatabaseError
from modules.infra.database.trino import TrinoAdapter


@pytest.fixture()
def adapter() -> TrinoAdapter:
    """Create a TrinoAdapter with test parameters."""
    with patch("modules.infra.database.trino.trino.dbapi.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        handler = TrinoAdapter(
            host="trino.example.com",
            user="test_user",
            catalog="test_catalog",
            port=443,
            schema="test_schema",
        )
        # Force connection initialization
        _ = handler.conn
        return handler


class TestTrinoAdapterInit:
    def test_init_stores_parameters(self) -> None:
        adapter = TrinoAdapter(
            host="trino.example.com",
            user="test_user",
            catalog="test_catalog",
            port=8080,
            schema="myschema",
            http_scheme="http",
            verify=False,
        )
        assert adapter.host == "trino.example.com"
        assert adapter.user == "test_user"
        assert adapter.catalog == "test_catalog"
        assert adapter.port == 8080
        assert adapter.schema == "myschema"
        assert adapter.http_scheme == "http"
        assert adapter.verify is False

    def test_init_defaults(self) -> None:
        adapter = TrinoAdapter(host="h", user="u", catalog="c")
        assert adapter.port == 443
        assert adapter.schema is None
        assert adapter.http_scheme == "https"
        assert adapter.verify is True


class TestTrinoAdapterGetUri:
    def test_get_uri(self) -> None:
        adapter = TrinoAdapter(host="trino.example.com", user="user", catalog="cat", port=443)
        uri = adapter.get_uri()
        assert uri == "https://user@trino.example.com:443/cat"


class TestTrinoAdapterGetConn:
    def test_get_conn_returns_connection(self, adapter: TrinoAdapter) -> None:
        conn = adapter.get_conn()
        assert conn is not None


class TestTrinoAdapterFetchOne:
    def test_fetch_one_returns_dict(self, adapter: TrinoAdapter) -> None:
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1, "Alice")
        mock_cursor.description = [("id",), ("name",)]
        cast("MagicMock", adapter.conn).cursor.return_value = mock_cursor

        result = adapter.fetch_one("SELECT * FROM users WHERE id = 1")
        assert result == {"id": 1, "name": "Alice"}

    def test_fetch_one_returns_none(self, adapter: TrinoAdapter) -> None:
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        cast("MagicMock", adapter.conn).cursor.return_value = mock_cursor

        result = adapter.fetch_one("SELECT * FROM users WHERE id = 999")
        assert result is None


class TestTrinoAdapterFetchAll:
    def test_fetch_all_returns_list_of_dicts(self, adapter: TrinoAdapter) -> None:
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_cursor.description = [("id",), ("name",)]
        cast("MagicMock", adapter.conn).cursor.return_value = mock_cursor

        results = adapter.fetch_all("SELECT * FROM users")
        assert len(results) == 2
        assert results[0] == {"id": 1, "name": "Alice"}
        assert results[1] == {"id": 2, "name": "Bob"}


class TestTrinoAdapterFetchDf:
    def test_fetch_df_returns_dataframe(self, adapter: TrinoAdapter) -> None:
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "Alice")]
        mock_cursor.description = [("id",), ("name",)]
        cast("MagicMock", adapter.conn).cursor.return_value = mock_cursor

        df = adapter.fetch_df("SELECT * FROM users")
        assert len(df) == 1
        assert list(df.columns) == ["id", "name"]


class TestTrinoAdapterReadOnlyMethods:
    """All write operations should raise NotImplementedError."""

    def test_execute_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.execute("INSERT INTO t VALUES (1)")

    def test_insert_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.insert("t", {"id": 1})

    def test_bulk_insert_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.bulk_insert("t", [{"id": 1}])

    def test_update_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.update("t", {"name": "x"}, {"id": 1})

    def test_delete_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.delete("t", {"id": 1})

    def test_begin_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.begin()

    def test_commit_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.commit()

    def test_rollback_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.rollback()

    def test_copy_expert_raises(self, adapter: TrinoAdapter) -> None:
        with pytest.raises(NotImplementedError, match="read-only"):
            adapter.copy_expert("COPY ...", "/tmp/file.csv")


class TestTrinoAdapterConnectionError:
    @patch("modules.infra.database.trino.trino.dbapi.connect")
    def test_connection_error_raises_database_error(self, mock_connect: MagicMock) -> None:
        mock_connect.side_effect = Exception("cannot connect")
        adapter = TrinoAdapter(host="bad", user="u", catalog="c")
        with pytest.raises(DatabaseError, match="Error connecting to Trino"):
            _ = adapter.conn
