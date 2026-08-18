"""Tests for DBInterface (base database handler interface)."""

import pytest

from modules.infra.database.base import DBInterface


class TestDBInterfaceCannotBeInstantiatedDirectly:
    def test_abstract_class_raises(self) -> None:
        with pytest.raises(TypeError):
            DBInterface()  # type: ignore[abstract]

    def test_all_abstract_methods_exist(self) -> None:
        expected_methods = {
            "get_uri",
            "get_conn",
            "execute",
            "fetch_one",
            "fetch_all",
            "fetch_df",
            "insert",
            "bulk_insert",
            "update",
            "delete",
            "begin",
            "commit",
            "rollback",
            "copy_expert",
            "connection",
        }
        assert expected_methods == DBInterface.__abstractmethods__
