"""Tests for FSInterface (base file handler interface)."""

from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import BinaryIO

import pytest

from modules.infra.file_system.base import FileMetadata, FSInterface
from modules.infra.file_system.exceptions import FileNotFoundError


class ConcreteFileHandler(FSInterface):
    """Minimal concrete implementation of FSInterface for testing."""

    def __init__(self):
        super().__init__()
        self._files: dict[str, bytes] = {}

    def read(self, file_path: str | Path, validate: bool = True) -> BinaryIO:
        key = str(file_path)
        if key not in self._files:
            raise FileNotFoundError(f"File not found: {file_path}")
        return BytesIO(self._files[key])

    def write(self, file_path: str | Path, content: str | bytes | BinaryIO) -> None:
        if isinstance(content, str):
            self._files[str(file_path)] = content.encode()
        elif isinstance(content, bytes):
            self._files[str(file_path)] = content
        else:
            self._files[str(file_path)] = content.read()  # type: ignore

    def delete(self, file_path: str | Path) -> None:
        key = str(file_path)
        if key in self._files:
            del self._files[key]

    def delete_single(self, file_path: str | Path) -> None:
        self.delete(file_path)

    def exists(self, file_path: str | Path) -> bool:
        return str(file_path) in self._files

    def get_metadata(self, file_path: str | Path) -> FileMetadata:
        key = str(file_path)
        if key not in self._files:
            raise FileNotFoundError(f"File not found: {file_path}")
        now = datetime.now()
        return FileMetadata(
            name=Path(file_path).name,
            size=len(self._files[key]),
            created_at=now,
            modified_at=now,
            mime_type="application/octet-stream",
            checksum="abc123",
        )

    def list_files(self, directory: str | Path, pattern: str | None = None) -> list[str]:
        return [k for k in self._files if k.startswith(str(directory))]

    def move(self, source: str | Path, destination: str | Path) -> None:
        self._files[str(destination)] = self._files.pop(str(source))

    def copy(self, source: str | Path, destination: str | Path) -> None:
        self._files[str(destination)] = self._files[str(source)]


class TestFSInterfaceCannotBeInstantiatedDirectly:
    def test_abstract_class_raises(self) -> None:
        with pytest.raises(TypeError):
            FSInterface()  # type: ignore[abstract]


class TestFileMetadata:
    def test_metadata_fields(self) -> None:
        now = datetime.now()
        meta = FileMetadata(
            name="file.txt",
            size=100,
            created_at=now,
            modified_at=now,
            mime_type="text/plain",
            checksum="abc",
        )
        assert meta.name == "file.txt"
        assert meta.size == 100
        assert meta.mime_type == "text/plain"
        assert meta.checksum == "abc"
        assert meta.extra == {}

    def test_metadata_extra(self) -> None:
        now = datetime.now()
        meta = FileMetadata(
            name="f.txt",
            size=0,
            created_at=now,
            modified_at=now,
            mime_type="text/plain",
            checksum="x",
            extra={"key": "value"},
        )
        assert meta.extra == {"key": "value"}


class TestConcreteHandlerInit:
    def test_no_base_path(self) -> None:
        handler = ConcreteFileHandler()
        assert handler.base_path is None

    def test_string_base_path(self) -> None:
        handler = ConcreteFileHandler(base_path="/tmp/data")
        assert handler.base_path == Path("/tmp/data")

    def test_path_base_path(self) -> None:
        handler = ConcreteFileHandler(base_path=Path("/tmp/data"))
        assert handler.base_path == Path("/tmp/data")


class TestGetAbsolutePath:
    def test_relative_path_with_base(self) -> None:
        handler = ConcreteFileHandler(base_path="/data")
        result = handler.get_absolute_path("subdir/file.txt")
        assert result == Path("/data/subdir/file.txt")

    def test_absolute_path_with_base(self) -> None:
        handler = ConcreteFileHandler(base_path="/data")
        result = handler.get_absolute_path("/other/file.txt")
        assert result == Path("/other/file.txt")

    def test_relative_path_without_base(self) -> None:
        handler = ConcreteFileHandler()
        result = handler.get_absolute_path("subdir/file.txt")
        assert result == Path("subdir/file.txt")


class TestValidate:
    def test_validate_existing_file(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("test.txt", b"content")
        assert handler.validate("test.txt") is True

    def test_validate_missing_file_raises(self) -> None:
        handler = ConcreteFileHandler()
        with pytest.raises(FileNotFoundError, match="File not found"):
            handler.validate("missing.txt")


class TestConcreteHandlerOperations:
    def test_write_and_read_bytes(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("test.bin", b"hello")
        result = handler.read("test.bin")
        assert result.read() == b"hello"

    def test_write_and_read_string(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("test.txt", "hello")
        result = handler.read("test.txt")
        assert result.read() == b"hello"

    def test_write_and_read_binary_io(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("test.bin", BytesIO(b"stream"))
        result = handler.read("test.bin")
        assert result.read() == b"stream"

    def test_read_missing_raises(self) -> None:
        handler = ConcreteFileHandler()
        with pytest.raises(FileNotFoundError):
            handler.read("nope.txt")

    def test_exists(self) -> None:
        handler = ConcreteFileHandler()
        assert handler.exists("a.txt") is False
        handler.write("a.txt", b"x")
        assert handler.exists("a.txt") is True

    def test_delete(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("a.txt", b"x")
        handler.delete("a.txt")
        assert handler.exists("a.txt") is False

    def test_delete_single(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("a.txt", b"x")
        handler.delete_single("a.txt")
        assert handler.exists("a.txt") is False

    def test_get_metadata(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("data/file.csv", b"col1,col2")
        meta = handler.get_metadata("data/file.csv")
        assert meta.name == "file.csv"
        assert meta.size == len(b"col1,col2")

    def test_get_metadata_missing_raises(self) -> None:
        handler = ConcreteFileHandler()
        with pytest.raises(FileNotFoundError):
            handler.get_metadata("missing.csv")

    def test_list_files(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("dir/a.txt", b"a")
        handler.write("dir/b.txt", b"b")
        handler.write("other/c.txt", b"c")
        files = handler.list_files("dir")
        assert sorted(files) == ["dir/a.txt", "dir/b.txt"]

    def test_move(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("txt", b"data")
        handler.move("txt", "dst.txt")
        assert handler.exists("txt") is False
        assert handler.read("dst.txt").read() == b"data"

    def test_copy(self) -> None:
        handler = ConcreteFileHandler()
        handler.write("txt", b"data")
        handler.copy("txt", "dst.txt")
        assert handler.exists("txt") is True
        assert handler.read("dst.txt").read() == b"data"
