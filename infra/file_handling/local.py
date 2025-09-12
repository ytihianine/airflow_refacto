"""Local filesystem implementation of file handler."""

import os
import shutil
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, List, Optional, Union

from .base import BaseFileHandler, FileMetadata
from .exceptions import FileHandlerError, FileNotFoundError, FilePermissionError


class LocalFileHandler(BaseFileHandler):
    """Handler for local filesystem operations."""

    def read(self, file_path: Union[str, Path], validate: bool = True) -> BinaryIO:
        """Read file content from local filesystem."""
        abs_path = self.get_absolute_path(file_path)
        try:
            if validate:
                self.validate(abs_path)
            return open(abs_path, "rb")
        except PermissionError as e:
            raise FilePermissionError(f"Permission denied: {abs_path}") from e
        except OSError as e:
            raise FileHandlerError(f"Error reading file: {abs_path}") from e

    def write(
        self, file_path: Union[str, Path], content: Union[str, bytes, BinaryIO]
    ) -> None:
        """Write content to local filesystem."""
        abs_path = self.get_absolute_path(file_path)
        try:
            # Create directory if it doesn't exist
            abs_path.parent.mkdir(parents=True, exist_ok=True)

            mode = "wb" if isinstance(content, (bytes, BinaryIO)) else "w"
            with open(abs_path, mode) as f:
                if isinstance(content, (str, bytes)):
                    f.write(content)
                else:
                    shutil.copyfileobj(content, f)
        except PermissionError as e:
            raise FilePermissionError(f"Permission denied: {abs_path}") from e
        except OSError as e:
            raise FileHandlerError(f"Error writing file: {abs_path}") from e

    def delete(self, file_path: Union[str, Path]) -> None:
        """Delete file from local filesystem."""
        abs_path = self.get_absolute_path(file_path)
        try:
            if not abs_path.exists():
                raise FileNotFoundError(f"File not found: {abs_path}")
            abs_path.unlink()
        except PermissionError as e:
            raise FilePermissionError(f"Permission denied: {abs_path}") from e
        except OSError as e:
            raise FileHandlerError(f"Error deleting file: {abs_path}") from e

    def exists(self, file_path: Union[str, Path]) -> bool:
        """Check if file exists in local filesystem."""
        return self.get_absolute_path(file_path).exists()

    def get_metadata(self, file_path: Union[str, Path]) -> FileMetadata:
        """Get metadata for local file."""
        abs_path = self.get_absolute_path(file_path)
        if not abs_path.exists():
            raise FileNotFoundError(f"File not found: {abs_path}")

        try:
            stat = abs_path.stat()
            mime_type, _ = mimetypes.guess_type(str(abs_path))

            return FileMetadata(
                name=abs_path.name,
                size=stat.st_size,
                created_at=datetime.fromtimestamp(stat.st_ctime),
                modified_at=datetime.fromtimestamp(stat.st_mtime),
                mime_type=mime_type or "application/octet-stream",
                checksum=self.validator.calculate_checksum(abs_path),
                extra={
                    "permissions": oct(stat.st_mode)[-3:],
                    "owner": stat.st_uid,
                    "group": stat.st_gid,
                },
            )
        except OSError as e:
            raise FileHandlerError(f"Error getting metadata: {abs_path}") from e

    def list_files(
        self, directory: Union[str, Path], pattern: Optional[str] = None
    ) -> List[str]:
        """List files in local directory."""
        abs_path = self.get_absolute_path(directory)
        if not abs_path.exists():
            raise FileNotFoundError(f"Directory not found: {abs_path}")
        if not abs_path.is_dir():
            raise FileHandlerError(f"Not a directory: {abs_path}")

        try:
            if pattern:
                return [str(p) for p in abs_path.glob(pattern)]
            return [str(p) for p in abs_path.iterdir() if p.is_file()]
        except PermissionError as e:
            raise FilePermissionError(f"Permission denied: {abs_path}") from e
        except OSError as e:
            raise FileHandlerError(f"Error listing directory: {abs_path}") from e

    def move(self, source: Union[str, Path], destination: Union[str, Path]) -> None:
        """Move file in local filesystem."""
        src_path = self.get_absolute_path(source)
        dst_path = self.get_absolute_path(destination)

        if not src_path.exists():
            raise FileNotFoundError(f"Source file not found: {src_path}")

        try:
            # Create destination directory if it doesn't exist
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src_path), str(dst_path))
        except PermissionError as e:
            raise FilePermissionError(
                f"Permission denied: {src_path} -> {dst_path}"
            ) from e
        except OSError as e:
            raise FileHandlerError(
                f"Error moving file: {src_path} -> {dst_path}"
            ) from e

    def copy(self, source: Union[str, Path], destination: Union[str, Path]) -> None:
        """Copy file in local filesystem."""
        src_path = self.get_absolute_path(source)
        dst_path = self.get_absolute_path(destination)

        if not src_path.exists():
            raise FileNotFoundError(f"Source file not found: {src_path}")

        try:
            # Create destination directory if it doesn't exist
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src_path), str(dst_path))
        except PermissionError as e:
            raise FilePermissionError(
                f"Permission denied: {src_path} -> {dst_path}"
            ) from e
        except OSError as e:
            raise FileHandlerError(
                f"Error copying file: {src_path} -> {dst_path}"
            ) from e
