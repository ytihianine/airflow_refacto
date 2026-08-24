"""S3 implementation of file handler using Airflow's S3 hooks or a boto3 client."""

import io
import logging
import mimetypes
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from modules.infra.file_system.base import FileMetadata, FSInterface
from modules.infra.file_system.exceptions import FileHandlerError, FileNotFoundError


@dataclass
class S3FS(FSInterface):
    """Handler for S3 storage operations using Airflow's S3Hook or a boto3 client."""

    bucket: str
    connection_id: str | None = None
    _client: Any | None = None
    _hook: Any | None = None  # Internal S3Hook instance, lazily initialized

    @property
    def client(self) -> Any:
        """Return the boto3 S3 client, lazily initialised from S3Hook if needed."""
        if self._client is None:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            if self._hook is None:
                self._hook = S3Hook(aws_conn_id=self.connection_id)
            self._client = self._hook.get_conn()
        return self._client

    def read(self, file_path: str | Path, validate: bool = True) -> BinaryIO:
        """Read file from S3."""
        key = str(file_path)
        try:
            logging.info(msg=f"Reading file from {key}")
            if validate:
                self.validate(key)

            if not self.exists(key):
                raise FileNotFoundError(f"File not found in S3: {key}")

            response = self.client.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read()
            return io.BytesIO(content)

        except FileNotFoundError:
            raise
        except Exception as e:
            raise FileHandlerError(f"Error reading file from S3: {key}") from e

    def write(
        self,
        file_path: str | Path,
        content: str | bytes | BinaryIO,
        content_type: str | None = None,
    ) -> None:
        """
        Write content to S3.

        Supports all file types: CSV, TSV, XLSX, Parquet, JSON, etc.

        Args:
            file_path: S3 key/path for the file
            content: Content to write (str, bytes, or file-like object)
            content_type: Optional MIME type. If not provided, will be inferred from file extension
        """
        key = str(file_path)
        try:
            logging.info(msg=f"Writing file to {key}")

            # Infer content type if not provided
            if content_type is None:
                content_type, _ = mimetypes.guess_type(key)
                if content_type is None:
                    content_type = "application/octet-stream"

            # Convert content to bytes if it's a string
            if isinstance(content, str):
                content_bytes = content.encode("utf-8")
                file_obj = io.BytesIO(initial_bytes=content_bytes)
            # If already bytes, wrap in BytesIO
            elif isinstance(content, bytes):
                file_obj = io.BytesIO(initial_bytes=content)
            # If it's already a file-like object, use it directly
            elif hasattr(content, "read"):
                file_obj = content
            else:
                raise ValueError(f"Unsupported content type: {type(content)}")

            # Ensure we're at the beginning of the stream
            if hasattr(file_obj, "seek"):
                file_obj.seek(0)  # type: ignore

            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=file_obj,
                ContentType=content_type,
            )

            logging.info(msg=f"Successfully wrote {key} with content type: {content_type}")

        except Exception as e:
            raise FileHandlerError(f"Error writing file to S3: {key}") from e

    def delete_single(self, file_path: str | Path) -> None:
        key = str(file_path)
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def delete(self, file_path: str | Path) -> None:
        """Delete file from S3."""
        key = str(file_path)
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
        except Exception as e:
            raise FileHandlerError(f"Error deleting file from S3: {key}") from e

    def exists(self, file_path: str | Path) -> bool:
        """Check if file exists in S3."""
        key = str(file_path)
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def get_metadata(self, file_path: str | Path) -> FileMetadata:
        """Get S3 file metadata."""
        key = str(file_path)
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=key)

            mime_type, _ = mimetypes.guess_type(key)

            return FileMetadata(
                name=Path(key).name,
                size=response["ContentLength"],
                created_at=response["LastModified"],
                modified_at=response["LastModified"],
                mime_type=mime_type or response.get("ContentType", "application/octet-stream"),
                checksum=response["ETag"].strip('"'),
                extra={
                    "storage_class": response.get("StorageClass"),
                    "version_id": response.get("VersionId"),
                    "metadata": response.get("Metadata", {}),
                },
            )
        except self.client.exceptions.NoSuchKey as err:
            raise FileNotFoundError(f"File not found in S3: {key}") from err
        except Exception as e:
            raise FileHandlerError(f"Error getting S3 metadata: {key}") from e

    def list_files(self, directory: str | Path, pattern: str | None = None) -> list[str]:
        """List files in S3 directory."""
        prefix = str(directory).rstrip("/") + "/"
        logging.info(msg=f"Listing files in S3 directory: {prefix} with pattern: {pattern}")
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

        if not keys:
            return []

        if pattern:
            import fnmatch

            keys = fnmatch.filter(names=keys, pat=pattern)

        logging.info(msg=f"Found {len(keys)} files in S3 directory")
        return keys

    def move(self, source: str | Path, destination: str | Path) -> None:
        """Move/rename file in S3."""
        src_key = str(source)
        dst_key = str(destination)

        try:
            if not self.exists(src_key):
                raise FileNotFoundError(f"Source file not found in S3: {src_key}")

            self.client.copy_object(
                Bucket=self.bucket,
                Key=dst_key,
                CopySource={"Bucket": self.bucket, "Key": src_key},
            )
            self.delete(src_key)
        except FileNotFoundError:
            raise
        except Exception as e:
            raise FileHandlerError(f"Error moving file in S3: {src_key} -> {dst_key}") from e

    def copy(self, source: str | Path, destination: str | Path) -> None:
        """Copy file in S3."""
        src_key = str(source)
        dst_key = str(destination)

        try:
            if not self.exists(src_key):
                raise FileNotFoundError(f"Source file not found in S3: {src_key}")

            self.client.copy_object(
                Bucket=self.bucket,
                Key=dst_key,
                CopySource={"Bucket": self.bucket, "Key": src_key},
            )
        except FileNotFoundError:
            raise
        except Exception as e:
            raise FileHandlerError(f"Error copying file in S3: {src_key} -> {dst_key}") from e
