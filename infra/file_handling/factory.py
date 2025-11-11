"""Factory for creating file handlers."""

from typing import Optional, Union
from pathlib import Path

from utils.config.types import FileHandlerType

from .base import BaseFileHandler
from .local import LocalFileHandler
from .s3 import S3FileHandler

# Default values
DEFAULT_S3_CONNECTION_ID = "minio_bucket_dsci"
DEFAULT_S3_BUCKET = "dsci"


def create_file_handler(
    handler_type: FileHandlerType,
    base_path: Optional[Union[str, Path]] = None,
    **kwargs,
) -> BaseFileHandler:
    """
    Create and return a file handler instance.

    Args:
        handler_type: Type of handler ('local' or 's3')
        base_path: Optional base path for relative paths
        **kwargs: Additional arguments for specific handlers
            For S3: bucket, connection_id

    Returns:
        BaseFileHandler: Instance of the requested file handler

    Raises:
        ValueError: If handler_type is unsupported or required args are missing

    Examples:
        >>> handler = create_file_handler("local", base_path="/data")
        >>> handler = create_file_handler("s3", bucket="my-bucket", connection_id="s3_conn")
    """

    if handler_type == FileHandlerType.LOCAL:
        return LocalFileHandler(base_path=base_path)

    elif handler_type == FileHandlerType.S3:
        required_args = {"bucket", "connection_id"}
        missing_args = required_args - kwargs.keys()
        if missing_args:
            raise ValueError(
                f"Missing required arguments for S3 handler: {missing_args}"
            )
        return S3FileHandler(**kwargs)

    else:
        raise ValueError(
            f"Unsupported handler type: '{handler_type}'. "
            f"Supported types: 'local', 's3'"
        )


def create_default_s3_handler(
    connection_id: Optional[str] = None,
    bucket: Optional[str] = None,
) -> BaseFileHandler:
    """
    Create S3 handler with default DSCI bucket configuration.

    This is the most commonly used S3 handler in the codebase.

    Args:
        connection_id: S3 connection ID (default: minio_bucket_dsci)
        bucket: S3 bucket name (default: dsci)

    Returns:
        BaseFileHandler: Configured S3 handler instance
    """
    connection_id = connection_id or DEFAULT_S3_CONNECTION_ID
    bucket = bucket or DEFAULT_S3_BUCKET
    return create_file_handler(
        FileHandlerType.S3, connection_id=connection_id, bucket=bucket
    )


def create_local_handler(
    base_path: Optional[Union[str, Path]] = None
) -> BaseFileHandler:
    """
    Create local file handler.

    Args:
        base_path: Optional base path for relative paths

    Returns:
        BaseFileHandler: Configured local handler instance
    """
    base_path = base_path or None
    return create_file_handler(FileHandlerType.LOCAL, base_path=base_path)
