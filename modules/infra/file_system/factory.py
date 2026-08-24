"""Factory for creating file handlers."""

from dataclasses import dataclass
from pathlib import Path

from modules.constants import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID
from modules.enums.filesystem import FileHandlerType
from modules.infra.file_system.base import FSInterface
from modules.infra.file_system.local import LocalFS
from modules.infra.file_system.s3 import S3FS


@dataclass(frozen=True)
class FSConfig:
    """Configuration for file system handlers."""

    # LocalFS
    base_path: str | Path | None = None
    # S3FS
    bucket: str = DEFAULT_S3_BUCKET
    connection_id: str = DEFAULT_S3_CONN_ID


def _create_local_handler(
    config: FSConfig,
) -> FSInterface:
    if not config.base_path:
        raise ValueError("Missing required argument for Local handler: 'base_path'")
    return LocalFS(base_path=Path(config.base_path))


def _create_s3_handler(
    config: FSConfig,
) -> FSInterface:
    if not config.bucket:
        raise ValueError("Missing required argument for S3 handler: 'bucket'")
    if not config.connection_id:
        raise ValueError("S3 handler requires 'connection_id'")
    return S3FS(bucket=config.bucket, connection_id=config.connection_id)


def create_file_handler(
    handler_type: FileHandlerType,
    config: FSConfig,
) -> FSInterface:
    """
    Create and return a file handler instance.

    Args:
        handler_type: Type of handler ('local' or 's3')
        config: FSConfig instance containing handler configuration

    Returns:
        FSInterface: Instance of the requested file handler

    Raises:
        ValueError: If handler_type is unsupported or required args are missing

    Examples:
        >>> handler = create_file_handler(FileHandlerType.LOCAL, config=FSConfig(base_path="/data"))
        >>> handler = create_file_handler(FileHandlerType.S3, config=FSConfig(bucket="my-bucket", connection_id="s3_conn"))
    """
    _handler_registry = {
        FileHandlerType.LOCAL: _create_local_handler,
        FileHandlerType.S3: _create_s3_handler,
    }

    try:
        handler_factory = _handler_registry[handler_type]
    except KeyError as error:
        raise ValueError(f"Unsupported handler type: '{handler_type}'. Supported types: 'local', 's3'") from error

    return handler_factory(config=config)
