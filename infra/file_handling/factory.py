"""Factory for creating file handlers."""

from typing import Optional, Union
from pathlib import Path

from .base import BaseFileHandler
from .local import LocalFileHandler
from .s3 import S3FileHandler


class FileHandlerFactory:
    """Factory class for creating file handlers."""

    @staticmethod
    def create_handler(
        handler_type: str, base_path: Optional[Union[str, Path]] = None, **kwargs
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
        """
        if handler_type.lower() == "local":
            return LocalFileHandler(base_path=base_path)
        elif handler_type.lower() == "s3":
            required_args = {"bucket", "connection_id"}
            if not all(arg in kwargs for arg in required_args):
                raise ValueError(
                    f"Missing required arguments for S3 handler: {required_args - kwargs.keys()}"
                )
            return S3FileHandler(**kwargs)
        else:
            raise ValueError(f"Unsupported handler type: {handler_type}")
