"""Common exceptions for utilities package."""

from typing import Any


class UtilsError(Exception):
    """Base class for errors raised by utils package."""

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(message or "An error occurred in utils")
        self.context = context


class ConfigError(UtilsError):
    """Raised when configuration is missing or invalid."""

    def __init__(self, message: str | None = None, **context: Any) -> None:
        super().__init__(message or "Configuration error", **context)
