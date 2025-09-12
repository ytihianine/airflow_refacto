"""Custom exceptions for file handling operations."""


class FileHandlerError(Exception):
    """Base exception for file handling errors."""

    pass


class FileValidationError(FileHandlerError):
    """Raised when file validation fails."""

    pass


class FileNotFoundError(FileHandlerError):
    """Raised when a file cannot be found."""

    pass


class FilePermissionError(FileHandlerError):
    """Raised when there are permission issues."""

    pass


class FileTypeError(FileHandlerError):
    """Raised when file type is not supported."""

    pass


class FileCorruptError(FileHandlerError):
    """Raised when file is corrupted."""

    pass
