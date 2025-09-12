"""Database handling exceptions."""


class DatabaseError(Exception):
    """Base exception for database operations."""

    pass


class ConnectionError(DatabaseError):
    """Exception raised when database connection fails."""

    pass


class QueryError(DatabaseError):
    """Exception raised when a query fails to execute."""

    pass


class TransactionError(DatabaseError):
    """Exception raised when transaction operations fail."""

    pass
