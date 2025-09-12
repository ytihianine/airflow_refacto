"""Exceptions for HTTP client operations."""


class HTTPClientError(Exception):
    """Base exception for HTTP client errors."""

    def __init__(self, message: str, status_code: int = None, response=None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class ConnectionError(HTTPClientError):
    """Raised when there are network connection issues."""

    pass


class TimeoutError(HTTPClientError):
    """Raised when the request times out."""

    pass


class RequestError(HTTPClientError):
    """Raised when there's an error with the request format/data."""

    pass


class ResponseError(HTTPClientError):
    """Raised when there's an error with the response."""

    pass


class AuthenticationError(HTTPClientError):
    """Raised for authentication-related errors (401)."""

    pass


class AuthorizationError(HTTPClientError):
    """Raised for authorization-related errors (403)."""

    pass


class APIError(HTTPClientError):
    """Raised for API-specific errors (4xx, 5xx not covered by other exceptions)."""

    pass


class RateLimitError(HTTPClientError):
    """Raised when API rate limits are exceeded."""

    pass
