"""Base interface for HTTP clients."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Union
from urllib.parse import urljoin

from .config import ClientConfig

ResponseType = Union[Dict[str, Any], str, bytes]


class AbstractHTTPClient(ABC):
    """Abstract base class for HTTP clients."""

    def __init__(self, config: ClientConfig):
        self.config = config
        self._session = None

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint."""
        if self.config.base_url:
            return urljoin(self.config.base_url, endpoint)
        return endpoint

    @abstractmethod
    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        **kwargs
    ) -> ResponseType:
        """Make an HTTP request."""
        raise NotImplementedError

    def get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None, **kwargs
    ) -> ResponseType:
        """Make a GET request."""
        return self.request("GET", endpoint, params=params, **kwargs)

    def post(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> ResponseType:
        """Make a POST request."""
        return self.request("POST", endpoint, data=data, json=json, **kwargs)

    def put(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> ResponseType:
        """Make a PUT request."""
        return self.request("PUT", endpoint, data=data, json=json, **kwargs)

    def patch(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> ResponseType:
        """Make a PATCH request."""
        return self.request("PATCH", endpoint, data=data, json=json, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> ResponseType:
        """Make a DELETE request."""
        return self.request("DELETE", endpoint, **kwargs)

    @abstractmethod
    def close(self) -> None:
        """Close the client and release resources."""
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
