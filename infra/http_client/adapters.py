"""HTTP client implementations."""

import time
from typing import Any, Dict, Optional

import httpx
import requests

from .base import AbstractHTTPClient
from .config import ClientConfig
from .types import HTTPResponse
from .exceptions import (
    APIError,
    AuthenticationError,
    AuthorizationError,
    ConnectionError,
    HTTPClientError,
    RateLimitError,
    RequestError,
    ResponseError,
    TimeoutError,
)


class HttpxClient(AbstractHTTPClient):
    """HTTPX-based HTTP client implementation."""

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self._last_request_time = 0
        self._setup_client()

    def _setup_client(self):
        limits = httpx.Limits(
            max_keepalive_connections=5,
            max_connections=10,
            keepalive_expiry=5.0,
        )
        self._session = httpx.Client(
            headers=self.config.default_headers,
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
            limits=limits,
        )

    def _handle_response(self, response: httpx.Response) -> HTTPResponse:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status == 401:
                raise AuthenticationError(
                    "Authentication failed", status_code=401, response=response
                )
            if status == 403:
                raise AuthorizationError(
                    "Authorization failed", status_code=403, response=response
                )
            if status == 429:
                raise RateLimitError(
                    "Rate limit exceeded", status_code=429, response=response
                )
            if 400 <= status < 500:
                raise RequestError(
                    f"Client error: {e}", status_code=status, response=response
                )
            if 500 <= status < 600:
                raise APIError(
                    f"Server error: {e}", status_code=status, response=response
                )
            raise ResponseError(
                f"HTTP error occurred: {e}", status_code=status, response=response
            )

        return HTTPResponse(response)

    def _handle_rate_limit(self):
        if self.config.rate_limit:
            current_time = time.time()
            time_since_last = current_time - self._last_request_time
            if time_since_last < 1.0 / self.config.rate_limit:
                time.sleep(1.0 / self.config.rate_limit - time_since_last)
            self._last_request_time = time.time()

    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        **kwargs,
    ) -> HTTPResponse:
        url = self._build_url(endpoint)
        self._handle_rate_limit()

        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=headers,
                timeout=timeout or self.config.timeout,
                **kwargs,
            )
            return self._handle_response(response)

        except httpx.TimeoutException as e:
            raise TimeoutError(f"Request timed out: {e}")
        except httpx.NetworkError as e:
            raise ConnectionError(f"Network error occurred: {e}")
        except httpx.HTTPError as e:
            raise HTTPClientError(f"HTTP error occurred: {e}")
        except Exception as e:
            raise HTTPClientError(f"An unexpected error occurred: {e}")

    def close(self) -> None:
        if self._session:
            self._session.close()


class RequestsClient(AbstractHTTPClient):
    """Requests-based HTTP client implementation."""

    def __init__(self, config: ClientConfig) -> None:
        super().__init__(config)
        self._setup_client()

    def _setup_client(self) -> None:
        self._session = requests.Session()
        if self.config.default_headers:
            self._session.headers.update(self.config.default_headers)

        if self.config.proxy:
            proxies = {"http": self.config.proxy, "https": self.config.proxy}
            self._session.proxies.update(proxies)

    def _handle_response(self, response: requests.Response) -> HTTPResponse:
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            status = response.status_code
            if status == 401:
                raise AuthenticationError(
                    "Authentication failed", status_code=401, response=response
                )
            if status == 403:
                raise AuthorizationError(
                    "Authorization failed", status_code=403, response=response
                )
            if status == 429:
                raise RateLimitError(
                    "Rate limit exceeded", status_code=429, response=response
                )
            if 400 <= status < 500:
                raise RequestError(
                    f"Client error: {e}", status_code=status, response=response
                )
            if 500 <= status < 600:
                raise APIError(
                    f"Server error: {e}", status_code=status, response=response
                )
            raise ResponseError(
                f"HTTP error occurred: {e}", status_code=status, response=response
            )

        return HTTPResponse(response)

    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        **kwargs,
    ) -> HTTPResponse:
        url = self._build_url(endpoint)

        try:
            response = self._session.request(
                method=method,
                url=url,
                params=params,
                data=data,
                json=json,
                headers=headers,
                timeout=timeout or self.config.timeout,
                verify=self.config.verify_ssl,
                **kwargs,
            )
            return self._handle_response(response)

        except requests.Timeout as e:
            raise TimeoutError(f"Request timed out: {e}") from e
        except requests.ConnectionError as e:
            raise ConnectionError(f"Connection error occurred: {e}") from e
        except requests.RequestException as e:
            raise HTTPClientError(f"HTTP error occurred: {e}") from e
        except Exception as e:
            raise HTTPClientError(f"An unexpected error occurred: {e}") from e

    def close(self) -> None:
        if self._session:
            self._session.close()
