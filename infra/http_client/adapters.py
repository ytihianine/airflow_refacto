"""HTTP client implementations."""

import time
from typing import Any, Dict, Optional, Union

import httpx
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import AbstractHTTPClient, ResponseType
from .config import ClientConfig
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
        """Initialize the HTTPX client with configuration."""
        limits = httpx.Limits(
            max_keepalive_connections=5, max_connections=10, keepalive_expiry=5.0
        )

        self._session = httpx.Client(
            headers=self.config.default_headers,
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
            proxies=self.config.proxies if self.config.proxy else None,
            limits=limits,
        )

    def _handle_response(self, response: httpx.Response) -> ResponseType:
        """Handle the response and raise appropriate exceptions."""
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise AuthenticationError(
                    "Authentication failed", status_code=401, response=e.response
                )
            elif e.response.status_code == 403:
                raise AuthorizationError(
                    "Authorization failed", status_code=403, response=e.response
                )
            elif e.response.status_code == 429:
                raise RateLimitError(
                    "Rate limit exceeded", status_code=429, response=e.response
                )
            elif 400 <= e.response.status_code < 500:
                raise RequestError(
                    f"Client error: {e}",
                    status_code=e.response.status_code,
                    response=e.response,
                )
            elif 500 <= e.response.status_code < 600:
                raise APIError(
                    f"Server error: {e}",
                    status_code=e.response.status_code,
                    response=e.response,
                )
            else:
                raise ResponseError(
                    f"HTTP error occurred: {e}",
                    status_code=e.response.status_code,
                    response=e.response,
                )

        # Try to parse JSON response
        try:
            return response.json()
        except ValueError:
            return response.text

    def _handle_rate_limit(self):
        """Handle rate limiting between requests."""
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
    ) -> ResponseType:
        """Make an HTTP request using HTTPX."""
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

        except httpx.TimeoutError as e:
            raise TimeoutError(f"Request timed out: {e}")
        except httpx.NetworkError as e:
            raise ConnectionError(f"Network error occurred: {e}")
        except httpx.HTTPError as e:
            raise HTTPClientError(f"HTTP error occurred: {e}")
        except Exception as e:
            raise HTTPClientError(f"An unexpected error occurred: {e}")

    def close(self) -> None:
        """Close the client session."""
        if self._session:
            self._session.close()


class RequestsAPIClient(AbstractApiClient):
    def __init__(self, proxy: str = None, user_agent: str = None):
        self.session = requests.Session()

        if user_agent:
            self.session.headers.update({"User-Agent": user_agent})

        if proxy:
            # Configure proxies
            self.proxies = {"http": proxy, "https": proxy}
            self.session.proxies = self.proxies

            # Ensure proxy headers include the User-Agent
            if user_agent:
                self.session.get_adapter("https://").proxy_manager_for(
                    f"http://{proxy}"
                ).proxy_headers["User-Agent"] = user_agent

    def get(self, endpoint: str, **kwargs):
        try:
            response = self.session.get(endpoint, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Error during GET request: {e}")
            raise

    def post(self, endpoint: str, data: dict[any, any] = None, json=None, **kwargs):
        try:
            response = self.session.post(endpoint, data=data, json=json, **kwargs)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            print(f"Error during POST request: {e}")
            raise

    def put(self):
        pass

    def patch(self):
        pass

    def delete(self, url: str, **kwargs):
        pass
