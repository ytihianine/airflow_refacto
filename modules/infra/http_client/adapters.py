"""HTTP client implementations."""

import time
from typing import TYPE_CHECKING, Any, cast

import httpx
import requests

from modules.infra.http_client.base import HttpInterface
from modules.infra.http_client.config import ClientConfig
from modules.infra.http_client.exceptions import (
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
from modules.infra.http_client.types import HTTPResponse

if TYPE_CHECKING:
    from requests.adapters import HTTPAdapter


class HttpxClient(HttpInterface):
    """HTTPX-based HTTP client implementation."""

    def __init__(self, config: ClientConfig) -> None:
        super().__init__(config)
        self._last_request_time = 0
        self._setup_client()

    def _setup_client(self) -> None:
        limits = httpx.Limits(
            max_keepalive_connections=5,
            max_connections=10,
            keepalive_expiry=5.0,
        )

        proxy_mounts = {}
        if self.config.proxy:
            proxy_headers = {}
            if self.config.user_agent:
                proxy_headers["User-Agent"] = self.config.user_agent

            proxy = httpx.Proxy(
                url=self.config.http_proxy,
                headers=proxy_headers or None,
            )
            proxy_mounts = {
                "http://": httpx.HTTPTransport(proxy=proxy),
                "https://": httpx.HTTPTransport(proxy=proxy),
            }

        self._session = httpx.Client(
            headers=self.config.default_headers,
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
            mounts=proxy_mounts,
            trust_env=self.config.proxy_trust_env,
            limits=limits,
        )

    def _handle_response(self, response: httpx.Response) -> HTTPResponse:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as err:
            status = response.status_code
            if status == 401:
                raise AuthenticationError(message="Authentication failed", status_code=401, response=response) from err
            if status == 403:
                raise AuthorizationError(message="Authorization failed", status_code=403, response=response) from err
            if status == 429:
                raise RateLimitError(message="Rate limit exceeded", status_code=429, response=response) from err
            if 400 <= status < 500:
                raise RequestError(message=f"Client error: {err}", status_code=status, response=response) from err
            if 500 <= status < 600:
                raise APIError(message=f"Server error: {err}", status_code=status, response=response) from err
            raise ResponseError(
                message=f"HTTP error occurred: {err}",
                status_code=status,
                response=response,
            ) from err

        return HTTPResponse(raw=response)

    def _handle_rate_limit(self) -> None:
        if self.config.rate_limit:
            current_time = time.time()
            time_since_last = current_time - self._last_request_time
            if time_since_last < 1.0 / self.config.rate_limit:
                time.sleep(1.0 / self.config.rate_limit - time_since_last)
            self._last_request_time = time.time()

    def request(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        check_response_statut: bool = True,
        **kwargs,
    ) -> HTTPResponse:
        url = self._build_url(url)
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
            if check_response_statut:
                return self._handle_response(response)
            else:
                return HTTPResponse(raw=response)

        except httpx.TimeoutException as e:
            raise TimeoutError(message=f"Request timed out: {e}") from e
        except httpx.NetworkError as e:
            raise ConnectionError(message=f"Network error occurred: {e}") from e
        except httpx.HTTPError as e:
            raise HTTPClientError(message=f"HTTP error occurred: {e}") from e
        except HTTPClientError:
            # Preserve domain-specific errors (e.g. RateLimitError 429) for retry logic.
            raise
        except Exception as e:
            raise HTTPClientError(message=f"An unexpected error occurred: {e}") from e

    def close(self) -> None:
        if self._session:
            self._session.close()


class RequestsClient(HttpInterface):
    """Requests-based HTTP client implementation."""

    def __init__(self, config: ClientConfig) -> None:
        super().__init__(config)
        self._setup_client()

    def _setup_client(self) -> None:
        self._session = requests.Session()
        self._session.trust_env = self.config.proxy_trust_env

        if self.config.default_headers:
            self._session.headers.update(self.config.default_headers)

        if self.config.proxy:
            proxies = {"http": self.config.proxy, "https": self.config.proxy}
            self._session.proxies.update(proxies)
            if self.config.user_agent:
                https_adapter = cast(
                    "HTTPAdapter",
                    self._session.get_adapter(url="https://"),
                )
                https_adapter.proxy_manager_for(self.config.http_proxy).proxy_headers[
                    "User-Agent"
                ] = self.config.user_agent

    def _handle_response(self, response: requests.Response) -> HTTPResponse:
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            status = response.status_code
            if status == 401:
                raise AuthenticationError(message="Authentication failed", status_code=401, response=response) from err
            if status == 403:
                raise AuthorizationError(message="Authorization failed", status_code=403, response=response) from err
            if status == 429:
                raise RateLimitError(message="Rate limit exceeded", status_code=429, response=response) from err
            if 400 <= status < 500:
                raise RequestError(message=f"Client error: {err}", status_code=status, response=response) from err
            if 500 <= status < 600:
                raise APIError(message=f"Server error: {err}", status_code=status, response=response) from err
            raise ResponseError(
                message=f"HTTP error occurred: {err}",
                status_code=status,
                response=response,
            ) from err

        return HTTPResponse(raw=response)

    def request(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        check_response_statut: bool = True,
        **kwargs,
    ) -> HTTPResponse:
        url = self._build_url(url)

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
            if check_response_statut:
                return self._handle_response(response)
            else:
                return HTTPResponse(raw=response)

        except requests.Timeout as e:
            raise TimeoutError(message=f"Request timed out: {e}") from e
        except requests.ConnectionError as e:
            raise ConnectionError(message=f"Connection error occurred: {e}") from e
        except requests.RequestException as e:
            raise HTTPClientError(message=f"HTTP error occurred: {e}") from e
        except HTTPClientError:
            # Preserve domain-specific errors (e.g. RateLimitError 429) for retry logic.
            raise
        except Exception as e:
            raise HTTPClientError(message=f"An unexpected error occurred: {e}") from e

    def close(self) -> None:
        if self._session:
            self._session.close()
