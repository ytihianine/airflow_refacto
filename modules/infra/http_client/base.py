from abc import ABC, abstractmethod
from typing import Any
from urllib.parse import urljoin

from tenacity import RetryCallState, retry, retry_if_exception, stop_after_attempt

from modules.infra.http_client.config import ClientConfig
from modules.infra.http_client.exceptions import RateLimitError
from modules.infra.http_client.types import HTTPResponse

DEFAULT_RETRY_AFTER_SECONDS = 2
MAX_RETRY_WAIT_SECONDS = 120
MAX_RETRY_ATTEMPTS = 5


def _is_rate_limit_error(exception: BaseException) -> bool:
    return isinstance(exception, RateLimitError) or (getattr(exception, "status_code", None) == 429)


def _retry_wait_from_429(retry_state: RetryCallState) -> float:
    attempt = max(retry_state.attempt_number, 1)
    wait_seconds = min(
        DEFAULT_RETRY_AFTER_SECONDS * (2 ** (attempt - 1)),
        MAX_RETRY_WAIT_SECONDS,
    )

    if not retry_state.outcome:
        return float(wait_seconds)

    exception = retry_state.outcome.exception()
    if not exception:
        return float(wait_seconds)

    response = getattr(exception, "response", None)
    headers = getattr(response, "headers", {}) if response else {}
    retry_after = headers.get("Retry-After") if hasattr(headers, "get") else None

    if retry_after is None:
        return float(wait_seconds)

    try:
        wait_seconds = max(wait_seconds, max(int(retry_after), 0))
        return float(min(wait_seconds, MAX_RETRY_WAIT_SECONDS))
    except (TypeError, ValueError):
        return float(wait_seconds)


class HttpInterface(ABC):
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
        url: str,
        params: dict[str, Any] | None = None,
        data: Any | None = None,
        json: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
        check_response_statut: bool = True,
        **kwargs,
    ) -> HTTPResponse:
        """Make an HTTP request."""
        raise NotImplementedError

    @retry(
        retry=retry_if_exception(predicate=_is_rate_limit_error),
        wait=_retry_wait_from_429,
        stop=stop_after_attempt(max_attempt_number=MAX_RETRY_ATTEMPTS),
        reraise=True,
    )
    def get(self, url: str, params: dict[str, Any] | None = None, **kwargs) -> HTTPResponse:
        return self.request(method="GET", url=url, params=params, **kwargs)

    @retry(
        retry=retry_if_exception(_is_rate_limit_error),
        wait=_retry_wait_from_429,
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        reraise=True,
    )
    def post(
        self,
        url: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        **kwargs,
    ) -> HTTPResponse:
        return self.request(method="POST", url=url, data=data, json=json, **kwargs)

    @retry(
        retry=retry_if_exception(_is_rate_limit_error),
        wait=_retry_wait_from_429,
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        reraise=True,
    )
    def put(
        self,
        url: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        **kwargs,
    ) -> HTTPResponse:
        return self.request(method="PUT", url=url, data=data, json=json, **kwargs)

    @retry(
        retry=retry_if_exception(_is_rate_limit_error),
        wait=_retry_wait_from_429,
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        reraise=True,
    )
    def patch(
        self,
        url: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        **kwargs,
    ) -> HTTPResponse:
        return self.request(method="PATCH", url=url, data=data, json=json, **kwargs)

    @retry(
        retry=retry_if_exception(_is_rate_limit_error),
        wait=_retry_wait_from_429,
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
        reraise=True,
    )
    def delete(self, url: str, **kwargs) -> HTTPResponse:
        return self.request(method="DELETE", url=url, **kwargs)

    @abstractmethod
    def close(self) -> None:
        """Close the client and release resources."""
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
