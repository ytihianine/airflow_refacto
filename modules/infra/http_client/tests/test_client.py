"""Tests for HttpInterface (base HTTP client interface)."""

from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest

from modules.infra.http_client.base import HttpInterface
from modules.infra.http_client.config import ClientConfig
from modules.infra.http_client.exceptions import RateLimitError, RequestError
from modules.infra.http_client.types import HTTPResponse


class ConcreteHttpClient(HttpInterface):
    """Minimal concrete implementation of HttpInterface for testing."""

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self.last_call: dict[str, Any] = {}

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
        self.last_call = {
            "method": method,
            "url": url,
            "endpoint": urlsplit(url).path,
            "params": params,
            "data": data,
            "json": json,
            "headers": headers,
            "timeout": timeout,
            **kwargs,
        }
        raw = MagicMock()
        raw.status_code = 200
        raw.json.return_value = {}
        return HTTPResponse(raw=raw)

    def close(self) -> None:
        self._session = None


@pytest.fixture
def config() -> ClientConfig:
    return ClientConfig(base_url="https://api.example.com/")


@pytest.fixture
def client(config: ClientConfig) -> ConcreteHttpClient:
    return ConcreteHttpClient(config=config)


class TestInit:
    def test_stores_config(self, config: ClientConfig) -> None:
        client = ConcreteHttpClient(config=config)
        assert client.config is config

    def test_session_initially_none(self, client: ConcreteHttpClient) -> None:
        assert client._session is None


class TestBuildUrl:
    def test_with_base_url(self, client: ConcreteHttpClient) -> None:
        url = client._build_url("/users")
        assert url == "https://api.example.com/users"

    def test_with_base_url_trailing_slash(self, client: ConcreteHttpClient) -> None:
        url = client._build_url("users/123")
        assert url == "https://api.example.com/users/123"

    def test_without_base_url(self) -> None:
        config = ClientConfig(base_url=None)
        client = ConcreteHttpClient(config=config)
        url = client._build_url("https://other.com/endpoint")
        assert url == "https://other.com/endpoint"


class TestGet:
    def test_delegates_to_request(self, client: ConcreteHttpClient) -> None:
        client.get("/users", params={"page": "1"})
        assert client.last_call["method"] == "GET"
        assert client.last_call["endpoint"] == "/users"
        assert client.last_call["params"] == {"page": "1"}

    def test_params_default_none(self, client: ConcreteHttpClient) -> None:
        client.get("/users")
        assert client.last_call["params"] is None

    def test_returns_http_response(self, client: ConcreteHttpClient) -> None:
        result = client.get("/users")
        assert isinstance(result, HTTPResponse)

    def test_retries_on_rate_limit_429(self, client: ConcreteHttpClient) -> None:
        attempts = 0

        def side_effect(*_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                response = MagicMock()
                response.headers = {"Retry-After": "0"}
                raise RateLimitError(
                    message="Rate limit exceeded",
                    status_code=429,
                    response=response,
                )

            raw = MagicMock()
            raw.status_code = 200
            raw.json.return_value = {}
            return HTTPResponse(raw=raw)

        client.request = MagicMock(side_effect=side_effect)

        response = client.get("/users")

        assert response.status_code == 200
        assert attempts == 3

    def test_does_not_retry_on_non_429_error(self, client: ConcreteHttpClient) -> None:
        attempts = 0

        def side_effect(*_args, **_kwargs):
            nonlocal attempts
            attempts += 1
            raise RequestError(message="Bad request", status_code=400)

        client.request = MagicMock(side_effect=side_effect)

        with pytest.raises(RequestError):
            client.get("/users")

        assert attempts == 1


class TestPost:
    def test_delegates_to_request(self, client: ConcreteHttpClient) -> None:
        client.post("/users", json={"name": "test"})
        assert client.last_call["method"] == "POST"
        assert client.last_call["endpoint"] == "/users"
        assert client.last_call["json"] == {"name": "test"}

    def test_json_defaults_to_none(self, client: ConcreteHttpClient) -> None:
        client.post("/users")
        assert client.last_call["json"] is None
        assert client.last_call["data"] is None

    def test_with_data(self, client: ConcreteHttpClient) -> None:
        client.post("/upload", data=b"raw bytes")
        assert client.last_call["data"] == b"raw bytes"


class TestPut:
    def test_delegates_to_request(self, client: ConcreteHttpClient) -> None:
        client.put("/users/1", json={"name": "updated"})
        assert client.last_call["method"] == "PUT"
        assert client.last_call["json"] == {"name": "updated"}

    def test_json_defaults_to_none(self, client: ConcreteHttpClient) -> None:
        client.put("/users/1")
        assert client.last_call["json"] is None


class TestPatch:
    def test_delegates_to_request(self, client: ConcreteHttpClient) -> None:
        client.patch("/users/1", json={"name": "patched"})
        assert client.last_call["method"] == "PATCH"
        assert client.last_call["json"] == {"name": "patched"}

    def test_json_defaults_to_none(self, client: ConcreteHttpClient) -> None:
        client.patch("/users/1")
        assert client.last_call["json"] is None


class TestDelete:
    def test_delegates_to_request(self, client: ConcreteHttpClient) -> None:
        client.delete("/users/1")
        assert client.last_call["method"] == "DELETE"
        assert client.last_call["endpoint"] == "/users/1"

    def test_forwards_kwargs(self, client: ConcreteHttpClient) -> None:
        client.delete("/users/1", custom_arg="value")
        assert client.last_call["custom_arg"] == "value"


class TestContextManager:
    def test_enter_returns_self(self, client: ConcreteHttpClient) -> None:
        with client as ctx:
            assert ctx is client

    def test_exit_calls_close(self, client: ConcreteHttpClient) -> None:
        client._session = "something"  # type: ignore
        with client:
            pass
        assert client._session is None


class TestCannotInstantiateAbstract:
    def test_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            HttpInterface(config=ClientConfig())  # type: ignore[abstract]
