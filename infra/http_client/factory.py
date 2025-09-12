"""Factory for creating HTTP clients."""

from typing import Optional, Type

from .adapters import HttpxClient
from .base import AbstractHTTPClient
from .config import ClientConfig


class HTTPClientFactory:
    """Factory for creating HTTP clients."""

    _clients = {
        "httpx": HttpxClient,
    }

    @classmethod
    def register_client(cls, name: str, client_class: Type[AbstractHTTPClient]) -> None:
        """Register a new client implementation."""
        cls._clients[name.lower()] = client_class

    @classmethod
    def create(
        cls, client_type: str = "httpx", config: Optional[ClientConfig] = None, **kwargs
    ) -> AbstractHTTPClient:
        """
        Create a new HTTP client instance.

        Args:
            client_type: Type of client to create ('httpx' by default)
            config: Client configuration
            **kwargs: Additional configuration parameters

        Returns:
            AbstractHTTPClient: Configured client instance

        Example:
            ```python
            # Create with minimal config
            client = HTTPClientFactory.create(
                base_url='https://api.example.com',
                timeout=30
            )

            # Create with full config
            config = ClientConfig(
                base_url='https://api.example.com',
                timeout=30,
                auth_token='your-token',
                rate_limit=10
            )
            client = HTTPClientFactory.create(config=config)
            ```
        """
        client_class = cls._clients.get(client_type.lower())
        if not client_class:
            raise ValueError(f"Unknown client type: {client_type}")

        # If no config provided, create one from kwargs
        if not config:
            config = ClientConfig(**kwargs)
        elif kwargs:
            # If both config and kwargs provided, merge them
            config = config.with_updates(**kwargs)

        return client_class(config)
