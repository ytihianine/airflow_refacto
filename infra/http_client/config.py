"""Configuration for HTTP client."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union
from urllib.parse import urlparse


@dataclass
class ClientConfig:
    """Configuration for HTTP client."""

    # Basic settings
    base_url: Optional[str] = None
    timeout: int = 30
    verify_ssl: bool = True

    # Authentication
    auth_token: Optional[str] = None
    auth_type: str = "Bearer"  # 'Bearer', 'Basic', etc.

    # Headers
    default_headers: Dict[str, str] = field(default_factory=dict)
    user_agent: Optional[str] = None

    # Proxy configuration
    proxy: Optional[str] = None
    proxy_auth: Optional[tuple] = None

    # Retry configuration
    max_retries: int = 3
    retry_statuses: tuple = (429, 500, 502, 503, 504)
    retry_methods: tuple = ("GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE")

    # Rate limiting
    rate_limit: Optional[int] = None  # requests per second

    def __post_init__(self):
        """Validate and process the configuration after initialization."""
        # Process base URL
        if self.base_url:
            parsed = urlparse(self.base_url)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError("base_url must be a valid URL with scheme and domain")

        # Process proxy
        if self.proxy:
            if not self.proxy.startswith(("http://", "https://")):
                self.proxy = f"http://{self.proxy}"

        # Process headers
        if self.user_agent:
            self.default_headers["User-Agent"] = self.user_agent

        # Process auth token
        if self.auth_token:
            self.default_headers["Authorization"] = (
                f"{self.auth_type} {self.auth_token}"
            )

    @property
    def proxies(self) -> Dict[str, str]:
        """Get proxy configuration dictionary."""
        if not self.proxy:
            return {}
        return {"http": self.proxy, "https": self.proxy}

    def with_updates(self, **kwargs) -> "ClientConfig":
        """Create a new config with updated values."""
        new_data = {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }
        new_data.update(kwargs)
        return ClientConfig(**new_data)
