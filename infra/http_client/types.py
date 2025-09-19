from typing import Any, Dict, Optional


class HTTPResponse:
    """Wrapper around the raw HTTP response to provide a unified interface."""

    def __init__(self, raw: Any):
        self._raw = raw

    def json(self) -> Dict[str, Any]:
        """Return response body as JSON (if possible)."""
        try:
            return self._raw.json()
        except Exception:
            raise ValueError("Response content is not valid JSON")

    @property
    def text(self) -> str:
        """Return response body as text."""
        return getattr(self._raw, "text", str(self._raw))

    @property
    def content(self) -> bytes:
        """Return response body as raw bytes."""
        return getattr(self._raw, "content", str(self._raw).encode("utf-8"))

    @property
    def status_code(self) -> int:
        """Return HTTP status code."""
        return getattr(self._raw, "status_code", 0)

    @property
    def headers(self) -> Dict[str, Any]:
        """Return response headers."""
        return getattr(self._raw, "headers", {})

    def raise_for_status(self) -> None:
        if hasattr(self._raw, "raise_for_status"):
            self._raw.raise_for_status()
