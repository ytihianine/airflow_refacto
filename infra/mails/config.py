"""Configuration for mail notifications."""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Dict, Any
from pathlib import Path


class MailStatus(Enum):
    """Mail notification status types."""

    START = "start"
    SUCCESS = "success"
    ERROR = "error"
    SKIP = "skip"
    WARNING = "warning"
    INFO = "info"


class MailPriority(Enum):
    """Mail priority levels."""

    LOW = 1
    NORMAL = 3
    HIGH = 5


@dataclass
class MailTemplate:
    """Mail template configuration."""

    name: str
    subject: str
    path: Path
    default_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MailConfig:
    """Mail configuration settings."""

    smtp_conn_id: str
    templates_dir: Path
    from_email: Optional[str] = None
    enable_ssl: bool = True
    retry_limit: int = 3
    charset: str = "utf-8"
    templates: Dict[MailStatus, MailTemplate] = field(default_factory=dict)
    default_priority: MailPriority = MailPriority.NORMAL

    def __post_init__(self):
        """Validate and process configuration after initialization."""
        if not self.templates_dir.exists():
            raise ValueError(f"Templates directory not found: {self.templates_dir}")

    @classmethod
    def from_airflow_context(cls, context: Dict[str, Any]) -> "MailConfig":
        """Create mail configuration from Airflow context."""
        params = context.get("params", {})
        mail_params = params.get("mail", {})

        return cls(
            smtp_conn_id=mail_params.get("smtp_conn_id", "smtp_default"),
            templates_dir=Path(
                mail_params.get("templates_dir", "utils/mails/templates")
            ),
            from_email=mail_params.get("from_email"),
            enable_ssl=mail_params.get("enable_ssl", True),
            retry_limit=mail_params.get("retry_limit", 3),
        )


@dataclass
class MailMessage:
    """Mail message configuration."""

    to: List[str]
    subject: str
    template_name: str
    context: Dict[str, Any]
    cc: Optional[List[str]] = None
    bcc: Optional[List[str]] = None
    priority: MailPriority = MailPriority.NORMAL
    attachments: Optional[List[Path]] = None

    def __post_init__(self):
        """Validate mail message configuration."""
        if not self.to:
            raise ValueError("Recipient list cannot be empty")
        if not self.subject:
            raise ValueError("Subject cannot be empty")
        if not self.template_name:
            raise ValueError("Template name cannot be empty")
