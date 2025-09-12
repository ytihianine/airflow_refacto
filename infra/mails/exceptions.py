"""Custom exceptions for mail notification system."""


class MailError(Exception):
    """Base exception for mail-related errors."""

    pass


class TemplateError(MailError):
    """Raised when there are template-related errors."""

    pass


class SendError(MailError):
    """Raised when there are errors sending mail."""

    pass


class ConfigError(MailError):
    """Raised when there are configuration errors."""

    pass
