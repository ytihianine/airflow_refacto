"""Mail sending functionality."""

from pathlib import Path
from typing import List, Optional, Dict, Any, Union, Callable
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from airflow.providers.smtp.hooks.smtp import SmtpHook

from infra.mails.config import MailConfig, MailMessage, MailStatus, MailPriority
from infra.mails.template import TemplateManager
from infra.mails.exceptions import SendError


class MailSender:
    """Handles email sending with templates and attachments."""

    def __init__(self, config: MailConfig):
        """
        Initialize mail sender.

        Args:
            config: Mail configuration
        """
        self.config = config
        self.template_manager = TemplateManager(config.templates_dir)
        self._hook = None

    @property
    def hook(self) -> SmtpHook:
        """Lazy initialization of SMTP hook."""
        if self._hook is None:
            self._hook = SmtpHook(smtp_conn_id=self.config.smtp_conn_id)
        return self._hook

    def send(self, message: MailMessage) -> None:
        """
        Send an email message.

        Args:
            message: Mail message configuration

        Raises:
            SendError: If there's an error sending the mail
        """
        try:
            # Render template
            html_content = self.template_manager.render(
                message.template_name, message.context
            )

            # Create message
            msg = MIMEMultipart("mixed")
            msg["Subject"] = message.subject
            msg["From"] = self.config.from_email or self.hook.smtp_user
            msg["To"] = ", ".join(message.to)
            if message.cc:
                msg["Cc"] = ", ".join(message.cc)
            if message.bcc:
                msg["Bcc"] = ", ".join(message.bcc)

            # Set priority
            if message.priority != MailPriority.NORMAL:
                msg["X-Priority"] = str(message.priority.value)

            # Attach HTML content
            msg.attach(MIMEText(html_content, "html", self.config.charset))

            # Add attachments
            if message.attachments:
                for attachment in message.attachments:
                    self._add_attachment(msg, attachment)

            # Get all recipients
            recipients = message.to.copy()
            if message.cc:
                recipients.extend(message.cc)
            if message.bcc:
                recipients.extend(message.bcc)

            # Send mail
            self.hook.send_email_smtp(
                to=recipients,
                subject=message.subject,
                html_content=html_content,
                from_email=msg["From"],
            )

        except Exception as e:
            raise SendError(f"Error sending mail: {e}")

    def _add_attachment(self, msg: MIMEMultipart, file_path: Union[str, Path]) -> None:
        """Add an attachment to the email message."""
        path = Path(file_path)
        if not path.exists():
            raise SendError(f"Attachment not found: {path}")

        try:
            with open(path, "rb") as f:
                part = MIMEApplication(f.read(), Name=path.name)
                part["Content-Disposition"] = f'attachment; filename="{path.name}"'
                msg.attach(part)
        except Exception as e:
            raise SendError(f"Error adding attachment {path}: {e}")

    def send_status_notification(
        self,
        status: MailStatus,
        to: List[str],
        context: Dict[str, Any],
        cc: Optional[List[str]] = None,
        bcc: Optional[List[str]] = None,
        priority: Optional[MailPriority] = None,
    ) -> None:
        """
        Send a status notification email.

        Args:
            status: Status of the notification
            to: List of primary recipients
            context: Template context
            cc: List of CC recipients
            bcc: List of BCC recipients
            priority: Mail priority
        """
        template = self.template_manager.get_default_template(status)

        message = MailMessage(
            to=to,
            subject=template.subject,
            template_name=template.name,
            context={**template.default_context, **context},
            cc=cc,
            bcc=bcc,
            priority=priority or self.config.default_priority,
        )

        self.send(message)


def create_airflow_callback(mail_status: MailStatus) -> Callable:
    """
    Create an Airflow callback function for mail notifications.

    Args:
        mail_status: Status to send notification for

    Returns:
        Callable: Callback function for Airflow
    """

    def callback(context: Dict[str, Any]) -> None:
        # Get mail configuration from context
        try:
            config = MailConfig.from_airflow_context(context)
            sender = MailSender(config)

            # Get mail configuration from context
            params = context.get("params", {}).get("mail", {})

            # Check if mail notifications are enabled
            if not params.get("enable", False):
                print("Mail notifications are disabled for this DAG")
                return

            to = params.get("to", [])
            cc = params.get("cc", [])

            # Get task information
            task = context.get("task_instance")
            dag = context.get("dag")

            # Create context for template
            template_context = {
                "task": task,
                "dag": dag,
                "execution_date": context.get("execution_date"),
                "next_execution_date": context.get("next_execution_date"),
                "params": context.get("params", {}),
            }

            # Send notification
            sender.send_status_notification(
                status=mail_status,
                to=to,
                cc=cc,
                context=template_context,
                priority=MailPriority.HIGH if mail_status == MailStatus.ERROR else None,
            )

        except Exception as e:
            # Log error but don't raise to prevent DAG failure
            print(f"Error sending mail notification: {e}")

    return callback
