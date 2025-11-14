import os
from enum import Enum
import textwrap
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from jinja2 import Environment, FileSystemLoader

from airflow.utils.email import send_email_smtp

from utils.config.dag_params import get_mail_info
from utils.config.vars import get_root_folder, DEFAULT_SMTP_CONN_ID


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

    NORMAL = 0
    LOW = 1
    HIGH = 2


default_mail_config = {
    MailStatus.START: {
        "template_path": "pipeline_start.html",
        "subject": "[DEBUT] - Lancement de la pipeline",
        "priority": MailPriority.NORMAL,
    },
    MailStatus.SUCCESS: {
        "template_path": "pipeline_end_success.html",
        "subject": "[FIN] - Fin de la pipeline",
        "priority": MailPriority.NORMAL,
    },
    MailStatus.ERROR: {
        "template_path": "pipeline_end_error.html",
        "subject": "[ECHEC] - Une erreur est survenue dans la pipeline",
        "priority": MailPriority.HIGH,
    },
    MailStatus.SKIP: {
        "template_path": "",
        "subject": "",
        "priority": MailPriority.NORMAL,
    },
    MailStatus.WARNING: {
        "template_path": "",
        "subject": "",
        "priority": MailPriority.NORMAL,
    },
    MailStatus.INFO: {
        "template_path": "logs_recap.html",
        "subject": "Notification - Nettoyage des logs",
        "priority": MailPriority.LOW,
    },
}


@dataclass
class MailMessage:
    to: list[str]
    mail_status: Optional[MailStatus] = None
    subject: Optional[str] = None
    html_content: Optional[str] = None
    template_parameters: dict = field(default_factory=dict)
    cc: Optional[list[str]] = None
    bcc: Optional[list[str]] = None
    from_email: Optional[str] = None
    files: Optional[list[str]] = None
    custom_headers: Optional[dict[str, Any]] = None

    def __post_init__(self) -> None:
        # Cas 1: mail_status est précisé -> génération automatique du template
        if self.mail_status is not None:
            config = default_mail_config.get(self.mail_status)

            if not config or not config.get("template_path"):
                raise ValueError(
                    f"Aucun template configuré pour le statut {self.mail_status.value}"
                )

            self.html_content = render_template(
                template_name=config["template_path"],
                template_parameters=self.template_parameters,
            )
            self.subject = config["subject"]
            self.custom_headers = {"X-Priority": config["priority"]}

        # Cas 2: mail_status n'est pas précisé -> html_content doit être fourni
        elif self.html_content is None or self.subject is None:
            raise ValueError(
                "html_content et subject doivent être fournis si mail_status n'est pas précisé"
            )


def render_template(template_name: str, template_parameters: dict) -> str:
    root_folder = get_root_folder()
    template_dir = os.path.join(
        root_folder,
        "infra",
        "mails",
        "templates",
    )
    # Set up the Jinja environment with a loader pointing to the templates directory
    env = Environment(loader=FileSystemLoader(searchpath=template_dir))

    # Load the template by name (just the filename, not full path)
    template = env.get_template(name=template_name)

    # Render with context
    return template.render(**template_parameters)


def send_mail(mail_message: MailMessage, conn_id: str = DEFAULT_SMTP_CONN_ID) -> None:

    if not mail_message.subject or not mail_message.html_content:
        raise ValueError("html_content et subject undefined ...")

    send_email_smtp(
        conn_id=conn_id,
        to=mail_message.to,
        cc=mail_message.cc,
        bcc=mail_message.bcc,
        subject=mail_message.subject,
        html_content=mail_message.html_content,
        files=mail_message.files,
        custom_headers=mail_message.custom_headers,
    )


def create_airflow_callback(mail_status: MailStatus) -> Callable:
    """
    Create an Airflow callback function for mail notifications.

    Args:
        mail_status: Status to send notification for

    Returns:
        Callable: Callback function for Airflow
    """

    def _callback(context: dict[str, Any]) -> None:
        # If debug mode is ON, we don't want to send any mail
        mail_info = get_mail_info(context=context)

        if not mail_info["enable"]:
            print("Skipping! Mails are disabled for this dag ...")
            return

        mail_message = MailMessage(
            mail_status=mail_status,
            to=mail_info["to"],
            cc=mail_info["cc"],
            bcc=mail_info["bcc"],
        )

        if mail_info["enable"]:
            send_mail(mail_message=mail_message)
        else:
            print(
                textwrap.dedent(
                    text="""
            mail are not enabled for this dag !
            To enable it, update mail.enable: True in dag parameters definition
            """
                )
            )

    return _callback
