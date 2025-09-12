import os
from enum import Enum, auto
from jinja2 import Environment, FileSystemLoader
from airflow.utils.state import TaskInstanceState
from airflow.providers.smtp.hooks.smtp import SmtpHook


from utils.common.vars import get_root_folder


class MailStatus(Enum):
    START = auto()
    SUCCESS = auto()
    ERROR = auto()
    SKIP = auto()


# Path des templates HTML pour les mails
class MailTemplateName(str, Enum):
    mail_start_path = "pipeline_start.html"
    mail_end_success_path = "pipeline_end_success.html"
    mail_end_error_path = "pipeline_end_error.html"
    mail_logs_recap_path = "logs_recap.html"


class MAIL_OBJET(str, Enum):
    start = "[DEBUT] - Lancement de la pipeline"
    end_success = "[FIN] - Fin de la pipeline"
    end_error = "[ECHEC] - Une erreur est survenue dans la pipeline"
    logs_recap = "Notification - Nettoyage des logs"


def render_template(template_name: str, template_parameters: dict) -> str:
    root_folder = get_root_folder()
    template_dir = os.path.join(
        root_folder,
        "utils",
        "mails",
        "templates",
    )
    # Set up the Jinja environment with a loader pointing to the templates directory
    env = Environment(loader=FileSystemLoader(template_dir))

    # Load the template by name (just the filename, not full path)
    template = env.get_template(template_name)

    # Render with context
    return template.render(**template_parameters)


def send_mail(mail_objet: str, To: list[str], CC: list[str], html_content: str) -> None:
    """
    Simple function to send email
    """
    smtp_hook = SmtpHook(smtp_conn_id="smtp_nubonyxia")

    with smtp_hook as smtp:
        smtp.send_email_smtp(
            to=To,
            cc=CC,
            subject=mail_objet,
            html_content=html_content,
            from_email=smtp.smtp_user,
        )


def make_mail_func_callback(mail_statut: MailStatus):
    def _callback(context):
        # If debug mode is ON, we don't want to send any mail
        send_mail: bool = context.get("params").get("mail").get("enable", False)
        if not send_mail:
            print("Skipping! Debug mode is active.")
            return

        To: list[str] = context.get("params").get("mail").get("To")
        CC: list[str] = context.get("params").get("mail").get("To")

        if mail_statut == MailStatus.START:
            send_mail_start(context, To, CC)
        elif mail_statut == MailStatus.SUCCESS:
            send_mail_success(context, To, CC)
        elif mail_statut == MailStatus.ERROR:
            send_mail_error(context, To, CC)
        elif mail_statut == MailStatus.SKIP:
            send_mail_skip(context)

    return _callback


def send_mail_success(
    context: dict[str, any],
    To: list[str],
    CC: list[str],
):
    template_name = MailTemplateName.mail_end_success_path.value
    mail_objet = MAIL_OBJET.end_success.value

    # Render template
    html_content = render_template(
        template_name=template_name,
        template_parameters={
            "pipeline_name": context["dag"].dag_id,
            "pipeline_statut": "Terminé",
            "start_date": context["execution_date"],
            "link_documentation_pipeline": context["params"]["docs"]["lien_pipeline"],
            "link_documentation_donnees": context["params"]["docs"]["lien_donnees"],
        },
    )

    # Send mail
    send_mail(mail_objet=mail_objet, To=To, CC=CC, html_content=html_content)


def send_mail_error(context: dict[str, any], To: list[str], CC: list[str]):
    template_name = MailTemplateName.mail_end_error_path.value
    mail_objet = MAIL_OBJET.end_error.value

    # Check s'il y a eu une erreur
    print(context.keys())
    error_msg = context.get("exception", None)
    if error_msg:
        error_msg = str(error_msg)
        print(f"error_msg: {error_msg}")

    # Render template
    html_content = render_template(
        template_name=template_name,
        template_parameters={
            "pipeline_name": context["dag"].dag_id,
            "pipeline_statut": "Échec",
            "start_date": context["execution_date"],
            "link_documentation_pipeline": context["params"][
                "link_documentation_pipeline"
            ],
            "link_documentation_donnees": context["params"][
                "link_documentation_donnees"
            ],
            "error_msg": error_msg,
        },
    )

    # Send mail
    send_mail(
        mail_objet=mail_objet,
        To=To,
        CC=CC,
        html_content=html_content,
    )


def send_mail_start(context: dict[str, any], To: list[str], CC: list[str]):
    template_name = MailTemplateName.mail_start_path.value
    mail_objet = MAIL_OBJET.start.value

    task_instance = context.get("task_instance")
    if task_instance:
        # For some reason, if a task is skipped
        # the dag or the task is still considered as successful fome times
        if (
            task_instance.state == TaskInstanceState.SKIPPED.value
            or task_instance.state == TaskInstanceState.RUNNING.value
        ):
            print("Task is skipped or being skipped. No email should be sent.")
            return
        else:
            # Render template
            html_content = render_template(
                template_name=template_name,
                template_parameters={
                    "pipeline_name": context["dag"].dag_id,
                    "pipeline_statut": "En cours",
                    "start_date": context["execution_date"],
                    "link_documentation_pipeline": context["params"][
                        "link_documentation_pipeline"
                    ],
                    "link_documentation_donnees": context["params"][
                        "link_documentation_donnees"
                    ],
                },
            )

            # Send mail
            send_mail(
                mail_objet=mail_objet,
                To=To,
                CC=CC,
                html_content=html_content,
            )


def send_mail_skip(context: dict[str, any]):
    """ "No need to send a mail if task or dag is skipped !"""
    print("Mail has been skipped !")


def send_notification_mail(
    To: list[str],
    CC: list[str],
    link_documentation_pipeline: str,
    files_paths: list[str],
    **context,
):
    values = context["ti"].xcom_pull(key="return_value", task_ids="return_xcom")
    nb_runs_id_deleted = values["runs_id_deleted"]
    nb_folders_deleted = values["folders_deleted"]
    nb_files_deleted = values["files_deleted"]

    send_mail(
        template_path=MailTemplateName.mail_logs_recap_path.value,
        mail_objet=MAIL_OBJET.logs_recap.value,
        To=To,
        CC=CC,
        files=files_paths,
        link_documentation_pipeline=link_documentation_pipeline,
        context=context,
        nb_runs_id_deleted=nb_runs_id_deleted,
        nb_folders_deleted=nb_folders_deleted,
        nb_files_deleted=nb_files_deleted,
    )
