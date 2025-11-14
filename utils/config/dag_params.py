from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from typing import Optional

from utils.config.types import DBParams, MailParams

DEFAULT_OWNER = "airflow"
DEFAULT_EMAIL_TO = ["yanis.tihianine@finances.gouv.fr"]
DEFAULT_EMAIL_CC = ["labo-data@finances.gouv.fr"]
DEFAULT_TMP_SCHEMA = "temporaire"


def get_project_name(context: dict) -> str:
    """Extract and validate project name from context."""
    nom_projet = context.get("params", {}).get("nom_projet")
    if not nom_projet:
        raise ValueError("nom_projet must be defined in DAG parameters")
    return nom_projet


def get_execution_date(context: dict) -> datetime:
    """Extract and validate execution date from context."""
    execution_date = context.get("execution_date")

    if not execution_date or not isinstance(execution_date, datetime):
        raise ValueError("Invalid execution date in Airflow context")

    return execution_date


def get_db_info(context: dict) -> DBParams:
    """Extract and validate database info from context."""
    db_params = context.get("params", {}).get("db", {})
    prod_schema = db_params.get("prod_schema")
    tmp_schema = db_params.get("tmp_schema")

    if not prod_schema:
        raise ValueError("prod_schema must be defined in DAG parameters under db")
    if not tmp_schema:
        raise ValueError("tmp_schema must be defined in DAG parameters under db")

    return {
        "prod_schema": prod_schema,
        "tmp_schema": tmp_schema,
    }


def get_mail_info(context: dict) -> MailParams:
    """Extract and validate database info from context."""
    mail_params = context.get("params", {}).get("mail", {})
    mail_enabled = mail_params.get("enable", False)
    mail_to = mail_params.get("to")
    mail_cc = mail_params.get("cc")
    mail_bcc = mail_params.get("bcc")

    if mail_enabled is None:
        raise ValueError(
            "mail_enabled must be defined in DAG parameters under mail section"
        )
    if not mail_to:
        raise ValueError("mail_to must be defined in DAG parameters under mail section")

    return {"enable": mail_enabled, "to": mail_to, "cc": mail_cc, "bcc": mail_bcc}


def create_default_args(
    retries: int = 0, retry_delay: Optional[timedelta] = None, **kwargs
) -> dict:
    """Create standard default_args for DAGs."""
    args = {
        "owner": DEFAULT_OWNER,
        "depends_on_past": False,
        "start_date": days_ago(1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retries,
    }
    if retry_delay:
        args["retry_delay"] = retry_delay
    args.update(kwargs)
    return args


def create_dag_params(
    nom_projet: str,
    prod_schema: str,
    lien_pipeline: str = "Non renseigné",
    lien_donnees: str = "Non renseigné",
    tmp_schema: str = DEFAULT_TMP_SCHEMA,
    mail_enable: bool = True,
    mail_to: Optional[list[str]] = None,
    mail_cc: Optional[list[str]] = None,
) -> dict:
    """Create standard params for DAGs."""
    if mail_to is None:
        mail_to = DEFAULT_EMAIL_TO

    if isinstance(mail_cc, list):
        mail_cc = list(set(mail_cc + DEFAULT_EMAIL_CC))
    if mail_cc is None:
        mail_cc = DEFAULT_EMAIL_CC

    return {
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": prod_schema,
            "tmp_schema": tmp_schema,
        },
        "mail": {
            "enable": mail_enable,
            "to": mail_to,
            "cc": mail_cc,
        },
        "docs": {
            "lien_pipeline": lien_pipeline,
            "lien_donnees": lien_donnees,
        },
    }
