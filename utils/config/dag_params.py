from datetime import timedelta
from airflow.utils.dates import days_ago
from typing import Optional

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
