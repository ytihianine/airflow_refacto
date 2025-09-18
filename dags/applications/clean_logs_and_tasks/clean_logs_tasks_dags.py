from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from utils.mails.mails import make_mail_func_callback, MailStatus

from dags.applications.clean_logs_and_tasks.task import (
    clean_s3,
    clean_old_logs,
    clean_skipped_logs,
)


# Mails
To = ["yanis.tihianine@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]

# Liens
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/catalogue?ref_type=heads"  # noqa
LINK_DOC_DATA = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"  # noqa
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# Définition du DAG
@dag(
    "clean_logs_tasks",
    schedule_interval="@weekly",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "LOGS"],
    description="Pipeline qui nettoie la base de données et S3",
    default_args=default_args,
    params={
        "nom_projet": "Clean tasks, logs and S3",
        "mail": {
            "enable": False,
            "To": To,
            "CC": CC,
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
    on_failure_callback=make_mail_func_callback(
        mail_statut=MailStatus.ERROR,
    ),
    on_success_callback=make_mail_func_callback(mail_statut=MailStatus.SUCCESS),
)
def clean_logs_tasks():
    # nom_projet = "Clean tasks and logs"

    """Task definitions"""
    chain(clean_s3(), clean_old_logs(), clean_skipped_logs())


clean_logs_tasks()
