from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.default_smtp import create_airflow_callback, MailStatus

from dags.applications.clean_s3.task import validate_params, clean_s3_task_group


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
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# Définition du DAG
@dag(
    "clean_s3",
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "OLD", "S3"],
    description="Pipeline qui nettoie les anciens objets du bucket S3",
    default_args=default_args,
    params={
        "nom_projet": "Clean old S3 objects",
        "mail": {
            "enable": False,
            "to": ["yanis.tihianine@finances.gouv.fr"],
            "cc": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
)
def clean_logs_tasks():
    # nom_projet = "Clean tasks and logs"

    """Task definitions"""
    chain(validate_params(), clean_s3_task_group())


clean_logs_tasks()
