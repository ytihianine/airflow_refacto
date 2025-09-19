from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.sender import create_airflow_callback, MailStatus

from dags.applications.db_backup.tasks import validate_params, create_dump_files


LINK_DOC_PIPELINE = "TO COMPLETE"

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
    "sauvegarde_database",
    schedule_interval=timedelta(hours=12),
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "RECETTE", "SAUVEGARDE", "DATABASE"],
    description="""Pipeline qui réalise des sauvegarde de la base de données""",
    default_args=default_args,
    params={
        "nom_projet": "Sauvegarde databases",
        "mail": {
            "enable": False,
            "to": ["yanis.tihianine@finances.gouv.fr"],
            "cc": ["labo-data@finances.gouv.fr"],
        },
        "docs": {"lien_pipeline": LINK_DOC_PIPELINE},
    },
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
)
def sauvegarde_database():
    """Task order"""
    chain(validate_params(), create_dump_files())


sauvegarde_database()
