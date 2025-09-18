from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.models import Variable

from utils.mails.mails import make_mail_func_callback, MailStatus

from dags.applications.db_backup.tasks import create_dump_files


To = ["yanis.tihianine@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]

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
            "To": To,
            "CC": CC,
        },
        "docs": {
            "lien_pipeline": "",
            "lien_donnees": "",
        },
    },
    on_failure_callback=make_mail_func_callback(
        mail_statut=MailStatus.ERROR,
    ),
    on_success_callback=make_mail_func_callback(mail_statut=MailStatus.SUCCESS),
)
def sauvegarde_database():
    # Variables
    # Database
    databases = Variable.get("db_main_databases")  # Must be parsed

    # databases = "airflow_config;chartsgouv_mef_sg_config;data_store"
    db_names = [db_name.strip() for db_name in databases.split(";")]

    """ Task order """
    chain(
        create_dump_files.partial().expand(
            db_name=db_names,
        )
    )


sauvegarde_database()
