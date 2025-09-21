from airflow.decorators import dag
from datetime import timedelta
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.config.tasks import get_projet_config
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.sg.siep.mmsi.api_operat.task import taches


needs_debug = False
if needs_debug:
    from http.client import HTTPConnection  # py3

    HTTPConnection.debuglevel = 1


nom_projet = "API Opera"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/api_operat?ref_type=heads"  # noqa
LINK_DOC_DONNEE = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa

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
    "api_operat_ademe",
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "ADEME"],
    description="Pipeline qui réalise des appels sur l'API Operat (ADEME)",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "siep",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": ["mmsi.siep@finances.gouv.fr"],
            "cc": ["labo-data@finances.gouv.fr", "yanis.tihianine@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DONNEE,
        },
    },
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
)
def api_operat_ademe():

    # Ordre des tâches
    chain(
        taches(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
    )


api_operat_ademe()
