from airflow.decorators import dag
from datetime import timedelta
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.config.tasks import get_storage_rows
from utils.tasks.sql import (
    get_project_config,
    get_tbl_names_from_postgresql,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
)

# from utils.tasks.s3 import (
#     copy_s3_files,
#     del_s3_files,
# )
from dags.sg.siep.mmsi.api_operat.task import liste_declaration, consommation_by_id

needs_debug = False
if needs_debug:
    from http.client import HTTPConnection  # py3

    HTTPConnection.debuglevel = 1

# Mails
To = ["mmsi.siep@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]
link_documentation_pipeline = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/api_operat?ref_type=heads"  # noqa
link_documentation_donnees = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa

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
        "nom_projet": "API Opera",
        "mail": {
            "enable": False,
            "To": To,
            "CC": CC,
        },
        "docs": {
            "lien_pipeline": link_documentation_pipeline,
            "lien_donnees": link_documentation_donnees,
        },
    },
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
)
def api_operat_ademe():
    nom_projet = "API Opera"
    tmp_schema = "temporaire"
    prod_schema = "siep"
    url = "https://prd-x-ademe-externe-api.de-c1.eu1.cloudhub.io"

    # Ordre des tâches
    projet_config = get_project_config()

    chain(
        projet_config,
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        liste_declaration(nom_projet=nom_projet, url=url),
        consommation_by_id(nom_projet=nom_projet, url=url),
        import_file_to_db.expand(
            storage_row=get_storage_rows(nom_projet=nom_projet).to_dict("records")
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
    )


api_operat_ademe()
