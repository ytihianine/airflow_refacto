from datetime import timedelta
from airflow.decorators import dag
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

from dags.sg.siep.mmsi.eligibilite_fcu.task import eligibilite_fcu


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}


# Mails
To = ["mmsi.siep@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/eligibilite_fcu?ref_type=heads"  # noqa
LINK_DOC_DONNEE = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa


# Définition du DAG
@dag(
    "eligibilite_fcu",
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "FCU"],
    description="Récupérer pour chaque bâtiment de l'OAD son éligibilité au réseau Franche Chaleur Urbaine (FCU)",  # noqa
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "nom_projet": "France Chaleur Urbaine (FCU)",
        "mail": {
            "enable": False,
            "To": To,
            "CC": CC,
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DONNEE,
        },
    },
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def eligibilite_fcu_dag():
    nom_projet = "France Chaleur Urbaine (FCU)"
    tmp_schema = "temporaire"
    prod_schema = "siep"

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
        eligibilite_fcu(nom_projet=nom_projet),
        import_file_to_db.expand(
            storage_row=get_storage_rows(nom_projet=nom_projet).to_dict("records")
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        # copy_s3_files.partial(
        #     s3_hook=MINIO_FILE_HANDLER, bucket=BUCKET, dest_key=S3_DEST_KEY
        # ).expand(source_key=storage_paths.loc[:, "s3_tmp_filepath"].to_list()),
        # del_s3_files(
        #     s3_hook=MINIO_FILE_HANDLER,
        #     bucket=BUCKET,
        #     s3_filepaths=storage_paths.loc[:, "s3_tmp_filepath"].to_list(),
        # ),
    )


eligibilite_fcu_dag()
