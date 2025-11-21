from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.default_smtp import create_airflow_callback, MailStatus

from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    LoadStrategy,
    get_projet_snapshot,
    refresh_views,
    # set_dataset_last_update_date,
)
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.cbcm.ref_service_prescripteur.tasks import grist_source, validate_params


# Mails
nom_projet = "Données comptable - référentiel"
LINK_DOC_PIPELINE = "Non-défini"  # noqa
LINK_DOC_DATA = "Non-défini"  # noqa


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(n=1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


# Définition du DAG
@dag(
    dag_id="cbcm_chorus",
    schedule_interval="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["CBCM", "DEV", "CHORUS"],
    description="Traitement du référentiel des services prescripteurs (données comptables)",  # noqa
    default_args=default_args,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "cbcm",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": None,
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
)
def cbcm_chorus() -> None:
    """Task definition"""

    # Ordre des tâches
    chain(
        validate_params(),
        # get_projet_snapshot(nom_projet=nom_projet),
        download_grist_doc_to_s3(
            selecteur="grist_doc", workspace_id="dsci", doc_id_key="grist_doc_id_cbcm"
        ),
        grist_source(),
        # create_tmp_tables(),
        # import_file_to_db.expand(
        #     selecteur_config=get_projet_config(nom_projet=nom_projet)
        # ),
        # copy_tmp_table_to_real_table(
        #     load_strategy=LoadStrategy.FULL_LOAD,
        # ),
        # refresh_views(),
        # copy_s3_files(bucket="dsci"),
        # del_s3_files(bucket="dsci"),
        # delete_tmp_tables(),
        # set_dataset_last_update_date(
        #     dataset_ids=[49, 50, 51, 52, 53, 54],
        # ),
    )


cbcm_chorus()
