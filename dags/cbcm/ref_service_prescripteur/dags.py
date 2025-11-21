from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.default_smtp import create_airflow_callback, MailStatus

from utils.config.dag_params import create_default_args, create_dag_params
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


# Variables
nom_projet = "Données comptable - référentiel"
LINK_DOC_PIPELINE = "Non-défini"  # noqa
LINK_DOC_DATA = "Non-défini"  # noqa


# Définition du DAG
@dag(
    dag_id="chorus_service_prescripteur",
    schedule_interval="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=2,
    catchup=False,
    tags=["CBCM", "DEV", "CHORUS"],
    description="Traitement du référentiel des services prescripteurs (données comptables)",  # noqa
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        prod_schema="donnee_comptable",
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
        mail_enable=False,
    ),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def chorus_service_prescripteur() -> None:
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


chorus_service_prescripteur()
