from datetime import timedelta

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.sender import create_airflow_callback, MailStatus

from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
    ensure_partition,
    LoadStrategy,
    create_projet_snapshot,
    get_projet_snapshot,
    refresh_views,
    # set_dataset_last_update_date,
)
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import get_s3_keys_source, get_projet_config

from dags.cbcm.chorus.tasks import source_files, validate_params


# Mails
nom_projet = "Données comptable"
LINK_DOC_PIPELINE = ""  # noqa
LINK_DOC_DATA = ""  # noqa


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


# Définition du DAG
@dag(
    "cbcm_chorus",
    schedule_interval="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["CBCM", "DEV", "CHORUS"],
    description="Traitement des données comptables issues de Chorus",  # noqa
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
def cbcm_chorus():
    """Task definition"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_airflow_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_airflow_callback(mail_status=MailStatus.START),
    )

    # Ordre des tâches
    chain(
        validate_params(),
        looking_for_files,
        create_projet_snapshot(nom_projet=nom_projet),
        # get_projet_snapshot(nom_projet=nom_projet),
        source_files(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        ensure_partition(),
        copy_tmp_table_to_real_table(
            load_strategy=LoadStrategy.APPEND,
        ),
        refresh_views(),
        # copy_s3_files(bucket="dsci"),
        # del_s3_files(bucket="dsci"),
        # delete_tmp_tables(),
        # set_dataset_last_update_date(
        #     dataset_ids=[49, 50, 51, 52, 53, 54],
        # ),
    )


cbcm_chorus()
