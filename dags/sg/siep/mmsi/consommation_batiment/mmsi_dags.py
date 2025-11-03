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
    get_projet_snapshot,
    # set_dataset_last_update_date,
)
from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import get_s3_keys_source, get_projet_config

from dags.sg.siep.mmsi.consommation_batiment.tasks import (
    validate_params,
    conso_mens_parquet,
    source_files,
    additionnal_files,
)


# Mails
nom_projet = "Consommation des bâtiments"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/consommation_batiment?ref_type=heads"  # noqa
LINK_DOC_DATA = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa


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
    "consommation_des_batiments",
    schedule_interval="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "CONSOMMATION"],
    description="Pipeline de traitement des données de consommation des bâtiments. Source des données: OSFI",  # noqa
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
def consommation_des_batiments():
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
        get_projet_snapshot(nom_projet="Outil aide diagnostic"),
        # conso_mens_parquet(),
        source_files(),
        additionnal_files(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        ensure_partition(),
        copy_tmp_table_to_real_table(
            load_strategy=LoadStrategy.APPEND,
        ),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
        delete_tmp_tables(),
        # set_dataset_last_update_date(
        #     dataset_ids=[49, 50, 51, 52, 53, 54],
        # ),
    )


consommation_des_batiments()
