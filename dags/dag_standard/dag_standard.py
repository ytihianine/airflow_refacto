from datetime import timedelta

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.smtp.config import MailStatus
from infra.smtp.sender import create_airflow_callback

from utils.tasks.sql import (
    get_project_config,
    get_tbl_names_from_postgresql,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    set_dataset_last_update_date,
)

from utils.tasks.s3 import copy_files_to_s3, del_files_from_s3
from utils.config.tasks import get_s3_keys_source


LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/consommation_batiment?ref_type=heads"  # noqa
LINK_DOC_DONNEES = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa


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
    "dag_standard",
    schedule_interval=timedelta(seconds=30),
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "STANDARD"],
    description="Dag qui sert de standard pour l'ensemble des dags.",  # noqa
    default_args=default_args,
    params={
        "nom_projet": "Dag standard",
        "db": {
            "prod_schema": "siep",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": [
                "yanis.tihianine@finances.gouv.fr"
            ],  # Changed To -> to to match the code
            "cc": ["labo-data@finances.gouv.fr"],  # Changed CC -> cc to match the code
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DONNEES,
        },
    },
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
)
def consommation_des_batiments():
    # Variables
    nom_projet = "Dag standard"
    tmp_schema = "temporaire"
    prod_schema = "siep"

    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(minutes=1),
        timeout=timedelta(minutes=15),
        soft_fail=True,
        on_skipped_callback=create_airflow_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_airflow_callback(mail_status=MailStatus.START),
    )

    # Ordre des tâches
    chain(
        get_project_config(),
        looking_for_files,
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        set_dataset_last_update_date(
            dataset_ids=[894651, 7451],
        ),
        copy_files_to_s3(bucket="dsci"),
        del_files_from_s3(bucket="dsci"),
    )
