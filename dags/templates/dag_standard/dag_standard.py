from datetime import timedelta

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from utils.mails.mails import make_mail_func_callback, MailStatus

from utils.common.tasks_sql import (
    get_project_config,
    get_tbl_names_from_postgresql,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    set_dataset_last_update_date,
)

from utils.common.tasks_minio import (
    copy_files_to_minio,
    del_files_from_minio,
)
from utils.common.config_func import get_s3_keys_source


link_documentation_pipeline = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/consommation_batiment?ref_type=heads"  # noqa
link_documentation_donnees = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa


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
        "mail": {
            "enable": False,
            "To": ["yanis.tihianine@finances.gouv.fr"],
            "CC": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": link_documentation_pipeline,
            "lien_donnees": link_documentation_donnees,
        },
    },
    on_failure_callback=make_mail_func_callback(mail_statut=MailStatus.ERROR),
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
        on_skipped_callback=make_mail_func_callback(mail_statut=MailStatus.SKIP),
        on_success_callback=make_mail_func_callback(mail_statut=MailStatus.START),
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
        copy_files_to_minio(bucket="dsci"),
        del_files_from_minio(bucket="dsci"),
    )
