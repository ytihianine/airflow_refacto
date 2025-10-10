from datetime import timedelta
from airflow.decorators import dag, task_group
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.sender import MailStatus, create_airflow_callback
from utils.config.tasks import get_s3_keys_source, get_projet_config
from utils.tasks.sql import (
    LoadStrategy,
    create_tmp_tables,
    ensure_partition,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    refresh_views,
    delete_tmp_tables,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.sg.siep.mmsi.oad.caracteristiques.tasks import (
    validate_params,
    oad_carac_to_parquet,
    tasks_oad_caracteristiques,
)
from dags.sg.siep.mmsi.oad.indicateurs.tasks import (
    oad_indic_to_parquet,
    tasks_oad_indicateurs,
)


# Mails
nom_projet = "Outil aide diagnostic"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/cgefi/barometre?ref_type=heads"  # noqa
LINK_DOC_DONNEE = "Non-défini"  # noqa


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
    "outil_aide_diagnostic",
    schedule_interval="*/15 6-22 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des données de l'immobilier. Base""",
    default_args=default_args,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "siep",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": ["yanis.tihianine@finances.gouv.fr"],
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
)
def oad():
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
        on_success_callback=create_airflow_callback(
            mail_status=MailStatus.START,
        ),
    )

    @task_group
    def convert_file_to_parquet():
        chain(
            [
                oad_carac_to_parquet(),
                oad_indic_to_parquet(),
            ]
        )

    # end_task = EmptyOperator(
    #     task_id="end_task",
    #     on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
    # )

    # Ordre des tâches
    chain(
        validate_params(),
        looking_for_files,
        convert_file_to_parquet(),
        tasks_oad_caracteristiques(),
        tasks_oad_indicateurs(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        ensure_partition(),
        copy_tmp_table_to_real_table(
            load_strategy=LoadStrategy.APPEND,
        ),
        # refresh_views(),
        # copy_s3_files(bucket="dsci"),
        # del_s3_files(bucket="dsci"),
        delete_tmp_tables(),
        # end_task,
    )


oad()
