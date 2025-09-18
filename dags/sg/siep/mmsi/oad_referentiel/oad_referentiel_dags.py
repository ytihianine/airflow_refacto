from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import (
    get_s3_keys_source,
    get_projet_config,
)

from dags.sg.siep.mmsi.oad_referentiel.tasks import bien_typologie


# Mails
nom_projet = "Outil aide diagnostic - référentiel"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/cgefi/barometre?ref_type=heads"  # noqa
LINK_DOC_DATA = ""  # noqa


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# Définition du DAG
@dag(
    "outil_aide_diagnostic_referentiel",
    schedule_interval=None,  # timedelta(seconds=30),
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des référentiels issus de l'OAD.""",
    max_consecutive_failed_dag_runs=1,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "siep",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": [
                "brigitte.lekime@finances.gouv.fr",
                "yanis.tihianine@finances.gouv.fr",
            ],
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
    default_args=default_args,
)
def oad_referentiel():
    """Task definition"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),  # timedelta(minutes=1),
        timeout=timedelta(minutes=1),
        soft_fail=True,
        on_skipped_callback=create_airflow_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_airflow_callback(
            mail_status=MailStatus.START,
        ),
    )

    """ Task order """
    chain(
        looking_for_files,
        bien_typologie(),
        create_tmp_tables(),
        import_file_to_db.expand(storage_row=get_projet_config(nom_projet=nom_projet)),
        copy_tmp_table_to_real_table(),
        copy_s3_files(
            bucket="dsci",
        ),
        del_s3_files(
            bucket="dsci",
        ),
    )


oad_referentiel()
