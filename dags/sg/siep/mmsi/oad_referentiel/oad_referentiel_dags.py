from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.default_smtp import create_airflow_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args
from utils.tasks.sql import (
    LoadStrategy,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    delete_tmp_tables,
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

from dags.sg.siep.mmsi.oad_referentiel.tasks import validate_params, bien_typologie


# Mails
nom_projet = "Outil aide diagnostic - référentiel"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/cgefi/barometre?ref_type=heads"  # noqa
LINK_DOC_DATA = ""  # noqa


# Définition du DAG
@dag(
    "outil_aide_diagnostic_referentiel",
    schedule_interval=None,  # timedelta(seconds=30),
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des référentiels issus de l'OAD.""",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        prod_schema="siep",
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
        mail_enable=False,
    ),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
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
        # validate_params(),
        looking_for_files,
        bien_typologie(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(load_strategy=LoadStrategy.INCREMENTAL),
        copy_s3_files(
            bucket="dsci",
        ),
        del_s3_files(
            bucket="dsci",
        ),
        delete_tmp_tables(),
    )


oad_referentiel()
