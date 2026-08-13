from datetime import timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.fichiers.config import storage_options
from dags.dge.carto_rem.fichiers.tasks import (
    source_files,
)
from modules.common_tasks.projet import get_selecteur_config
from modules.common_tasks.s3 import (
    copy_s3_files,
    copy_staging_to_prod,
    del_iceberg_staging_table,
    del_s3_files,
    import_file_to_iceberg,
)
from modules.common_tasks.sql import create_projet_snapshot
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args
from modules.utils.config.tasks import get_list_source_fichier

# Mails
nom_projet = "Cartographie rémunération"


# Définition du DAG
@dag(
    dag_id="cartographie_remuneration",
    schedule="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DGE", "RH"],
    description="""DGE - Cartographie rémunération""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="cartographie_remuneration"),
        feature_flags=FeatureFlagsEnable(
            db=True,
            mail=True,
            s3=True,
            convert_files=False,
            download_grist_doc=False,
        ),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def cartographie_remuneration() -> None:
    """Task definitions"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_list_source_fichier(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.START),
    )
    selecteur_configs = get_selecteur_config(storage_options=storage_options)

    """ Task order """
    chain(
        validate_dag_parameters(),
        looking_for_files,
        create_projet_snapshot(),
        del_iceberg_staging_table(),
        source_files(),
        import_file_to_iceberg.expand(selecteur_config=selecteur_configs),
        copy_staging_to_prod.expand(selecteur_config=selecteur_configs),
        del_iceberg_staging_table(),
        copy_s3_files(
            storage_options=storage_options,
        ),
        del_s3_files(
            storage_options=storage_options,
        ),
    )


cartographie_remuneration()
