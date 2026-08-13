from datetime import timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.snum.certificats_igc.config import storage_options
from dags.sg.snum.certificats_igc.tasks import source_files
from modules.common_tasks.projet import get_selecteur_config
from modules.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from modules.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
    ensure_partition,
    import_file_to_db,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args
from modules.utils.config.tasks import get_list_source_fichier

nom_projet = "Certificat IGC"


# Définition du DAG
@dag(
    dag_id="certificats_igc",
    schedule="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "SNUM"],
    description="""SG - Certificat IGC""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="certificat_igc"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=False, convert_files=False, download_grist_doc=False),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def certificats_igc() -> None:

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
        selecteur_configs,
        looking_for_files,
        create_projet_snapshot(),
        source_files(),
        ensure_partition.expand(selecteur_config=selecteur_configs),
        create_tmp_tables(storage_options=storage_options),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(storage_options=storage_options),
        copy_s3_files(storage_options=storage_options),
        del_s3_files(storage_options=storage_options),
        delete_tmp_tables(storage_options=storage_options),
    )


certificats_igc()
