from datetime import timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.consommation_batiment.config import dag_id_osfi, nom_projet_osfi, storage_options
from dags.sg.siep.mmsi.consommation_batiment.tasks import (
    additionnal_files,
    convert_file_to_parquet,
    source_files,
)
from dags.sg.siep.mmsi.oad.config import nom_projet_oad
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
    refresh_views,
    update_projet_snapshot_status,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args
from modules.utils.config.tasks import get_list_source_fichier


# Définition du DAG
@dag(
    dag_id=dag_id_osfi,
    schedule="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "CONSOMMATION"],
    description="Pipeline de traitement des données de consommation des bâtiments. Source des données: OSFI",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet_osfi,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=True, download_grist_doc=False),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def consommation_des_batiments() -> None:
    """Task definition"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_list_source_fichier(nom_projet=nom_projet_osfi),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.START),
    )

    selecteur_configs = get_selecteur_config(storage_options=storage_options)

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        looking_for_files,
        create_projet_snapshot(nom_projet_parent=nom_projet_oad),
        convert_file_to_parquet(),
        source_files(),
        additionnal_files(),
        create_tmp_tables(storage_options=storage_options, reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        ensure_partition.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(storage_options=storage_options),
        refresh_views(),
        copy_s3_files(storage_options=storage_options),
        del_s3_files(storage_options=storage_options),
        update_projet_snapshot_status(),
        delete_tmp_tables(storage_options=storage_options),
    )


consommation_des_batiments()
