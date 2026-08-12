from datetime import timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.oad_referentiel.tasks import ref_typologie
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
    import_file_to_db,
    refresh_views,
    # set_dataset_last_update_date,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args
from modules.utils.config.tasks import get_list_source_fichier

# Mails
nom_projet = "Outil aide diagnostic - référentiel"


# Définition du DAG
@dag(
    dag_id="outil_aide_diagnostic_referentiel",
    schedule=None,  # timedelta(seconds=30),
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des référentiels issus de l'OAD.""",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def oad_referentiel() -> None:
    """Task definition"""
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_list_source_fichier(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),  # timedelta(minutes=1),
        timeout=timedelta(minutes=1),
        soft_fail=True,
        on_skipped_callback=create_send_mail_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_send_mail_callback(
            mail_status=MailStatus.START,
        ),
    )

    selecteur_configs = get_selecteur_config(storage_options={})

    """ Task order """
    chain(
        validate_dag_parameters(),
        selecteur_configs,
        looking_for_files,
        create_projet_snapshot(nom_projet_parent="Outil aide diagnostic"),
        ref_typologie,
        create_tmp_tables(),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(),
        refresh_views(),
        copy_s3_files(),
        del_s3_files(),
        delete_tmp_tables(),
    )


oad_referentiel()
