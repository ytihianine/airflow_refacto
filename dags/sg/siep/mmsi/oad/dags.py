from datetime import timedelta

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.eligibilite_fcu.config import dag_id_fcu
from dags.sg.siep.mmsi.georisques.config import dag_id_georisque
from dags.sg.siep.mmsi.oad.caracteristiques.tasks import (
    oad_carac_to_parquet,
    tasks_oad_caracteristiques,
)
from dags.sg.siep.mmsi.oad.config import storage_options
from dags.sg.siep.mmsi.oad.indicateurs.tasks import (
    oad_indic_to_parquet,
    tasks_oad_indicateurs,
)
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

# Mails
nom_projet = "Outil aide diagnostic"


# Définition du DAG
@dag(
    dag_id="outil_aide_diagnostic",
    schedule="*/15 6-22 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des données de l'immobilier. Base""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=True, download_grist_doc=False),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def oad() -> None:
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
        on_success_callback=create_send_mail_callback(
            mail_status=MailStatus.START,
        ),
    )

    selecteur_configs = get_selecteur_config(storage_options=storage_options)

    @task_group
    def trigger_linked_dags() -> None:
        trigger_fcu_dag = TriggerDagRunOperator(
            task_id="trigger_fcu_dag",
            trigger_dag_id=dag_id_fcu,
        )
        trigger_georisques_dag = TriggerDagRunOperator(
            task_id="trigger_georisques_dag",
            trigger_dag_id=dag_id_georisque,
        )

        chain([trigger_fcu_dag, trigger_georisques_dag])

    @task_group
    def convert_file_to_parquet() -> None:
        chain(
            [
                oad_carac_to_parquet(),
                oad_indic_to_parquet(),
            ]
        )

    end_task = EmptyOperator(
        task_id="end_task",
        on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
    )

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        looking_for_files,
        create_projet_snapshot(),
        convert_file_to_parquet(),
        tasks_oad_caracteristiques(),
        tasks_oad_indicateurs(),
        create_tmp_tables(storage_options=storage_options),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        ensure_partition.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(storage_options=storage_options),
        refresh_views(),
        copy_s3_files(storage_options=storage_options),
        del_s3_files(storage_options=storage_options),
        delete_tmp_tables(storage_options=storage_options),
        update_projet_snapshot_status(),
        end_task,
        trigger_linked_dags(),
    )


oad()
