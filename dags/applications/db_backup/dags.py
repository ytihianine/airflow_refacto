from datetime import timedelta

from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.applications.db_backup.config import storage_options
from dags.applications.db_backup.tasks import export_database
from modules.common_tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args

nom_projet = "Sauvegarde databases"


# Définition du DAG
@dag(
    dag_id="sauvegarde_database",
    schedule=timedelta(hours=12),
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "RECETTE", "SAUVEGARDE", "DATABASE"],
    description="""Pipeline qui réalise des sauvegarde de la base de données""",
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=None,
        feature_flags=FeatureFlagsEnable(db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
)
def sauvegarde_database() -> None:
    """Task order"""
    chain(
        validate_dag_parameters(),
        export_database.expand(db_conn_id=["db_config", "db_data_store"]),
        copy_s3_files(storage_options=storage_options),
        del_s3_files(storage_options=storage_options),
    )


sauvegarde_database()
