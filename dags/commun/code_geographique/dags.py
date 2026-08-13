from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.commun.code_geographique.tasks import code_geographique, code_iso, geojson
from modules.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    refresh_views,
    # set_dataset_last_update_date,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args

nom_projet = "Code géographique"


# Définition du DAG
@dag(
    dag_id="informations_geographiques",
    schedule="00 00 7 * *",
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "COMMUN", "DSCI"],
    description="""Récupération des codes géographiques""",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.DEV,
        db_params=DBParams(prod_schema="commun"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=False, download_grist_doc=False),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def informations_geographiques() -> None:
    """Récupération de toutes les données géographiques"""

    """ Hooks """

    # Ordre des tâches
    chain(
        validate_dag_parameters(),
        create_projet_snapshot(),
        code_geographique(),
        geojson(),
        code_iso(),
        create_tmp_tables(),
        copy_tmp_table_to_real_table(),
        refresh_views(),
    )


informations_geographiques()
