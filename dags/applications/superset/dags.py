from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.applications.superset.tasks import update_admin_ownership
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args

nom_projet = "Superset opérations"


# Définition du DAG
@dag(
    dag_id="superset_operations",
    schedule="0 0,12 * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "RECETTE", "SUPERSET", "DATABASE"],
    description="""Pipeline qui réalise différentes opérations sur Superset""",
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
        update_admin_ownership(db_conn_id="db_config"),
    )


sauvegarde_database()
