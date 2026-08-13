from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.sircom.tdb_interne.config import (
    storage_options,
)
from dags.sg.sircom.tdb_interne.tasks import (
    abonnes_visites,
    budget,
    enquetes,
    metiers,
    ressources_humaines,
)
from modules.common_tasks.grist import download_grist_doc_to_s3
from modules.common_tasks.projet import get_selecteur_config
from modules.common_tasks.sql import (
    copy_tmp_table_to_real_table,
    create_projet_snapshot,
    create_tmp_tables,
    delete_tmp_tables,
    import_file_to_db,
    update_projet_snapshot_status,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args

# Mails
nom_projet = "TdB interne - SIRCOM"


# Définition du DAG
@dag(
    dag_id="tdb_sircom",
    schedule="*/15 8-13,14-19 * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIRCOM", "PRODUCTION", "TABLEAU DE BORD"],
    description="""Pipeline qui scanne les nouvelles données dans Grist
        pour actualiser le tableau de bord du SIRCOM""",
    max_consecutive_failed_dag_runs=2,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="sircom"),
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=True, convert_files=False, download_grist_doc=True),
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def tdb_sircom() -> None:
    """Task order"""
    selecteur_configs = get_selecteur_config(storage_options=storage_options)

    chain(
        validate_dag_parameters(),
        selecteur_configs,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
            doc_id_key="grist_doc_id_tdb_sircom",
        ),
        create_projet_snapshot(),
        abonnes_visites(),
        budget(),
        enquetes(),
        metiers(),
        ressources_humaines(),
        create_tmp_tables(reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(),
        update_projet_snapshot_status(),
        delete_tmp_tables(),
    )


tdb_sircom()
