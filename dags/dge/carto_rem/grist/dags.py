from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.grist.config import storage_options
from dags.dge.carto_rem.grist.tasks import (
    referentiels,
    source_grist,
    # load_to_grist,
)
from modules.common_tasks.grist import download_grist_doc_to_s3
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

# Mails
nom_projet = "Cartographie rémunération - Grist"


# Définition du DAG
@dag(
    dag_id="cartographie_remuneration_grist",
    schedule="*/8 8-20 * * 1-5",
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
        feature_flags=FeatureFlagsEnable(db=True, mail=False, s3=False, convert_files=False, download_grist_doc=True),
    ),
    on_failure_callback=create_send_mail_callback(mail_status=MailStatus.ERROR),
)
def cartographie_remuneration_grist() -> None:
    """Task order"""
    selecteur_configs = get_selecteur_config(storage_options=storage_options)

    chain(
        validate_dag_parameters(),
        create_projet_snapshot(nom_projet="Cartographie rémunération"),
        del_iceberg_staging_table(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci-dge",
            doc_id_key="grist_doc_id_carto_rem",
        ),
        [referentiels(), source_grist()],
        # load_to_grist(),
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


cartographie_remuneration_grist()
