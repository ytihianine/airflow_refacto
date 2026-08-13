from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.applications.configuration_projets.config import (
    storage_options,
)
from dags.applications.configuration_projets.tasks import (
    process_data,
)
from modules.common_tasks.grist import download_grist_doc_to_s3
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
    update_projet_snapshot_status,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args

nom_projet = "Configuration des projets"


@dag(
    dag_id="configuration_projets",
    schedule="0 0 * * 1-5",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    catchup=False,
    params=create_dag_params(
        nom_projet=nom_projet,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="conf_projets"),
        feature_flags=FeatureFlagsEnable(db=True, mail=True, s3=True, convert_files=False, download_grist_doc=True),
    ),
    on_success_callback=create_send_mail_callback(
        mail_status=MailStatus.SUCCESS,
    ),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def configuration_projets() -> None:
    """Tasks order"""

    selecteur_configs = get_selecteur_config(storage_options=storage_options)

    chain(
        validate_dag_parameters(),
        selecteur_configs,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
        ),
        create_projet_snapshot(),
        process_data(),
        delete_tmp_tables(storage_options=storage_options),
        create_tmp_tables(
            storage_options=storage_options,
            reset_id_seq=False,
        ),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(storage_options=storage_options),
        copy_s3_files(
            storage_options=storage_options,
        ),
        del_s3_files(
            storage_options=storage_options,
        ),
        delete_tmp_tables(storage_options=storage_options),
        update_projet_snapshot_status(),
    )


configuration_projets()
