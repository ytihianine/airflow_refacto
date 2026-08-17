from airflow.sdk import dag
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.eligibilite_fcu.config import dag_id_fcu, nom_projet_fcu, storage_options
from dags.sg.siep.mmsi.eligibilite_fcu.task import (
    get_eligibilite_fcu,
    process_fcu_result,
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
    update_projet_snapshot_status,
)
from modules.common_tasks.validation import validate_dag_parameters
from modules.enums.dags import DagStatus
from modules.infra.mails.default_smtp import MailStatus, create_send_mail_callback
from modules.types.dags import DBParams, FeatureFlagsEnable
from modules.utils.config.dag_params import create_dag_params, create_default_args


# Définition du DAG
@dag(
    dag_id=dag_id_fcu,
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "FCU"],
    description="Récupérer pour chaque bâtiment son éligibilité au réseau Franche Chaleur Urbaine (FCU)",
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(),
    params=create_dag_params(
        nom_projet=nom_projet_fcu,
        dag_status=DagStatus.RUN,
        db_params=DBParams(prod_schema="siep"),
        feature_flags=FeatureFlagsEnable(db=True, mail=True, s3=True, convert_files=False, download_grist_doc=False),
    ),
    on_success_callback=create_send_mail_callback(mail_status=MailStatus.SUCCESS),
    on_failure_callback=create_send_mail_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def eligibilite_fcu_dag() -> None:

    selecteur_configs = get_selecteur_config(storage_options=storage_options)

    chain(
        validate_dag_parameters(),
        create_projet_snapshot(nom_projet_parent="Outil aide diagnostic"),
        get_eligibilite_fcu(),
        process_fcu_result(),
        create_tmp_tables(storage_options=storage_options, reset_id_seq=False),
        import_file_to_db.expand(selecteur_config=selecteur_configs),
        ensure_partition.expand(selecteur_config=selecteur_configs),
        copy_tmp_table_to_real_table(storage_options=storage_options),
        copy_s3_files(storage_options=storage_options),
        del_s3_files(storage_options=storage_options),
        update_projet_snapshot_status(),
        delete_tmp_tables(storage_options=storage_options),
    )


eligibilite_fcu_dag()
