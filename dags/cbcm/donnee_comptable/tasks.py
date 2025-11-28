from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_PARAM_PATHS
from utils.tasks.etl import (
    create_action_from_multi_input_files_etl_task,
    create_file_etl_task,
)

from dags.cbcm.donnee_comptable import process
from dags.cbcm.donnee_comptable import actions


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


@task_group(group_id="source_files")
def source_files() -> None:
    demande_achat = create_file_etl_task(
        selecteur="demande_achat",
        process_func=process.process_demande_achat,
        read_options={"skiprows": 3},
        add_snapshot_id=False,
    )
    engagement_juridique = create_file_etl_task(
        selecteur="engagement_juridique",
        process_func=process.process_engagement_juridique,
        add_snapshot_id=False,
    )
    demande_paiement = create_file_etl_task(
        selecteur="demande_paiement",
        process_func=process.process_demande_paiement,
        add_snapshot_id=False,
    )
    demande_paiement_flux = create_file_etl_task(
        selecteur="demande_paiement_flux",
        process_func=process.process_demande_paiement_flux,
        read_options={"skiprows": 3},
        add_snapshot_id=False,
    )
    demande_paiement_sfp = create_file_etl_task(
        selecteur="demande_paiement_sfp",
        process_func=process.process_demande_paiement_sfp,
        add_snapshot_id=False,
    )
    demande_paiement_carte_achat = create_file_etl_task(
        selecteur="demande_paiement_carte_achat",
        process_func=process.process_demande_paiement_carte_achat,
        add_snapshot_id=False,
    )
    demande_achat_journal_pieces = create_file_etl_task(
        selecteur="demande_achat_journal_pieces",
        process_func=process.process_demande_achat_journal_pieces,
        add_snapshot_id=False,
    )
    delai_global_paiement = create_file_etl_task(
        selecteur="delai_global_paiement",
        process_func=process.process_delai_global_paiement,
        read_options={"skiprows": 3},
        add_snapshot_id=False,
    )

    chain(
        [
            demande_achat(),
            engagement_juridique(),
            demande_paiement(),
            demande_paiement_flux(),
            demande_paiement_sfp(),
            demande_paiement_carte_achat(),
            demande_achat_journal_pieces(),
            delai_global_paiement(),
        ]
    )


add_new_sp = create_action_from_multi_input_files_etl_task(
    task_id="add_new_sp",
    input_selecteurs=[
        "demande_achat",
        "engagement_juridique",
        "demande_achat_journal_pieces",
        "demande_paiement_carte_achat",
        "demande_paiement_sfp",
    ],
    action_func=actions.load_new_sp,
)
