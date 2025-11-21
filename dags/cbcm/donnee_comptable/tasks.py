from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_PARAM_PATHS
from utils.tasks.etl import create_file_etl_task

from dags.cbcm.donnee_comptable import process


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
    )
    engagement_juridique = create_file_etl_task(
        selecteur="engagement_juridique",
        process_func=process.process_engagement_juridique,
    )
    demande_paiement = create_file_etl_task(
        selecteur="demande_paiement",
        process_func=process.process_demande_paiement,
    )
    demande_paiement_flux = create_file_etl_task(
        selecteur="demande_paiement_flux",
        process_func=process.process_demande_paiement_flux,
    )
    demande_paiement_sfp = create_file_etl_task(
        selecteur="demande_paiement_sfp",
        process_func=process.process_demande_paiement_sfp,
    )
    demande_paiement_carte_achat = create_file_etl_task(
        selecteur="demande_paiement_carte_achat",
        process_func=process.process_demande_paiement_carte_achat,
    )
    demande_achat_journal_pieces = create_file_etl_task(
        selecteur="demande_achat_journal_pieces",
        process_func=process.process_demande_achat_journal_pieces,
    )
    delai_global_paiement = create_file_etl_task(
        selecteur="delai_global_paiement",
        process_func=process.process_delai_global_paiement,
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
