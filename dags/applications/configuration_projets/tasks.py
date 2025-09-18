from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_KEYS
from utils.tasks.etl import create_grist_etl_task

from dags.applications.configuration_projets import process


validate_params = create_validate_params_task(
    required_paths=ALL_KEYS,
    require_truthy=None,
    task_id="validate_dag_params",
)


@task_group
def process_data() -> None:
    ref_direction = create_grist_etl_task(
        selecteur="direction",
        normalisation_process_func=process.clean_normalize_df,
        process_func=process.process_direction,
    )
    ref_service = create_grist_etl_task(
        selecteur="service",
        normalisation_process_func=process.clean_normalize_df,
        process_func=process.process_service,
    )
    projets = create_grist_etl_task(
        selecteur="projets",
        normalisation_process_func=process.clean_normalize_df,
        process_func=process.process_projets,
    )
    selecteur = create_grist_etl_task(
        selecteur="selecteur",
        normalisation_process_func=process.clean_normalize_df,
        process_func=process.process_selecteur,
    )
    source = create_grist_etl_task(
        selecteur="source",
        normalisation_process_func=process.clean_normalize_df,
        process_func=process.process_source,
    )
    storage_paths = create_grist_etl_task(
        selecteur="storage_path",
        normalisation_process_func=process.clean_normalize_df,
        process_func=process.process_storage_path,
    )
    col_mapping = create_grist_etl_task(
        selecteur="col_mapping",
        normalisation_process_func=process.clean_normalize_df,
        process_func=process.process_col_mapping,
    )
    col_requises = create_grist_etl_task(
        selecteur="col_requises",
        normalisation_process_func=process.clean_normalize_df,
        process_func=process.process_col_requises,
    )

    chain(
        [
            ref_direction(),
            ref_service(),
            projets(),
            selecteur(),
            source(),
            storage_paths(),
            col_mapping(),
            col_requises(),
        ]
    )
