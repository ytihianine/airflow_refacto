from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_PARAM_PATHS
from utils.tasks.file import create_parquet_converter_task
from utils.tasks.etl import create_multi_files_input_etl_task

from dags.sg.siep.mmsi.oad.caracteristiques import process


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


oad_carac_to_parquet = create_parquet_converter_task(
    selecteur="oad_carac",
    task_params={"task_id": "convert_oad_caracteristique_to_parquet"},
    process_func=process.process_oad_file,
)


@task_group
def tasks_oad_caracteristiques():
    sites = create_multi_files_input_etl_task(
        input_selecteurs=["oad_carac"],
        output_selecteur="sites",
        process_func=process.process_sites,
        use_required_cols=True,
    )
    biens = create_multi_files_input_etl_task(
        input_selecteurs=["oad_carac"],
        output_selecteur="biens",
        process_func=process.process_biens,
        use_required_cols=True,
    )
    gestionnaires = create_multi_files_input_etl_task(
        input_selecteurs=["oad_carac"],
        output_selecteur="gestionnaires",
        process_func=process.process_gestionnaires,
        use_required_cols=True,
    )
    biens_gestionnaires = create_multi_files_input_etl_task(
        input_selecteurs=["oad_carac"],
        output_selecteur="biens_gest",
        process_func=process.process_biens_gestionnaires,
        use_required_cols=True,
    )
    biens_occupants = create_multi_files_input_etl_task(
        input_selecteurs=["oad_carac"],
        output_selecteur="biens_occupants",
        process_func=process.process_biens_occupants,
        use_required_cols=True,
    )

    chain(
        [
            sites(),
            biens(),
            gestionnaires(),
            biens_gestionnaires(),
            biens_occupants(),
        ],
    )
