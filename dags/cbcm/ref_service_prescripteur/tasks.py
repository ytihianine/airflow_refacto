from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_PARAM_PATHS
from utils.tasks.etl import create_grist_etl_task

from dags.cbcm.ref_service_prescripteur import process


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


@task_group(group_id="grist_source")
def grist_source() -> None:
    ref_prog = create_grist_etl_task(
        selecteur="ref_prog",
        process_func=process.process_ref_prog,
    )
    ref_bop = create_grist_etl_task(
        selecteur="ref_bop",
        process_func=process.process_ref_bop,
    )
    ref_uo = create_grist_etl_task(
        selecteur="ref_uo",
        process_func=process.process_ref_uo,
    )
    ref_cc = create_grist_etl_task(
        selecteur="ref_cc",
        process_func=process.process_ref_cc,
    )
    ref_sdep = create_grist_etl_task(
        selecteur="ref_sdep",
        process_func=process.process_ref_sdep,
    )
    ref_sp_choisi = create_grist_etl_task(
        selecteur="ref_sp_choisi",
        process_func=process.process_ref_sp_choisi,
    )
    ref_sp_pilotage = create_grist_etl_task(
        selecteur="ref_sp_pilotage",
        process_func=process.process_ref_sp_pilotage,
    )
    sp = create_grist_etl_task(
        selecteur="sp",
        process_func=process.process_service_prescripteur,
    )

    chain(
        [
            ref_prog(),
            ref_bop(),
            ref_uo(),
            ref_cc(),
            ref_sdep(),
            ref_sp_choisi(),
            ref_sp_pilotage(),
            sp(),
        ]
    )
