from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_PARAM_PATHS
from utils.tasks.etl import (
    create_grist_etl_task,
    create_file_etl_task,
    create_multi_files_input_etl_task,
)

from dags.dge.carto_rem import process

validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


@task_group
def source_files() -> None:
    agent_carto_rem = create_file_etl_task(
        selecteur="agent_carto_rem",
        process_func=process.process_agent_carto_rem,
        add_import_date=False,
        add_snapshot_id=False,
    )
    agent_info_carriere = create_file_etl_task(
        selecteur="agent_info_carriere",
        process_func=process.process_agent_info_carriere,
        add_import_date=False,
        add_snapshot_id=False,
    )
    agent_r4 = create_file_etl_task(
        selecteur="agent_r4",
        process_func=process.process_agent_r4,
        add_import_date=False,
        add_snapshot_id=False,
    )
    agent_fonction_anais = create_file_etl_task(
        selecteur="agent_fonction_anais",
        process_func=process.process_agent_r4,
        add_import_date=False,
        add_snapshot_id=False,
    )

    # ordre des tâches
    chain(
        [agent_carto_rem(), agent_info_carriere(), agent_r4(), agent_fonction_anais()]
    )


@task_group
def source_grist() -> None:
    agent_diplome = create_grist_etl_task(
        selecteur="agent_diplome",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_agent_diplome,
    )
    agent_revalorisation = create_grist_etl_task(
        selecteur="agent_revalorisation",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_agent_revalorisation,
    )
    agent_contrat = create_grist_etl_task(
        selecteur="agent_contrat",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_agent_contrat,
    )
    agent_rem_variable = create_grist_etl_task(
        selecteur="agent_rem_variable",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_agent_rem_variable,
    )

    # ordre des tâches
    chain(
        [agent_diplome(), agent_revalorisation(), agent_contrat(), agent_rem_variable()]
    )


@task_group
def output_files() -> None:
    agent = create_multi_files_input_etl_task(
        output_selecteur="agent",
        input_selecteurs=["agent_carto_rem", "agent_info_carriere"],
        process_func=process.process_agent,
        add_import_date=False,
        add_snapshot_id=False,
    )
    agent_poste = create_multi_files_input_etl_task(
        output_selecteur="agent_poste",
        input_selecteurs=[
            "agent",
            "agent_carto_rem",
            "agent_r4",
            "agent_fonction_anais",
        ],
        process_func=process.process_agent_poste,
        add_import_date=False,
        add_snapshot_id=False,
    )
    agent_remuneration = create_multi_files_input_etl_task(
        output_selecteur="agent_remuneration",
        input_selecteurs=["agent_carto_rem", "agent_rem_variable"],
        process_func=process.process_agent_remuneration,
        add_import_date=False,
        add_snapshot_id=False,
    )
    # ordre des tâches
    chain(
        agent(),
        [
            agent_poste(),
            agent_remuneration(),
        ],
    )


@task_group
def referentiels() -> None:
    ref_base_remuneration = create_grist_etl_task(
        selecteur="ref_base_remuneration",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_base_remuneration,
    )
    ref_base_revalorisation = create_grist_etl_task(
        selecteur="ref_base_revalorisation",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_base_revalorisation,
    )
    ref_experience_pro = create_grist_etl_task(
        selecteur="ref_experience_pro",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_experience_pro,
    )
    ref_niveau_diplome = create_grist_etl_task(
        selecteur="ref_niveau_diplome",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_niveau_diplome,
    )
    ref_valeur_point_indice = create_grist_etl_task(
        selecteur="ref_valeur_point_indice",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_valeur_point_indice,
    )

    # ordre des tâches
    chain(
        [
            ref_base_remuneration(),
            ref_base_revalorisation(),
            ref_experience_pro(),
            ref_niveau_diplome(),
            ref_valeur_point_indice(),
        ]
    )
