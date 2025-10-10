from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_PARAM_PATHS
from utils.tasks.file import create_parquet_converter_task
from utils.tasks.etl import create_file_etl_task, create_multi_files_input_etl_task

from dags.sg.siep.mmsi.consommation_batiment import process


validate_params = create_validate_params_task(
    required_paths=ALL_PARAM_PATHS,
    require_truthy=None,
    task_id="validate_dag_params",
)


conso_mens_parquet = create_parquet_converter_task(
    selecteur="conso_mens_source",
    task_params={"task_id": "convert_cons_mens_to_parquet"},
    process_func=process.process_source_conso_mens,
)


@task_group(group_id="source_files")
def source_files():
    informations_batiments = create_file_etl_task(
        selecteur="bien_info_complementaire",
        process_func=process.process_source_bien_info_comp,
    )
    conso_mensuelles = create_multi_files_input_etl_task(
        output_selecteur="conso_mens",
        input_selecteurs=["conso_mens_source"],
        process_func=process.process_conso_mensuelles,
    )
    chain([informations_batiments(), conso_mensuelles()])


@task_group(group_id="additionnal_files")
def additionnal_files():
    unpivot_conso_mens_corrigee = create_multi_files_input_etl_task(
        output_selecteur="conso_mens_corr_unpivot",
        input_selecteurs=["conso_mens"],
        process_func=process.process_unpivot_conso_mens_corrigee,
    )
    unpivot_conso_mens_brute = create_multi_files_input_etl_task(
        output_selecteur="conso_mens_brute_unpivot",
        input_selecteurs=["conso_mens"],
        process_func=process.process_unpivot_conso_mens_brute,
    )
    conso_annuelle = create_multi_files_input_etl_task(
        output_selecteur="conso_annuelle",
        input_selecteurs=["conso_mens"],
        process_func=process.process_conso_annuelle,
    )
    conso_statut_par_fluide = create_multi_files_input_etl_task(
        output_selecteur="conso_statut_par_fluide",
        input_selecteurs=["conso_annuelle"],
        process_func=process.process_conso_statut_par_fluide,
    )
    conso_avant_2019 = create_multi_files_input_etl_task(
        output_selecteur="conso_avant_2019",
        input_selecteurs=["conso_annuelle"],
        process_func=process.process_conso_avant_2019,
    )
    conso_statut_fluide_global = create_multi_files_input_etl_task(
        output_selecteur="conso_statut_fluide_global",
        input_selecteurs=["conso_statut_par_fluide"],
        process_func=process.process_conso_statut_fluide_global,
    )
    conso_statut_batiment = create_multi_files_input_etl_task(
        output_selecteur="conso_statut_batiment",
        input_selecteurs=["conso_statut_fluide_global", "conso_avant_2019"],
        process_func=process.process_conso_statut_batiment,
    )

    chain(
        [
            unpivot_conso_mens_corrigee(),
            unpivot_conso_mens_brute(),
        ],
        conso_annuelle(),
        conso_statut_par_fluide(),
        conso_avant_2019(),
        conso_statut_fluide_global(),
        conso_statut_batiment(),
    )
