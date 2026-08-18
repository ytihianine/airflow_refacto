from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.oad.caracteristiques import process
from modules.common_tasks.etl import create_task
from modules.common_tasks.file import create_parquet_converter_task
from modules.types.dags import ETLStep, TaskConfig

oad_carac_to_parquet = create_parquet_converter_task(
    selecteur="oad_carac",
    task_params={"task_id": "convert_oad_caracteristique_to_parquet"},
    process_func=process.process_oad_file,
)


@task_group
def tasks_oad_caracteristiques():
    sites = create_task(
        task_config=TaskConfig(task_id="sites"),
        output_selecteur="sites",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_sites, read_data=True)],
    )
    biens = create_task(
        task_config=TaskConfig(task_id="biens"),
        output_selecteur="biens",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_biens, read_data=True)],
    )
    gestionnaires = create_task(
        task_config=TaskConfig(task_id="gestionnaires"),
        output_selecteur="gestionnaires",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_gestionnaires, read_data=True)],
    )
    biens_gestionnaires = create_task(
        task_config=TaskConfig(task_id="biens_gestionnaires"),
        output_selecteur="biens_gest",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_biens_gestionnaires, read_data=True)],
    )
    biens_occupants = create_task(
        task_config=TaskConfig(task_id="biens_occupants"),
        output_selecteur="biens_occupants",
        input_selecteurs=["oad_carac"],
        steps=[ETLStep(fn=process.process_biens_occupants, read_data=True)],
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
