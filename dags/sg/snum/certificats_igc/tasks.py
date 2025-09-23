from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_file_etl_task

from dags.sg.snum.certificats_igc import process


@task_group
def source_files() -> None:
    agents = create_file_etl_task(
        selecteur="agents", process_func=process.process_agents
    )
    aip = create_file_etl_task(
        selecteur="aip", process_func=process.process_aip, read_options={"sep": ";"}
    )
    certificats = create_file_etl_task(
        selecteur="certificats",
        process_func=process.process_certificats,
        read_options={"sep": ";"},
    )
    igc = create_file_etl_task(selecteur="igc", process_func=process.process_igc)

    # ordre des tâches
    chain([agents(), aip(), certificats(), igc()])


@task_group
def output_files() -> None:
    pass
