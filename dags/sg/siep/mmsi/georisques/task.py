from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_action_etl_task

from dags.sg.siep.mmsi.georisques.actions import get_georisques


@task_group
def georisques_group() -> None:
    """Task group for the Georisques pipeline."""

    georisques_task = create_action_etl_task(
        task_id="get_georisques", action_func=get_georisques
    )

    chain(
        georisques_task(),
    )
