from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.georisques import actions
from modules.common_tasks.etl import (
    create_task,
)
from modules.types.dags import ETLStep, TaskConfig


@task_group
def georisques_group() -> None:
    """Task group for the Georisques pipeline."""

    bien_db = create_task(
        task_config=TaskConfig(task_id="bien_db"),
        output_selecteur="bien_db",
        steps=[
            ETLStep(
                fn=actions.get_bien_from_db,
                use_context=True,
            )
        ],
    )

    georisques = create_task(
        task_config=TaskConfig(task_id="georisques"),
        output_selecteur="georisques",
        input_selecteurs=["bien_db"],
        steps=[ETLStep(fn=actions.get_georisques, read_data=True)],
    )

    chain(
        bien_db(),
        georisques(),
    )
