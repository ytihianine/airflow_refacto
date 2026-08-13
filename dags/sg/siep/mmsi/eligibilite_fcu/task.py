from dags.sg.siep.mmsi.eligibilite_fcu import actions, process
from modules.common_tasks.etl import (
    create_task,
)
from modules.types.dags import ETLStep, TaskConfig

get_eligibilite_fcu = create_task(
    task_config=TaskConfig(task_id="eligibilite_fcu_to_file"),
    output_selecteur="fcu",
    steps=[
        ETLStep(
            fn=actions.eligibilite_fcu,
            use_context=True,
        )
    ],
)

process_fcu_result = create_task(
    task_config=TaskConfig(task_id="fcu_result"),
    input_selecteurs=["fcu"],
    output_selecteur="fcu_result",
    steps=[ETLStep(fn=process.process_result, read_data=True)],
)
