from dags.sg.srh.mentorat_merci import actions, process
from modules.common_tasks.etl import create_task
from modules.types.dags import ETLStep, TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy

agent_inscrit = ETLTask(
    task_config=TaskConfig(task_id="agent_inscrit"),
    target="agent_inscrit",
    reader=GristReaderStrategy(),
    steps=[
        SingleInputStep(
            fn=process.clean_data,
            input_key="agent_inscrit",
            output_key="agent_inscrit",
        )
    ],
    writers=[FileWriterStrategy()],
    add_metadata=True,
)


generer_binomes = create_task(
    task_config=TaskConfig(task_id="generer_binomes"),
    output_selecteur="generer_binome",
    input_selecteurs=["agent_inscrit"],
    steps=[
        ETLStep(fn=actions.trouver_meilleurs_binomes, read_data=True),
        ETLStep(fn=actions.send_result, use_context=True, use_previous_output=True),
    ],
    export_output=False,
)
