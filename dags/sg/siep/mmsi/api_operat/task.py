from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.api_operat import actions, process
from dags.sg.siep.mmsi.api_operat.types import ApiOperat
from modules.common_tasks.etl import create_task
from modules.types.dags import ETLStep, TaskConfig


@task_group
def source() -> None:
    declarations = create_task(
        task_config=TaskConfig(task_id="declarations"),
        output_selecteur="declarations",
        steps=[
            ETLStep(
                fn=actions.liste_declaration,
                kwargs={"api_operat": ApiOperat()},
                use_context=False,
            ),
        ],
        export_output=True,
    )
    consommations = create_task(
        task_config=TaskConfig(task_id="consommation_by_id"),
        output_selecteur="consommations",
        input_selecteurs=["declarations"],
        steps=[
            ETLStep(
                fn=actions.consommation_by_id,
                kwargs={"api_operat": ApiOperat()},
                use_context=False,
                read_data=True,
            ),
        ],
        export_output=True,
    )

    chain(
        declarations(),
        consommations(),
    )


@task_group
def output() -> None:
    declarations = create_task(
        task_config=TaskConfig(task_id="declaration_ademe"),
        input_selecteurs=["declarations"],
        output_selecteur="declaration_ademe",
        steps=[
            ETLStep(
                fn=process.process_declarations,
                use_context=False,
                read_data=True,
            ),
        ],
        export_output=True,
    )
    activite = create_task(
        task_config=TaskConfig(task_id="activite"),
        input_selecteurs=["consommations"],
        output_selecteur="activite",
        steps=[
            ETLStep(
                fn=process.process_detail_conso_activite,
                use_context=False,
                read_data=True,
            ),
        ],
        export_output=True,
    )
    indicateur = create_task(
        task_config=TaskConfig(task_id="indicateur"),
        input_selecteurs=["consommations"],
        output_selecteur="indicateur",
        steps=[
            ETLStep(
                fn=process.process_detail_conso_indicateur,
                use_context=False,
                read_data=True,
            ),
        ],
        export_output=True,
    )
    detail = create_task(
        task_config=TaskConfig(task_id="detail"),
        input_selecteurs=["consommations"],
        output_selecteur="detail",
        steps=[
            ETLStep(
                fn=process.process_detail_conso,
                use_context=False,
                read_data=True,
            ),
        ],
        export_output=True,
    )

    chain(
        declarations(),
        activite(),
        indicateur(),
        detail(),
    )
