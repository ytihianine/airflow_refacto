from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_action_etl_task

from dags.sg.siep.mmsi.api_operat.actions import liste_declaration, consommation_by_id


@task_group
def taches():
    declarations = create_action_etl_task(
        action_func=liste_declaration,
        task_id="liste_declaration",
        action_kwargs={"nom_projet": "API Opera"},
    )
    consommations = create_action_etl_task(
        action_func=consommation_by_id,
        task_id="consommation_by_id",
        action_kwargs={"nom_projet": "API Opera"},
    )

    chain(
        declarations(),
        consommations(),
    )
