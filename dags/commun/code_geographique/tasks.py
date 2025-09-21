from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_action_to_file_etl_task

from dags.commun.code_geographique import actions


@task_group
def code_geographique() -> None:
    communes = create_action_to_file_etl_task(
        output_selecteur="communes",
        action_func=actions.communes,
        task_id="communes_to_file",
    )
    departements = create_action_to_file_etl_task(
        output_selecteur="departements",
        action_func=actions.departements,
        task_id="departements_to_file",
    )
    regions = create_action_to_file_etl_task(
        output_selecteur="regions",
        action_func=actions.regions,
        task_id="regions_to_file",
    )
    chain(
        communes(),
        departements(),
        regions(),
    )


@task_group
def geojson() -> None:
    departements_geojson = create_action_to_file_etl_task(
        output_selecteur="departements_geojson",
        action_func=actions.departement_geojson,
        task_id="departements_geojson_to_file",
    )
    regions_geojson = create_action_to_file_etl_task(
        output_selecteur="regions_geojson",
        action_func=actions.region_geojson,
        task_id="regions_geojson_to_file",
    )
    chain([departements_geojson(), regions_geojson()])


@task_group
def code_iso() -> None:
    departements_iso = create_action_to_file_etl_task(
        output_selecteur="code_iso_departement",
        action_func=actions.code_iso_departement,
        task_id="code_iso_departement_to_file",
    )
    regions_iso = create_action_to_file_etl_task(
        output_selecteur="code_iso_region",
        action_func=actions.code_iso_region,
        task_id="code_iso_region_to_file",
    )
    chain([departements_iso(), regions_iso()])
