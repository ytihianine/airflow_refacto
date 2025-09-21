# TODO:
# - Update referentiels
# - Get and process catalogue from Grist
# - Get and process catalogue from Database
# - Compare both catalogues and log differences
# - Save the final catalogue to s3
# - Sync Grist and Database with the new catalogue

from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_grist_etl_task


@task_group()
def update_referentiels() -> None:
    ref_frequence = create_grist_etl_task(
        selecteur="ref_frequence",
    )
    ref_contact = create_grist_etl_task(
        selecteur="ref_contact",
    )
    ref_couverture_geographique = create_grist_etl_task(
        selecteur="ref_couverture_geographique",
    )
    ref_licence = create_grist_etl_task(
        selecteur="ref_licence",
    )
    ref_service = create_grist_etl_task(
        selecteur="ref_service",
    )
    ref_source_format = create_grist_etl_task(
        selecteur="ref_source_format",
    )
    ref_structures = create_grist_etl_task(
        selecteur="ref_structures",
    )
    ref_systeme_info = create_grist_etl_task(
        selecteur="ref_systeme_info",
    )
    ref_theme = create_grist_etl_task(
        selecteur="ref_theme",
    )
    ref_type_donnees = create_grist_etl_task(
        selecteur="ref_type_donnees",
    )

    """ Tasks order """
    chain(
        [
            ref_frequence(),
            ref_contact(),
            ref_couverture_geographique(),
            ref_licence(),
            ref_service(),
            ref_source_format(),
            ref_structures(),
            ref_systeme_info(),
            ref_theme(),
            ref_type_donnees(),
        ]
    )


@task_group()
def source_grist() -> None:
    pass


@task_group()
def source_database() -> None:
    pass


@task_group()
def compare_catalogues() -> None:
    pass


@task_group()
def sync_catalogues() -> None:
    pass
