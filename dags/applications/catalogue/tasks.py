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

from dags.applications.catalogue import process


@task_group()
def update_referentiels() -> None:
    ref_frequence = create_grist_etl_task(
        selecteur="ref_frequence", normalisation_process_func=process.normalize_df
    )
    ref_contact = create_grist_etl_task(
        selecteur="ref_contact", normalisation_process_func=process.normalize_df
    )
    ref_couverture_geographique = create_grist_etl_task(
        selecteur="ref_couverture_geographique",
        normalisation_process_func=process.normalize_df,
    )
    ref_licence = create_grist_etl_task(
        selecteur="ref_licence", normalisation_process_func=process.normalize_df
    )
    ref_service = create_grist_etl_task(
        selecteur="ref_service", normalisation_process_func=process.normalize_df
    )
    ref_source_format = create_grist_etl_task(
        selecteur="ref_source_format", normalisation_process_func=process.normalize_df
    )
    ref_structures = create_grist_etl_task(
        selecteur="ref_structures", normalisation_process_func=process.normalize_df
    )
    ref_systeme_info = create_grist_etl_task(
        selecteur="ref_systeme_info", normalisation_process_func=process.normalize_df
    )
    ref_theme = create_grist_etl_task(
        selecteur="ref_theme", normalisation_process_func=process.normalize_df
    )
    ref_type_donnees = create_grist_etl_task(
        selecteur="ref_type_donnees", normalisation_process_func=process.normalize_df
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
    datasets = create_grist_etl_task(
        selecteur="datasets", normalisation_process_func=process.normalize_df
    )
    datasets_dictionnaire = create_grist_etl_task(
        selecteur="datasets_dictionnaire",
        normalisation_process_func=process.normalize_df,
    )

    """ Tasks order """
    chain(
        [
            datasets(),
            datasets_dictionnaire(),
        ]
    )


@task_group()
def source_database() -> None:
    pass


@task_group()
def compare_catalogues() -> None:
    pass


@task_group()
def sync_catalogues() -> None:
    pass
