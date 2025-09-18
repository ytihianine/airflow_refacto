from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_grist_etl_task

from dags.sg.dsci.accompagnements_dsci import process


# Création des tâches
@task_group
def referentiels() -> None:
    ref_bureau = create_grist_etl_task(
        selecteur="ref_bureau",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_bureau,
    )
    ref_certification = create_grist_etl_task(
        selecteur="ref_certification",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_certification,
    )
    ref_competence_particuliere = create_grist_etl_task(
        selecteur="ref_competence_particuliere",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_competence_particuliere,
    )
    ref_direction = create_grist_etl_task(
        selecteur="ref_direction",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_direction,
    )
    ref_profil_correspondant = create_grist_etl_task(
        selecteur="ref_profil_correspondant",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_profil_correspondant,
    )
    ref_promotion_fac = create_grist_etl_task(
        selecteur="ref_promotion_fac",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_promotion_fac,
    )
    ref_qualite_service = create_grist_etl_task(
        selecteur="ref_qualite_service",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_qualite_service,
    )
    ref_region = create_grist_etl_task(
        selecteur="ref_region",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_region,
    )
    ref_semainier = create_grist_etl_task(
        selecteur="ref_semainier",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_semainier,
    )
    ref_typologie_accompagnement = create_grist_etl_task(
        selecteur="ref_typologie_accompagnement",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_typologie_accompagnement,
    )
    ref_pole = create_grist_etl_task(
        selecteur="ref_pole",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_pole,
    )
    ref_type_accompagnement = create_grist_etl_task(
        selecteur="ref_type_accompagnement",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_type_accompagnement,
    )

    # Ordre des tâches
    chain(
        [
            ref_bureau,
            ref_certification,
            ref_competence_particuliere,
            ref_direction,
            ref_profil_correspondant,
            ref_promotion_fac,
            ref_qualite_service,
            ref_region,
            ref_semainier,
            ref_typologie_accompagnement,
            ref_pole,
            ref_type_accompagnement,
        ],
    )


@task_group
def bilaterales() -> None:
    struc_bilaterales = create_grist_etl_task(
        selecteur="struc_bilaterales",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_struc_bilaterales,
    )
    struc_bilaterale_remontee = create_grist_etl_task(
        selecteur="struc_bilaterale_remontee",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_struc_bilaterale_remontee,
    )
    # Ordre des tâches
    chain([struc_bilaterales, struc_bilaterale_remontee])


@task_group
def correspondant() -> None:
    struc_correspondant = create_grist_etl_task(
        selecteur="struc_correspondant",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_struc_correspondant,
    )
    struc_correspondant_profil = create_grist_etl_task(
        selecteur="struc_correspondant_profil",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_struc_correspondant_profil,
    )
    # struc_correspondant_certification = create_grist_etl_task(
    #     selecteur="struc_correspondant_certification",
    #     process_func=process.process_struc_correspondant_certification,
    # )

    # Ordre des tâches
    chain([struc_correspondant, struc_correspondant_profil])


@task_group
def mission_innovation() -> None:
    struc_accompagnement_mi = create_grist_etl_task(
        selecteur="struc_accompagnement_mi",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_struc_accompagnement_mi,
    )
    struc_accompagnement_mi_satisfaction = create_grist_etl_task(
        selecteur="struc_accompagnement_mi_satisfaction",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_struc_accompagnement_mi_satisfaction,
    )

    # Ordre des tâches
    chain([struc_accompagnement_mi, struc_accompagnement_mi_satisfaction])
