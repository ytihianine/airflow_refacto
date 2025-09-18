from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_grist_etl_task


from dags.cgefi.suivi_activite import process


@task_group
def referentiels() -> None:
    ref_actif_type = create_grist_etl_task(
        selecteur="ref_actif_type",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_actif_type,
    )
    ref_actif_sous_type = create_grist_etl_task(
        selecteur="ref_actif_sous_type",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_actif_sous_type,
    )
    ref_actions = create_grist_etl_task(
        selecteur="ref_actions",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_actions,
    )
    ref_formation_specialite = create_grist_etl_task(
        selecteur="ref_formation_specialite",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_formation_specialite,
    )
    ref_passif_type = create_grist_etl_task(
        selecteur="ref_passif_type",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_passif_type,
    )
    ref_passif_sous_type = create_grist_etl_task(
        selecteur="ref_passif_sous_type",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_passif_sous_type,
    )
    ref_secteur_professionnel = create_grist_etl_task(
        selecteur="ref_secteur_professionnel",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_secteur_professionnel,
    )
    ref_type_de_frais = create_grist_etl_task(
        selecteur="ref_type_de_frais",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_type_de_frais,
    )
    ref_sous_type_de_frais = create_grist_etl_task(
        selecteur="ref_sous_type_de_frais",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_sous_type_de_frais,
    )
    ref_type_organisme = create_grist_etl_task(
        selecteur="ref_type_organisme",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_ref_type_organisme,
    )

    # Ordre des tâches
    chain(
        [
            ref_actif_type(),
            ref_actif_sous_type(),
            ref_actions(),
            ref_formation_specialite(),
            ref_passif_type(),
            ref_passif_sous_type(),
            ref_secteur_professionnel(),
            ref_type_de_frais(),
            ref_sous_type_de_frais(),
            ref_type_organisme(),
        ]
    )


@task_group
def processus_4() -> None:
    pass


@task_group
def processus_6() -> None:
    process_6_a01 = create_grist_etl_task(
        selecteur="process_6_a01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_6_a01,
    )
    process_6_b01 = create_grist_etl_task(
        selecteur="process_6_b01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_6_b01,
    )
    process_6_d01 = create_grist_etl_task(
        selecteur="process_6_d01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_6_d01,
    )
    process_6_e01 = create_grist_etl_task(
        selecteur="process_6_e01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_6_e01,
    )
    process_6_g02 = create_grist_etl_task(
        selecteur="process_6_g02",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_6_g02,
    )
    process_6_g03 = create_grist_etl_task(
        selecteur="process_6_g03",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_6_g03,
    )
    process_6_h01 = create_grist_etl_task(
        selecteur="process_6_h01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_6_h01,
    )

    # Ordre des tâches
    chain(
        [
            process_6_a01(),
            process_6_b01(),
            process_6_d01(),
            process_6_e01(),
            process_6_g02(),
            process_6_g03(),
            process_6_h01(),
        ]
    )


@task_group
def processus_atpro() -> None:
    process_atpro_a01 = create_grist_etl_task(
        selecteur="process_atpro_a01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_atpro_a01,
    )
    process_atpro_f01 = create_grist_etl_task(
        selecteur="process_atpro_f01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_atpro_f01,
    )
    process_atpro_f02 = create_grist_etl_task(
        selecteur="process_atpro_f02",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_atpro_f02,
    )
    process_atpro_g02 = create_grist_etl_task(
        selecteur="process_atpro_g02",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_atpro_g02,
    )
    process_atpro_h01 = create_grist_etl_task(
        selecteur="process_atpro_h01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_atpro_h01,
    )
    process_atpro_j01 = create_grist_etl_task(
        selecteur="process_atpro_j01",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_process_atpro_j01,
    )

    # Ordre des tâches
    chain(
        [
            process_atpro_a01(),
            process_atpro_f01(),
            process_atpro_f02(),
            process_atpro_g02(),
            process_atpro_h01(),
            process_atpro_j01(),
        ]
    )


@task_group
def informations_generales() -> None:
    controleur = create_grist_etl_task(
        selecteur="controleur",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_controleur,
    )
    organisme = create_grist_etl_task(
        selecteur="organisme",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_organisme,
    )
    organisme_type = create_grist_etl_task(
        selecteur="organisme_type",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_organisme_type,
    )
    region_atpro = create_grist_etl_task(
        selecteur="region_atpro",
        normalisation_process_func=process.normalize_dataframe,
        process_func=process.process_region_atpro,
    )

    # Ordre des tâches
    chain([controleur(), organisme(), organisme_type(), region_atpro()])
