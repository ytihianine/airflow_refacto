from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_grist_etl_task

from dags.sg.dsci.carte_identite_mef import process


@task_group()
def effectif():
    teletravail = create_grist_etl_task(
        selecteur="teletravail",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_teletravail,
    )
    teletravail_frequence = create_grist_etl_task(
        selecteur="teletravail_frequence",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_teletravail_frequence,
    )
    teletravail_opinion = create_grist_etl_task(
        selecteur="teletravail_opinion",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_teletravail_opinion,
    )
    effectif_direction_perimetre = create_grist_etl_task(
        selecteur="effectif_direction_perimetre",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_mef_par_direction,
    )
    effectif_direction = create_grist_etl_task(
        selecteur="effectif_direction",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_effectif_direction,
    )
    effectif_perimetre = create_grist_etl_task(
        selecteur="effectif_perimetre",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_effectifs_par_perimetre,
    )
    effectif_departements = create_grist_etl_task(
        selecteur="effectif_departements",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_effectif_par_departements,
    )
    masse_salariale = create_grist_etl_task(
        selecteur="masse_salariale",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_masse_salariale,
    )

    """ Task order """
    chain(
        [
            teletravail,
            teletravail_frequence,
            teletravail_opinion,
            effectif_direction_perimetre,
            effectif_direction,
            effectif_perimetre,
            effectif_departements,
            masse_salariale,
        ]
    )


@task_group()
def budget():
    budget_total = create_grist_etl_task(
        selecteur="budget_total",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_budget_total,
    )
    budget_pilotable = create_grist_etl_task(
        selecteur="budget_pilotable",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_budget_pilotable,
    )
    budget_general = create_grist_etl_task(
        selecteur="budget_general",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_budget_general,
    )
    evolution_budget_mef = create_grist_etl_task(
        selecteur="evolution_budget_mef",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_evolution_budget_mef,
    )
    montant_intervention_invest = create_grist_etl_task(
        selecteur="montant_intervention_invest",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_montant_invest,
    )
    budget_ministere = create_grist_etl_task(
        selecteur="budget_ministere",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_budget_ministere,
    )

    """ Task order """
    chain(
        [
            budget_total,
            budget_pilotable,
            budget_general,
            evolution_budget_mef,
            montant_intervention_invest,
            budget_ministere,
        ]
    )


@task_group()
def taux_agent():
    engagement_agent = create_grist_etl_task(
        selecteur="engagement_agent",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_engagement_agent,
    )
    election_resultat = create_grist_etl_task(
        selecteur="election_resultat",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_resultat_elections,
    )
    taux_participation = create_grist_etl_task(
        selecteur="taux_participation",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_taux_participation,
    )

    """ Task order """
    chain(
        [
            engagement_agent,
            election_resultat,
            taux_participation,
        ]
    )


@task_group()
def plafond():
    plafond_etpt = create_grist_etl_task(
        selecteur="plafond_etpt",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_plafond_etpt,
    )
    db_plafond_etpt = create_grist_etl_task(
        selecteur="db_plafond_etpt",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_db_plafond_etpt,
    )
    """ Task order """
    chain([plafond_etpt, db_plafond_etpt])
