from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_grist_etl_task


from dags.sg.sircom.tdb_interne import process


@task_group(group_id="abonnes_visites")
def abonnes_visites():
    reseaux_sociaux = create_grist_etl_task(
        selecteur="reseaux_sociaux",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_reseaux_sociaux,
    )
    visites_portail = create_grist_etl_task(
        selecteur="visites_portail",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_visites_portail,
    )
    visites_bercyinfo = create_grist_etl_task(
        selecteur="visites_bercyinfo",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_visites_bercyinfo,
    )
    visites_alize = create_grist_etl_task(
        selecteur="visites_alize",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_visites_alize,
    )
    visites_intranet_sg = create_grist_etl_task(
        selecteur="visites_intranet_sg",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_visites_intranet_sg,
    )
    performances_lettres = create_grist_etl_task(
        selecteur="performances_lettres",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_performances_lettres,
    )
    abonnes_aux_lettres = create_grist_etl_task(
        selecteur="abonnes_lettres",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_abonnes_aux_lettres,
    )
    ouverture_lettre_alize = create_grist_etl_task(
        selecteur="ouverture_lettre_alize",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_ouverture_lettre_alize,
    )
    chain(
        [
            reseaux_sociaux(),
            visites_portail(),
            visites_bercyinfo(),
            visites_alize(),
            visites_intranet_sg(),
            performances_lettres(),
            abonnes_aux_lettres(),
            ouverture_lettre_alize(),
        ]
    )


@task_group(group_id="budget")
def budget():
    budget_depense = create_grist_etl_task(
        selecteur="synthese_depenses",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_budget_depense,
    )
    chain(budget_depense())


@task_group(group_id="enquetes")
def enquetes():
    engagement_agents_mef = create_grist_etl_task(
        selecteur="engagement_agents_mef",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_engagement_agents_mef,
    )
    qualite_vie_travail = create_grist_etl_task(
        selecteur="qualite_vie_travail",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_qualite_vie_travail,
    )
    collab_inter_structure = create_grist_etl_task(
        selecteur="collab_inter_structure",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_collab_inter_structure,
    )
    obs_interne = create_grist_etl_task(
        selecteur="obs_interne",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_obs_interne,
    )
    enquete_360 = create_grist_etl_task(
        selecteur="enquete_360",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_enquete_360,
    )
    obs_interne_participation = create_grist_etl_task(
        selecteur="obs_interne_participation",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_obs_interne_participation,
    )
    engagement_environnement = create_grist_etl_task(
        selecteur="engagement_environnement",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_engagement_environnement,
    )
    chain(
        [
            engagement_agents_mef(),
            qualite_vie_travail(),
            collab_inter_structure(),
            obs_interne(),
            enquete_360(),
            obs_interne_participation(),
            engagement_environnement(),
        ]
    )


@task_group(group_id="metiers")
def metiers():
    indicateurs_metiers = create_grist_etl_task(
        selecteur="indicateurs_metiers",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_indicateurs_metiers,
    )
    enquete_satisfaction = create_grist_etl_task(
        selecteur="enquete_satisfaction",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_enquete_satisfaction,
    )
    etudes = create_grist_etl_task(
        selecteur="etudes",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_etudes,
    )
    communique_presse = create_grist_etl_task(
        selecteur="communique_presse",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_communique_presse,
    )
    studio_graphique = create_grist_etl_task(
        selecteur="studio_graphique",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_studio_graphique,
    )
    chain(
        [
            indicateurs_metiers(),
            enquete_satisfaction(),
            etudes(),
            communique_presse(),
            studio_graphique(),
        ]
    )


@task_group(group_id="ressources_humaines")
def ressources_humaines():
    rh_formation = create_grist_etl_task(
        selecteur="rh_formation",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_rh_formation,
    )
    rh_turnover = create_grist_etl_task(
        selecteur="rh_turnover",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_rh_turnover,
    )
    rh_contractuel = create_grist_etl_task(
        selecteur="rh_contractuel",
        normalisation_process_func=process.clean_and_normalize_df,
        process_func=process.process_rh_contractuel,
    )
    chain([rh_formation(), rh_turnover(), rh_contractuel()])
