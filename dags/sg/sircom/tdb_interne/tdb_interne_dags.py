from airflow.decorators import dag, task_group
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import timedelta

from utils.mails.mails import make_mail_func_callback, MailStatus
from utils.tasks.sql import (
    get_project_config,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    get_tbl_names_from_postgresql,
)
from utils.tasks.grist import download_grist_doc_to_s3
from dags.sg.sircom.tdb_interne.tasks import create_task
from dags.sg.sircom.tdb_interne import process


# Mails
to = ["brigitte.lekime@finances.gouv.fr", "yanis.tihianine@finances.gouv.fr"]
cc = ["labo-data@finances.gouv.fr"]
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/carte_identite_mef?ref_type=heads"  # noqa
LINK_DOC_DONNEE = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


# Définition du DAG
@dag(
    "tdb_sircom",
    schedule_interval="*/8 8-13,14-19 * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIRCOM", "PRODUCTION", "TABLEAU DE BORD"],
    description="""Pipeline qui scanne les nouvelles données dans Grist
        pour actualiser le tableau de bord du SIRCOM""",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "nom_projet": "TdB interne - SIRCOM",
        "mail": {
            "enable": False,
            "to": to,
            "cc": cc,
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DONNEE,
        },
    },
    on_failure_callback=make_mail_func_callback(
        mail_statut=MailStatus.ERROR,
    ),
)
def tdb_sircom():
    sqlite_file_s3_filepath = "SG/SIRCOM/tdb_interne/tdb_interne.db"
    # Database
    prod_schema = "sircom"
    tmp_schema = "temporaire"

    """ Task definition """

    @task_group(group_id="abonnes_visites")
    def abonnes_visites():
        reseaux_sociaux = create_task(
            selecteur="reseaux_sociaux",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_reseaux_sociaux,
        )
        visites_portail = create_task(
            selecteur="visites_portail",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_visites_portail,
        )
        visites_bercyinfo = create_task(
            selecteur="visites_bercyinfo",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_visites_bercyinfo,
        )
        visites_alize = create_task(
            selecteur="visites_alize",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_visites_alize,
        )
        visites_intranet_sg = create_task(
            selecteur="visites_intranet_sg",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_visites_intranet_sg,
        )
        performances_lettres = create_task(
            selecteur="performances_lettres",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_performances_lettres,
        )
        abonnes_aux_lettres = create_task(
            selecteur="abonnes_lettres",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_abonnes_aux_lettres,
        )
        ouverture_lettre_alize = create_task(
            selecteur="ouverture_lettre_alize",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_ouverture_lettre_alize,
        )
        chain(
            [
                reseaux_sociaux,
                visites_portail,
                visites_bercyinfo,
                visites_alize,
                visites_intranet_sg,
                performances_lettres,
                abonnes_aux_lettres,
                ouverture_lettre_alize,
            ]
        )

    @task_group(group_id="budget")
    def budget():
        budget_depense = create_task(
            selecteur="synthese_depenses",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_budget_depense,
        )
        chain(budget_depense)

    @task_group(group_id="enquetes")
    def enquetes():
        engagement_agents_mef = create_task(
            selecteur="engagement_agents_mef",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_engagement_agents_mef,
        )
        qualite_vie_travail = create_task(
            selecteur="qualite_vie_travail",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_qualite_vie_travail,
        )
        collab_inter_structure = create_task(
            selecteur="collab_inter_structure",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_collab_inter_structure,
        )
        obs_interne = create_task(
            selecteur="obs_interne",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_obs_interne,
        )
        enquete_360 = create_task(
            selecteur="enquete_360",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_enquete_360,
        )
        obs_interne_participation = create_task(
            selecteur="obs_interne_participation",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_obs_interne_participation,
        )
        engagement_environnement = create_task(
            selecteur="engagement_environnement",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_engagement_environnement,
        )
        chain(
            [
                engagement_agents_mef,
                qualite_vie_travail,
                collab_inter_structure,
                obs_interne,
                enquete_360,
                obs_interne_participation,
                engagement_environnement,
            ]
        )

    @task_group(group_id="metiers")
    def metiers():
        indicateurs_metiers = create_task(
            selecteur="indicateurs_metiers",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_indicateurs_metiers,
        )
        enquete_satisfaction = create_task(
            selecteur="enquete_satisfaction",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_enquete_satisfaction,
        )
        etudes = create_task(
            selecteur="etudes",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_etudes,
        )
        communique_presse = create_task(
            selecteur="communique_presse",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_communique_presse,
        )
        studio_graphique = create_task(
            selecteur="studio_graphique",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_studio_graphique,
        )
        chain(
            [
                indicateurs_metiers,
                enquete_satisfaction,
                etudes,
                communique_presse,
                studio_graphique,
            ]
        )

    @task_group(group_id="ressources_humaines")
    def ressources_humaines():
        rh_formation = create_task(
            selecteur="rh_formation",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_rh_formation,
        )
        rh_turnover = create_task(
            selecteur="rh_turnover",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_rh_turnover,
        )
        rh_contractuel = create_task(
            selecteur="rh_contractuel",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_rh_contractuel,
        )
        chain([rh_formation, rh_turnover, rh_contractuel])

    projet_config = get_project_config()

    chain(
        projet_config,
        download_grist_doc_to_s3(
            workspace_id="dsci",
            doc_id_key="grist_doc_id_tdb_sircom",
            s3_filepath=sqlite_file_s3_filepath,
        ),
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        [abonnes_visites(), budget(), enquetes(), metiers(), ressources_humaines()],
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
    )


tdb_sircom()
