from airflow.decorators import dag, task_group
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import timedelta

from utils.tasks.sql import (
    get_project_config,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    get_tbl_names_from_postgresql,
)
from utils.tasks.grist import download_grist_doc_to_s3
from dags.sg.dsci.carte_identite_mef.tasks import create_task
from dags.sg.dsci.carte_identite_mef import process

LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/carte_identite_mef?ref_type=heads"  # noqa
LINK_DOC_DATA = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=20),
}


@dag(
    dag_id="carte_identite_mef",
    default_args=default_args,
    schedule_interval="*/8 8-13,14-19 * * 1-5",
    catchup=False,
    max_consecutive_failed_dag_runs=1,
    params={
        "nom_projet": "Carte_Identite_MEF",
        "mail": {
            "enable": False,
            "To": ["yanis.tihianine@finances.gouv.fr"],
            "CC": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
)
def carte_identite_mef_dag():
    # Variables
    sqlite_file_s3_filepath = "SG/DSCI/carte_identite_mef/carte_identite_mef.db"
    # Database
    prod_schema = "dsci"
    tmp_schema = "temporaire"

    """ Tasks definition """

    @task_group()
    def effectif():
        teletravail = create_task(
            selecteur="teletravail",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_teletravail,
        )
        teletravail_frequence = create_task(
            selecteur="teletravail_frequence",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_teletravail_frequence,
        )
        teletravail_opinion = create_task(
            selecteur="teletravail_opinion",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_teletravail_opinion,
        )
        effectif_direction_perimetre = create_task(
            selecteur="effectif_direction_perimetre",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_mef_par_direction,
        )
        effectif_direction = create_task(
            selecteur="effectif_direction",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_effectif_direction,
        )
        effectif_perimetre = create_task(
            selecteur="effectif_perimetre",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_effectifs_par_perimetre,
        )
        effectif_departements = create_task(
            selecteur="effectif_departements",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_effectif_par_departements,
        )
        masse_salariale = create_task(
            selecteur="masse_salariale",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
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
        budget_total = create_task(
            selecteur="budget_total",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_budget_total,
        )
        budget_pilotable = create_task(
            selecteur="budget_pilotable",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_budget_pilotable,
        )
        budget_general = create_task(
            selecteur="budget_general",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_budget_general,
        )
        evolution_budget_mef = create_task(
            selecteur="evolution_budget_mef",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_evolution_budget_mef,
        )
        montant_intervention_invest = create_task(
            selecteur="montant_intervention_invest",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_montant_invest,
        )
        budget_ministere = create_task(
            selecteur="budget_ministere",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
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
        engagement_agent = create_task(
            selecteur="engagement_agent",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_engagement_agent,
        )
        election_resultat = create_task(
            selecteur="election_resultat",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_resultat_elections,
        )
        taux_participation = create_task(
            selecteur="taux_participation",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
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
        plafond_etpt = create_task(
            selecteur="plafond_etpt",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_plafond_etpt,
        )
        db_plafond_etpt = create_task(
            selecteur="db_plafond_etpt",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            process_func=process.process_db_plafond_etpt,
        )
        """ Task order """
        chain([plafond_etpt, db_plafond_etpt])

    projet_config = get_project_config()

    chain(
        projet_config,
        download_grist_doc_to_s3(
            workspace_id="dsci",
            doc_id_key="grist_doc_id_tdb_carte_identite_mef",
            s3_filepath=sqlite_file_s3_filepath,
        ),
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        [effectif(), budget(), taux_agent(), plafond()],
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
    )


carte_identite_mef_dag()
