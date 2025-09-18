from airflow.decorators import dag, task_group
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.sql import get_tbl_names_from_postgresql, create_tmp_tables

from dags.applications.catalogue.tasks import (
    create_task,
    add_new_datasets,
    sync_datasets,
)


To = ["yanis.tihianine@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


# Définition du DAG
@dag(
    "catalogue",
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "CATALOGUE", "DOCUMENTATION"],
    description="""Pipeline qui scanne les nouvelles tables créées dans la base de données
        et synchronise la base de données et le catalogue GRIST""",
    default_args=default_args,
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
    params={
        "nom_projet": "Catalogue des données",
        "mail": {
            "enable": False,
            "To": To,
            "CC": CC,
        },
        "docs": {
            "lien_pipeline": "",
            "lien_donnees": "",
        },
    },
)
def catalogue_dag():
    """Config"""
    # Variables
    prod_schema = "documentation"
    sqlite_file_s3_filepath = "SG/DSCI/catalogue/catalogue_sqlite.db"

    """ Tasks definition """

    @task_group()
    def update_referentiels() -> None:
        ref_frequence = create_task(
            selecteur="ref_frequence", sqlite_file_s3_filepath=sqlite_file_s3_filepath
        )
        ref_contact = create_task(
            selecteur="ref_contact", sqlite_file_s3_filepath=sqlite_file_s3_filepath
        )
        ref_couverture_geographique = create_task(
            selecteur="ref_couverture_geographique",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
        )
        ref_licence = create_task(
            selecteur="ref_licence", sqlite_file_s3_filepath=sqlite_file_s3_filepath
        )
        ref_service = create_task(
            selecteur="ref_service", sqlite_file_s3_filepath=sqlite_file_s3_filepath
        )
        ref_source_format = create_task(
            selecteur="ref_source_format",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
        )
        ref_structures = create_task(
            selecteur="ref_structures", sqlite_file_s3_filepath=sqlite_file_s3_filepath
        )
        ref_systeme_info = create_task(
            selecteur="ref_systeme_info",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
        )
        ref_theme = create_task(
            selecteur="ref_theme", sqlite_file_s3_filepath=sqlite_file_s3_filepath
        )
        ref_type_donnees = create_task(
            selecteur="ref_type_donnees",
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
        )

        """ Tasks order """
        chain(
            [
                ref_frequence,
                ref_contact,
                ref_couverture_geographique,
                ref_licence,
                ref_service,
                ref_source_format,
                ref_structures,
                ref_systeme_info,
                ref_theme,
                ref_type_donnees,
            ]
        )

    @task_group
    def update_datasets() -> None:
        chain(
            add_new_datasets(tbl_name="datasets"),
            sync_datasets(
                selecteur="datasets",
                prod_schema=prod_schema,
                sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            ),
        )

    """ Task order """
    chain(
        download_grist_doc_to_s3(
            workspace_id="catalogue",
            doc_id_key="grist_doc_id_catalogue",
            s3_filepath=sqlite_file_s3_filepath,
        ),
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema, tbl_names_task_id="get_tbl_names_from_postgresql"
        ),
        [update_referentiels(), update_datasets()],
    )


catalogue_dag()
