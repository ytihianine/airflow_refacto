from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.sql import create_tmp_tables, copy_tmp_table_to_real_table
from utils.tasks.s3 import copy_s3_files, del_s3_files

from dags.applications.catalogue.tasks import (
    update_referentiels,
    source_grist,
    source_database,
    compare_catalogues,
    sync_catalogues,
)


nom_projet = "Catalogue"
LINK_DOC_PIPELINE = ""  # noqa
LINK_DOC_DATA = ""  # noqa

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
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "documentation",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": ["yanis.tihianine@finances.gouv.fr"],
            "cc": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
)
def catalogue_dag():
    """Task order"""
    chain(
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="catalogue",
            doc_id_key="grist_doc_id_catalogue",
        ),
        update_referentiels(),
        [
            source_grist(),
            source_database(),
        ],
        compare_catalogues(),
        sync_catalogues(),
        create_tmp_tables(),
        copy_tmp_table_to_real_table(),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
    )


catalogue_dag()
