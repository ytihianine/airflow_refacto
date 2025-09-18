from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.tasks.sql import (
    create_tmp_tables,
    import_files_to_db,
    copy_tmp_table_to_real_table,
)
from utils.tasks.grist import download_grist_doc_to_s3

from dags.applications.configuration_projets.tasks import (
    validate_params,
    process_data,
)


nom_projet = "Configuration des projets"
LINK_DOC_PIPELINE = ""
LINK_DOC_DONNEE = ""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


@dag(
    dag_id="configuration_projets",
    schedule_interval="*/15 * * * 1-5",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    catchup=False,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "conf_projets",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": ["yanis.tihianine@finances.gouv.fr"],
            "cc": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DONNEE,
        },
    },
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def configuration_projets():
    """Tasks order"""
    chain(
        validate_params(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
            doc_id_key="grist_doc_id_gestion_interne",
        ),
        create_tmp_tables(),
        process_data(),
        import_files_to_db(keep_file_id_col=True),
        copy_tmp_table_to_real_table(),
    )


configuration_projets()
