from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from utils.mails.mails import make_mail_func_callback, MailStatus
from utils.tasks.sql import (
    get_project_config,
    get_tbl_names_from_postgresql,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db_at_once,
)
from utils.tasks.grist import download_grist_doc_to_s3

from dags.applications.configuration_projets import process
from dags.applications.configuration_projets.tasks import (
    create_task,
)

# Mails
to = ["yanis.tihianine@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]

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
        "nom_projet": "Configuration des projets",
        "mail": {
            "enable": True,
            "to": to,
            "CC": CC,
        },
        "docs": {
            "lien_pipeline": "",
            "lien_donnees": "",
        },
    },
    on_failure_callback=make_mail_func_callback(
        mail_statut=MailStatus.ERROR,
    ),
)
def configuration_projets():
    """Variables"""
    sqlite_file_s3_filepath = "SG/DSCI/conf_projets/conf_projets.db"
    # Database
    prod_schema = "conf_projets"

    nom_projet = "Configuration des projets"

    """ Tasks definition """
    ref_direction = create_task(
        selecteur="direction",
        process_func=process.process_direction,
        sqlite_file_s3_filepath=sqlite_file_s3_filepath,
    )
    ref_service = create_task(
        selecteur="service",
        process_func=process.process_service,
        sqlite_file_s3_filepath=sqlite_file_s3_filepath,
    )
    projets = create_task(
        selecteur="projets",
        process_func=process.process_projets,
        sqlite_file_s3_filepath=sqlite_file_s3_filepath,
    )
    selecteur = create_task(
        selecteur="selecteur",
        process_func=process.process_selecteur,
        sqlite_file_s3_filepath=sqlite_file_s3_filepath,
    )
    source = create_task(
        selecteur="source",
        process_func=process.process_source,
        sqlite_file_s3_filepath=sqlite_file_s3_filepath,
    )
    storage_paths = create_task(
        selecteur="storage_path",
        process_func=process.process_storage_path,
        sqlite_file_s3_filepath=sqlite_file_s3_filepath,
    )
    col_mapping = create_task(
        selecteur="col_mapping",
        process_func=process.process_col_mapping,
        sqlite_file_s3_filepath=sqlite_file_s3_filepath,
    )
    col_requises = create_task(
        selecteur="col_requises",
        process_func=process.process_col_requises,
        sqlite_file_s3_filepath=sqlite_file_s3_filepath,
    )

    """ Tasks order"""
    projet_config = get_project_config()

    chain(
        projet_config,
        download_grist_doc_to_s3(
            workspace_id="dsci",
            doc_id_key="grist_doc_id_gestion_interne",
            s3_filepath=sqlite_file_s3_filepath,
        ),
        get_tbl_names_from_postgresql(nom_projet=nom_projet),
        create_tmp_tables(
            prod_schema=prod_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
            pg_conn_id="db_depose_fichier",
        ),
        [
            ref_direction,
            ref_service,
            projets,
            selecteur,
            source,
            storage_paths,
            col_mapping,
            col_requises,
        ],
        import_file_to_db_at_once(
            nom_projet=nom_projet,
            sqlite_file_s3_filepath=sqlite_file_s3_filepath,
            pg_conn_id="db_depose_fichier",
            s3_conn_id="minio_bucket_dsci",
            keep_file_id_col=True,
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
            pg_conn_id="db_depose_fichier",
        ),
    )


configuration_projets()
