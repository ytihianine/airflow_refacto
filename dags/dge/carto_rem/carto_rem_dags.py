from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.sql import (
    get_tbl_names_from_postgresql,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    get_tbl_names_from_postgresql,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import get_s3_keys_source, get_storage_rows

from dags.dge.carto_rem.tasks import (
    source_files,
    output_files,
    referentiels,
    source_grist,
)


# Mails
To = ["yanis.tihianine@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]
link_documentation_pipeline = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/cgefi/barometre?ref_type=heads"  # noqa
link_documentation_donnees = ""  # noqa


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# Définition du DAG
@dag(
    "cartographie_remuneration",
    schedule_interval="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DGE", "RH"],
    description="""DGE - Cartographie rémunération""",
    default_args=default_args,
    params={
        "nom_projet": "Cartographie rémunération",
        "sqlite_file_s3_filepath": "sg/dge/cartographie_remuneration/carto_rem.db",
        "mail": {
            "enable": False,
            "To": To,
            "CC": CC,
        },
        "docs": {
            "lien_pipeline": link_documentation_pipeline,
            "lien_donnees": link_documentation_donnees,
        },
    },
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
)
def cartographie_remuneration():
    nom_projet = "Cartographie rémunération"
    sqlite_file_s3_filepath = "sg/dge/cartographie_remuneration/carto_rem.db"
    prod_schema = "cartographie_remuneration"

    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=create_airflow_callback(mail_status=MailStatus.SKIP),
        on_success_callback=create_airflow_callback(mail_status=MailStatus.START),
    )

    """ Task order """
    chain(
        looking_for_files,
        download_grist_doc_to_s3(
            workspace_id="dsci",
            doc_id_key="grist_doc_id_carto_rem",
            s3_filepath=sqlite_file_s3_filepath,
        ),
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema, tbl_names_task_id="get_tbl_names_from_postgresql"
        ),
        [source_files(), referentiels(), source_grist()],
        output_files(),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            storage_row=get_storage_rows(nom_projet=nom_projet).to_dict("records")
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema, tbl_names_task_id="get_tbl_names_from_postgresql"
        ),
    )


cartographie_remuneration()
