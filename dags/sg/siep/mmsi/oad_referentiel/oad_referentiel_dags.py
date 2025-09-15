from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from utils.mails.mails import make_mail_func_callback, MailStatus
from utils.tasks.sql import (
    get_project_config,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    get_tbl_names_from_postgresql,
    # set_dataset_last_update_date,
)

# from utils.common.tasks_minio import (
#     copy_files_to_minio,
#     del_files_from_minio,
# )
from utils.config.tasks import (
    get_s3_keys_source,
    get_storage_rows,
)

from dags.sg.siep.mmsi.oad_referentiel.tasks import bien_typologie


# Mails
to = ["yanis.tihianine@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/cgefi/barometre?ref_type=heads"  # noqa
LINK_DOC_DATA = ""  # noqa


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
    "outil_aide_diagnostic_referentiel",
    schedule_interval=None,  # timedelta(seconds=30),
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des référentiels issus de l'OAD.""",
    max_consecutive_failed_dag_runs=1,
    params={
        "nom_projet": "Outil aide diagnostic - référentiel",
        "send_mail": True,
        "link_documentation_pipeline": LINK_DOC_PIPELINE,
        "link_documentation_donnees": LINK_DOC_DATA,
    },
    on_failure_callback=make_mail_func_callback(
        mail_statut=MailStatus.ERROR,
    ),
    default_args=default_args,
)
def oad_referentiel():
    nom_projet = "Outil aide diagnostic - référentiel"
    bucket = "dsci"
    # Databases
    tmp_schema = "temporaire"
    prod_schema = "siep"

    """ Task definition """
    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name=bucket,
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),  # timedelta(minutes=1),
        timeout=timedelta(minutes=1),
        soft_fail=True,
        on_skipped_callback=make_mail_func_callback(mail_statut=MailStatus.SKIP),
        on_success_callback=make_mail_func_callback(
            mail_statut=MailStatus.START,
        ),
    )

    """ Task order """
    projet_config = get_project_config()

    chain(
        projet_config,
        looking_for_files,
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        [bien_typologie(nom_projet=nom_projet)],
        import_file_to_db.expand(
            storage_row=get_storage_rows(nom_projet=nom_projet).to_dict("records")
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        # copy_files_to_minio.partial(
        #     s3_hook=MINIO_FILE_HANDLER, bucket=BUCKET, dest_key=S3_DEST_KEY
        # ).expand(
        #     source_key=storage_paths.loc[
        #         storage_paths["type_fichier"] == "Source", "s3_filepath"
        #     ].to_list()
        # ),
        # del_files_from_minio(
        #     s3_hook=MINIO_FILE_HANDLER,
        #     bucket=bucket,
        #     s3_filepaths=storage_paths.loc[
        #         storage_paths["type_fichier"] == "Source", "s3_filepath"
        #     ].to_list(),
        # ),
    )


oad_referentiel()
