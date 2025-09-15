from datetime import timedelta
from airflow.decorators import dag, task_group
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from utils.mails.mails import make_mail_func_callback, MailStatus
from utils.config.tasks import get_storage_rows, get_s3_keys_source
from utils.tasks.sql import (
    get_project_config,
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
    get_tbl_names_from_postgresql,
    refresh_views,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_files_to_minio,
    del_files_from_minio,
)

from dags.sg.siep.mmsi.oad.caracteristiques.tasks import (
    convert_oad_caracteristique_to_parquet,
    tasks_oad_caracteristiques,
)
from dags.sg.siep.mmsi.oad.indicateurs.tasks import (
    convert_oad_indic_to_parquet,
    tasks_oad_indicateurs,
)


# Mails
to = ["yanis.tihianine@finances.gouv.fr"]
cc = ["labo-data@finances.gouv.fr", "yanis.tihianine@finances.gouv.fr"]
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/cgefi/barometre?ref_type=heads"  # noqa
LINK_DOC_DONNEE = ""  # noqa


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
    "outil_aide_diagnostic",
    schedule_interval="*/15 6-22 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["DEV", "SG", "SIEP", "MMSI", "OAD"],
    description="""Traitement des données de l'immobilier. Base""",
    default_args=default_args,
    params={
        "nom_projet": "Outil aide diagnostic",
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
def oad():
    # Variables
    nom_projet = "Outil aide diagnostic"
    prod_schema = "siep"

    looking_for_files = S3KeySensor(
        task_id="looking_for_files",
        aws_conn_id="minio_bucket_dsci",
        bucket_name="dsci",
        bucket_key=get_s3_keys_source(nom_projet=nom_projet),
        mode="reschedule",
        poke_interval=timedelta(seconds=30),
        timeout=timedelta(minutes=13),
        soft_fail=True,
        on_skipped_callback=make_mail_func_callback(mail_statut=MailStatus.SKIP),
        on_success_callback=make_mail_func_callback(
            mail_statut=MailStatus.START,
        ),
    )

    @task_group
    def convert_file_to_parquet():
        chain(
            [
                convert_oad_caracteristique_to_parquet(
                    nom_projet=nom_projet, selecteur="oad_carac"
                ),
                convert_oad_indic_to_parquet(
                    nom_projet=nom_projet, selecteur="oad_indic"
                ),
            ]
        )

    end_task = EmptyOperator(
        task_id="end_task",
        on_success_callback=make_mail_func_callback(mail_statut=MailStatus.SUCCESS),
    )

    # Ordre des tâches
    chain(
        get_project_config(),
        looking_for_files,
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema, tbl_names_task_id="get_tbl_names_from_postgresql"
        ),
        convert_file_to_parquet(),
        tasks_oad_caracteristiques(),
        tasks_oad_indicateurs(),
        import_file_to_db.expand(
            storage_row=get_storage_rows(nom_projet=nom_projet).to_dict("records")
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema, tbl_names_task_id="get_tbl_names_from_postgresql"
        ),
        refresh_views(views=["siep.bien_caracteristiques_complet_gestionnaire_vw"]),
        copy_files_to_minio(bucket="dsci"),
        del_files_from_minio(bucket="dsci"),
        end_task,
    )


oad()
