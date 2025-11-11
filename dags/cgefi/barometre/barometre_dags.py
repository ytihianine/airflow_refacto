from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_files_to_db,
    # set_dataset_last_update_date,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)
from utils.config.tasks import (
    get_s3_keys_source,
)

from dags.cgefi.barometre.tasks import (
    source_files,
)


nom_projet = "Baromètre"
mail_to = ["corpus.cgefi@finances.gouv.fr"]
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
    "Barometre",
    schedule_interval="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["CGEFI", "BAROMETRE"],
    description="""Pipeline de traitement des données pour le Baromètre""",
    default_args=create_default_args(retries=0),
    params=create_dag_params(
        nom_projet=nom_projet,
        prod_schema="cgefi",
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
        mail_enable=False,
    ),
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
)
def barometre():
    """Tasks definition"""
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

    end_task = EmptyOperator(
        task_id="end_task",
        on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
    )

    """ Task order """
    chain(
        looking_for_files,
        source_files(),
        create_tmp_tables(),
        import_files_to_db(pg_conn_id="db_data_store"),
        copy_tmp_table_to_real_table(),
        # set_dataset_last_update_date(db_hook=POSTGRE_HOOK, dataset_ids=[3]),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
        end_task,
    )


barometre()
