from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from utils.mails.mails import make_mail_func_callback, MailStatus
from utils.common.tasks_sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db_at_once,
    get_tbl_names_from_postgresql,
    # set_dataset_last_update_date,
)

from utils.common.tasks_minio import (
    copy_files_to_minio,
    del_files_from_minio,
)
from utils.common.config_func import (
    get_s3_keys_source,
)

from dags.cgefi.barometre.tasks import (
    organisme,
    cartographie,
    efc,
    recommandation,
    fiche_signaletique,
    # rapport_hors_corpus,
    rapport_annuel,
)


# Mails
To = ["corpus.cgefi@finances.gouv.fr"]
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
    "Barometre",
    schedule_interval="*/15 8-20 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["CGEFI", "BAROMETRE"],
    description="""Pipeline de traitement des données pour le Baromètre""",
    default_args=default_args,
    params={
        "nom_projet": "Baromètre",
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
    on_failure_callback=make_mail_func_callback(mail_statut=MailStatus.ERROR),
)
def barometre():
    nom_projet = "Baromètre"
    prod_schema = "cgefi"
    tmp_schema = "temporaire"

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
        on_success_callback=make_mail_func_callback(mail_statut=MailStatus.START),
    )

    end_task = EmptyOperator(
        task_id="end_task",
        on_success_callback=make_mail_func_callback(mail_statut=MailStatus.SUCCESS),
    )

    """ Task order """
    chain(
        looking_for_files,
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        organisme(
            nom_projet=nom_projet, selecteur="organisme", selecteur_organisme_hc=""
        ),
        [
            recommandation(nom_projet=nom_projet, selecteur="recommandations"),
            fiche_signaletique(nom_projet=nom_projet, selecteur="fiches_signaletiques"),
            cartographie(nom_projet=nom_projet, selecteur="cartographie"),
            efc(nom_projet=nom_projet, selecteur="efc"),
            rapport_annuel(
                nom_projet=nom_projet,
                selecteur_rapport_annuel="rapports_annuels",
                selecteur_date_retour_attendue="date_retour_attendue",
            ),
        ],
        import_file_to_db_at_once(pg_conn_id="db_data_store"),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        # set_dataset_last_update_date(db_hook=POSTGRE_HOOK, dataset_ids=[3]),
        copy_files_to_minio(bucket="dsci"),
        del_files_from_minio(bucket="dsci"),
        end_task,
    )


barometre()
