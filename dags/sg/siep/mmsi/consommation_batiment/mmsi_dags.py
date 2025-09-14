from datetime import timedelta

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from utils.mails.mails import make_mail_func_callback, MailStatus

from utils.tasks.sql import (
    get_project_config,
    get_tbl_names_from_postgresql,
    create_tmp_tables,
    import_file_to_db_at_once,
    copy_tmp_table_to_real_table,
    # set_dataset_last_update_date,
)
from utils.tasks.s3 import (
    copy_files_to_minio,
    del_files_from_minio,
)
from utils.config.tasks import get_s3_keys_source

from dags.sg.siep.mmsi.consommation_batiment.tasks import (
    convert_cons_mens_to_parquet,
    informations_batiments,
    conso_mensuelles,
    unpivot_conso_mens_brute,
    unpivot_conso_mens_corrigee,
    conso_annuelles,
    conso_avant_2019,
    conso_statut_par_fluide,
    conso_statut_fluide_global,
    conso_statut_batiment,
)


# Mails
To = ["mmsi.siep@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]
link_documentation_pipeline = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/consommation_batiment?ref_type=heads"  # noqa
link_documentation_donnees = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa


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
    "consommation_des_batiments",
    schedule_interval="*/15 8-19 * * 1-5",
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "CONSOMMATION"],
    description="Pipeline de traitement des données de consommation des bâtiments. Source des données: OSFI",  # noqa
    default_args=default_args,
    params={
        "nom_projet": "Consommation des bâtiments",
        "mail": {"enable": False, "To": To, "CC": CC},
        "docs": {
            "lien_pipeline": link_documentation_pipeline,
            "lien_donnees": link_documentation_donnees,
        },
    },
    on_failure_callback=make_mail_func_callback(
        mail_statut=MailStatus.ERROR,
    ),
)
def consommation_des_batiments():
    # Variables
    nom_projet = "Consommation des bâtiments"
    tmp_schema = "temporaire"
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
        on_success_callback=make_mail_func_callback(mail_statut=MailStatus.START),
    )

    # Ordre des tâches
    chain(
        get_project_config(),
        looking_for_files,
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        informations_batiments(selecteur="bien_info_complementaire"),
        convert_cons_mens_to_parquet(selecteur="conso_mens_source"),
        conso_mensuelles(
            selecteur="conso_mens", selecteur_cons_mens_src="conso_mens_source"
        ),
        [
            unpivot_conso_mens_brute(
                selecteur="conso_mens_brute_unpivot", selecteur_conso_mens="conso_mens"
            ),
            unpivot_conso_mens_corrigee(
                selecteur="conso_mens_corr_unpivot", selecteur_conso_mens="conso_mens"
            ),
        ],
        conso_annuelles(selecteur="conso_annuelle", selecteur_conso_mens="conso_mens"),
        conso_avant_2019(
            selecteur="conso_avant_2019", selecteur_conso_annuelle="conso_annuelle"
        ),
        conso_statut_par_fluide(
            selecteur="conso_statut_par_fluide",
            selecteur_conso_annuelle="conso_annuelle",
        ),
        conso_statut_fluide_global(
            selecteur="conso_statut_fluide_global",
            selecteur_conso_statut_par_fluide="conso_statut_par_fluide",
        ),
        conso_statut_batiment(
            selecteur="conso_statut_batiment",
            selecteur_conso_statut_fluide_global="conso_statut_fluide_global",
            selecteur_conso_statut_avant_2019="conso_avant_2019",
        ),
        import_file_to_db_at_once(pg_conn_id="db_data_store"),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema,
            tmp_schema=tmp_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        copy_files_to_minio(bucket="dsci"),
        del_files_from_minio(bucket="dsci"),
        # set_dataset_last_update_date(
        #     dataset_ids=[49, 50, 51, 52, 53, 54],
        # ),
    )


consommation_des_batiments()
