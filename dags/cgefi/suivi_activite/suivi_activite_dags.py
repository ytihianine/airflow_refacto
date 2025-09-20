from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import timedelta

from infra.mails.sender import create_airflow_callback, MailStatus
from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
)
from utils.config.tasks import get_projet_config
from utils.tasks.grist import download_grist_doc_to_s3
from utils.tasks.s3 import del_s3_files

from dags.cgefi.suivi_activite.tasks import (
    referentiels,
    informations_generales,
    processus_4,
    processus_6,
    processus_atpro,
)

nom_projet = "Emploi et formation"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/carte_identite_mef?ref_type=heads"  # noqa
LINK_DOC_DONNEE = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=10),
}


# Définition du DAG
@dag(
    "cgefi_suivi_activite",
    schedule_interval="*/5 * * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["CGEFI", "ACTIVITE", "POC", "TABLEAU DE BORD"],
    description="""Pipeline qui récupère les nouvelles données dans Grist
        pour actualiser le tableau de bord de suivi d'activité du CGEFI""",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "cgefi_poc",
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
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
)
def suivi_activite():
    """Task order"""
    chain(
        # projet_config,
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci-cgefi",
            doc_id_key="grist_doc_id_cgefi_suivi_activite",
        ),
        [
            referentiels(),
            informations_generales(),
            processus_4(),
            processus_6(),
            processus_atpro(),
        ],
        create_tmp_tables(),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        del_s3_files(bucket="dsci"),
        delete_tmp_tables(),
    )


suivi_activite()
