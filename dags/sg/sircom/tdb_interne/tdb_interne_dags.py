from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import timedelta

from utils.mails.mails import make_mail_func_callback, MailStatus
from utils.common.tasks_sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
)
from utils.common.tasks_grist import download_grist_doc_to_s3
from dags.sg.sircom.tdb_interne.tasks import (
    abonnes_visites,
    budget,
    enquetes,
    metiers,
    ressources_humaines,
)


# Mails
nom_projet = "TdB interne - SIRCOM"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/carte_identite_mef?ref_type=heads"  # noqa
LINK_DOC_DATA = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


# Définition du DAG
@dag(
    "tdb_sircom",
    schedule_interval="*/8 8-13,14-19 * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIRCOM", "PRODUCTION", "TABLEAU DE BORD"],
    description="""Pipeline qui scanne les nouvelles données dans Grist
        pour actualiser le tableau de bord du SIRCOM""",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "sircom",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": [
                "brigitte.lekime@finances.gouv.fr",
                "yanis.tihianine@finances.gouv.fr",
            ],
            "cc": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
    on_failure_callback=make_mail_func_callback(
        mail_statut=MailStatus.ERROR,
    ),
)
def tdb_sircom():
    """Task order"""
    chain(
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
            doc_id_key="grist_doc_id_tdb_sircom",
        ),
        [abonnes_visites(), budget(), enquetes(), metiers(), ressources_humaines()],
        create_tmp_tables(),
        copy_tmp_table_to_real_table(),
    )


tdb_sircom()
