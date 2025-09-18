from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import timedelta

from utils.tasks.sql import create_tmp_tables, copy_tmp_table_to_real_table
from utils.tasks.grist import download_grist_doc_to_s3
from dags.sg.dsci.carte_identite_mef.tasks import (
    validate_params,
    effectif,
    budget,
    taux_agent,
    plafond,
)


nom_projet = "Carte_Identite_MEF"
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
    "retries": 0,
    "retry_delay": timedelta(seconds=20),
}


@dag(
    dag_id="carte_identite_mef",
    default_args=default_args,
    schedule_interval="*/8 8-13,14-19 * * 1-5",
    catchup=False,
    max_consecutive_failed_dag_runs=1,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "dsci",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": ["yanis.tihianine@finances.gouv.fr"],
            "cc": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
)
def carte_identite_mef_dag():
    """Tasks order"""
    chain(
        validate_params(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
            doc_id_key="grist_doc_id_tdb_carte_identite_mef",
        ),
        [effectif(), budget(), taux_agent(), plafond()],
        create_tmp_tables(),
        copy_tmp_table_to_real_table(),
    )


carte_identite_mef_dag()
