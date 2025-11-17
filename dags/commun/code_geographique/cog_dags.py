from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago


from infra.mails.default_smtp import create_airflow_callback, MailStatus

from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    refresh_views,
    # set_dataset_last_update_date,
)

from dags.commun.code_geographique.tasks import code_geographique, geojson, code_iso

nom_projet = "Code géographique"
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
    "informations_geographiques",
    schedule_interval="00 00 7 * *",
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "COMMUN", "DSCI"],
    description="""Récupération des codes géographiques""",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "commun",
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
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def informations_geographiques():
    """Récupération de toutes les données géographiques"""

    """ Hooks """
    # Database
    TBL_NAMES = (
        "region",
        "departement",
        "commune",
        "code_iso_region",
        "code_iso_departement",
        "region_geojson",
        "departement_geojson",
    )

    # Ordre des tâches
    chain(
        code_geographique(),
        geojson(),
        code_iso(),
        create_tmp_tables(),
        copy_tmp_table_to_real_table(),
        refresh_views(),
    )


informations_geographiques()
