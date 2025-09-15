from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from infra.http_client.adapters import HttpxAPIClient
from infra.grist.client import GristAPI
from utils.config.vars import PROXY, AGENT
from utils.mails.mails import make_mail_func_callback, MailStatus

from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    # set_dataset_last_update_date,
)


from dags.commun.code_geographique.tasks import (
    communes,
    departements,
    regions,
    code_iso,
    region_geojson,
    departement_geojson,
    refresh_views,
)

# Mails
to = ["yanis.tihianine@finances.gouv.fr"]
cc = ["labo-data@finances.gouv.fr"]
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
    "code_geographique",
    schedule_interval="00 00 7 * *",
    max_active_runs=1,
    catchup=False,
    tags=["DEV", "COMMUN", "DSCI"],
    description="""Récupération des codes géographiques""",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "nom_projet": "Code géographique",
        "mail": {
            "enable": False,
            "to": to,
            "cc": cc,
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
def code_geographique():
    """Récupération de toutes les données géographiques"""

    """ Hooks """
    # Database
    POSTGRE_HOOK = PostgresHook(postgres_conn_id="db_data_store")
    TMP_SCHEMA = "temporaire"
    PROD_SCHEMA = "commun"
    TBL_NAMES = (
        "region",
        "departement",
        "commune",
        "code_iso_region",
        "code_iso_departement",
        "region_geojson",
        "departement_geojson",
    )

    # API Info
    httpx_internet_client = HttpxAPIClient(proxy=PROXY, user_agent=AGENT)

    # Grist
    grist_api = GristAPI(
        api_client=httpx_internet_client,
        base_url="https://grist.numerique.gouv.fr",
        workspace_id="dsci",
        doc_id=Variable.get("grist_doc_id_data_commune"),
        api_token=Variable.get("grist_secret_key"),
    )

    """ Variables """
    # Données COG
    PAGE_SIZE = "?page_size=0"  # get all data at once
    BASE_URL = "https://tabular-api.data.gouv.fr/api/resources"
    ID_DATASET_REGION = "2486b351-5d85-4e1a-8d12-5df082c75104"
    ID_DATASET_DEPARTEMENT = "54a8263d-6e2d-48d5-b214-aa17cc13f7a0"
    ID_DATASET_COMMUNE = "91a95bee-c7c8-45f9-a8aa-f14cc4697545"
    ID_DATASET_COMMUNE_OUTRE_MER = "b797d73d-663c-4d3d-baf0-2d24b2d3a321"
    URL_REGION = "/".join([BASE_URL, ID_DATASET_REGION, "data", PAGE_SIZE])
    URL_DEPARTEMENT = "/".join([BASE_URL, ID_DATASET_DEPARTEMENT, "data", PAGE_SIZE])
    URL_COMMUNE = "/".join([BASE_URL, ID_DATASET_COMMUNE, "data", PAGE_SIZE])
    URL_COMMUNE_OUTRE_MER = "/".join(
        [BASE_URL, ID_DATASET_COMMUNE_OUTRE_MER, "data", PAGE_SIZE]
    )

    # Données GeoJson
    URL_REGION_GEOJSON = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/refs/heads/master/regions-avec-outre-mer.geojson"  # noqa
    URL_DEPARTEMENT_GEOJSON = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/refs/heads/master/departements-avec-outre-mer.geojson"  # noqa

    # Ordre des tâches
    chain(
        create_tmp_tables(
            db_hook=POSTGRE_HOOK,
            prod_schema=PROD_SCHEMA,
            tmp_schema=TMP_SCHEMA,
            tbl_names=TBL_NAMES,
        ),
        regions(
            url_region=URL_REGION,
            tbl_name=TBL_NAMES[0],
            api_client=httpx_internet_client,
            db_hook=POSTGRE_HOOK,
        ),
        departements(
            url_departement=URL_DEPARTEMENT,
            tbl_name=TBL_NAMES[1],
            api_client=httpx_internet_client,
            db_hook=POSTGRE_HOOK,
        ),
        communes(
            url_communes=URL_COMMUNE,
            url_communes_outremer=URL_COMMUNE_OUTRE_MER,
            tbl_name=TBL_NAMES[2],
            api_client=httpx_internet_client,
            db_hook=POSTGRE_HOOK,
        ),
        code_iso(grist_api_client=grist_api, db_hook=POSTGRE_HOOK),
        region_geojson(
            url_region_geojson=URL_REGION_GEOJSON,
            http_client=httpx_internet_client,
            db_hook=POSTGRE_HOOK,
        ),
        departement_geojson(
            url_departement_geojson=URL_DEPARTEMENT_GEOJSON,
            http_client=httpx_internet_client,
            db_hook=POSTGRE_HOOK,
        ),
        copy_tmp_table_to_real_table(
            db_hook=POSTGRE_HOOK,
            prod_schema=PROD_SCHEMA,
            tmp_schema=TMP_SCHEMA,
            tbl_names=TBL_NAMES,
        ),
        refresh_views(db_hook=POSTGRE_HOOK),
    )


code_geographique()
