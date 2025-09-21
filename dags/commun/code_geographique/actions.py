import pandas as pd
import numpy as np

from airflow.models.variable import Variable

from infra.grist.client import GristAPI
from infra.http_client.adapters import ClientConfig, HttpxClient
from utils.config.vars import AGENT, PROXY
from utils.dataframe import df_info


from dags.commun.code_geographique import process

# Données COG
PAGE_SIZE = "?page_size=0"  # get all data at once
BASE_URL = "https://tabular-api.data.gouv.fr/api/resources"
ID_DATASET_REGION = "2486b351-5d85-4e1a-8d12-5df082c75104"
ID_DATASET_DEPARTEMENT = "54a8263d-6e2d-48d5-b214-aa17cc13f7a0"
ID_DATASET_COMMUNE = "91a95bee-c7c8-45f9-a8aa-f14cc4697545"
ID_DATASET_COMMUNE_OUTRE_MER = "b797d73d-663c-4d3d-baf0-2d24b2d3a321"


def make_httpx_client() -> HttpxClient:
    config = ClientConfig(timeout=60, proxy=PROXY, user_agent=AGENT)
    return HttpxClient(config=config)


def communes() -> pd.DataFrame:
    # API Client
    api_client = make_httpx_client()
    # URL
    url_communes = "/".join([BASE_URL, ID_DATASET_COMMUNE, "data", PAGE_SIZE])
    url_communes_outre_mer = "/".join(
        [BASE_URL, ID_DATASET_COMMUNE_OUTRE_MER, "data", PAGE_SIZE]
    )
    # Communes métropoles
    response = api_client.get(endpoint=url_communes)
    raw_communes = response.json()["data"]
    df = pd.DataFrame(raw_communes)
    df_info(df=df, df_name="DF Communes - Init")
    df = process.process_communes(df=df)
    df_info(df=df, df_name="DF Communes - Fin")

    # Communes outre-mers
    response_outre_mer = api_client.get(endpoint=url_communes_outre_mer)
    raw_communes_outre_mer = response_outre_mer.json()["data"]
    df_outre_mer = pd.DataFrame(raw_communes_outre_mer)
    df_info(df=df_outre_mer, df_name="DF Communes outres mers - Init")
    df_outre_mer = process.process_communes_outre_mer(df=df_outre_mer)
    df_info(df=df_outre_mer, df_name="DF Communes outres mers - Fin")

    # Concaténation des dataframes
    df_communes = pd.concat([df, df_outre_mer])
    df_communes = df_communes.reset_index(drop=True)
    df_info(df=df_outre_mer, df_name="DF Communes complet - Fin")
    return df_communes


def departements() -> pd.DataFrame:
    # API Client
    api_client = make_httpx_client()
    # URL
    url_departement = "/".join([BASE_URL, ID_DATASET_DEPARTEMENT, "data", PAGE_SIZE])
    response = api_client.get(endpoint=url_departement)
    raw_departements = response.json()["data"]
    df = pd.DataFrame(raw_departements)
    df = (
        df.set_axis([colname.lower() for colname in df.columns], axis="columns")
        .rename(columns={"__id": "id"})
        .fillna(np.nan)
    )
    df_info(df=df, df_name="DF departements - Fin")
    return df


def regions() -> pd.DataFrame:
    # API Client
    api_client = make_httpx_client()
    # URL
    url_region = "/".join([BASE_URL, ID_DATASET_REGION, "data", PAGE_SIZE])
    response = api_client.get(endpoint=url_region)
    raw_regions = response.json()["data"]
    df = pd.DataFrame(raw_regions)
    df = (
        df.set_axis([colname.lower() for colname in df.columns], axis="columns")
        .rename(columns={"__id": "id"})
        .fillna(np.nan)
    )
    df_info(df=df, df_name="DF regions - Fin")
    return df


def code_iso_region() -> pd.DataFrame:
    # API Client
    api_client = make_httpx_client()
    # Grist
    grist_api = GristAPI(
        http_client=api_client,
        base_url="https://grist.numerique.gouv.fr",
        workspace_id="dsci",
        doc_id=Variable.get("grist_doc_id_data_commune"),
        api_token=Variable.get("grist_secret_key"),
    )

    df = grist_api.get_df_from_records(tbl_name="Code_ISO_3166_2")

    df_info(df=df, df_name="DF ISO - Initial")

    df = process.process_code_iso(df=df)

    df_info(df=df, df_name="DF ISO - Après processing")

    df_region = process.get_code_iso_region(df=df)

    return df_region


def code_iso_departement() -> pd.DataFrame:
    # API Client
    api_client = make_httpx_client()
    # Grist
    grist_api = GristAPI(
        http_client=api_client,
        base_url="https://grist.numerique.gouv.fr",
        workspace_id="dsci",
        doc_id=Variable.get("grist_doc_id_data_commune"),
        api_token=Variable.get("grist_secret_key"),
    )

    df = grist_api.get_df_from_records(tbl_name="Code_ISO_3166_2")

    df_info(df=df, df_name="DF ISO - Initial")

    df = process.process_code_iso(df=df)

    df_info(df=df, df_name="DF ISO - Après processing")

    df_departement = process.get_code_iso_departement(df=df)

    return df_departement


def region_geojson() -> pd.DataFrame:
    # API Client
    http_client = make_httpx_client()
    url_region_geojson = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/refs/heads/master/regions-avec-outre-mer.geojson"  # noqa

    response = http_client.get(endpoint=url_region_geojson)
    geojson = response.json()

    region_rows = []
    for feature in geojson["features"]:
        libelle = feature["properties"]["nom"]
        type_contour = feature["geometry"]["type"]
        polygon = process.transform_multipolygon_to_polygon(geojson_feature=feature)

        region_rows.append(
            {
                "libelle": libelle,
                "type_contour": type_contour,
                "coordonnees": polygon,
            }
        )

    df = pd.DataFrame(region_rows)
    df_info(df=df, df_name="DF Region GeoJson - Initial")
    return df


def departement_geojson() -> pd.DataFrame:
    # API Client
    http_client = make_httpx_client()
    url_departement_geojson = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/refs/heads/master/departements-avec-outre-mer.geojson"  # noqa

    response = http_client.get(endpoint=url_departement_geojson)
    geojson = response.json()

    departement_rows = []
    for feature in geojson["features"]:
        libelle = feature["properties"]["nom"]
        type_contour = feature["geometry"]["type"]
        polygon = process.transform_multipolygon_to_polygon(geojson_feature=feature)

        departement_rows.append(
            {
                "libelle": libelle,
                "type_contour": type_contour,
                "coordonnees": polygon,
            }
        )

    df = pd.DataFrame(departement_rows)
    df_info(df=df, df_name="DF Departement GeoJson - Initial")
    return df
