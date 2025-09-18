from airflow.decorators import task
import pandas as pd
import numpy as np

from airflow.providers.common.sql.hooks.sql import DbApiHook
from utils.api_client.base import AbstractApiClient
from utils.grist import GristAPI
from utils.dataframe import df_info

from dags.commun.code_geographique import process


@task
def communes(
    url_communes: str,
    url_communes_outremer: str,
    tbl_name: str,
    api_client: AbstractApiClient,
    db_hook: DbApiHook,
) -> None:
    # Communes métropoles
    response = api_client.get(endpoint=url_communes)
    raw_communes = response.json()["data"]
    df = pd.DataFrame(raw_communes)
    df_info(df=df, df_name="DF Communes - Init")
    df = process.process_communes(df=df)
    df_info(df=df, df_name="DF Communes - Fin")

    # Communes outre-mers
    response_outre_mer = api_client.get(endpoint=url_communes_outremer)
    raw_communes_outre_mer = response_outre_mer.json()["data"]
    df_outre_mer = pd.DataFrame(raw_communes_outre_mer)
    df_info(df=df_outre_mer, df_name="DF Communes outres mers - Init")
    df_outre_mer = process.process_communes_outre_mer(df=df_outre_mer)
    df_info(df=df_outre_mer, df_name="DF Communes outres mers - Fin")

    # Concaténation des dataframes
    df_communes = pd.concat([df, df_outre_mer])
    df_communes = df_communes.fillna(np.nan).replace([np.nan], [None])
    df_communes = df_communes.reset_index(drop=True)
    df_info(df=df_outre_mer, df_name="DF Communes complet - Fin")

    # Envoi des données
    db_hook.insert_rows(
        table=f"temporaire.tmp_{tbl_name}",
        rows=df_communes.values.tolist(),
        target_fields=list(df_communes.columns),
        commit_every=10000,
        executemany=True,
    )


@task
def departements(
    url_departement: str,
    tbl_name: str,
    api_client: AbstractApiClient,
    db_hook: DbApiHook,
) -> None:
    response = api_client.get(endpoint=url_departement)
    raw_departements = response.json()["data"]
    df = pd.DataFrame(raw_departements)
    df = (
        df.set_axis([colname.lower() for colname in df.columns], axis="columns")
        .rename(columns={"__id": "id"})
        .fillna(np.nan)
        .replace([np.nan], [None])
    )
    df_info(df=df, df_name="DF departements - Fin")

    db_hook.insert_rows(
        table=f"temporaire.tmp_{tbl_name}",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
        commit_every=1000,
        executemany=True,
    )


@task
def regions(
    url_region: str, tbl_name: str, api_client: AbstractApiClient, db_hook: DbApiHook
) -> None:
    response = api_client.get(endpoint=url_region)
    raw_regions = response.json()["data"]
    df = pd.DataFrame(raw_regions)
    df = (
        df.set_axis([colname.lower() for colname in df.columns], axis="columns")
        .rename(columns={"__id": "id"})
        .fillna(np.nan)
        .replace([np.nan], [None])
    )
    df_info(df=df, df_name="DF regions - Fin")

    db_hook.insert_rows(
        table=f"temporaire.tmp_{tbl_name}",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
        commit_every=1000,
        executemany=True,
    )


@task
def code_iso(grist_api_client: GristAPI, db_hook: DbApiHook) -> None:
    df = grist_api_client.get_df_from_records(tbl_name="Code_ISO_3166_2")

    df_info(df=df, df_name="DF ISO - Initial")

    df = process.process_code_iso(df=df)

    df_info(df=df, df_name="DF ISO - Après processing")

    df_region = process.get_code_iso_region(df=df)
    df_departement = process.get_code_iso_departement(df=df)

    db_hook.insert_rows(
        table="temporaire.tmp_code_iso_region",
        rows=df_region.values.tolist(),
        target_fields=list(df_region.columns),
        commit_every=1000,
        executemany=True,
    )

    db_hook.insert_rows(
        table="temporaire.tmp_code_iso_departement",
        rows=df_departement.values.tolist(),
        target_fields=list(df_departement.columns),
        commit_every=1000,
        executemany=True,
    )


@task
def region_geojson(
    url_region_geojson: str, http_client: AbstractApiClient, db_hook: DbApiHook
) -> None:
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

    db_hook.insert_rows(
        table="temporaire.tmp_region_geojson",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
        commit_every=1000,
        executemany=True,
    )


@task
def departement_geojson(
    url_departement_geojson: str, http_client: AbstractApiClient, db_hook: DbApiHook
) -> None:
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

    db_hook.insert_rows(
        table="temporaire.tmp_departement_geojson",
        rows=df.values.tolist(),
        target_fields=list(df.columns),
        commit_every=1000,
        executemany=True,
    )


@task
def refresh_views(db_hook: DbApiHook) -> None:
    view_list = ("commun.vw_commune_full_info",)
    sql_queries = [f"REFRESH MATERIALIZED VIEW {view_name};" for view_name in view_list]
    db_hook.run(sql=sql_queries)
