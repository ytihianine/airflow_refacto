import pandas as pd
import json


def process_communes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_axis([colname.lower() for colname in df.columns], axis="columns").drop(
        columns=["__id"]
    )
    return df


def process_communes_outre_mer(df: pd.DataFrame) -> pd.DataFrame:
    cols_mapping = {"com_comer": "com", "nature_zonage": "typecom"}
    df = (
        df.set_axis([colname.lower() for colname in df.columns], axis="columns")
        .rename(columns=cols_mapping, errors="raise")
        .assign(
            dep=lambda df: df["comer"],
            reg=lambda df: df["comer"],
        )
    )
    df = df.drop(columns=["__id", "libelle_comer", "comer"])
    return df


def get_code_iso_region(df: pd.DataFrame) -> pd.DataFrame:
    region_division = [
        "metropolitan region",
        "metropolitan collectivity with special status",
        "overseas territory",
        "overseas departmental collectivity",
        "overseas unique territorial collectivity",
    ]
    df_region = df.loc[df["categorie_division"].isin(region_division)].copy()
    # Retrait de Paris et Lyon dans la liste des régions
    df_region = df_region.loc[
        ~df_region["libelle"].isin(["Métropole de Lyon", "Paris"])
    ]

    return df_region


def get_code_iso_departement(df: pd.DataFrame) -> pd.DataFrame:
    departement_division = [
        "metropolitan department",
        "metropolitan collectivity with special status",
        "overseas territory",
        "overseas departmental collectivity",
        "overseas unique territorial collectivity",
    ]
    df_departement = df.loc[df["categorie_division"].isin(departement_division)].copy()
    # Retrait de Paris et Lyon dans la liste des régions
    df_departement = df_departement.loc[
        ~df_departement["libelle"].isin(["Corse", "Métropole de Lyon"])
    ]

    return df_departement


def process_code_iso(df: pd.DataFrame) -> pd.DataFrame:
    cols_mapping = {
        "Subdivision_category": "categorie_division",
        "c3166_2_code": "code_iso_3166_2",
        "Subdivision_name": "libelle",
        "Local_variant": "variant_local",
        "Language_code": "code_langue",
        "Romanization_system": "en_langue_romaine",
        "Parent_subdivision": "code_iso_parent",
    }

    df = df.rename(columns=cols_mapping, errors="raise").assign(
        categorie_division=lambda df: df["categorie_division"]
        .str.split()
        .str.join(" "),
        code_iso_3166_2=lambda df: df["code_iso_3166_2"].str.split().str.join(" "),
        libelle=lambda df: df["libelle"]
        .str.split("(")
        .str[0]
        .str.split()
        .str.join(" "),
    )
    df = df.drop(
        columns=["variant_local", "code_langue", "en_langue_romaine", "code_iso_parent"]
    )
    return df


def transform_multipolygon_to_polygon(
    geojson_feature: dict[str, any]
) -> list[list[float, float]]:
    type_contour = geojson_feature["geometry"]["type"]
    raw_coords = geojson_feature["geometry"]["coordinates"]

    if type_contour == "MultiPolygon":
        # Récupérer tous les polygones qui composent le MultiPolygon
        polygons = [polygon[0] for polygon in raw_coords]
        points_by_polygon = [len(polygon) for polygon in polygons]

        # On cherche le polygon qui a le plus de points et on récupère son index
        biggest_polygon = max(points_by_polygon)
        index_biggest_polygon = points_by_polygon.index(biggest_polygon)
        coordonnees = json.dumps(polygons[index_biggest_polygon])
    else:
        # Récupérer le polygone
        coordonnees = json.dumps(raw_coords[0])

    return coordonnees
