import pandas as pd
import numpy as np
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONF_SCHEMA = "conf_projets"


def get_s3_filepath(storage_paths: pd.DataFrame, selecteur: str) -> str:
    sub_storage_paths = storage_paths.loc[
        storage_paths["selecteur"] == selecteur
    ].reset_index(drop=True)
    if len(sub_storage_paths) == 0:
        raise ValueError(f"No rows have been found for selecteur {selecteur}")

    s3_filepath = sub_storage_paths.loc[0, "s3_filepath"]
    print(f"{selecteur} path: {s3_filepath}")
    return s3_filepath


def get_s3_tmp_filepath(storage_paths: pd.DataFrame, selecteur: str) -> str:
    sub_storage_paths = storage_paths.loc[
        storage_paths["selecteur"] == selecteur
    ].reset_index(drop=True)
    if len(sub_storage_paths) == 0:
        raise ValueError(f"No rows have been found for selecteur {selecteur}")

    s3_tmp_filepath = sub_storage_paths.loc[0, "s3_tmp_filepath"]
    print(f"{selecteur} tmp path: {s3_tmp_filepath}")
    return s3_tmp_filepath


def format_cols_mapping(
    df_cols_map: pd.DataFrame, selecteur: str = None
) -> dict[str, str]:
    print("Colonnes du dataframe de mapping: ", df_cols_map.columns)
    print("Selecteurs du dataframe de mapping: ", df_cols_map["selecteur"].unique())
    if selecteur is not None:
        df_cols_map = df_cols_map.loc[df_cols_map["selecteur"] == selecteur]
    records_cols_map = df_cols_map.to_dict("records")
    cols_map = {}
    for record in records_cols_map:
        cols_map[record["colname_source"]] = record["colname_dest"]

    return cols_map


def get_cols_mapping(
    nom_projet: str,
    db_hook: DbApiHook = None,
    selecteur: str = None,
) -> pd.DataFrame:
    """
    Permet de récupérer la correspondance des colonnes entre
    le fichier source et la sortie.
    Output:
        Dataframe
        Columns: selecteur, colname_source, colname_dest
    """
    if db_hook is None:
        db_hook = PostgresHook(postgres_conn_id="db_depose_fichier")

    df = db_hook.get_pandas_df(
        f"""SELECT cpvcm.nom_projet, cpvcm.selecteur, cpvcm.colname_source, cpvcm.colname_dest
            FROM {CONF_SCHEMA}.vue_cols_mapping cpvcm
            WHERE cpvcm.selecteur='{selecteur}';
        """
    )

    return df


def get_required_cols(
    nom_projet: str, selecteur: str, db_hook: DbApiHook = None
) -> pd.DataFrame:
    """
    Permet de récupérer les colonnes d'un fichier.
    Cas d'usage: 1 fichier source doit être séparé en plusieurs fichiers.
    Output:
        Dataframe
        Columns: selecteur, colname_source, colname_dest
    """
    if db_hook is None:
        db_hook = PostgresHook(postgres_conn_id="db_depose_fichier")

    df = db_hook.get_pandas_df(
        f"""SELECT cpvcr.nom_projet, cpvcr.selecteur, cpvcr.colname_dest
            FROM {CONF_SCHEMA}.vue_cols_requises cpvcr
            WHERE cpvcr.nom_projet = '{nom_projet}'
                AND cpvcr.selecteur='{selecteur}';
        """
    )

    return df


def get_tbl_names(nom_projet: str, db_hook: DbApiHook) -> list[str]:
    df = db_hook.get_pandas_df(
        f"""SELECT vcp.tbl_name
            FROM {CONF_SCHEMA}.vue_conf_projets vcp
            WHERE vcp.tbl_name IS NOT NULL
                AND vcp.tbl_name <> ''
                AND vcp.nom_projet='{nom_projet}'
            ORDER BY vcp.tbl_order;
            ;
        """
    )

    return df["tbl_name"].values.tolist()


def get_s3_keys_source(nom_projet: str, db_hook: DbApiHook = None) -> list[str]:
    """Utile pour les KeySensors"""
    if db_hook is None:
        db_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    df = db_hook.get_pandas_df(
        f"""SELECT vcp.filepath_source_s3
            FROM {CONF_SCHEMA}.vue_conf_projets vcp
            WHERE vcp.filepath_source_s3 IS NOT NULL
                AND vcp.nom_projet='{nom_projet}'
            ;
        """
    )

    return df["filepath_source_s3"].values.tolist()


def get_s3_tmp_keys_dest(nom_projet: str, db_hook: DbApiHook = None) -> list[str]:
    """Utile pour les KeySensors"""
    if db_hook is None:
        db_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    df = db_hook.get_pandas_df(
        f"""SELECT vcp.filepath_tmp_s3
            FROM {CONF_SCHEMA}.vue_conf_projets vcp
            WHERE vcp.filepath_source_s3 IS NOT NULL
                AND vcp.nom_projet='{nom_projet}'
            ;
        """
    )

    return df["filepath_tmp_s3"].values.tolist()


def get_storage_rows(
    nom_projet: str,
    selecteur: str | None = None,
    pg_conn_id: str = "db_depose_fichier",
    db_hook: DbApiHook = None,
) -> pd.DataFrame:
    """
    Récupère les chemins d'accès local et S3 pour les fichiers d'un projet.

    Si `selecteur` est fourni → retourne uniquement la/les ligne(s) correspondante(s).
    Si `selecteur` est None → retourne toutes les lignes du projet.

    Output:
        DataFrame
        Colonnes: nom_projet, selecteur, nom_source, s3_key, filename, filepath_local,
                  filepath_s3, filepath_source_s3, filepath_tmp_s3, tbl_name, tbl_order
    """
    if db_hook is None:
        db_hook = PostgresHook(postgres_conn_id=pg_conn_id)

    query = """
        SELECT vcp.nom_projet, vcp.selecteur, vcp.nom_source, vcp.filename,
                vcp.s3_key,
                vcp.filepath_source_s3, vcp.filepath_local, vcp.filepath_s3,
                vcp.filepath_tmp_s3, vcp.tbl_name, vcp.tbl_order
        FROM conf_projets.vue_conf_projets vcp
        WHERE vcp.nom_projet = %(nom_projet)s
          AND vcp.filename IS NOT NULL
    """

    if selecteur:
        query += f" AND vcp.selecteur = '{selecteur}'"

    df = db_hook.get_pandas_df(sql=query, parameters={"nom_projet": nom_projet})

    if selecteur and len(df) == 0:
        raise ValueError(
            f"Aucune configuration trouvée pour le projet <{nom_projet}> "
            f"et le selecteur <{selecteur}>"
        )

    df = df.fillna(np.nan).replace([np.nan], [None])

    return df
