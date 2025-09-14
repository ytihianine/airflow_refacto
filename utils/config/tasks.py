"""Functions for retrieving and managing project configurations."""

import logging
from typing import List, Optional, cast

import pandas as pd

from utils.config.types import SelecteurConfig
from utils.config.vars import DEFAULT_PG_CONFIG_CONN_ID
from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler

CONF_SCHEMA = "conf_projets"


def get_config(nom_projet: str, selecteur: Optional[str] = None) -> pd.DataFrame:
    """Get configuration for a specific selecteur in a project.

    Args:
        nom_projet: Project name
        selecteur: Configuration selector

    Returns:
        SelecteurConfig dictionary with configuration details

    Raises:
        ValueError: If no configuration is found for the given project and selector
    """
    db = cast(PostgresDBHandler, create_db_handler(DEFAULT_PG_CONFIG_CONN_ID))

    query = f"""
        SELECT nom_projet, selecteur, nom_source, filename, s3_key,
               filepath_source_s3, filepath_local, filepath_s3,
               filepath_tmp_s3, tbl_name, tbl_order
        FROM {CONF_SCHEMA}.vue_conf_projets
        WHERE nom_projet = %s;
    """

    if selecteur:
        query += " AND selecteur = %s"

    df = db.fetch_df(query, (nom_projet, selecteur))

    if df.empty:
        raise ValueError(
            f"No configuration found for project {nom_projet} and selector {selecteur}"
        )

    return df.sort_values(by=["tbl_order"])


def get_projet_config(nom_projet: str) -> list[SelecteurConfig]:
    """Get configuration for a specific selecteur in a project.

    Args:
        nom_projet: Project name
        selecteur: Configuration selector

    Returns:
        SelecteurConfig dictionary with configuration details

    Raises:
        ValueError: If no configuration is found for the given project and selector
    """
    df_projet_config = get_config(nom_projet=nom_projet)
    if df_projet_config.empty:
        raise ValueError(f"No configuration found for project {nom_projet}")

    records = df_projet_config.to_dict("records")
    # Ensure all keys are strings for dataclass compatibility
    records = [{str(k): v for k, v in record.items()} for record in records]
    return [SelecteurConfig(**record) for record in records]


def get_selecteur_config(nom_projet: str, selecteur: str) -> SelecteurConfig:
    """Get configuration for a specific selecteur in a project.

    Args:
        nom_projet: Project name
        selecteur: Configuration selector

    Returns:
        SelecteurConfig dictionary with configuration details

    Raises:
        ValueError: If no configuration is found for the given project and selector
    """
    df_selecteur_config = get_config(nom_projet=nom_projet, selecteur=selecteur)
    if df_selecteur_config.empty:
        raise ValueError(
            f"No configuration found for project {nom_projet} and selector {selecteur}"
        )

    record = df_selecteur_config.iloc[0].to_dict()
    return SelecteurConfig(**record)


def get_cols_mapping(
    nom_projet: str,
    selecteur: str,
) -> pd.DataFrame:
    """
    Permet de récupérer la correspondance des colonnes entre
    le fichier source et la sortie.
    Output:
        Dataframe
        Columns: selecteur, colname_source, colname_dest
    """
    db = cast(PostgresDBHandler, create_db_handler(DEFAULT_PG_CONFIG_CONN_ID))

    df = db.fetch_df(
        f"""SELECT cpvcm.nom_projet, cpvcm.selecteur, cpvcm.colname_source, cpvcm.colname_dest
            FROM {CONF_SCHEMA}.vue_cols_mapping cpvcm
            WHERE cpvcm.nom_projet = %s AND cpvcm.selecteur = %s;
        """,
        parameters=(nom_projet, selecteur),
    )
    return df


def format_cols_mapping(
    df_cols_map: pd.DataFrame, selecteur: Optional[str] = None
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


def get_col_names(nom_projet: str, nom_tbl: str) -> List[str]:
    """Get column names for a specific project table.

    Args:
        nom_projet: Project name
        nom_tbl: Table name

    Returns:
        List of column names
    """
    db = cast(PostgresDBHandler, create_db_handler(DEFAULT_PG_CONFIG_CONN_ID))

    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        AND nom_projet = %s;
    """

    df = db.fetch_df(query, (CONF_SCHEMA, nom_tbl, nom_projet))

    if df.empty:
        return []

    return df["column_name"].tolist()


def process_new_cols_map(
    df_cols_map: pd.DataFrame, selecteur: Optional[str] = None
) -> tuple[List[str], List[str]]:
    """Process column mappings and identify special columns.

    Creates dictionaries for error columns and unused columns based on mapping configuration.
    Returns lists of columns that need special attention:
        - Columns of type 'error' or with non-empty mappings
        - Unused columns

    Args:
        df_cols_map: DataFrame containing column mappings
        selecteur: Optional selector to filter mappings

    Returns:
        Tuple of (error_columns, unused_columns)
    """
    if selecteur is not None:
        df_cols_map = df_cols_map.loc[df_cols_map["selecteur"] == selecteur].copy()

        if df_cols_map.empty:
            raise ValueError(f"No mappings found for selector {selecteur}")
        logging.info(f"Mapping columns for {selecteur}: {df_cols_map.to_dict()}")

    # Error columns if type is 'error' or mapping is non-empty
    error_columns = df_cols_map.loc[
        ((df_cols_map["type"] == "error") | df_cols_map["corresp"].notna()), "colname"
    ].tolist()

    # Unused columns if type is 'not_used'
    unused_columns = df_cols_map.loc[
        df_cols_map["type"] == "not_used", "colname"
    ].tolist()

    return error_columns, unused_columns


def get_required_cols(nom_projet: str, selecteur: str) -> pd.DataFrame:
    """
    Permet de récupérer les colonnes d'un fichier.
    Cas d'usage: 1 fichier source doit être séparé en plusieurs fichiers.
    Output:
        Dataframe
        Columns: selecteur, colname_source, colname_dest
    """
    db = create_db_handler(DEFAULT_PG_CONFIG_CONN_ID)

    df = db.fetch_df(
        f"""SELECT cpvcr.nom_projet, cpvcr.selecteur, cpvcr.colname_dest
            FROM {CONF_SCHEMA}.vue_cols_requises cpvcr
            WHERE cpvcr.nom_projet = '{nom_projet}'
                AND cpvcr.selecteur='{selecteur}';
        """
    )

    return df


def get_nom_tbl_insert(nom_projet: str, selecteur: str) -> str:
    """Get the target table name for data insertion.

    Args:
        nom_projet: Project name
        selecteur: Configuration selector

    Returns:
        Target table name or empty string if not found
    """
    db = create_db_handler(DEFAULT_PG_CONFIG_CONN_ID)

    query = """
    SELECT nom_tbl_insert
    FROM conf_projets.nom_tbl_insert
    WHERE nom_projet = %s
    AND selecteur = %s;
    """

    df = db.fetch_df(query, (nom_projet, selecteur))

    if df.empty:
        return ""

    return str(df.iloc[0]["nom_tbl_insert"])


def get_tbl_names(nom_projet: str, order_tbl: bool = False) -> List[str]:
    """Get all table names for a project.

    Used for temporary table creation.

    Args:
        nom_projet: Project name

    Returns:
        List of table names
    """
    db = cast(PostgresDBHandler, create_db_handler(DEFAULT_PG_CONFIG_CONN_ID))

    query = """
        SELECT DISTINCT nom_tbl
        FROM conf_projets.relations_colonnes
        WHERE nom_projet = %s
    """
    if order_tbl:
        query += " ORDER BY tbl_order ASC"

    df = db.fetch_df(query, (nom_projet,))

    if df.empty:
        return []

    return df["nom_tbl"].tolist()


def get_s3_keys_source(nom_projet: str) -> List[str]:
    """Get all source S3 filepaths for a project.

    Used for KeySensors.

    Args:
        nom_projet: Project name

    Returns:
        List of S3 source filepaths
    """
    db = cast(PostgresDBHandler, create_db_handler(DEFAULT_PG_CONFIG_CONN_ID))

    query = """
        SELECT vcp.filepath_source_s3
        FROM conf_projets.vue_conf_projets vcp
        WHERE vcp.filepath_source_s3 IS NOT NULL
        AND vcp.nom_projet = %s;
    """

    df = db.fetch_df(query, (nom_projet,))
    return df["filepath_source_s3"].tolist()
