"""Functions for retrieving and managing project configurations."""

from typing import List, Optional

import pandas as pd

from utils.config.dag_params import get_project_name
from utils.config.types import SelecteurConfig
from utils.exceptions import ConfigError
from infra.database.factory import create_db_handler
from utils.config.vars import DEFAULT_PG_CONFIG_CONN_ID

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
    db = create_db_handler(DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""
        SELECT nom_projet, selecteur, nom_source, filename, s3_key,
               filepath_source_s3, filepath_local, filepath_s3,
               filepath_tmp_s3, tbl_name, tbl_order
        FROM {CONF_SCHEMA}.vue_conf_projets
        WHERE nom_projet = %s
    """

    params = [nom_projet]

    if selecteur:
        query += " AND selecteur = %s"
        params.append(selecteur)

    # remove trailing semicolon to avoid DB API issues
    query = query.strip() + ";"

    df = db.fetch_df(query, tuple(params))

    if df.empty:
        raise ConfigError(
            f"No configuration found for project {nom_projet} and selector {selecteur}",
            nom_projet=nom_projet,
            selecteur=selecteur,
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
        raise ConfigError(
            f"No configuration found for project {nom_projet}", nom_projet=nom_projet
        )

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
        raise ConfigError(
            f"No configuration found for project {nom_projet} and selector {selecteur}",
            nom_projet=nom_projet,
            selecteur=selecteur,
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
    db = create_db_handler(DEFAULT_PG_CONFIG_CONN_ID)

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


def get_tbl_names(nom_projet: str, order_tbl: bool = False) -> List[str]:
    """Get all table names for a project.

    Used for temporary table creation.

    Args:
        nom_projet: Project name

    Returns:
        List of table names
    """
    db = create_db_handler(DEFAULT_PG_CONFIG_CONN_ID)

    query = f"""SELECT vcp.tbl_name
            FROM {CONF_SCHEMA}.vue_conf_projets vcp
            WHERE vcp.tbl_name IS NOT NULL
                AND vcp.tbl_name <> ''
                AND vcp.nom_projet=%s
            ORDER BY vcp.tbl_order;
            ;
        """

    df = db.fetch_df(query, (nom_projet,))

    if df.empty:
        return []

    return df.loc[:, "tbl_name"].tolist()


def get_s3_keys_source(
    context: Optional[dict] = None, nom_projet: Optional[str] = None
) -> List[str]:
    """Get all source S3 filepaths for a project.

    Used for KeySensors.

    Args:
        context: task context. Provided automatically by airflow
        nom_projet: Project name

    Returns:
        List of S3 source filepaths
    """
    if nom_projet is None and context is None:
        raise ValueError("nom_projet or context must be provided")
    if nom_projet is None and context:
        nom_projet = get_project_name(context=context)

    db = create_db_handler(connection_id=DEFAULT_PG_CONFIG_CONN_ID)

    query = """
        SELECT vcp.filepath_source_s3
        FROM conf_projets.vue_conf_projets vcp
        WHERE vcp.filepath_source_s3 IS NOT NULL
            AND vcp.type_source='Fichier'
            AND vcp.nom_projet = %s;
    """

    df = db.fetch_df(query, parameters=(nom_projet,))
    return df.loc[:, "filepath_source_s3"].tolist()
