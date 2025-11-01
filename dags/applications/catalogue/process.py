from typing import cast
import pandas as pd

from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_axis([colname.lower() for colname in df.columns], axis="columns")
    df = df.drop(df.filter(regex="^(grist|manual)").columns, axis=1)
    return df


def process_pg_catalog(df: pd.DataFrame) -> pd.DataFrame:
    df["data_type"] = (
        df["data_type"].replace("double precision", "float").str.capitalize()
    )

    return df


def process_db_datasets(df: pd.DataFrame) -> pd.DataFrame:
    # Hook
    db_handler = cast(
        PostgresDBHandler, create_db_handler(connection_id="db_data_store")
    )

    # Get postgres datasets
    df = db_handler.fetch_df(
        query="""
            SELECT
                id, is_visible, is_public, title, description, keyword
                id_structure, id_service, id_system_information, id_contact_point
                issued, modified, id_frequency, id_spatiale, landing_page
                id_format, id_licence, id_theme, donnees_ouvertes, url_open_data
            FROM documentation.dataset;
        """
    )

    return df


def process_db_dictionnary(df: pd.DataFrame) -> pd.DataFrame:
    # Hook
    db_handler = cast(
        PostgresDBHandler, create_db_handler(connection_id="db_data_store")
    )

    # Get postgres datasets
    df = db_handler.fetch_df(
        query="""
        """
    )

    return df
