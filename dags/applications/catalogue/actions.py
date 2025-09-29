from typing import cast
import pandas as pd

from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler


def get_db_dataset_dictionnaire() -> pd.DataFrame:
    # Hook
    db_handler = cast(
        PostgresDBHandler, create_db_handler(connection_id="db_data_store")
    )
    df = db_handler.fetch_df(
        query="""
            SELECT
                t.table_schema,
                t.table_name,
                c.column_name,
                c.data_type
            FROM information_schema.tables t
            JOIN information_schema.columns c
                ON t.table_schema = c.table_schema
                AND t.table_name = c.table_name
            WHERE
                t.table_schema NOT IN ('pg_catalog', 'information_schema',
                    'documentation', 'temporaire')
                AND t.table_type = 'BASE TABLE'
                AND t.table_schema NOT LIKE '%_file_upload';
        """
    )

    return df
