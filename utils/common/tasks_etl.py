from typing import Callable, Dict, Optional, Any
from pathlib import Path
import pandas as pd

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from infra.file_handling.dataframe import read_dataframe
from utils.file_handler import MinioFileHandler
from utils.common.config_func import (
    get_storage_rows,
    get_cols_mapping,
    get_required_cols,
    format_cols_mapping,
)
from utils.df_utility import df_info


"""Module for ETL task creation and execution."""

import io
import logging
from pathlib import Path
from typing import Any, Callable, Optional, Union

from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

from infra.file_handling.s3 import S3FileHandler
from utils.df_utility import df_info
from utils.common.config_func import (
    get_cols_mapping,
    format_cols_mapping,
    get_s3_filepath,
)

CONF_SCHEMA = "conf_projets"


def create_etl_task(
    selecteur: str,
    file_format: Optional[str] = None,
    process_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    read_options: Optional[dict[str, Any]] = None,
    write_options: Optional[dict[str, Any]] = None,
) -> Callable[..., BaseOperator]:
    """Create an ETL task for extracting, transforming and loading data.

    Args:
        selecteur: The identifier for this ETL task
        file_format: Optional format of the source file (csv, excel, parquet)
        process_func: Optional function to process the DataFrame
        read_options: Optional options for reading the source file
        write_options: Optional options for writing the parquet file

    Returns:
        An Airflow task that performs the ETL operation
    """
    """
    Create a generic ETL task that:
    1. Gets configuration using selecteur
    2. Reads file from S3
    3. Processes the data (optional)
    4. Writes result to S3 as parquet

    Args:
        selecteur: Configuration selector key
        process_func: Optional function to process the DataFrame
        file_format: Override file format (default: auto-detect from extension)
        read_options: Additional options for reading files (e.g., CSV separator)
        write_options: Additional options for writing parquet (e.g., compression)

    Returns:
        Callable: Airflow task function

    Example:
        ```python
        def process_data(df: pd.DataFrame) -> pd.DataFrame:
            # Your processing logic here
            return df

        etl_task = create_etl_task(
            selecteur='my_data',
            process_func=process_data,
            read_options={'sep': ';'},
            write_options={'compression': 'snappy'}
        )
        ```
    """

    @task(task_id=selecteur)
    def _task(**context) -> None:
        """The actual ETL task function."""
        # Get project name from context
        params = context.get("params", {})
        nom_projet = params.get("nom_projet")
        if not nom_projet:
            raise ValueError("nom_projet must be defined in DAG parameters")

        # Initialize hooks
        s3_hook = S3FileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

        # Get config values related to the task
        task_config = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
        grist_tbl_name = task_config.loc[0, "nom_source"]

        # Get data of table
        df = read_dataframe(
            file_handler=s3_hook,
            file_path=str(task_config.loc[0, "filepath_source_s3"]),
        )

        df_info(df=df, df_name=f"{grist_tbl_name} - Source normalisée")
        if process_func is not None:
            df = process_func(df)
        else:
            print("No process function provided. Skipping the processing step ...")
        df_info(df=df, df_name=f"{grist_tbl_name} - After processing")

        # Export
        s3_hook.write(
            file_path=str(task_config.loc[0, "filepath_tmp_s3"]),
            content=df.to_parquet(path=None, index=False),
        )

    return _task


@task(task_id="convert_oad_indic_to_parquet")
def convert_file_to_parquet(
    nom_projet: str, selecteur: str, process_func: Callable, **context
) -> None:
    # Variables

    # Hooks
    db_hook = PostgresHook(postgres_conn_id="db_data_store")
    db_conf_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

    # Config
    row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

    df_oad_indic = s3_hook.read_excel(
        file_name=row_selecteur.loc[0, "filepath_source_s3"]
    )

    # Cleaning df
    df_info(df=df_oad_indic, df_name="DF OAD indicateurs - INITIALISATION")
    df = process_func
    df_info(df=df_oad_indic, df_name="DF OAD indicateurs - After processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_oad_indic.to_parquet(path=None, index=False),
        key=row_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )
