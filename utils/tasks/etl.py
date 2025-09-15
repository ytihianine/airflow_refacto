"""Module for ETL task creation and execution."""

from typing import Callable, Optional, Any
from airflow import XComArg
import pandas as pd

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from infra.file_handling.dataframe import read_dataframe
from infra.file_handling.s3 import S3FileHandler
from utils.tasks.sql import get_conn_from_s3_sqlite, get_data_from_s3_sqlite_file
from utils.dataframe import df_info
from utils.config.tasks import (
    get_selecteur_config,
    get_cols_mapping,
    format_cols_mapping,
)


def create_grist_etl_task(
    selecteur: str,
    doc_selecteur: str,
    process_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
) -> Callable[..., XComArg]:
    """
    Create an ETL task for extracting, transforming and loading data from a Grist table that:
    1. Gets configuration using selecteur
    2. Reads file from S3
    3. Processes the data (optional)
    4. Writes result to S3 as parquet

    Args:
        selecteur: Configuration selector key
        doc_selecteur: Configuration selector for the Grist document
        process_func: Optional function to process the DataFrame

    Returns:
        Callable: Airflow task function
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
        task_config = get_selecteur_config(nom_projet=nom_projet, selecteur=selecteur)
        doc_config = get_selecteur_config(
            nom_projet=nom_projet, selecteur=doc_selecteur
        )

        # Get data of table
        conn = get_conn_from_s3_sqlite(
            sqlite_file_s3_filepath=doc_config.filepath_source_s3
        )
        df = get_data_from_s3_sqlite_file(
            table_name=task_config.nom_source,
            sqlite_s3_filepath=doc_config.filepath_source_s3,
            sqlite_conn=conn,
        )
        df = read_dataframe(
            file_handler=s3_hook,
            file_path=str(task_config.filepath_source_s3),
        )

        df_info(df=df, df_name=f"{selecteur} - Source normalisée")
        if process_func is not None:
            df = process_func(df)
        else:
            print("No process function provided. Skipping the processing step ...")
        df_info(df=df, df_name=f"{selecteur} - After processing")

        # Export
        s3_hook.write(
            file_path=str(task_config.filepath_tmp_s3),
            content=df.to_parquet(path=None, index=False),
        )

    return _task


def create_file_etl_task(
    selecteur: str,
    process_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    read_options: Optional[dict[str, Any]] = None,
) -> Callable[..., XComArg]:
    """Create an ETL task for extracting, transforming and loading data from a file.

    Args:
        selecteur: The identifier for this ETL task
        file_format: Optional format of the source file (csv, excel, parquet)
        process_func: Optional function to process the DataFrame
        read_options: Optional options for reading the source file

    Returns:
        An Airflow task that performs the ETL operation
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
        task_config = get_selecteur_config(nom_projet=nom_projet, selecteur=selecteur)

        # Get data of table
        df = read_dataframe(
            file_handler=s3_hook,
            file_path=str(task_config.filepath_source_s3),
            read_options=read_options,
        )

        df_info(df=df, df_name=f"{selecteur} - Source normalisée")
        if process_func is None:
            print("No process function provided. Skipping the processing step ...")
        else:
            df = process_func(df)
        df_info(df=df, df_name=f"{selecteur} - After processing")

        # Export
        s3_hook.write(
            file_path=str(task_config.filepath_tmp_s3),
            content=df.to_parquet(path=None, index=False),
        )

    return _task


def create_action_etl_task(
    task_id: str,
    action_func: Callable,
) -> Callable[..., XComArg]:
    """Create an ETL task for extracting, transforming and loading data from a file.

    Args:
        selecteur: The identifier for this ETL task
        file_format: Optional format of the source file (csv, excel, parquet)
        process_func: Optional function to process the DataFrame
        read_options: Optional options for reading the source file

    Returns:
        An Airflow task that performs the ETL operation
    """

    @task(task_id=task_id)
    def _task(**context) -> None:
        """The actual ETL task function."""
        # Execute the provided action function
        action_func(**context)

    return _task
