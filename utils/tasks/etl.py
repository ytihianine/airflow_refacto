"""Module for ETL task creation and execution."""

from typing import Callable, Optional, Any
from airflow import XComArg
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.models.xcom import XCom
import pandas as pd

from airflow.decorators import task

from infra.file_handling.dataframe import read_dataframe
from infra.file_handling.local import LocalFileHandler
from infra.file_handling.s3 import S3FileHandler
from infra.database.factory import create_db_handler
from utils.dataframe import df_info
from utils.config.tasks import (
    get_selecteur_config,
    get_cols_mapping,
    format_cols_mapping,
)
from utils.config.types import P, R


def create_grist_etl_task(
    selecteur: str,
    doc_selecteur: Optional[str] = None,
    normalisation_process_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
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
        normalisation_process_func: Optional normalization process to run before the process function
        process_func: Optional function to process the DataFrame

    Returns:
        Callable: Airflow task function
        ```
    """
    if doc_selecteur is None:
        doc_selecteur = "grist_doc"

    @task(task_id=selecteur)
    def _task(**context) -> None:
        """The actual ETL task function."""
        # Get project name from context
        params = context.get("params", {})
        nom_projet = params.get("nom_projet")
        if not nom_projet:
            raise ValueError("nom_projet must be defined in DAG parameters")

        # Get config values related to the task
        task_config = get_selecteur_config(nom_projet=nom_projet, selecteur=selecteur)
        doc_config = get_selecteur_config(
            nom_projet=nom_projet, selecteur=doc_selecteur
        )
        if task_config.nom_source is None:
            raise ValueError(f"nom_source must be defined for selecteur {selecteur}")

        # Initialize hooks
        s3_handler = S3FileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
        local_handler = LocalFileHandler()
        sqlite_handler = create_db_handler(
            connection_id=doc_config.filepath_local, db_type="sqlite"
        )

        # Download Grist doc from S3 to local temp file
        grist_doc = s3_handler.read(file_path=str(doc_config.filepath_source_s3))
        local_handler.write(file_path=str(doc_config.filepath_local), content=grist_doc)

        # Get data of table
        df = sqlite_handler.fetch_df(
            query=f"SELECT * FROM ?", parameters=(task_config.nom_source,)
        )

        if normalisation_process_func is not None:
            df = normalisation_process_func(df)
        else:
            print(
                "No normalisation process function provided. Skipping the normalisation step ..."
            )

        df_info(df=df, df_name=f"{selecteur} - Source normalisée")
        if process_func is not None:
            df = process_func(df)
        else:
            print("No process function provided. Skipping the processing step ...")
        df_info(df=df, df_name=f"{selecteur} - After processing")

        # Export
        s3_handler.write(
            file_path=str(task_config.filepath_tmp_s3),
            content=df.to_parquet(path=None, index=False),
        )

    return _task


def create_file_etl_task(
    selecteur: str,
    process_func: Optional[Callable[..., pd.DataFrame]] = None,
    read_options: Optional[dict[str, Any]] = None,
    apply_cols_mapping: bool = True,
) -> Callable[..., XComArg]:
    """Create an ETL task for extracting, transforming and loading data from a file.

    Args:
        selecteur: The identifier for this ETL task
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
            file_path=task_config.filepath_source_s3,
            read_options=read_options,
        )

        df_info(df=df, df_name=f"{selecteur} - Source normalisée")
        if apply_cols_mapping:
            # Apply column mapping if available
            cols_mapping = get_cols_mapping(nom_projet=nom_projet, selecteur=selecteur)
            if cols_mapping.empty:
                print(f"No column mapping found for selecteur {selecteur}")
            else:
                df = df.rename(columns=format_cols_mapping(cols_mapping))

        if process_func is None:
            print("No process function provided. Skipping the processing step ...")
        else:
            df_info(df=df, df_name=f"{selecteur} - After column mapping")
            df = process_func(df)
        df_info(df=df, df_name=f"{selecteur} - After processing")

        # Export
        s3_hook.write(
            file_path=str(task_config.filepath_tmp_s3),
            content=df.to_parquet(path=None, index=False),
        )

    return _task


def create_multi_files_input_etl_task(
    output_selecteur: str,
    input_selecteurs: list[str],
    process_func: Callable[..., pd.DataFrame],
    read_options: dict[str, Any] | None = None,
    use_required_cols: bool = False,
) -> Callable[..., XComArg]:
    """
    Create an ETL task that:
      1. Reads multiple input datasets (from S3 or configured sources)
      2. Processes them with a custom process_func
      3. Writes the result to the output_selecteur location in S3

    There must be a unique DataFrame returned by process_func.

    Args:
        output_selecteur: The config selector key for the merged dataset
        input_selecteurs: List of selector keys for the input datasets
        process_func: A function that merges/processes (*dfs) -> DataFrame
        read_options: Optional file read options (csv, excel, parquet, etc.)

    Returns:
        Callable: Airflow task function
    """
    if read_options is None:
        read_options = {}

    @task(task_id=output_selecteur)
    def _task(**context) -> None:
        # Get project name from context
        params = context.get("params", {})
        nom_projet = params.get("nom_projet")
        if not nom_projet:
            raise ValueError("nom_projet must be defined in DAG parameters")

        # Initialize handler
        s3_handler = S3FileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

        # Resolve configs
        output_config = get_selecteur_config(
            nom_projet=nom_projet, selecteur=output_selecteur
        )

        # Load all input datasets
        dfs: list[pd.DataFrame] = []
        for sel in input_selecteurs:
            cfg = get_selecteur_config(nom_projet=nom_projet, selecteur=sel)
            if use_required_cols:
                required_cols = get_cols_mapping(nom_projet=nom_projet, selecteur=sel)
                if not required_cols.empty:
                    read_options["columns"] = required_cols["colname_dest"].to_list()
            df = read_dataframe(
                file_handler=s3_handler,
                file_path=cfg.filepath_source_s3,
                read_options=read_options,
            )
            df_info(df=df, df_name=f"{sel} - Source normalisée")
            dfs.append(df)

        # Process all datasets
        merged_df = process_func(*dfs)

        df_info(df=merged_df, df_name=f"{output_selecteur} - After processing")

        # Export merged result
        s3_handler.write(
            file_path=str(output_config.filepath_tmp_s3),
            content=merged_df.to_parquet(path=None, index=False),
        )

    return _task


def create_action_etl_task(
    task_id: str,
    action_func: Callable[P, R],
    action_args: Optional[tuple] = None,
    action_kwargs: Optional[dict[str, Any]] = None,
):
    """Create an ETL task that executes a given action function with parameters."""

    if action_args is None:
        action_args = ()
    if action_kwargs is None:
        action_kwargs = {}

    @task(task_id=task_id)
    def _task(**context):
        merged_kwargs = {**action_kwargs, **context}
        action_func(*action_args, **merged_kwargs)

    return _task
