"""File processing task utilities using infrastructure handlers."""

import logging
from typing import Any, Callable, Dict, Optional

from airflow.decorators import task
import pandas as pd

from infra.file_handling.s3 import S3FileHandler
from infra.file_handling.dataframe import read_dataframe

from utils.dataframe import df_info
from utils.config.tasks import (
    get_selecteur_config,
    get_cols_mapping,
    format_cols_mapping,
)
from utils.config.vars import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID

TaskParams = Dict[str, Any]
ProcessFunc = Callable[[pd.DataFrame, Optional[dict[str, str]]], pd.DataFrame]


def create_parquet_converter_task(
    task_params: TaskParams,
    selecteur: str,
    process_func: Optional[ProcessFunc] = None,
    read_options: Optional[dict[str, Any]] = None,
) -> Callable:
    """Create a task that converts files to Parquet format.

    Args:
        task_params: Airflow task parameters
        selecteur: Selector to process
        process_func: Optional function to process DataFrame
        read_options: Optional read options for the input file

    Returns:
        Task function that performs the conversion

    Raises:
        ValueError: If task_id not provided in task_params
    """
    if "task_id" not in task_params:
        raise ValueError("task_params must include 'task_id'")

    @task(**task_params)
    def convert_to_parquet(**context) -> None:
        """Convert file to Parquet format and upload to S3."""
        s3_handler = S3FileHandler(
            connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
        )
        params = context.get("params", {})
        nom_projet = params.get("nom_projet")
        if not nom_projet:
            raise ValueError("nom_projet must be defined in DAG parameters")

        # Get input file path
        logging.info(
            f"Getting configuration for project {nom_projet} and selector {selecteur}"
        )
        config = get_selecteur_config(nom_projet=nom_projet, selecteur=selecteur)

        # Read input file based on extension
        logging.info(f"Reading file from {config.filepath_source_s3}")
        df = read_dataframe(
            file_handler=s3_handler,
            file_path=config.filepath_source_s3,
            read_options=read_options,
        )

        df_info(df, f"{task_params['task_id']} - Initial state")

        # Apply column mapping
        cols_map = get_cols_mapping(nom_projet=selecteur, selecteur=selecteur)
        cols_map = format_cols_mapping(df_cols_map=cols_map.copy())

        # Apply custom processing
        if process_func:
            df = process_func(df, cols_map)
            df_info(df, f"{task_params['task_id']} - After processing")

        # Convert to parquet and save
        parquet_data = df.to_parquet(path=None, index=False)
        logging.info(f"Saving to {config.filepath_tmp_s3}")
        s3_handler.write(config.filepath_tmp_s3, parquet_data)
        logging.info(f"Successfully saved parquet file to {config.filepath_tmp_s3}")

    return convert_to_parquet
