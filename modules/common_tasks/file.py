"""File processing task utilities using infrastructure handlers."""

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from airflow.sdk import task

from modules.enums.dags import FeatureFlags
from modules.enums.filesystem import FileHandlerType
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import FSConfig, create_file_handler
from modules.utils.config.dag_params import get_project_name, should_skip_task
from modules.utils.config.tasks import (
    column_mapping_dataframe,
    column_mapping_dict,
    get_selecteur_storage_info,
)
from modules.utils.logs import df_info

TaskParams = dict[str, Any]


def create_parquet_converter_task(
    selecteur: str,
    task_params: TaskParams | None = None,
    process_func: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    read_options: dict[str, Any] | None = None,
    apply_cols_mapping: bool = True,
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
    if task_params is None:
        task_params = {"task_id": selecteur}

    if "task_id" not in task_params:
        raise ValueError("task_params must include 'task_id'")

    @task(**task_params)
    def convert_to_parquet(**context) -> None:
        """Convert file to Parquet format and upload to S3."""
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(),
        )

        nom_projet = get_project_name(context=context)

        if should_skip_task(context=context, feature_flag=FeatureFlags.CONVERT_FILES):
            return

        # Get input file path
        logging.info(msg=f"Getting configuration for project {nom_projet} and selector {selecteur}")
        selecteur_config = get_selecteur_storage_info(nom_projet=nom_projet, selecteur=selecteur)
        source_key = selecteur_config.get_full_s3_key(use_id_source=True)
        dest_tmp_key = selecteur_config.get_full_s3_key(with_tmp_segment=True, use_id_source=False)

        # Read input file based on extension
        logging.info(msg=f"Reading file from {source_key}")
        df = read_dataframe(
            file_handler=s3_handler,
            file_path=source_key,
            read_options=read_options,
        )

        df_info(df, df_name=f"{task_params['task_id']} - Initial state")

        if apply_cols_mapping:
            # Apply column mapping if available
            cols_mapping = column_mapping_dataframe(nom_projet=nom_projet, selecteur=selecteur)
            if cols_mapping.empty:
                print(f"No column mapping found for selecteur {selecteur}")
            else:
                cols_mapping = column_mapping_dict(df_cols_map=cols_mapping)
                df = df.set_axis(
                    labels=[" ".join(colname.split()) for colname in df.columns],
                    axis="columns",
                )
                df = df.rename(columns=cols_mapping, errors="raise")
                df = df.drop(columns=list(set(df.columns) - set(cols_mapping.values())))

        # Apply custom processing
        if process_func:
            df = process_func(df)
            df_info(df, df_name=f"{task_params['task_id']} - After processing")

        # Convert to parquet and save
        parquet_data = df.to_parquet(path=None, index=False)
        logging.info(msg=f"Saving to {dest_tmp_key}")
        s3_handler.write(file_path=dest_tmp_key, content=parquet_data)
        logging.info(msg=f"Successfully saved parquet file to {dest_tmp_key}")

    return convert_to_parquet
