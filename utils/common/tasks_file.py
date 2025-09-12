from typing import Callable
from airflow.decorators import task
import pandas as pd

from utils.file_handler import MinioFileHandler
from utils.df_utility import df_info
from utils.common.config_func import format_cols_mapping


def create_parquet_converter_task(
    task_params: dict[str, any],
    selecteurs: list[str],
    all_cols_map: pd.DataFrame,
    bucket: str = "dsci",
    sheet_name: str = None,
    file_selector_func: Callable[..., str] = None,
    output_key_func: Callable[..., str] = None,
    process_func: Callable[..., any] = None,
):
    if "task_id" not in task_params.keys():
        raise ValueError(
            """task_id must be defined in task_params.
            Example: task_params = {"task_id": "my_task_id"}"""
        )

    @task(**task_params)
    def convert_to_parquet(s3_file_handler: MinioFileHandler) -> None:
        # Get S3 path for the input file
        s3_filepath = file_selector_func()

        # Read file (Excel by default, but can be adapted for other formats)
        df = s3_file_handler.read_excel(
            file_name=s3_filepath,
            sheet_name=sheet_name,
        )

        df_info(df=df, df_name=f"{task_params['task_id'].upper()} - INITIALISATION")

        # Prepare column mapping and process
        cols_map = all_cols_map.loc[all_cols_map["selecteur"].isin(selecteurs)]
        cols_map = format_cols_mapping(df_cols_map=cols_map.copy())
        df = df.rename(columns=cols_map, errors="raise")
        df = process_func()

        df_info(df=df, df_name=f"{task_params['task_id'].upper()} - Après processing")

        # Export to Parquet
        output_key = output_key_func()
        s3_file_handler.load_bytes(
            bytes_data=df.to_parquet(path=None, index=False),
            bucket_name=bucket,
            key=output_key,
            replace=True,
        )

    return convert_to_parquet
