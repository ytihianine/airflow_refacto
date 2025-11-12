"""Module for ETL task creation and execution."""

from airflow.decorators.base import Task
from typing import Callable, Optional, Any
from airflow import XComArg
import pandas as pd

from airflow.decorators import task

from infra.file_handling.dataframe import read_dataframe
from infra.file_handling.factory import create_default_s3_handler, create_local_handler
from infra.database.factory import create_db_handler
from utils.control.structures import remove_grist_internal_cols
from utils.dataframe import df_info
from utils.config.tasks import (
    get_selecteur_config,
    get_cols_mapping,
    format_cols_mapping,
    get_required_cols,
)
from utils.config.dag_params import get_execution_date, get_project_name
from utils.config.types import R, DatabaseType


def _add_import_metadata(df: pd.DataFrame, context: dict) -> pd.DataFrame:
    """Add import timestamp and date columns."""
    execution_date = get_execution_date(context=context)

    dt_no_timezone = execution_date.replace(tzinfo=None)
    df["import_timestamp"] = dt_no_timezone
    df["import_date"] = dt_no_timezone.date()
    return df


def _add_snapshot_id_metadata(df: pd.DataFrame, context: dict) -> pd.DataFrame:
    """Add snapshot_id column."""
    snapshot_id = context["ti"].xcom_pull(
        key="snapshot_id", task_ids="get_projet_snapshot"
    )
    if not snapshot_id:
        raise ValueError("snapshot_id is not defined")

    df["snapshot_id"] = snapshot_id
    return df


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
        normalisation_process_func: Optional normalization process to run
            before the process function
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
        nom_projet = get_project_name(context=context)

        # Get config values related to the task
        task_config = get_selecteur_config(nom_projet=nom_projet, selecteur=selecteur)
        doc_config = get_selecteur_config(
            nom_projet=nom_projet, selecteur=doc_selecteur
        )
        if task_config.nom_source is None:
            raise ValueError(f"nom_source must be defined for selecteur {selecteur}")

        # Initialize hooks
        s3_handler = create_default_s3_handler()
        local_handler = create_local_handler()
        sqlite_handler = create_db_handler(
            connection_id=doc_config.filepath_local, db_type=DatabaseType.SQLITE
        )

        # Download Grist doc from S3 to local temp file
        grist_doc = s3_handler.read(file_path=doc_config.filepath_tmp_s3)
        local_handler.write(file_path=str(doc_config.filepath_local), content=grist_doc)

        # Get data of table
        df = sqlite_handler.fetch_df(query=f"SELECT * FROM {task_config.nom_source}")

        if normalisation_process_func is not None:
            df = normalisation_process_func(df)
        else:
            print(
                "No normalisation process function provided. Skipping the normalisation step ..."
            )

        # Removing Grist internal colums
        df = remove_grist_internal_cols(df=df)

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
    add_import_date: bool = True,
    add_snapshot_id: bool = True,
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
        nom_projet = get_project_name(context=context)

        # Initialize hooks
        s3_handler = create_default_s3_handler()

        # Get config values related to the task
        task_config = get_selecteur_config(nom_projet=nom_projet, selecteur=selecteur)

        # Get data of table
        df = read_dataframe(
            file_handler=s3_handler,
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
                print(
                    "apply_cols_mapping set to True. Renaming the dataframe labels ..."
                )
                cols_mapping = format_cols_mapping(df_cols_map=cols_mapping)
                print(f"Columns mapping: \n{cols_mapping}")
                df = df.set_axis(
                    labels=[" ".join(colname.split()) for colname in df.columns],
                    axis="columns",
                )
                df = df.rename(columns=cols_mapping)
                df = df.loc[list(cols_mapping.values())]

        if process_func is None:
            print("No process function provided. Skipping the processing step ...")
        else:
            df_info(df=df, df_name=f"{selecteur} - After column mapping")
            df = process_func(df)

        if add_import_date:
            df = _add_import_metadata(df=df, context=context)

        if add_snapshot_id:
            df = _add_snapshot_id_metadata(df=df, context=context)

        df_info(df=df, df_name=f"{selecteur} - After processing")

        # Export
        s3_handler.write(
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
    add_import_date: bool = True,
    add_snapshot_id: bool = True,
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
        nom_projet = get_project_name(context=context)

        # Initialize handler
        s3_handler = create_default_s3_handler()

        # Resolve configs
        output_config = get_selecteur_config(
            nom_projet=nom_projet, selecteur=output_selecteur
        )

        # Load all input datasets
        dfs: list[pd.DataFrame] = []
        if use_required_cols:
            required_cols = get_required_cols(
                nom_projet=nom_projet, selecteur=output_selecteur
            )
            print(required_cols)
            if not required_cols.empty:
                read_options["columns"] = required_cols["colname_dest"].to_list()
        for sel in input_selecteurs:
            cfg = get_selecteur_config(nom_projet=nom_projet, selecteur=sel)
            df = read_dataframe(
                file_handler=s3_handler,
                file_path=cfg.filepath_tmp_s3,
                read_options=read_options,
            )
            df_info(df=df, df_name=f"{sel} - Source normalisée")
            dfs.append(df)

        # Process all datasets
        merged_df = process_func(*dfs)

        if add_import_date:
            merged_df = _add_import_metadata(df=merged_df, context=context)

        if add_snapshot_id:
            merged_df = _add_snapshot_id_metadata(df=merged_df, context=context)

        df_info(df=merged_df, df_name=f"{output_selecteur} - After processing")

        # Export merged result
        s3_handler.write(
            file_path=str(output_config.filepath_tmp_s3),
            content=merged_df.to_parquet(path=None, index=False),
        )

    return _task


def create_action_etl_task(
    task_id: str,
    action_func: Callable[..., R],
    action_args: Optional[tuple] = None,
    action_kwargs: Optional[dict[str, Any]] = None,
) -> Task[..., None]:
    """Create an ETL task that executes a given action function with parameters."""

    if action_args is None:
        action_args = ()
    if action_kwargs is None:
        action_kwargs = {}

    @task(task_id=task_id)
    def _task(**context) -> None:
        merged_kwargs = {**action_kwargs}
        action_func(*action_args, **merged_kwargs)

    return _task


def create_action_to_file_etl_task(
    output_selecteur: str,
    task_id: str,
    action_func: Callable[..., pd.DataFrame],
    action_args: Optional[tuple] = None,
    action_kwargs: Optional[dict[str, Any]] = None,
    use_context: bool = False,
) -> Task[..., None]:
    """
    Create an ETL task that executes a given action function with parameters. The action
    function must return a DataFrame that will be saved to a file in S3 according
    to the output_selecteur configuration.

    Args:
        output_selecteur: The config selector key for the output dataset
        task_id: The identifier for this ETL task
        action_func: A function that returns a DataFrame
        action_args: Optional positional arguments for the action function
        action_kwargs: Optional keyword arguments for the action function
    Returns:
        An Airflow task that performs the action and saves the result to S3
    """

    if action_args is None:
        action_args = ()
    if action_kwargs is None:
        action_kwargs = {}

    @task(task_id=task_id)
    def _task(**context) -> None:
        # Get project name from context
        nom_projet = get_project_name(context=context)

        # Initialize handler
        s3_handler = create_default_s3_handler()

        # Resolve configs
        output_config = get_selecteur_config(
            nom_projet=nom_projet, selecteur=output_selecteur
        )

        # Execute action
        if use_context:
            merged_kwargs = {**action_kwargs, **context}
        else:
            merged_kwargs = {**action_kwargs}
        df = action_func(*action_args, **merged_kwargs)

        # Export
        s3_handler.write(
            file_path=str(output_config.filepath_tmp_s3),
            content=df.to_parquet(path=None, index=False),
        )

    return _task
