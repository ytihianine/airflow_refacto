"""Module for ETL task creation and execution."""

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
from airflow.sdk import XComArg, task

from modules.infra.file_system.dataframe import read_dataframe, write_dataframe
from modules.infra.file_system.factory import (
    create_default_s3_handler,
)
from modules.types.dags import ETLStep, TaskConfig
from modules.utils.config.dag_params import get_project_name
from modules.utils.config.tasks import (
    get_projet_metadata,
    get_selecteur_storage_info,
)
from modules.utils.logs import df_info


def _add_metadata(df: pd.DataFrame, nom_projet: str) -> pd.DataFrame:
    metadata = get_projet_metadata(nom_projet=nom_projet)

    df["import_timestamp"] = metadata.import_timestamp
    df["snapshot_id"] = str(metadata.snapshot_id)
    df["snapshot_id_parent"] = str(metadata.snapshot_id_parent) if metadata.snapshot_id_parent is not None else None

    return df


def _execute_step(
    *,
    step: ETLStep,
    previous_output: Any,
    context: dict,
    s3_handler,
    nom_projet: str,
    input_selecteurs: list[str] | None,
) -> Any:
    fn = step.fn
    kwargs = dict(step.kwargs or {})

    if step.read_data and input_selecteurs is None:
        raise ValueError("input_selecteurs must be provided if step.read_data is True")

    # Inject context if required
    if step.use_context:
        kwargs["context"] = context

    # Read data if required
    if step.read_data and input_selecteurs:
        for sel in input_selecteurs:
            cfg = get_selecteur_storage_info(nom_projet=nom_projet, selecteur=sel)
            df = read_dataframe(
                file_handler=s3_handler,
                file_path=cfg.get_full_s3_key(with_tmp_segment=True),
            )
            df_info(df=df, df_name=f"{sel} - Input dataframe")
            # Use "df" as key if only one input, otherwise "df_{selecteur}"
            key = "df" if len(input_selecteurs) == 1 else f"df_{sel}"
            kwargs[key] = df

    # Execute function
    logging.info(msg=f"Executing function: {fn.__name__} with kwargs: {kwargs.keys()}")
    if step.use_previous_output:
        return fn(previous_output, **kwargs)

    return fn(**kwargs)


def create_task(
    task_config: TaskConfig,
    output_selecteur: str,
    steps: list[ETLStep],
    input_selecteurs: list[str] | None = None,
    add_metadata: bool = True,
    export_output: bool = True,
) -> Callable[..., XComArg]:
    """
    Create a generic Airflow task based on the provided TaskConfig.

    Args:
        task_config: Configuration for the task
        output_selecteur: Selector for the output configuration
        steps: List of ETL steps to execute
        input_selecteurs: (Optional) list of selectors for input data
        add_metadata: Whether to add import date and snapshot ID metadata to the output
        export_output: Whether to export the final output to S3

    Returns:
        An Airflow task that performs the defined ETL steps

    Note:
        input_selecteurs:
            - must be provided if any step requires reading data
            - if there is a single input selector, the DataFrame will be passed as "df".
            - if multiple input selectors, DataFrames will be passed as "df_{selecteur}"
    """

    @task(
        task_id=task_config.task_id,
        retries=task_config.retries,
        retry_delay=task_config.retry_delay,
        retry_exponential_backoff=task_config.retry_exponential_backoff,
        max_retry_delay=task_config.max_retry_delay,
        on_execute_callback=task_config.on_execute_callback,
        on_failure_callback=task_config.on_failure_callback,
        on_success_callback=task_config.on_success_callback,
        on_retry_callback=task_config.on_retry_callback,
        on_skipped_callback=task_config.on_skipped_callback,
    )
    def _task(**context) -> None:
        """The actual generic task function."""
        # Get project name from context
        nom_projet = get_project_name(context=context)

        # Initialize handler
        s3_handler = create_default_s3_handler()

        # Execute steps
        result: Any = None
        for idx, step in enumerate(steps):
            logging.info(msg=f"▶ Executing step: step_{idx}")
            logging.info(msg=f"Step information: {step}")

            result = _execute_step(
                step=step,
                previous_output=result,
                context=context,
                s3_handler=s3_handler,
                nom_projet=nom_projet,
                input_selecteurs=input_selecteurs,
            )

            if isinstance(result, pd.DataFrame):
                df_info(df=result, df_name=f"step_{idx} - output")

        # Export final result - always a DataFrame and the last step output
        if not export_output:
            return

        # Resolve configs
        output_config = get_selecteur_storage_info(nom_projet=nom_projet, selecteur=output_selecteur)

        if add_metadata:
            result = _add_metadata(df=result, nom_projet=nom_projet)

        df_info(df=result, df_name=f"{output_selecteur} - df to export")

        # Export result to s3
        write_dataframe(
            df=result,
            file_handler=s3_handler,
            file_path=output_config.get_full_s3_key(with_tmp_segment=True),
            write_options=None,
        )

    return _task
