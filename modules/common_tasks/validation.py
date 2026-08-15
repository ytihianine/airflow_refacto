"""Utilities to validate DAG `params` at runtime."""

import logging
from collections.abc import Mapping
from typing import Any

from airflow.sdk import task

from modules.types.dags import DagParams
from modules.utils.exceptions import ConfigError


@task(task_id="validate_dag_params")
def validate_dag_parameters(**context: Mapping[str, Any]) -> None:
    """Validate that params conform to DagParams structure.

    Returns a list of error messages. Empty list means validation passed.
    """
    params = context.get("params")

    if params is None:
        raise ConfigError("DAG params are required")

    if not isinstance(params, dict):
        raise ConfigError("DAG params must be a dictionary")

    # Init class to check for errors.
    DagParams.from_dag_context(context_params=params)

    logging.info("DAG params validation passed")
