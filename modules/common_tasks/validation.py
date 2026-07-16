"""Utilities to validate DAG `params` at runtime.

This module provides a factory that creates an Airflow task which checks
that required parameters are present in the DAG `params` mapping and validates
the structure against the DagParams dataclass.

Usage example:
    validate_params = create_validate_dag_params_task(
        validate_db=True,
        validate_mail=True,
        task_id="validate_dag_params",
    )

    chain(
        validate_params(),
        other_tasks...
    )
"""

import logging
from collections.abc import Mapping
from typing import Any

from airflow.sdk import task

from modules.enums.dags import DagStatus
from modules.utils.exceptions import ConfigError


def _is_missing(value: Any) -> bool:
    """Decide whether a value should be considered "missing".

    - None, empty string, and empty iterable are missing.
    - Boolean False is considered present (use require_truthy to enforce True).
    """
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return bool(isinstance(value, (list, tuple, set, dict)) and len(value) == 0)


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

    errors: list[str] = []

    # Check nom_projet
    if "nom_projet" not in params:
        errors.append("Field 'nom_projet' is required")
    elif _is_missing(params["nom_projet"]):
        errors.append("Field 'nom_projet' cannot be empty")

    # Check dag_status
    if "dag_status" not in params:
        errors.append("Field 'dag_status' is required")
    else:
        dag_status = params["dag_status"]
        if isinstance(dag_status, str):
            try:
                DagStatus[dag_status.upper()]
            except KeyError:
                errors.append(
                    f"Invalid dag_status: {dag_status}. Must be one of: {', '.join([s.name for s in DagStatus])}"
                )
        elif isinstance(dag_status, int):
            valid_values = [s.value for s in DagStatus]
            if dag_status not in valid_values:
                errors.append(f"Invalid dag_status value: {dag_status}. Must be one of: {valid_values}")

    # Check db (optional, but if present must be valid)
    if "db" in params and params["db"] is not None:
        db = params["db"]
        if not isinstance(db, dict):
            errors.append("Field 'db' must be a dictionary")
        else:
            if "prod_schema" not in db:
                errors.append("Missing required field: db.prod_schema")
            elif _is_missing(db["prod_schema"]):
                errors.append("Field 'db.prod_schema' cannot be empty")

            # tmp_schema is optional with default value
            if "tmp_schema" in db and _is_missing(db["tmp_schema"]):
                errors.append("Field 'db.tmp_schema' cannot be empty if provided")

    # Check enable (required)
    if "enable" not in params:
        errors.append("Missing required field: enable")
    else:
        enable = params["enable"]
        if not isinstance(enable, dict):
            errors.append("Field 'enable' must be a dictionary")
        else:
            required_flags = ["db", "mail", "s3", "convert_files", "download_grist_doc"]
            for flag in required_flags:
                if flag not in enable:
                    errors.append(f"Missing required field: enable.{flag}")
                elif not isinstance(enable[flag], bool):
                    errors.append(f"Field 'enable.{flag}' must be a boolean, got {type(enable[flag]).__name__}")

    if len(errors) > 0:
        logging.error("Validation errors: %s", errors)
        raise ConfigError("DAG params validation failed")

    logging.info("DAG params validation passed")
