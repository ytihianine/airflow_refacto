"""Utilities to validate DAG `params` at runtime.

This module provides a factory that creates an Airflow task which checks
that required parameters are present in the DAG `params` mapping and
optionally enforces that some flags are truthy (e.g. `mail.enable`).

Usage example:
    validate_params = create_validate_params_task(
        required_paths=["nom_projet", "mail.to", "mail.enable"],
        require_truthy=["mail.enable"],
        task_id="validate_dag_params",
    )

    chain(
        validate_params(),
        other_tasks...
    )
"""

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import logging

from airflow import XComArg
from airflow.decorators import task

from utils.exceptions import ConfigError


def _get_by_path(mapping: Dict[str, Any], path: str) -> Tuple[Any, bool]:
    """Retrieve a nested value from mapping using dot-separated path.

    Returns (value, found) where found is False when any key in the path
    is missing.
    """
    current: Any = mapping
    if path == "":
        return current, True
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None, False
        current = current[part]
    return current, True


def _is_missing(value: Any) -> bool:
    """Decide whether a value should be considered "missing".

    - None, empty string, and empty iterable are missing.
    - Boolean False is considered present (use require_truthy to enforce True).
    """
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (list, tuple, set, dict)) and len(value) == 0:
        return True
    return False


def create_validate_params_task(
    required_paths: Iterable[str],
    require_truthy: Optional[Iterable[str]] = None,
    task_id: str = "validate_dag_params",
) -> Callable[..., XComArg]:
    """Create an Airflow @task that validates DAG `params`.

    Args:
        required_paths: iterable of dot-separated parameter paths that must exist
            in `params` (e.g. "mail.to", "nom_projet"). Presence is enforced;
            empty strings/lists/None count as missing.
        require_truthy: optional iterable of paths that must evaluate to True
            (useful for boolean flags like "mail.enable"). If a path is listed
            here and either missing or falsy, it will be reported as invalid.
        task_id: Airflow task id to assign to generated task

    Returns:
        A callable that when invoked returns the Airflow task (so you can
        include it in DAG definitions). Example: "validate = create_validate_params_task(...); validate()"
    """

    required_paths = list(required_paths)
    require_truthy = set(require_truthy or [])

    @task(task_id=task_id)
    def _validator(**context: Dict[str, Any]) -> None:
        params: Dict[str, Any] = context.get("params", {}) or {}

        missing: List[str] = []
        falsy: List[str] = []

        for path in required_paths:
            value, found = _get_by_path(params, path)
            if not found or _is_missing(value):
                missing.append(path)

        for path in require_truthy:
            value, found = _get_by_path(params, path)
            if not found:
                missing.append(path)
            else:
                # require actual truthy value for this flag
                if not bool(value):
                    falsy.append(path)

        if missing or falsy:
            msg_parts: List[str] = []
            if missing:
                msg_parts.append(f"Missing or empty params: {', '.join(missing)}")
            if falsy:
                msg_parts.append(
                    f"Params required to be truthy but falsy: {', '.join(falsy)}"
                )

            detail = "; ".join(msg_parts)
            logging.error("DAG params validation failed: %s", detail)
            raise ConfigError(
                "DAG params validation failed",
                detail=detail,
                missing=missing,
                falsy=falsy,
            )

        logging.info("DAG params validation passed. Required params present.")

    return _validator
