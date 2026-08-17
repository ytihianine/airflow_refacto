import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ParamSpec, TypeVar

from airflow.sdk.definitions._internal.abstractoperator import TaskStateChangeCallback

from modules.constants import DEFAULT_TMP_SCHEMA
from modules.enums.dags import DagStatus
from modules.utils.exceptions import ConfigError

# ==================
# Dags
# ==================
P = ParamSpec(name="P")
R = TypeVar(name="R")


@dataclass(frozen=True)
class TaskConfig:
    task_id: str
    retries: int = 0
    retry_delay: timedelta | float = 0
    retry_exponential_backoff: bool = False
    max_retry_delay: timedelta | float | None = None
    on_execute_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_failure_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_success_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_retry_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None
    on_skipped_callback: TaskStateChangeCallback | list[TaskStateChangeCallback] | None = None


@dataclass
class ETLStep:
    fn: Callable[..., Any]
    kwargs: dict[str, Any] | None = None
    use_context: bool = False
    read_data: bool = False
    use_previous_output: bool = False


@dataclass(frozen=True)
class DBParams:
    prod_schema: str
    tmp_schema: str = DEFAULT_TMP_SCHEMA

    @classmethod
    def from_dag_context(cls, context_params: dict) -> "DBParams":
        if "db" not in context_params:
            raise ConfigError("Field 'db' is required")

        db_params = context_params["db"]

        if not isinstance(db_params, dict):
            raise ConfigError("Field 'db' must be a dictionary")

        if "prod_schema" not in db_params:
            raise ConfigError("Field 'prod_schema' is required in 'db'")

        return cls(
            prod_schema=db_params["prod_schema"],
            tmp_schema=db_params.get("tmp_schema", DEFAULT_TMP_SCHEMA),
        )


@dataclass(frozen=True)
class FeatureFlagsEnable:
    db: bool
    mail: bool
    s3: bool
    convert_files: bool
    download_grist_doc: bool


def is_key_in_dict(key: str, d: dict[str, Any]) -> bool:
    """Check if a key is present in a dictionary and not None."""
    return key in d


@dataclass(frozen=True)
class DagParams:
    nom_projet: str
    dag_status: DagStatus | int
    db: DBParams | None
    enable: FeatureFlagsEnable

    @classmethod
    def from_dag_context(cls, context_params: dict) -> "DagParams":
        errors: list[str] = []

        # Check keys are in context
        if "nom_projet" not in context_params:
            errors.append("Field 'nom_projet' is required")

        if "dag_status" not in context_params:
            errors.append("Field 'dag_status' is required")

        if "db" not in context_params:
            errors.append("Field 'db' is required")

        if "enable" not in context_params:
            errors.append("Field 'enable' is required")

        if len(errors) > 0:
            logging.error("Validation errors: %s", errors)
            raise ConfigError(msg="DAG params validation failed.")

        return cls(
            nom_projet=context_params["nom_projet"],
            dag_status=DagStatus(context_params["dag_status"]),
            db=DBParams.from_dag_context(context_params),
            enable=FeatureFlagsEnable(**context_params["enable"]),
        )
