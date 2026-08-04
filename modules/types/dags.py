from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, ParamSpec, TypeVar

from airflow.sdk.definitions._internal.abstractoperator import TaskStateChangeCallback

from modules.constants import DEFAULT_TMP_SCHEMA
from modules.enums.dags import DagStatus

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


@dataclass(frozen=True)
class FeatureFlagsEnable:
    db: bool
    mail: bool
    s3: bool
    convert_files: bool
    download_grist_doc: bool


@dataclass(frozen=True)
class DagParams:
    nom_projet: str
    dag_status: DagStatus | int
    db: DBParams | None
    enable: FeatureFlagsEnable
