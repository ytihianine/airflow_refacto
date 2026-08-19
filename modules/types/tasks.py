import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd
from airflow.sdk import XComArg, task

from modules.types.dags import TaskConfig
from modules.types.projet import ProjetMetadata, SelecteurConfig
from modules.types.readers import DataContext, ReaderStrategy
from modules.types.writers import WriterStrategy
from modules.utils.config.dag_params import get_execution_date, get_project_name
from modules.utils.config.tasks import get_projet_metadata
from modules.utils.logs import df_info


class PipelineStep(ABC):
    @abstractmethod
    def __call__(self, data_context: DataContext) -> DataContext: ...


@dataclass(frozen=True)
class SingleInputStep(PipelineStep):
    fn: Callable[..., pd.DataFrame]
    input_key: str
    output_key: str

    def __call__(self, data_context: DataContext) -> DataContext:
        df = data_context.get(name=self.input_key)
        df_info(df=df, df_name=f"{self.input_key} -- Source")
        result = self.fn(df=df)
        df_info(df=result, df_name=f"{self.input_key} -- After processing")
        data_context.add(name=self.output_key, df=result)
        return data_context


@dataclass(frozen=True)
class MultiInputStep(PipelineStep):
    input_keys: list[str]
    output_key: str
    fn: Callable[..., pd.DataFrame]

    def __call__(self, data_context: DataContext) -> DataContext:
        dfs = [data_context.get(k) for k in self.input_keys]
        result = self.fn(*dfs)
        data_context.add(self.output_key, result)
        return data_context


@dataclass(frozen=True)
class RuntimeContext:
    airflow_context: dict[str, Any]
    project_name: str
    execution_date: datetime
    metadata: ProjetMetadata
    selecteurs: dict[str, SelecteurConfig]

    @classmethod
    def from_airflow(
        cls,
        context: dict[str, Any],
        selecteur_config_task_id: str,
    ) -> "RuntimeContext":
        project_name = get_project_name(context=context)
        execution_date = get_execution_date(context=context)
        metadata = get_projet_metadata(nom_projet=project_name)
        snapshot_id = metadata.snapshot_id

        if not snapshot_id:
            raise ValueError("snapshot_id is not defined")

        raw_selecteurs = context["ti"].xcom_pull(task_ids=selecteur_config_task_id)
        if not raw_selecteurs:
            raise ValueError("No selecteur config found in XCom for " f"task_id='{selecteur_config_task_id}'")

        selecteurs = {}
        for raw_selecteur in raw_selecteurs:
            if isinstance(raw_selecteur, SelecteurConfig):
                selecteur = raw_selecteur
            else:
                selecteur = SelecteurConfig.from_dict(data=raw_selecteur)
            selecteurs[selecteur.storage_info.selecteur] = selecteur

        return cls(
            airflow_context=context,
            project_name=project_name,
            execution_date=execution_date,
            metadata=metadata,
            selecteurs=selecteurs,
        )


@dataclass(frozen=True)
class ETLTask(ABC):
    task_config: TaskConfig
    target: str
    reader: ReaderStrategy
    steps: list[PipelineStep] = field(default_factory=list)
    writers: list[WriterStrategy] = field(default_factory=list)
    add_metadata: bool = True
    selecteur_config_task_id: str = "get_selecteur_config"

    def _add_metadata(
        self,
        df: pd.DataFrame,
        runtime: RuntimeContext,
    ) -> pd.DataFrame:
        df = df.copy()

        df["snapshot_id"] = runtime.metadata.snapshot_id
        df["snapshot_id_parent"] = runtime.metadata.snapshot_id_parent
        df["import_timestamp"] = runtime.metadata.import_timestamp

        return df

    def run_pipeline(self, context: dict[str, Any]) -> None:
        runtime = RuntimeContext.from_airflow(
            context=context,
            selecteur_config_task_id=self.selecteur_config_task_id,
        )

        if self.target not in runtime.selecteurs:
            raise ValueError(
                f"Target '{self.target}' not found in runtime selecteurs. "
                f"Available: {list(runtime.selecteurs.keys())}"
            )
        target_selecteur = runtime.selecteurs[self.target]

        # Fetch data
        data_context = self.reader.read(
            selecteur=target_selecteur,
            selecteurs=runtime.selecteurs,
        )

        for index, step in enumerate(self.steps):
            logging.info(f"Running step {index + 1}/{len(self.steps)}: {step}")
            data_context = step(data_context)

        if self.target not in data_context.datasets:
            raise ValueError(f"Target '{self.target}' not found. " f"Available: {list(data_context.datasets.keys())}")

        # extract output
        output_df = data_context.get(self.target)

        # metadata
        if self.add_metadata:
            output_df = self._add_metadata(output_df, runtime)
            data_context.replace(self.target, output_df)

        # write output
        for writer in self.writers:
            writer.write(df=output_df, selecteur=target_selecteur)

    def create_task(self) -> XComArg:
        @task(
            task_id=self.task_config.task_id,
            retries=self.task_config.retries,
            retry_delay=self.task_config.retry_delay,
            retry_exponential_backoff=self.task_config.retry_exponential_backoff,
            max_retry_delay=self.task_config.max_retry_delay,
            on_execute_callback=self.task_config.on_execute_callback,
            on_failure_callback=self.task_config.on_failure_callback,
            on_success_callback=self.task_config.on_success_callback,
            on_retry_callback=self.task_config.on_retry_callback,
            on_skipped_callback=self.task_config.on_skipped_callback,
        )
        def _task(**context) -> None:
            self.run_pipeline(context=context)

        return _task()
