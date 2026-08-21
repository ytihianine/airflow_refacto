from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from modules.enums.database import DatabaseType
from modules.enums.filesystem import FileHandlerType
from modules.infra.database.factory import create_db_handler
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.factory import FSConfig, create_file_handler
from modules.types.projet import SelecteurConfig


@dataclass
class DataContext:
    """Runtime container shared across the pipeline."""

    datasets: dict[str, pd.DataFrame] = field(default_factory=dict)

    def add(self, name: str, df: pd.DataFrame) -> None:
        self.datasets[name] = df

    def get(self, name: str) -> pd.DataFrame:
        return self.datasets[name]

    def replace(self, name: str, df: pd.DataFrame) -> None:
        self.datasets[name] = df

    def exists(self, name: str) -> bool:
        return name in self.datasets


@dataclass(frozen=True)
class ReaderStrategy(ABC):
    context: DataContext = field(default_factory=DataContext)

    @abstractmethod
    def read(
        self,
        selecteur: SelecteurConfig,
        selecteurs: Mapping[str, SelecteurConfig] | None = None,
    ) -> DataContext: ...


@dataclass(frozen=True)
class GristReaderStrategy(ReaderStrategy):
    doc_selecteur_name: str = "grist_doc"

    def read(
        self,
        selecteur: SelecteurConfig,
        selecteurs: Mapping[str, SelecteurConfig] | None = None,
    ) -> DataContext:

        if selecteurs is None:
            raise ValueError("selecteurs mapping is required for GristReaderStrategy")
        if self.doc_selecteur_name not in selecteurs:
            raise ValueError(f"Document selecteur '{self.doc_selecteur_name}' not found in runtime configs")

        table_info = selecteur.storage_info
        document_config = selecteurs[self.doc_selecteur_name]
        document_info = document_config.storage_info

        if table_info.id_source is None:
            raise ValueError(f"id_source must be defined for '{table_info.selecteur}'.")

        # Handlers
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            config=FSConfig(
                bucket=table_info.bucket,
                connection_id=selecteur.storage_options.s3_conn_id,
            ),
        )
        local_handler = create_file_handler(
            handler_type=FileHandlerType.LOCAL,
            config=FSConfig(
                base_path="/tmp",
            ),
        )

        doc_local_path = Path("/tmp") / document_info.filename

        sqlite_handler = create_db_handler(
            connection_id=str(doc_local_path),
            db_type=DatabaseType.SQLITE,
        )

        # Download the Grist document locally
        grist_doc = s3_handler.read(file_path=document_info.get_full_s3_key(with_tmp_segment=True))

        local_handler.write(
            file_path=str(doc_local_path),
            content=grist_doc,
        )

        # Read the requested table
        df = sqlite_handler.fetch_df(query=f"SELECT * FROM {table_info.id_source}")

        self.context.add(name=table_info.selecteur, df=df)
        return self.context


@dataclass(frozen=True)
class FileReaderStrategy(ReaderStrategy):
    fs_type: FileHandlerType = field(default=FileHandlerType.S3)

    def read(
        self,
        selecteur: SelecteurConfig,
        selecteurs: Mapping[str, SelecteurConfig] | None = None,
    ) -> DataContext:
        fs_handler = create_file_handler(
            handler_type=self.fs_type,
            config=FSConfig(
                bucket=selecteur.storage_info.bucket,
                connection_id=selecteur.storage_options.s3_conn_id,
            ),
        )
        df = read_dataframe(
            file_handler=fs_handler,
            file_path=selecteur.storage_info.get_full_s3_key(use_id_source=True),
            read_options=selecteur.storage_options.read_options,
        )
        self.context.add(name=selecteur.storage_info.selecteur, df=df)
        return self.context


@dataclass(frozen=True)
class DbReaderStrategy(ReaderStrategy):
    query: str = field(default="SELECT * FROM my_table")

    def read(
        self,
        selecteur: SelecteurConfig,
        selecteurs: Mapping[str, SelecteurConfig] | None = None,
    ) -> DataContext:
        db_handler = create_db_handler(
            connection_id=selecteur.storage_options.db_conn_id,
            db_type=DatabaseType.POSTGRES,
        )
        df = db_handler.fetch_df(query=self.query)
        self.context.add(name=selecteur.storage_info.selecteur, df=df)
        return self.context
