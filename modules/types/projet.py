from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict
from uuid import UUID

from modules.constants import DEFAULT_PG_DATA_CONN_ID, DEFAULT_S3_CONN_ID
from modules.enums.dags import TypeSource
from modules.enums.database import LoadStrategy, PartitionTimePeriod


def custom_asdict_factory(data) -> dict[str, Any]:
    """
    Custom factory function for dataclasses asdict function
    to convert Enum values to their actual values instead of Enum instances.
    """
    from enum import Enum

    def convert_value(obj) -> Any:
        if isinstance(obj, Enum):
            return obj.value
        return obj

    return dict((k, convert_value(obj=v)) for k, v in data)


class ProjetMetadata(TypedDict):
    snapshot_id: UUID
    snapshot_id_parent: UUID | None
    import_timestamp: datetime


@dataclass(frozen=True)
class ProjetS3:
    projet: str
    bucket: str
    key: str
    key_tmp: str


@dataclass(frozen=True)
class ColumnMapping:
    projet: str
    selecteur: str
    colname_source: str
    colname_dest: str


@dataclass(frozen=True)
class Documentation:
    projet: str
    type_documentation: str
    lien: str


@dataclass(frozen=True)
class Contact:
    projet: str
    contact_mail: str
    is_mail_generic: bool


# ==================
# Selecteur
# ==================
@dataclass(frozen=True)
class SelecteurStorageInfo:
    projet: str
    selecteur: str
    # s3 info
    bucket: str
    s3_key: str
    filename: str
    local_dir: str
    # db info
    tbl_name: str | None
    # Source info
    type_source: TypeSource
    id_source: str | None

    def __post_init__(self) -> None:
        if not isinstance(self.type_source, TypeSource) and self.type_source is not None:
            object.__setattr__(self, "type_source", TypeSource(value=self.type_source))

    def get_full_s3_key(
        self,
        with_bucket: bool = False,
        with_tmp_segment: bool = False,
        use_id_source: bool = False,
    ) -> str:
        segments = [self.s3_key]
        if with_bucket:
            segments.insert(0, self.bucket)
        if with_tmp_segment:
            segments.append("tmp")

        if use_id_source and self.id_source is not None:
            segments.append(self.id_source)
        else:
            segments.append(self.filename)

        return "/".join(segments)

    def get_local_path(self) -> str:
        if self.filename is None:
            return str(Path(self.local_dir) / "filename_undefined")
        return str(Path(self.local_dir) / self.filename)

    def get_iceberg_namespace(self, with_bucket: bool = False) -> str:
        s3_key = self.get_full_s3_key(with_bucket=with_bucket)
        namespace_split = s3_key.split(sep=".")[0].split(sep="/")[:-1]
        return ".".join(namespace_split)


@dataclass(frozen=True, kw_only=True)
class SelecteurStorageOptions:
    # S3
    s3_conn_id: str = DEFAULT_S3_CONN_ID
    write_to_s3: bool = True
    write_to_s3_with_iceberg: bool = True
    read_options: dict[str, Any] = field(default_factory=dict)
    # Database
    db_conn_id: str = DEFAULT_PG_DATA_CONN_ID
    write_to_db: bool = True
    use_prod_schema: bool = True
    tbl_order: int = 0
    keep_file_id_col: bool = True
    is_partitioned: bool = True
    partition_period: PartitionTimePeriod = PartitionTimePeriod.DAY
    load_strategy: LoadStrategy = LoadStrategy.INCREMENTAL

    def __post_init__(self) -> None:
        # Convert partition_period and load_strategy to their respective Enum types if they are provided as strings
        if not isinstance(self.partition_period, PartitionTimePeriod):
            object.__setattr__(
                self,
                "partition_period",
                PartitionTimePeriod(value=self.partition_period),
            )

        if not isinstance(self.load_strategy, LoadStrategy):
            object.__setattr__(self, "load_strategy", LoadStrategy(value=self.load_strategy))


@dataclass(frozen=True)
class SelecteurConfig:
    storage_info: SelecteurStorageInfo
    storage_options: SelecteurStorageOptions

    @classmethod
    def load(
        cls,
        storage_info: SelecteurStorageInfo,
        storage_options: SelecteurStorageOptions,
    ) -> "SelecteurConfig":
        return cls(
            storage_info=storage_info,
            storage_options=storage_options,
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SelecteurConfig":
        return cls(
            storage_info=SelecteurStorageInfo(**data["storage_info"]),
            storage_options=SelecteurStorageOptions(**data["storage_options"]),
        )

    # Methods to early-exit tasks based on the configuration options
    def should_write_to_s3(self) -> bool:
        """Return True if this selecteur should be written to S3."""
        return self.storage_options.write_to_s3

    def should_write_to_iceberg(self) -> bool:
        """Return True if this selecteur should be written to the Iceberg catalog.

        Returns False for the special 'grist_doc' selecteur and when the
        write_to_s3_with_iceberg option is disabled.
        """
        return self.storage_info.selecteur != "grist_doc" and self.storage_options.write_to_s3_with_iceberg

    def should_write_to_db(self) -> bool:
        """Return True if this selecteur should be written to the database.

        Returns False when the write_to_db option is disabled or when no
        tbl_name is defined for this selecteur.
        """
        return self.storage_options.write_to_db and bool(self.storage_info.tbl_name)
