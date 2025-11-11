"""Type definitions and Enums for configuration data structures."""

from enum import Enum, auto
from dataclasses import dataclass
from typing import Optional, TypedDict, List, ParamSpec, TypeVar


# ==================
# Enums
# ==================
class DatabaseType(Enum):
    """Database types enumeration."""

    POSTGRES = auto()
    SQLITE = auto()


class FileHandlerType(Enum):
    """File handler types enumeration."""

    S3 = auto()
    LOCAL = auto()


class PartitionTimePeriod(str, Enum):
    DAY = auto()
    WEEK = auto()
    MONTH = auto()
    YEAR = auto()


class LoadStrategy(Enum):
    """Load strategies for data ingestion."""

    FULL_LOAD = auto()
    INCREMENTAL = auto()
    APPEND = auto()


class FileFormat(str, Enum):
    """Supported file formats for ETL operations."""

    CSV = "csv"
    EXCEL = "excel"
    PARQUET = "parquet"
    JSON = "json"


# ==================
# Data Classes
# ==================


P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class SelecteurConfig:
    """Selecteur configuration structure matching database fields.

    Fields:
        nom_projet: Project name
        selecteur: Configuration selector
        nom_source: Source name/identifier
        filename: Name of the file
        s3_key: S3 storage key
        filepath_source_s3: Source file path in S3
        filepath_local: Local file system path
        filepath_s3: Main S3 file path
        filepath_tmp_s3: Temporary S3 file path
        tbl_name: Database table name
        tbl_order: Table processing order
    """

    nom_projet: str
    selecteur: str
    filename: str
    s3_key: str
    filepath_source_s3: str
    filepath_local: str
    filepath_s3: str
    filepath_tmp_s3: str
    nom_source: Optional[str] = None
    tbl_name: Optional[str] = None
    tbl_order: Optional[int] = None


class DBParams(TypedDict):
    prod_schema: str
    tmp_schema: str


class MailParams(TypedDict):
    enable: bool
    to: List[str]
    cc: Optional[List[str]]
    bcc: Optional[List[str]]


class DocsParams(TypedDict):
    lien_pipeline: str
    lien_donnees: str


class DagParams(TypedDict):
    nom_projet: str
    db: DBParams
    mail: MailParams
    docs: DocsParams


# Top level keys
KEY_NOM_PROJET = "nom_projet"
KEY_DB = "db"
KEY_MAIL = "mail"
KEY_MAIL_ENABLE = f"{KEY_MAIL}.enable"
KEY_MAIL_TO = f"{KEY_MAIL}.to"
KEY_MAIL_CC = f"{KEY_MAIL}.cc"
KEY_DOCS = "docs"
KEY_DOCS_LIEN_PIPELINE = f"{KEY_DOCS}.lien_pipeline"
KEY_DOCS_LIEN_DONNEES = f"{KEY_DOCS}.lien_donnees"


# Nested keys with their paths
ALL_PARAM_PATHS = [
    KEY_NOM_PROJET,
    f"{KEY_DB}.prod_schema",
    f"{KEY_DB}.tmp_schema",
    KEY_MAIL_ENABLE,
    KEY_MAIL_TO,
    KEY_MAIL_CC,
    KEY_DOCS_LIEN_PIPELINE,
    KEY_DOCS_LIEN_DONNEES,
]

# Optional keys that don't need validation
OPTIONAL_PATHS = [
    f"{KEY_MAIL}.bcc",
]
