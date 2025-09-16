"""Type definitions for configuration data structures."""

from dataclasses import dataclass
from typing import Optional, TypedDict, List, ParamSpec, TypeVar


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


class MailParams(TypedDict, total=False):
    enable: bool
    to: List[str]
    cc: List[str]
    bcc: List[str]


class DocsParams(TypedDict):
    lien_pipeline: str
    lien_donnees: str


class DagParams(TypedDict):
    nom_projet: str
    db: DBParams
    mail: MailParams
    docs: DocsParams


# Common keys (use these constants to avoid hard-coded strings in checks)
KEY_NOM_PROJET = "nom_projet"
KEY_DB = "db"
KEY_DB_PROD_SCHEMA = "prod_schema"
KEY_DB_TMP_SCHEMA = "tmp_schema"
KEY_MAIL = "mail"
KEY_MAIL_ENABLE = "enable"
KEY_MAIL_TO = "to"
KEY_MAIL_CC = "cc"
KEY_DOCS = "docs"
KEY_DOCS_LIEN_PIPELINE = "lien_pipeline"
KEY_DOCS_LIEN_DONNEES = "lien_donnees"
