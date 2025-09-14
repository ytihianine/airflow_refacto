"""Type definitions for configuration data structures."""

from dataclasses import dataclass
from typing import Optional


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
