"""Type definitions for ETL tasks."""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from enum import Enum


class FileFormat(str, Enum):
    """Supported file formats for ETL operations."""

    CSV = "csv"
    EXCEL = "excel"
    PARQUET = "parquet"
    JSON = "json"


@dataclass
class ETLConfig:
    """Configuration for ETL operations."""

    selecteur: str
    nom_projet: str
    format: FileFormat
    read_options: Optional[Dict[str, Any]] = None
    write_options: Optional[Dict[str, Any]] = None
    validate_required_columns: bool = True
