"""Centralised configuration for all scripts, loaded from a single .env file."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).parent / ".env"


class DatabaseSettings(BaseSettings):
    """PostgreSQL connection settings (shared across most scripts)."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(alias="CONFIG_DB_HOST", default="localhost")
    port: int = Field(alias="CONFIG_DB_PORT", default=5432)
    name: str = Field(alias="CONFIG_DB_NAME", default="dbname")
    user: str = Field(alias="CONFIG_DB_USER", default="postgres")
    password: str = Field(alias="CONFIG_DB_PASSWORD", default="postgres")


class S3Settings(BaseSettings):
    """S3 / MinIO credentials."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    access_key_id: str = Field(alias="S3_ACCESS_KEY_ID", default="")
    access_secret_key: str = Field(alias="S3_ACCESS_SECRET_KEY", default="")
    endpoint: str = Field(alias="S3_ENDPOINT", default="")


class CatalogSettings(BaseSettings):
    """Iceberg / Polaris catalog settings."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    client_id: str = Field(alias="CLIENT_ID", default="")
    client_secret: str = Field(alias="CLIENT_SECRET", default="")


class GristSettings(BaseSettings):
    """Grist webhook configuration."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    token: str = Field(alias="GRIST_TOKEN", default="")
    doc_id: str = Field(alias="GRIST_DOC_ID", default="")
    host: str = Field(alias="GRIST_HOST", default="")
    n8n_pipeline_url: str = Field(alias="GRIST_N8N_PIPELINE_URL", default="")


class HttpSettings(BaseSettings):
    """HTTP proxy settings."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    ca_bundle: str = Field(alias="CA_BUNDLE", default="")
    proxy: str = Field(alias="HTTP_PROXY_URL", default="")
    agent: str = Field(alias="HTTP_AGENT", default="")


class LoadDatasetSettings(BaseSettings):
    """Settings for load_dataset script."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    file_path: str = Field(alias="LOAD_DATASET_FILE_PATH", default="")
    db_schema: str = Field(alias="LOAD_DATASET_SCHEMA", default="")
    table_name: str = Field(alias="LOAD_DATASET_TABLE_NAME", default="")
    db_id_colname: str = Field(alias="LOAD_DATASET_ID_COLNAME", default="id")
    import_timestamp: str = Field(alias="LOAD_DATASET_IMPORT_TIMESTAMP", default="")
    snapshot_id: str = Field(alias="LOAD_DATASET_SNAPSHOT_ID", default="")
    tsv_file_path: str = Field(alias="LOAD_DATASET_TSV_FILE_PATH", default="/tmp/df_result.tsv")


class GenerateTablesSettings(BaseSettings):
    """Settings for generate_tables script."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    dossier_parquet: str = Field(alias="GENERATE_TABLES_DOSSIER_PARQUET", default="")
    fichier_sql: str = Field(alias="GENERATE_TABLES_FICHIER_SQL", default="create_tables.sql")


class S3UserSettings(BaseSettings):
    """Settings for S3 user creation script."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    alias_name: str = Field(alias="S3_USER_ALIAS_NAME", default="s3_admin")
    bucket: str = Field(alias="S3_USER_BUCKET", default="")
    key_access: str = Field(alias="S3_USER_KEY_ACCESS", default="*")
    policy_name: str = Field(alias="S3_USER_POLICY_NAME", default="user-policy")


class ScriptsSettings(BaseSettings):
    """Root settings aggregating all sub-settings."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    db: DatabaseSettings = DatabaseSettings()
    s3: S3Settings = S3Settings()
    catalog: CatalogSettings = CatalogSettings()
    grist: GristSettings = GristSettings()
    http: HttpSettings = HttpSettings()
    load_dataset: LoadDatasetSettings = LoadDatasetSettings()
    generate_tables: GenerateTablesSettings = GenerateTablesSettings()
    s3_user: S3UserSettings = S3UserSettings()


def get_settings() -> ScriptsSettings:
    """Return a cached instance of the global settings."""
    return ScriptsSettings()
