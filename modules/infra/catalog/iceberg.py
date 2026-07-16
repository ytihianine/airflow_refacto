import logging
import ssl
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
import pyarrow as pa
from airflow.sdk import Variable
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier
from tenacity import retry, stop_after_attempt, wait_fixed

from modules.constants import (
    DEFAULT_POLARIS_CATALOG,
    DEFAULT_POLARIS_CLIENT_ID,
    DEFAULT_POLARIS_CLIENT_SECRET,
)
from modules.enums.filesystem import IcebergTableStatus


def generate_catalog_properties(
    uri: str,
    warehouse: str = DEFAULT_POLARIS_CATALOG,
    client_id: str | None = DEFAULT_POLARIS_CLIENT_ID,
    client_secret: str | None = DEFAULT_POLARIS_CLIENT_SECRET,
    ca_bundle_path: str | None = None,
    options: dict[str, Any] | None = None,
) -> Mapping[str, Any]:
    if client_id is None:
        client_id = Variable.get(key="iceberg_client_id")

    if client_secret is None:
        client_secret = Variable.get(key="iceberg_client_secret")

    normalized_uri = uri.rstrip("/")
    if not normalized_uri.endswith("/api/catalog"):
        normalized_uri = f"{normalized_uri}/api/catalog"

    properties = {
        "uri": normalized_uri,
        "warehouse": warehouse,
        "credential": f"{client_id}:{client_secret}",
        "scope": "PRINCIPAL_ROLE:ALL",
        "header.X-Iceberg-Access-Delegation": None,
    }

    if ca_bundle_path:
        properties["ssl"] = {"cabundle": ca_bundle_path}
    else:
        # Use OS CA store — proven to work (trusts your self-signed CA)
        os_ca = ssl.get_default_verify_paths().cafile
        if os_ca:
            properties["ssl"] = {"cabundle": os_ca}

    if options:
        # Example additional options
        # options = {
        # "ssl": {"cabundle": ENV_VAR["SSL_CERT_FILE"]},
        # "s3.endpoint": "https://your-s3-endpoint",  # if not AWS
        # "s3.access-key-id": your_access_key,
        # "s3.secret-access-key": your_secret_key,
        # "s3.region": "your-region",
        # "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
        # }
        properties = properties | options

    return properties


@dataclass
class IcebergCatalog:
    name: str
    properties: Mapping[str, Any]
    catalog: Catalog = field(init=False)

    def __post_init__(self) -> None:
        self.catalog = self._load_catalog()

    def _load_catalog(self) -> Catalog:
        # Logic to load the Iceberg catalog using the provided properties
        logging.info(msg=f"Loading Iceberg catalog {self.name}...")
        catalog = load_catalog(name=self.name, **self.properties)
        logging.info(msg="Iceberg catalog loaded !")
        return catalog

    def _get_schema_from_dataframe(self, df: pd.DataFrame) -> pa.Schema:
        # Convertir le DataFrame en schéma PyArrow
        # Pour les DataFrames vides, on doit inférer le schéma à partir des dtypes pandas
        # car pa.Schema.from_pandas(df) retourne des types "null" pour les colonnes vides
        if df.empty:
            fields = []
            for col in df.columns:
                dtype = df[col].dtype
                # Mapper les dtypes pandas vers PyArrow
                if pd.api.types.is_integer_dtype(dtype):
                    pa_type = pa.int64()
                elif pd.api.types.is_float_dtype(dtype):
                    pa_type = pa.float64()
                elif pd.api.types.is_bool_dtype(dtype):
                    pa_type = pa.bool_()
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    pa_type = pa.timestamp("us")
                else:
                    # Default to string for object/unknown types
                    pa_type = pa.string()
                fields.append(pa.field(col, pa_type))
            return pa.schema(fields)
        return pa.Schema.from_pandas(df)

    def create_namespace(self, namespace: str) -> None:
        """Create namespace and all parent namespaces if they don't exist"""
        parts = namespace.split(sep=".")

        # Create each level of the namespace hierarchy
        for i in range(1, len(parts) + 1):
            parent_namespace = ".".join(parts[:i])
            logging.info(msg=f"Creating namespace: {parent_namespace}")
            self.catalog.create_namespace_if_not_exists(namespace=parent_namespace)

    def create_table(self, table_name: str, df: pd.DataFrame, location: str | None = None) -> Table:
        # Générer le schéma à partir de la structure du DataFrame
        logging.info(msg=f"Creating table with name: {table_name}")
        table = self.catalog.create_table_if_not_exists(
            identifier=table_name,
            schema=self._get_schema_from_dataframe(df=df),
            # location=location
        )
        return table

    def update_table(self, table_name: str, df: pd.DataFrame) -> Table:
        # Générer le schéma à partir de la structure du DataFrame
        logging.info(msg=f"Update table with name: {table_name}")
        table = self.catalog.load_table(identifier=table_name)
        new_schema = self._get_schema_from_dataframe(df=df)
        # Only union truly new columns to avoid ValidationError when an existing
        # column's type in the new data differs from the stored type (e.g. long vs double).
        existing_field_names = {f.name for f in table.schema().fields}
        new_fields = [f for f in new_schema if f.name not in existing_field_names]
        if new_fields:
            with table.update_schema() as update:
                update.union_by_name(pa.schema(new_fields))
        return table

    @retry(wait=wait_fixed(wait=2), stop=stop_after_attempt(max_attempt_number=5))
    def write_table(self, table_name: str, df: pd.DataFrame, overwrite: bool = False) -> None:
        self.create_table(table_name=table_name, df=df)
        table = self.update_table(table_name=table_name, df=df)

        logging.info(msg=f"Writing to table with name: {table_name}")
        pa_data = pa.Table.from_pandas(df, preserve_index=False)
        # Cast each column to the type stored in the table schema so that, for example,
        # an integer column (long) is written as double when the table expects double.
        table_arrow_schema = table.schema().as_arrow()
        cast_fields = [
            (table_arrow_schema.field(f.name) if f.name in table_arrow_schema.names else f) for f in pa_data.schema
        ]
        pa_data = pa_data.cast(pa.schema(cast_fields))

        # Logic to write data to a table in the Iceberg catalog
        if df.empty:
            logging.warning(msg=f"Empty DataFrame provided for table {table_name}. Skipping write.")
            return

        if overwrite:
            table.overwrite(df=pa_data)
        else:
            table.append(df=pa_data)

    def write_table_and_namespace(
        self,
        df: pd.DataFrame,
        table_status: IcebergTableStatus,
        namespace: str,
        table_name: str,
    ) -> None:
        # Create namespace
        self.create_namespace(namespace=namespace)

        # load data to table
        if table_status == IcebergTableStatus.STAGING:
            table_name = table_name + "_staging"

        table_name = namespace + "." + table_name
        self.write_table(table_name=table_name, df=df)

    def read_table(self, table_name: str) -> Table:
        # Logic to read data from a table in the Iceberg catalog
        logging.info(msg=f"Reading from table with name: {table_name}")
        table = self.catalog.load_table(identifier=table_name)
        return table

    def read_table_as_df(self, table_name: str) -> pd.DataFrame:
        # Logic to read data from a table in the Iceberg catalog
        logging.info(msg=f"Reading from table with name: {table_name}")
        table = self.catalog.load_table(identifier=table_name)
        df = table.scan().to_pandas()
        return df

    def drop_table(self, table_name: str, purge: bool = False) -> None:
        if purge:
            logging.info(msg=f"Purging table {table_name} - data will be removed")
            self.catalog.purge_table(identifier=table_name)
        else:
            logging.info(
                msg=f"Dropping table {table_name} - data will not be removed from s3. Set purge=True to delete data"
            )
            self.catalog.drop_table(identifier=table_name)

    def list_tables(self, namespace: str, pattern: str | None = None) -> list[Identifier]:
        # Logic to list all tables in the Iceberg catalog
        logging.info(msg=f"Listing tables in catalog {self.name}, namespace: {namespace} and pattern: {pattern}")
        tables = self.catalog.list_tables(namespace=namespace)
        if pattern:
            import fnmatch

            tables = [t for t in tables if fnmatch.fnmatch(name=t[-1], pat=pattern)]
        logging.info(msg=f"Found {len(tables)} tables.")
        return tables
