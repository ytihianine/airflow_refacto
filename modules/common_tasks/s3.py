"""MinIO/S3 task utilities using infrastructure handlers."""

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from airflow.sdk import get_current_context, task

from modules.constants import (
    DEFAULT_POLARIS_CATALOG,
    DEFAULT_POLARIS_HOST,
    DEFAULT_S3_CONN_ID,
)
from modules.enums.dags import FeatureFlags, TypeSource
from modules.enums.filesystem import FileHandlerType, IcebergTableStatus
from modules.infra.catalog.iceberg import IcebergCatalog, generate_catalog_properties
from modules.infra.file_system.dataframe import read_dataframe
from modules.infra.file_system.exceptions import FileHandlerError
from modules.infra.file_system.factory import FSConfig, create_file_handler
from modules.types.projet import SelecteurConfig, SelecteurStorageOptions
from modules.utils.config.dag_params import (
    get_execution_date,
    get_project_name,
    should_skip_task,
)
from modules.utils.config.tasks import (
    get_list_selecteur_storage_info,
    get_projet_s3_info,
    merge_selecteur_config,
)


@task
def copy_s3_files(
    storage_options: Mapping[str, SelecteurStorageOptions] = {},
    connection_id: str = DEFAULT_S3_CONN_ID,
    **context: Mapping[str, Any],
) -> None:
    """Copy files from one place to another in S3 storage.

    Args:
        storage_options: Mapping of selecteur options
        connection_id: S3 connection ID
        context: Airflow context

    Raises:
        ValueError: If project name not provided in params
        FileHandlerError: If file operations fail
    """
    # Récupérer les info du dag
    if should_skip_task(context=context, feature_flag=FeatureFlags.S3):
        return

    nom_projet = get_project_name(context=context)
    execution_date = get_execution_date(context=context, use_tz=False)
    curr_day = execution_date.strftime(format="%Y%m%d")
    curr_time = execution_date.strftime(format="%Hh%M")

    # Créer les hooks
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(connection_id=connection_id),
    )

    # Get selecteur config
    storage_info = get_list_selecteur_storage_info(nom_projet=nom_projet)
    selecteur_config = merge_selecteur_config(storage_info=storage_info, storage_options=storage_options)

    # Copier la liste des sources dans le dossier final
    for config in selecteur_config:
        if not config.should_write_to_s3():
            logging.info(msg=f"Skipping S3 copy for selecteur <{config.storage_info.selecteur}>")
            continue

        logging.info(
            msg=f"Processing copy to S3 for selecteur <{config.storage_info.selecteur}> "
            f"with type source <{config.storage_info.type_source}> ..."
        )

        target_key = f"{config.storage_info.s3_key}/{curr_day}/{curr_time}/{config.storage_info.filename}"
        try:
            # Copy tmp file if exists
            key = config.storage_info.get_full_s3_key(with_tmp_segment=True, use_id_source=False)
            logging.info(msg=f"Copying {key} to {target_key}")
            s3_handler.copy(source=key, destination=target_key)
            logging.info(msg="Copy successful")

        except Exception as e:
            logging.error(msg=f"Unexpected error copying tmp file to {target_key}: {e!s}")
            raise


@task
def del_s3_files(
    storage_options: Mapping[str, SelecteurStorageOptions] = {},
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    **context: Mapping[str, Any],
) -> None:
    """Delete files from MinIO/S3 storage.

    Args:
        storage_options: Mapping of selecteur options
        s3_conn_id: S3 connection ID
        context: Airflow context

    Raises:
        ValueError: If project name not provided in params
        FileHandlerError: If file operations fail
    """
    # Récupérer les info du dag
    if should_skip_task(context=context, feature_flag=FeatureFlags.S3):
        return

    nom_projet = get_project_name(context=context)

    # Créer les hooks
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(connection_id=s3_conn_id),
    )

    # Get selecteur config
    storage_info = get_list_selecteur_storage_info(nom_projet=nom_projet)
    selecteur_config = merge_selecteur_config(storage_info=storage_info, storage_options=storage_options)

    for config in selecteur_config:
        logging.info(msg=f"{config}")
        if config.storage_info.type_source != TypeSource.FILE:
            continue

        s3_key_source = config.storage_info.get_full_s3_key(use_id_source=True)
        try:
            logging.info(msg=f"Deleting source file {s3_key_source}")
            s3_handler.delete_single(file_path=s3_key_source)
            logging.info(msg="Source file deleted successfully")
        except FileHandlerError as e:
            logging.error(msg=f"Failed to delete source file {s3_key_source}: {e!s}")
            raise

        if config.storage_options.write_to_s3 is True:
            s3_key = config.storage_info.get_full_s3_key(with_tmp_segment=True)
            try:
                logging.info(msg=f"Deleting {s3_key} source files")
                s3_handler.delete_single(file_path=s3_key)
                logging.info(msg="Source files deleted successfully")
            except FileHandlerError as e:
                logging.error(msg=f"Failed to delete source files: {e!s}")
                raise


@task
def del_iceberg_staging_table(
    nom_projet: str | None = None,
    catalog_name: str = DEFAULT_POLARIS_CATALOG,
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    **context,
) -> None:
    """Delete Iceberg staging table."""
    if nom_projet is None:
        nom_projet = get_project_name(context=context)

    # Get project s3
    s3_key = get_projet_s3_info(nom_projet=nom_projet).key

    # Get catalog
    properties = generate_catalog_properties(
        uri=DEFAULT_POLARIS_HOST,
    )
    catalog = IcebergCatalog(name=catalog_name, properties=properties)

    # Get all tables
    tables = catalog.list_tables(namespace=s3_key.replace("/", "."), pattern="*_staging*")

    # Drop staging tables from Iceberg catalog
    for table in tables:
        logging.info(msg=f"Dropping staging table {table} ...")
        catalog.drop_table(table_name=".".join(table), purge=False)
        logging.info(msg=f"Staging table {table} dropped successfully !")

    # Delete staging files from S3
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(connection_id=s3_conn_id),
    )
    staging_keys = s3_handler.list_files(directory=catalog_name + "/" + s3_key, pattern="*_staging*")
    for key in staging_keys:
        logging.info(msg=f"Deleting staging file {key} ...")
        s3_handler.delete_single(file_path=key)
        logging.info(msg=f"Staging file {key} deleted successfully !")


@task(map_index_template="{{ task_name }}")
def copy_staging_to_prod(
    selecteur_config: Mapping[str, Any],
    catalog_uri: str = DEFAULT_POLARIS_HOST,
    catalog_name: str = DEFAULT_POLARIS_CATALOG,
) -> None:
    """Copy Iceberg tables from staging key to prod key"""
    # Init selecteur_config to SelecteurConfig if it's a dict
    config = SelecteurConfig.from_dict(data=selecteur_config)

    context = get_current_context()
    context["task_name"] = config.storage_info.selecteur  # type: ignore

    if not config.should_write_to_iceberg():
        logging.info(msg=f"Skipping Iceberg write for selecteur <{config.storage_info.selecteur}>")
        return

    # Dag info
    namespace = config.storage_info.get_iceberg_namespace(with_bucket=False)
    tbl_name = Path(config.storage_info.filename).stem

    # Get catalog
    properties = generate_catalog_properties(
        uri=catalog_uri,
    )
    catalog = IcebergCatalog(name=catalog_name, properties=properties)

    # Read staging table
    df = catalog.read_table_as_df(table_name=namespace + "." + tbl_name + "_staging")

    # Write prod table
    catalog.write_table_and_namespace(
        df=df,
        table_status=IcebergTableStatus.PROD,
        namespace=namespace,
        table_name=tbl_name,
    )
    catalog.drop_table(table_name=namespace + "." + tbl_name + "_staging", purge=True)


@task(map_index_template="{{ task_name }}")
def import_file_to_iceberg(
    selecteur_config: Mapping[str, Any],
    s3_conn_id: str = DEFAULT_S3_CONN_ID,
    catalog_uri: str = DEFAULT_POLARIS_HOST,
    catalog_name: str = DEFAULT_POLARIS_CATALOG,
) -> None:
    """Copy Iceberg tables from staging key to prod key"""
    # Init selecteur_config to SelecteurConfig if it's a dict
    config = SelecteurConfig.from_dict(data=selecteur_config)

    context = get_current_context()
    context["task_name"] = config.storage_info.selecteur  # type: ignore

    if not config.should_write_to_iceberg():
        logging.info(msg=f"Skipping Iceberg write for selecteur <{config.storage_info.selecteur}>")
        return

    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(connection_id=s3_conn_id),
    )
    properties = generate_catalog_properties(uri=catalog_uri)
    catalog = IcebergCatalog(name=catalog_name, properties=properties)

    # Dag info
    namespace = config.storage_info.get_iceberg_namespace(with_bucket=False)
    tbl_name = Path(config.storage_info.filename).stem

    # Read tmp data
    df = read_dataframe(
        file_handler=s3_handler,
        file_path=config.storage_info.get_full_s3_key(with_tmp_segment=True),
    )

    # Write prod table
    catalog.write_table_and_namespace(
        df=df,
        table_status=IcebergTableStatus.STAGING,
        namespace=namespace,
        table_name=tbl_name,
    )
