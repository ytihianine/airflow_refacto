"""MinIO/S3 task utilities using infrastructure handlers."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
import pytz

from airflow.decorators import task

from infra.file_handling.exceptions import FileHandlerError, FileNotFoundError
from infra.file_handling.factory import create_file_handler
from utils.config.dag_params import _get_project_name
from utils.config.tasks import get_projet_config
from utils.config.vars import (
    DEFAULT_S3_CONN_ID,
)


@task
def copy_s3_files(
    bucket: str, connection_id: str = DEFAULT_S3_CONN_ID, **context: Dict[str, Any]
) -> None:
    """Copy files to S3 storage.

    Args:
        bucket: Target S3 bucket
        source_key: Source key pattern
        dest_key: Destination key pattern
        connection_id: S3 connection ID
        context: Airflow context

    Raises:
        ValueError: If project name not provided in params
        FileHandlerError: If file operations fail
    """
    nom_projet = _get_project_name(context=context)
    s3_handler = create_file_handler(
        handler_type="s3", connection_id=connection_id, bucket=bucket
    )

    # Get timing information
    execution_date = context.get("execution_date")
    if not execution_date or not isinstance(execution_date, datetime):
        raise ValueError("Invalid execution date in Airflow context")

    paris_tz = pytz.timezone("Europe/Paris")
    execution_date = execution_date.astimezone(paris_tz)
    curr_day = execution_date.strftime("%Y%m%d")
    curr_time = execution_date.strftime("%Hh%M")

    # Get storage configuration
    projet_config = get_projet_config(nom_projet=nom_projet)

    for config in projet_config:
        src_key = config.filepath_tmp_s3
        dst_key = config.s3_key
        filename = config.filename

        if not src_key or not dst_key:
            continue

        if filename is None or filename == "":
            logging.info(
                f"Config filename is empty for project <{nom_projet}> and selecteur <{config.selecteur}>. \n Skipping s3 copy ..."
            )
        else:
            # Build destination path
            target_key = f"{dst_key}/{curr_day}/{curr_time}/{filename}"

            try:
                # Copy file
                logging.info(f"Copying {src_key} to {target_key}")
                s3_handler.copy(source=src_key, destination=target_key)
                logging.info("Copy successful")

            except (FileHandlerError, FileNotFoundError) as e:
                logging.error(f"Failed to copy {src_key} to {target_key}: {str(e)}")
                raise FileHandlerError(f"Failed to copy file: {e}") from e
            except Exception as e:
                logging.error(
                    f"Unexpected error copying {src_key} to {target_key}: {str(e)}"
                )
                raise


@task
def del_s3_files(
    bucket: str,
    keys_to_delete: Optional[List[str]] = None,
    connection_id: str = DEFAULT_S3_CONN_ID,
    **context: Dict[str, Any],
) -> None:
    """Delete files from MinIO/S3 storage.

    Args:
        bucket: Target S3/MinIO bucket
        keys_to_delete: Optional list of keys to delete
        connection_id: S3 connection ID
        context: Airflow context

    Raises:
        ValueError: If project name not provided in params
        FileHandlerError: If file operations fail
    """
    tmp_keys: List[str] = []
    source_keys: List[str] = []

    # Initialize S3 handler
    s3_handler = create_file_handler(
        handler_type="s3", connection_id=connection_id, bucket=bucket
    )

    if not keys_to_delete:
        # Get project name from context
        nom_projet = _get_project_name(context=context)

        # Get storage configuration
        projet_config = get_projet_config(nom_projet=nom_projet)

        # Extract temporary and source keys
        tmp_keys = [
            config.filepath_tmp_s3 for config in projet_config if config.filepath_tmp_s3
        ]

        source_keys = [
            config.filepath_source_s3
            for config in projet_config
            if config.filepath_source_s3
        ]
    else:
        tmp_keys = [str(key) for key in keys_to_delete if key and str(key).strip()]

    # Delete temporary files
    if tmp_keys:
        try:
            logging.info(f"Deleting {len(tmp_keys)} temporary files")
            for key in tmp_keys:
                s3_handler.delete(key)
            logging.info("Temporary files deleted successfully")
        except FileHandlerError as e:
            logging.error(f"Failed to delete temporary files: {str(e)}")
            raise

    # Delete source files
    if source_keys:
        try:
            logging.info(f"Deleting {len(source_keys)} source files")
            for key in source_keys:
                s3_handler.delete(key)
            logging.info("Source files deleted successfully")
        except FileHandlerError as e:
            logging.error(f"Failed to delete source files: {str(e)}")
            raise
