import logging

import pandas as pd
from airflow.sdk import task
from modules.constants import (
    DEFAULT_S3_CONN_ID,
)
from modules.enums.filesystem import FileHandlerType
from modules.infra.file_system.factory import FSConfig, create_file_handler


@task()
def delete_tmp_keys(connection_id: str = DEFAULT_S3_CONN_ID) -> None:
    """List all keys in the S3 bucket and return them as a DataFrame."""
    # Hook
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(connection_id=connection_id),
    )

    # List objects
    list_objects = s3_handler.list_files(directory="", pattern="*_tmp*")
    print(len(list_objects))
    print(list_objects[:5])

    # Delete objects
    for key in list_objects:
        s3_handler.delete_single(file_path=key)


@task()
def delete_keys_with_date(connection_id: str = DEFAULT_S3_CONN_ID) -> None:
    """List all keys in the S3 bucket and return them as a DataFrame."""
    # Hook
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(connection_id=connection_id),
    )

    # List objects
    list_objects = s3_handler.list_files(directory="", pattern="\\d{8}")
    print(len(list_objects))
    print(list_objects[:5])

    # Convert to DataFrame
    df = pd.DataFrame(data=list_objects, columns=["s3_keys"])  # type: ignore
    print(df.head())


@task()
def delete_airflow_keys(connection_id: str = DEFAULT_S3_CONN_ID) -> None:
    """List all keys in the S3 bucket and return them as a DataFrame."""
    # Hook
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        config=FSConfig(connection_id=connection_id),
    )

    # List objects
    list_objects = s3_handler.list_files(directory="", pattern="*_tmp*")

    # Convert to DataFrame
    df = pd.DataFrame(data=list_objects, columns=["s3_keys"])  # type: ignore
    print(df.head())


@task()
def remove_airflow_db_logs() -> None:
    import subprocess
    from datetime import datetime, timedelta

    days_to_keep = 60
    date_to_clean_before = datetime.now() - timedelta(days=days_to_keep)

    command = [f"airflow db clean --clean-before-timestamp '{date_to_clean_before}' --verbose"]

    logging.info(msg=command)

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    # Check for errors
    if result.returncode != 0:
        raise ValueError(f"Error occurred: {result.stderr}")
    else:
        logging.info(msg="Command executed successfully. Metadatadb has been cleared")


@task(task_id="clean_skipped_logs")
def clean_skipped_logs() -> None:
    pass
