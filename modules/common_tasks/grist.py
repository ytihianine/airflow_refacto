import logging
from collections.abc import Callable
from datetime import timedelta

import pandas as pd
from airflow.sdk import Variable, task

from modules.constants import (
    AGENT,
    DEFAULT_GRIST_HOST,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_CONN_ID,
    PROXY,
)
from modules.enums.dags import FeatureFlags
from modules.enums.filesystem import FileHandlerType
from modules.infra.file_system.factory import create_file_handler
from modules.infra.grist.client import GristClient
from modules.infra.http_client.adapters import RequestsClient
from modules.infra.http_client.config import ClientConfig
from modules.utils.config.dag_params import get_project_name, should_skip_task
from modules.utils.config.tasks import get_selecteur_storage_info
from modules.utils.process.dates import convert_grist_date_to_date
from modules.utils.process.structures import (
    handle_grist_boolean_columns,
    handle_grist_null_references,
    normalize_grist_dataframe,
)
from modules.utils.process.text import normalize_whitespace_columns


@task(
    task_id="download_grist_doc_to_s3",
    retries=5,
    retry_delay=timedelta(minutes=1),
    retry_exponential_backoff=True,
)
def download_grist_doc_to_s3(
    selecteur: str,
    workspace_id: str,
    grist_host: str = DEFAULT_GRIST_HOST,
    api_token_key: str = "grist_secret_key",
    use_proxy: bool = True,
    **context,
) -> None:
    """Download SQLite from a specific Grist doc to S3"""
    nom_projet = get_project_name(context=context)

    if should_skip_task(context=context, feature_flag=FeatureFlags.DOWNLOAD_GRIST_DOC):
        return

    selecteur_config = get_selecteur_storage_info(nom_projet=nom_projet, selecteur=selecteur)
    doc_id = selecteur_config.id_source
    dest_tmp_key = selecteur_config.get_full_s3_key(with_tmp_segment=True, use_id_source=False)

    if doc_id is None:
        raise ValueError(
            f"doc_id is None for selecteur {selecteur} in project {nom_projet}. Please check the configuration."
        )

    # Instanciate Grist client
    http_config = ClientConfig()
    if use_proxy:
        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)

    request_client = RequestsClient(config=http_config)

    grist_client = GristClient(
        http_client=request_client,
        grist_host=grist_host,
        api_token=Variable.get(key=api_token_key),
    )

    # Hooks
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        connection_id=DEFAULT_S3_CONN_ID,
        bucket=DEFAULT_S3_BUCKET,
    )

    # Get document data from Grist
    grist_response = grist_client.download_doc(doc_id=doc_id)

    # Export sqlite file to S3
    print(f"Exporting file to < {dest_tmp_key} >")
    s3_handler.write(
        file_path=dest_tmp_key,
        content=grist_response.content,
    )
    print("✅ Export done!")


def generic_grist_processing(
    *,
    df: pd.DataFrame,
    cols_mapping: dict[str, str] | None = None,
    cols_to_keep: list[str] | None = None,
    txt_columns: list[str] | None = None,
    ref_columns: list[str] | None = None,
    date_columns: list[str] | None = None,
    bool_columns: list[str] | None = None,
    num_columns: list[str] | None = None,
    custom_fn: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """
    Generic processing for Grist dataframes.
    """
    # Normalize source
    logging.info(msg="Normalizing Grist dataframe")
    df = normalize_grist_dataframe(df=df)

    # Keep only mandatory columns
    if cols_to_keep:
        logging.info(msg=f"Keeping only mandatory columns: {cols_to_keep}")
        df = df.loc[:, cols_to_keep]
    else:
        logging.info(msg="No mandatory columns provided. Using all available columns in the dataframe.")

    # Rename columns
    if cols_mapping:
        logging.info(msg=f"Renaming columns: {cols_mapping}")
        df = df.rename(columns=cols_mapping)
    else:
        logging.info(msg="No column renaming mapping provided. Skipping ...")

    # Normalizing text columns
    if txt_columns:
        logging.info(f"Normalizing text columns to string: {txt_columns}")
        df = normalize_whitespace_columns(df=df, columns=txt_columns)
    else:
        logging.info("No text columns provided. Skipping ...")

    # Convert numeric columns to float
    if num_columns:
        logging.info(msg=f"Converting numeric columns to float: {num_columns}")
        for col in num_columns:
            df[col] = pd.to_numeric(arg=df[col], errors="coerce")
    else:
        logging.info(msg="No numeric columns provided. Skipping ...")

    # Convert date columns to datetime
    if date_columns:
        logging.info(msg=f"Converting date columns to datetime: {date_columns}")
        df = convert_grist_date_to_date(df=df, columns=date_columns)
    else:
        logging.info(msg="No date columns provided. Skipping ...")

    # Convert boolean columns to boolean
    if bool_columns:
        logging.info(msg=f"Converting boolean columns to boolean: {bool_columns}")
        df = handle_grist_boolean_columns(df=df, columns=bool_columns)
    else:
        logging.info(msg="No boolean columns provided. Skipping ...")

    # Convert reference columns to string
    if ref_columns:
        logging.info(msg=f"Converting reference columns to string: {ref_columns}")
        df = handle_grist_null_references(df=df, columns=ref_columns)
    else:
        logging.info(msg="No reference columns provided. Skipping ...")

    if custom_fn:
        logging.info(msg=f"Applying custom processing function: {custom_fn.__name__}")
        df = custom_fn(df)
    else:
        logging.info(msg="No custom processing function provided.")

    return df
