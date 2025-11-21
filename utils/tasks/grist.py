from datetime import timedelta
from airflow.decorators import task
from airflow.models import Variable

from infra.file_handling.factory import create_file_handler
from infra.http_client.adapters import RequestsClient
from infra.http_client.config import ClientConfig
from infra.grist.client import GristAPI
from utils.config.dag_params import get_project_name
from utils.config.tasks import get_selecteur_config

from utils.config.types import FileHandlerType
from utils.config.vars import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID, PROXY, AGENT


@task(task_id="download_grist_doc_to_s3", retries=1, retry_delay=timedelta(seconds=20))
def download_grist_doc_to_s3(
    selecteur: str,
    workspace_id: str,
    doc_id_key: str,
    grist_host: str = "https://grist.numerique.gouv.fr",
    api_token_key: str = "grist_secret_key",
    use_proxy: bool = True,
    **context,
) -> None:
    """Download SQLite from a specific Grist doc to S3"""
    nom_projet = get_project_name(context=context)

    selecteur_config = get_selecteur_config(nom_projet=nom_projet, selecteur=selecteur)

    # Instanciate Grist client
    if use_proxy:
        http_config = ClientConfig(proxy=PROXY, user_agent=AGENT)
        request_client = RequestsClient(config=http_config)
    else:
        http_config = ClientConfig()
        request_client = RequestsClient(config=http_config)

    grist_client = GristAPI(
        http_client=request_client,
        base_url=grist_host,
        workspace_id=workspace_id,
        doc_id=Variable.get(doc_id_key),
        api_token=Variable.get(api_token_key),
    )

    # Hooks
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        connection_id=DEFAULT_S3_CONN_ID,
        bucket=DEFAULT_S3_BUCKET,
    )

    # Get document data from Grist
    grist_response = grist_client.get_doc_sqlite_file()

    # Export sqlite file to S3
    print(f"Exporting file to < {selecteur_config.filepath_tmp_s3} >")
    s3_handler.write(
        file_path=selecteur_config.filepath_tmp_s3,
        content=grist_response,
    )
    print("✅ Export done!")
