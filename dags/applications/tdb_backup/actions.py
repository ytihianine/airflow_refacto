from io import BytesIO
from datetime import datetime
from typing import Any
from airflow.models import Variable

from dags.applications.tdb_backup.process import convert_str_to_ascii_str
from infra.file_handling.factory import create_file_handler
from infra.http_client.config import ClientConfig
from infra.http_client.adapters import RequestsClient
from utils.config.tasks import get_selecteur_config
from utils.config.types import FileHandlerType
from utils.config.vars import (
    get_root_folder,
    AGENT,
    PROXY,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_CONN_ID,
)


CA_PATH = "/files/bercyCA.crt"

http_client_config = ClientConfig(
    timeout=30, verify_ssl=False, user_agent=AGENT, proxy=PROXY
)


def get_bearer_token() -> str:
    # Variables
    http_client = RequestsClient(http_client_config)
    base_url = Variable.get("chartsgouv_base_url")
    username = Variable.get("chartsgouv_admin_username")
    password = Variable.get("chartsgouv_admin_password")

    endpoint_login = "/api/v1/security/login"
    data = {
        "username": username,
        "password": password,
        "provider": "db",
    }
    try:
        response_login = http_client.post(
            f"{base_url + endpoint_login}",
            json=data,
            verify=f"{get_root_folder() + CA_PATH}",
        )
        access_token = response_login.json()
        return access_token["access_token"]
    except ValueError as e:
        raise ValueError(f"Failed to get bearer token: {e}")


def get_dashboard_ids_and_titles(access_token: str) -> list[dict[str, Any]]:
    # Variables
    http_client = RequestsClient(http_client_config)
    base_url = Variable.get("chartsgouv_base_url")

    access_token = get_bearer_token()
    endpoint_dashboards_ids = "/api/v1/dashboard/"
    headers = {"Authorization": f"Bearer {access_token}"}
    response_id_dashboards = http_client.get(
        f"{base_url}{endpoint_dashboards_ids}",
        headers=headers,
        verify=f"{get_root_folder() + CA_PATH}",
    )

    dashboards_id_title = []
    for dashboard_info in response_id_dashboards.json()["result"]:
        dashboards_id_title.append(
            {
                "id": dashboard_info["id"],
                "title": dashboard_info["dashboard_title"],
            }
        )
    return dashboards_id_title


def get_dashboard_export(
    access_token: str, dashboards_info: list[dict[str, str]]
) -> None:
    # config
    config = get_selecteur_config(nom_projet="", selecteur="")

    # Variables
    http_client = RequestsClient(http_client_config)
    base_url = Variable.get("chartsgouv_base_url")

    # hooks
    s3_handler = create_file_handler(
        handler_type=FileHandlerType.S3,
        connection_id=DEFAULT_S3_CONN_ID,
        bucket=DEFAULT_S3_BUCKET,
    )

    access_token = get_bearer_token()

    endpoint_dashboard_export = "/api/v1/dashboard/export/?q="
    headers = {"Authorization": f"Bearer {access_token}"}

    execution_date = datetime.now()
    curr_day = execution_date.strftime("%Y%m%d")
    curr_time = execution_date.strftime("%Hh%M")

    for dashboard in dashboards_info:
        url_export = f"{base_url}{endpoint_dashboard_export}[{dashboard['id']}]"

        response_dashboard_export = http_client.get(
            url_export, headers=headers, verify=f"{get_root_folder() + CA_PATH}"
        )

        # 2. Créer un objet BytesIO pour contenir les données du fichier ZIP
        zip_data = BytesIO(response_dashboard_export.content)

        content_disposition = response_dashboard_export.headers.get(
            "Content-Disposition"
        )

        filename = f"default_name_{dashboard['id']}.zip"
        if content_disposition:
            # Si l'en-tête est présent, extraire le nom du fichier
            filename = content_disposition.split("filename=")[-1].strip('"')

        dashboard_title = convert_str_to_ascii_str(dashboard["title"])
        s3_handler.write(
            content=zip_data,
            file_path=f"{config.s3_key}/{curr_day}/{curr_time}/{dashboard_title}/{filename}",
        )


# def export_user_roles(**context) -> None:
#     # Variables
#     NAMESPACE = "projet-dsci"  # Change to your namespace
#     # pour le trouver: kubectl describe pods your-pod
#     # et chercher dans Labels/app=your_value
#     POD_LABEL = "app=superset"
#     POD_FILEPATH = "/tmp/roles.json"
#     LOCAL_FILEPATH = "./tmp/roles.json"
#     bucket = "dsci"

#     # hooks
# s3_handler = create_file_handler(
#     handler_type="s3", connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
# )

#     pod_name = get_pod_name(namespace=NAMESPACE, pod_label=POD_LABEL)

#     sh_command = f"flask fab export-roles --path {POD_FILEPATH}"
#     status_command = execute_command_in_pod(
#         namespace=NAMESPACE, pod_name=pod_name, sh_command=sh_command
#     )

#     if status_command:
#         copy_file_from_pod(
#             namespace=NAMESPACE,
#             pod_name=pod_name,
#             pod_filepath=POD_FILEPATH,
#             local_filepath=LOCAL_FILEPATH,
#         )

#         # Date d'execution du dag
#         execution_date = context["dag_run"].execution_date.astimezone(paris_tz)
#         curr_day = execution_date.strftime("%Y%m%d")
#         curr_time = execution_date.strftime("%Hh%M")

#         s3_handler.load_file(
#             filename=LOCAL_FILEPATH,
#             bucket_name=bucket,
#             key=f"{base_key}/{curr_day}/{curr_time}/user-roles.json",
#             replace=True,
#         )
