from io import BytesIO
from airflow.decorators import task
from airflow.models import Variable

from utils.file_handler import MinioFileHandler
from utils.api_client.adapters import RequestsAPIClient
from utils.common.vars import get_root_folder, paris_tz

from dags.applications.sauvegarde.process import (
    convert_str_to_ascii_str,
    get_pod_name,
    execute_command_in_pod,
    copy_file_from_pod,
)

base_key = "sg/application/sauvegarde/chartsgouv"
CA_PATH = "/files/bercyCA.crt"


@task(task_id="get_access_token")
def bearer_token() -> str:
    # Variables
    http_client = RequestsAPIClient()
    base_url = Variable.get("chartsgouv_base_url")
    username = Variable.get("chartsgouv_admin_username")
    password = Variable.get("chartsgouv_admin_password")

    endpoint_login = "/api/v1/security/login"
    data = {
        "username": username,
        "password": password,
        "provider": "db",
    }
    response_login = http_client.post(
        f"{base_url + endpoint_login}",
        json=data,
        verify=f"{get_root_folder() + CA_PATH}",
    )

    if response_login.status_code == 200:
        access_token = response_login.json()["access_token"]
        return access_token

    raise ValueError(
        f"La requête n'est pas passée. Statut code: {response_login.status_code}"
    )


@task(task_id="get_dashboard_ids_and_titles")
def get_dashboard_ids_and_titles(**context) -> list[int]:
    # Variables
    http_client = RequestsAPIClient()
    base_url = Variable.get("chartsgouv_base_url")

    access_token = context["ti"].xcom_pull(
        key="return_value", task_ids="get_access_token"
    )
    endpoint_dashboards_ids = "/api/v1/dashboard/"
    headers = {"Authorization": f"Bearer {access_token}"}
    response_id_dashboards = http_client.get(
        f"{base_url}{endpoint_dashboards_ids}",
        headers=headers,
        verify=f"{get_root_folder() + CA_PATH}",
    )

    if response_id_dashboards.status_code == 200:
        dashboards_id_title = []
        for dashboard_info in response_id_dashboards.json()["result"]:
            dashboards_id_title.append(
                {
                    "id": dashboard_info["id"],
                    "title": dashboard_info["dashboard_title"],
                }
            )
        return dashboards_id_title

    raise ValueError(
        f"La requête n'est pas passée. Statut code: {response_id_dashboards.status_code}"
    )


@task(task_id="export_dashboard", multiple_outputs=True)
def get_dashboard_export(
    dashboard_id_title: dict[str, str], minio_hook, **context
) -> dict[str, any]:
    # Variables
    http_client = RequestsAPIClient()
    bucket = "dsci"
    base_url = Variable.get("chartsgouv_base_url")

    # hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket=bucket)

    # Date d'execution du dag
    execution_date = context["dag_run"].execution_date.astimezone(paris_tz)
    curr_day = execution_date.strftime("%Y%m%d")
    curr_time = execution_date.strftime("%Hh%M")

    access_token = context["ti"].xcom_pull(
        key="return_value", task_ids="get_access_token"
    )
    endpoint_dashboard_export = "/api/v1/dashboard/export/?q="
    headers = {"Authorization": f"Bearer {access_token}"}
    url_export = f"{base_url}{endpoint_dashboard_export}[{dashboard_id_title['id']}]"

    response_dashboard_export = http_client.get(
        url_export, headers=headers, verify=f"{get_root_folder() + CA_PATH}"
    )

    if response_dashboard_export.status_code == 200:
        # 2. Créer un objet BytesIO pour contenir les données du fichier ZIP
        zip_data = BytesIO(response_dashboard_export.content)

        content_disposition = response_dashboard_export.headers.get(
            "Content-Disposition"
        )

        filename = f"default_name_{execution_date.strftime('%Y%m%dT%H%M%S')}.zip"
        if content_disposition:
            # Si l'en-tête est présent, extraire le nom du fichier
            filename = content_disposition.split("filename=")[-1].strip('"')

        dashboard_title = convert_str_to_ascii_str(dashboard_id_title["title"])
        s3_hook.load_file_obj(
            file_obj=zip_data,
            bucket_name=bucket,
            key=f"{base_key}/{curr_day}/{curr_time}/{dashboard_title}/{filename}",
        )
    else:
        raise ValueError(
            f"La requête n'est pas passée. Statut code: {response_dashboard_export.status_code}"
        )


@task
def export_user_roles(**context) -> None:
    # Variables
    NAMESPACE = "projet-dsci"  # Change to your namespace
    # pour le trouver: kubectl describe pods your-pod
    # et chercher dans Labels/app=your_value
    POD_LABEL = "app=superset"
    POD_FILEPATH = "/tmp/roles.json"
    LOCAL_FILEPATH = "./tmp/roles.json"
    bucket = "dsci"

    # hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket=bucket)

    pod_name = get_pod_name(namespace=NAMESPACE, pod_label=POD_LABEL)

    sh_command = f"flask fab export-roles --path {POD_FILEPATH}"
    status_command = execute_command_in_pod(
        namespace=NAMESPACE, pod_name=pod_name, sh_command=sh_command
    )

    if status_command:
        copy_file_from_pod(
            namespace=NAMESPACE,
            pod_name=pod_name,
            pod_filepath=POD_FILEPATH,
            local_filepath=LOCAL_FILEPATH,
        )

        # Date d'execution du dag
        execution_date = context["dag_run"].execution_date.astimezone(paris_tz)
        curr_day = execution_date.strftime("%Y%m%d")
        curr_time = execution_date.strftime("%Hh%M")

        s3_hook.load_file(
            filename=LOCAL_FILEPATH,
            bucket_name=bucket,
            key=f"{base_key}/{curr_day}/{curr_time}/user-roles.json",
            replace=True,
        )
