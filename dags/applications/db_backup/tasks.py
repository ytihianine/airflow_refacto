import gzip
import logging
import os
import subprocess
import tempfile

from airflow.sdk import chain, get_current_context, task, task_group
from modules.constants import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID
from modules.enums.filesystem import FileHandlerType
from modules.infra.database.factory import create_db_handler
from modules.infra.file_system.factory import create_file_handler
from modules.utils.config.dag_params import get_project_name
from modules.utils.config.tasks import get_projet_s3_info


@task_group
def export_database(db_conn_id: str) -> None:
    @task
    def list_databases(db_conn_id: str) -> list[str]:
        # Variables
        db_handler = create_db_handler(connection_id=db_conn_id)

        query = """
            select
                datname
            from
                pg_database
            where
                datistemplate is false
                and datname not in ('postgres', 'defaultdb')
            ;
        """
        result = db_handler.fetch_df(query=query)

        if result.empty:
            logging.warning(msg="Aucun nom de base de données n'a été récupéré.")
            return []

        return result["datname"].values.tolist()

    @task(map_index_template="{{ db_name }}")
    def export_database(db_conn_id: str, db_name: str, **context) -> None:
        context = get_current_context()
        context["db_name"] = db_name  # pyright: ignore[reportGeneralTypeIssues]

        # Variables
        nom_projet = get_project_name(context=context)
        projet_info = get_projet_s3_info(nom_projet=nom_projet)
        dest_tmp_key = projet_info.key_tmp + f"/{db_name}_dump.sql.gz"

        # Hooks
        db_handler = create_db_handler(connection_id=db_conn_id)
        s3_handler = create_file_handler(
            handler_type=FileHandlerType.S3,
            connection_id=DEFAULT_S3_CONN_ID,
            bucket=DEFAULT_S3_BUCKET,
        )
        conn = db_handler.connection
        logging.info(msg=f"{conn}")

        # Environment variable for password - to avoid password prompt
        env = os.environ.copy()
        env["PGPASSWORD"] = conn["password"]

        # Export database and load it to MinIO
        logging.info(msg=f"Executing dump for database: {db_name}")

        with tempfile.NamedTemporaryFile(suffix=".sql.gz") as tmp:
            with gzip.open(tmp.name, "wb", compresslevel=9) as gz:
                proc = subprocess.Popen(
                    [
                        "pg_dump",
                        "--host",
                        conn["host"],
                        "--port",
                        str(conn["port"]),
                        "--username",
                        conn["username"],
                        "--format=plain",
                        "--no-owner",
                        "--no-privileges",
                        db_name,
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                for chunk in iter(
                    lambda: proc.stdout.read(1024 * 1024), b""  # pyright: ignore[reportOptionalMemberAccess]
                ):
                    gz.write(data=chunk)

            tmp.flush()

            s3_handler.write(
                file_path=dest_tmp_key,
                content=tmp.name,
            )
            logging.info(msg=f"Successfully dumped {db_name} to S3 with key {dest_tmp_key}")

    databases = list_databases(db_conn_id=db_conn_id)
    export_db = export_database.partial(db_conn_id=db_conn_id).expand(db_name=databases)

    chain(databases, export_db)
