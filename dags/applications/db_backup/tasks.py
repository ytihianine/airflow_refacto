import gzip
import logging
import os
import subprocess

from airflow.sdk import chain, task, task_group

# from modules.constants import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID
# from modules.enums.filesystem import FileHandlerType
# from modules.infra.file_system.factory import create_file_handler
from modules.infra.database.factory import create_db_handler
from modules.utils.config.dag_params import get_project_name
from modules.utils.config.tasks import get_projet_s3_info


@task_group
def export_database(db_conn_id: str, **context) -> None:
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

    @task
    def export_database(db_conn_id: str, db_name: str) -> None:
        # Variables
        nom_projet = get_project_name(context=context)
        projet_info = get_projet_s3_info(nom_projet=nom_projet)
        dest_tmp_key = projet_info.key_tmp + f"/{db_name}_dump.sql.gz"

        # Hooks
        db_handler = create_db_handler(connection_id=db_conn_id)
        # s3_handler = create_file_handler(
        #     handler_type=FileHandlerType.S3,
        #     connection_id=DEFAULT_S3_CONN_ID,
        #     bucket=DEFAULT_S3_BUCKET,
        # )
        conn = db_handler.get_uri()
        logging.debug(msg=f"{db_handler.get_conn()}")

        split_conn_dsn = conn.split(sep="://")[1].split(sep="/")[0].split(sep="@")
        logging.debug(msg=split_conn_dsn)
        credentials = split_conn_dsn[0].split(sep=":")
        username = credentials[0]
        connexion = split_conn_dsn[1].split(sep=":")
        host = connexion[0]
        port = connexion[1]

        # Environment variable for password - to avoid password prompt
        env = os.environ.copy()
        env["PGPASSWORD"] = "fake"

        # Export database and load it to MinIO
        logging.info(msg=f"Executing dump for database: {db_name}")
        output_file = f"/tmp/{db_name}_dump.sql.gz"
        with gzip.open(output_file, "wb", compresslevel=9) as gz:
            proc = subprocess.Popen(
                [
                    "pg_dump",
                    "--host",
                    host,
                    "--port",
                    str(port),
                    "--username",
                    username,
                    "--format=plain",
                    "--no-owner",
                    "--no-privileges",
                    db_name,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            try:
                for chunk in iter(lambda: proc.stdout.read(1024 * 1024), b""):  # type: ignore
                    gz.write(data=chunk)  # pyright: ignore[reportCallIssue]

                stderr = proc.stderr.read()  # type: ignore
                rc = proc.wait()

                if rc != 0:
                    raise RuntimeError(f"pg_dump failed with exit code {rc}\n" f"{stderr.decode()}")

            finally:
                if proc.stdout:
                    proc.stdout.close()
                if proc.stderr:
                    proc.stderr.close()
                logging.info(msg=f"Successfully dumped {db_name} to S3 with key {dest_tmp_key}")

    databases = list_databases(db_conn_id=db_conn_id)
    export_db = export_database.partial(db_conn_id=db_conn_id).expand(db_name=databases)

    chain(databases, export_db)
