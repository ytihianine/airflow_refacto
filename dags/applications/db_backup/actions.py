import os
import subprocess
from typing import cast

from airflow.models import Variable

from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler
from infra.file_handling.factory import create_file_handler
from infra.file_handling.local import LocalFileHandler
from utils.config.tasks import get_projet_config
from utils.config.vars import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID


def create_dump_files(nom_projet: str) -> None:
    """
    Perform a PostgreSQL pg_dumpall command and store the result in a local file.
    """
    # config
    projet_config = get_projet_config(nom_projet=nom_projet)

    # Variables
    db_handler = cast(
        PostgresDBHandler, create_db_handler(connection_id="db_data_store")
    )
    # Hooks
    s3_handler = create_file_handler(
        handler_type="s3", connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
    )
    local_handler = LocalFileHandler()
    conn = db_handler.get_uri()

    split_conn_dsn = conn.split("://")[1].split("/")[0].split("@")
    print(split_conn_dsn)
    credentials = split_conn_dsn[0].split(":")
    username = credentials[0]
    connexion = split_conn_dsn[1].split(":")
    host = connexion[0]
    port = connexion[1]

    # Environment variable for password - to avoid password prompt
    env = os.environ.copy()
    env["PGPASSWORD"] = Variable.get("db_main_password")

    for config in projet_config:
        # Construct pg_dump command (without file output)
        command = [
            "pg_dump",
            f"--host={host}",
            f"--port={port}",
            f"--username={username}",
            "-Fc",  # Custom format
            "--no-owner",
            "-d",
            config.nom_source,
        ]

        print(f"Executing dump for database: {config.nom_source}")

        # Local + S3 dump
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
        ) as proc:
            # Capture output and wait for process to finish
            stdout, stderr = proc.communicate()

            if proc.returncode != 0:
                raise ValueError(
                    f"Error dumping {config.nom_source}: {stderr.decode().strip() or 'Unknown error'}"
                )

            # --- 1. Write dump locally (atomic write via file_handler) ---
            local_handler.write(file_path=config.filepath_local, content=stdout)

            # --- 2. Upload local dump to S3 ---
            with open(config.filepath_local, "rb") as f:
                s3_handler.write(file_path=config.filepath_tmp_s3, content=f)

            print(
                f"Successfully dumped {config.nom_source} to local: {config.filepath_local}, and uploaded to S3: {config.filepath_tmp_s3}"  # noqa
            )

            local_handler.delete(file_path=config.filepath_local)
