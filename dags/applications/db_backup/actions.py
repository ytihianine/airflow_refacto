import os
import subprocess
from typing import cast

from airflow.models import Variable

from infra.database.factory import create_db_handler
from infra.database.postgres import PostgresDBHandler
from infra.file_handling.s3 import S3FileHandler
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
    s3_handler = S3FileHandler(
        connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
    )
    conn = db_handler.get_uri()

    split_conn_dsn = conn.split(" ")
    print(split_conn_dsn)
    host = split_conn_dsn[3].split("=")[1]
    port = split_conn_dsn[-1].split("=")[1]
    username = split_conn_dsn[0].split("=")[1]

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

        # Execute pg_dump and stream to S3
        with subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env
        ) as proc:
            # Stream the output directly to S3
            s3_key = f"{config.filepath_tmp_s3}"
            try:
                if proc.stdout is None:
                    raise ValueError("pg_dump did not produce any output.")
                s3_handler.write(file_path=s3_key, content=proc.stdout.read())

                # Check for errors
                if proc.stderr is None:
                    print("pg_dump did not produce any error output.")
                else:
                    stderr = proc.stderr.read()
                    if proc.returncode != 0:
                        raise ValueError(
                            f"Error dumping {config.nom_source}: {stderr.decode()}"
                        )

                print(
                    f"Successfully streamed dump of {config.nom_source} to S3: {s3_key}"
                )

            except Exception as e:
                raise ValueError(f"Failed to stream dump to S3: {str(e)}")
