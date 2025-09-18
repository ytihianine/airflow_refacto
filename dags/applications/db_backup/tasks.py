from airflow.decorators import task
from utils.common.vars import paris_tz


@task(map_index_template="{{ import_task_name }}")
def create_dump_files(
    db_name: str,
    **context,
) -> None:
    """
    Perform a PostgreSQL pg_dumpall command and store the result in a local file.
    """
    # task import
    import os
    import subprocess
    from airflow.operators.python import get_current_context
    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # Define task name
    task_context = get_current_context()
    task_context["import_task_name"] = f"dump_{db_name}"

    # Variables
    db_hook = PostgresHook(postgres_conn_id="db_data_store")
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")
    conn = db_hook.get_conn()

    split_conn_dsn = conn.dsn.split(" ")
    print(split_conn_dsn)
    host = split_conn_dsn[3].split("=")[1]
    port = split_conn_dsn[-1].split("=")[1]
    username = split_conn_dsn[0].split("=")[1]

    # Environment variable for password - to avoid password prompt
    env = os.environ.copy()
    env["PGPASSWORD"] = Variable.get("db_main_password")

    # os.makedirs(temp_dir, exist_ok=True)  # Ensure the directory exists

    filename = f"{db_name}.dump"
    db_dump_filepath = f"./{filename}"
    # Construct pg_dumpall command with Kubernetes service DNS
    command = [
        f"pg_dump --host={host} --port={port} --username={username} -Fc --no-owner -d {db_name.strip()} --file={db_dump_filepath}"
    ]

    print(command)
    print(db_dump_filepath)

    result = subprocess.run(
        command, shell=True, capture_output=True, env=env, text=True
    )

    # Check for errors
    if result.returncode != 0:
        raise ValueError(f"Error occurred: {result.stderr}")
    else:
        print(f"Command executed successfully. Output written to {db_dump_filepath}")

    # Send file to MinIo
    execution_date = context["dag_run"].execution_date.astimezone(paris_tz)
    curr_day = execution_date.strftime("%Y%m%d")
    curr_time = execution_date.strftime("%Hh%M")

    # Check if file exists
    if os.path.isfile(db_dump_filepath):
        base_key = "sg/application/sauvegarde/database"
        s3_path = f"{base_key}/{curr_day}/{curr_time}/{db_name}.dump"
        s3_hook.load_file(
            filename=db_dump_filepath,
            key=s3_path,
        )
        print(
            f"Local file < {db_dump_filepath} > has been sent to MinIO at < {s3_path} >"
        )
    else:
        raise ValueError(f"{filename} does not exists locally :(")
