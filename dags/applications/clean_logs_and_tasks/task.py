from airflow.decorators import task


@task(task_id="clean_s3")
def clean_s3() -> None:
    from datetime import datetime, timedelta

    s3_handler = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    days_to_keep = 60

    date_to_clean_before = datetime.now() - timedelta(days=days_to_keep)

    s3_keys = s3_handler.list_keys(to_datetime=date_to_clean_before)
    print(s3_keys)


@task(task_id="clean_old_logs")
def clean_old_logs() -> None:
    import subprocess
    from datetime import datetime, timedelta

    days_to_keep = 60
    date_to_clean_before = datetime.now() - timedelta(days=days_to_keep)

    # Construct pg_dumpall command with Kubernetes service DNS
    command = [
        f"airflow db clean --clean-before-timestamp '{date_to_clean_before}' --verbose"
    ]

    print(command)

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    # Check for errors
    if result.returncode != 0:
        raise ValueError(f"Error occurred: {result.stderr}")
    else:
        print("Command executed successfully. Metadatadb has been cleared")


@task(task_id="clean_skipped_logs")
def clean_skipped_logs() -> None:
    pass
