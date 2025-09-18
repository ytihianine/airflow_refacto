from datetime import datetime, timedelta
from airflow.decorators import task
import pandas as pd

from utils.file_handler import MinioFileHandler
from utils.df_utility import df_info

from dags.applications.clean_s3.process import (
    check_date_format,
    safe_parse_date,
)


@task(task_id="list_keys")
def list_keys() -> None:
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

    # Run through all objects until you meet the objects with date names
    list_objects = s3_hook.list_keys(prefix="SG/db_backup")

    df = pd.DataFrame(list_objects, columns=["s3_keys"])

    # Export to file
    s3_hook.load_bytes(
        bytes_data=df.to_parquet(path=None, index=False),
        key="SG/clean_s3/tmp/list_keys.parquet",
        replace=True,
    )


@task(task_id="process_keys")
def process_keys() -> None:
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

    # Read file
    df = s3_hook.read_parquet(file_name="SG/clean_s3/tmp/list_keys.parquet")

    # Process keys
    df["date_str"] = df["s3_keys"].str.split("/").str[-3]
    df["is_date_format"] = list(map(check_date_format, df["date_str"]))
    df["date"] = list(map(safe_parse_date, df["date_str"], df["is_date_format"]))
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    # Keep all rows exept those between the last 35 days
    cutoff_date = datetime.today() - timedelta(days=35)
    df_filtered = df[df["date"] <= cutoff_date].copy()

    # Step 2: Get the latest date per year-month
    max_dates = (
        df_filtered.dropna(subset=["date"])
        .groupby(["year", "month"], as_index=False)["date"]
        .max()
        .rename(columns={"date": "max_month_date"})
    )
    print(max_dates.head())

    # Step 3: Compare date to the max per group
    df_filtered = df_filtered.merge(max_dates, on=["year", "month"], how="left")
    df_filtered["is_latest_in_month"] = (
        df_filtered["date"] == df_filtered["max_month_date"]
    )
    df_info(df=df_filtered, df_name="S3 keys")

    # Export to file
    s3_hook.load_bytes(
        bytes_data=df_filtered.to_parquet(path=None, index=False),
        key="SG/clean_s3/tmp/list_keys_processed.parquet",
        replace=True,
    )


@task(task_id="delete_old_keys")
def delete_old_keys() -> None:
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

    # Read file
    df = s3_hook.read_parquet(file_name="SG/clean_s3/tmp/list_keys_processed.parquet")
    print(f"Total of keys: {len(df)}")
    df = df.loc[df["is_latest_in_month"] == False]
    keys_to_delete = df["s3_keys"].to_list()
    print(f"Total of keys to delete: {len(keys_to_delete)}")
    if len(keys_to_delete) > 0:
        s3_hook.delete_objects(keys=keys_to_delete)
    else:
        print("No keys to delete")
