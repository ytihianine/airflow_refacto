from datetime import datetime, timedelta
import pandas as pd

from dags.applications.clean_s3.process import check_date_format, safe_parse_date
from infra.file_handling.dataframe import read_dataframe
from infra.file_handling.s3 import S3FileHandler
from utils.config.tasks import get_selecteur_config
from utils.config.vars import DEFAULT_S3_BUCKET, DEFAULT_S3_CONN_ID
from utils.dataframe import df_info


def list_keys(selecteur: str) -> None:
    # config
    config = get_selecteur_config(nom_projet="dsci", selecteur=selecteur)
    # Hooks
    s3_handler = S3FileHandler(
        connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
    )

    # List objects
    list_objects = s3_handler.list_files(directory="")

    # Convert to DataFrame
    df = pd.DataFrame(list_objects, columns=["s3_keys"])

    # Export to file
    s3_handler.write(
        file_path=config.filepath_tmp_s3, content=df.to_parquet(path=None, index=False)
    )


def process_keys(input_selecteur: str, output_selecteur: str) -> None:
    # config
    config_input = get_selecteur_config(nom_projet="dsci", selecteur=input_selecteur)
    config_output = get_selecteur_config(nom_projet="dsci", selecteur=output_selecteur)
    # Hooks
    s3_handler = S3FileHandler(
        connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
    )

    # Read file
    df = read_dataframe(file_handler=s3_handler, file_path=config_input.filepath_tmp_s3)

    s3_handler.read(file_path="SG/clean_s3/tmp/list_keys.parquet")

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
    )  # type: ignore
    print(max_dates.head())

    # Step 3: Compare date to the max per group
    df_filtered = df_filtered.merge(max_dates, on=["year", "month"], how="left")
    df_filtered["is_latest_in_month"] = (
        df_filtered["date"] == df_filtered["max_month_date"]
    )
    df_info(df=df_filtered, df_name="S3 keys")

    # Export to file
    s3_handler.write(
        file_path=config_output.filepath_tmp_s3,  # "SG/clean_s3/tmp/list_keys_processed.parquet",
        content=df_filtered.to_parquet(path=None, index=False),
    )


def delete_old_keys(input_selecteur: str) -> None:
    # config
    config_input = get_selecteur_config(nom_projet="dsci", selecteur=input_selecteur)
    # Hooks
    s3_handler = S3FileHandler(
        connection_id=DEFAULT_S3_CONN_ID, bucket=DEFAULT_S3_BUCKET
    )

    # Read file
    df = read_dataframe(file_handler=s3_handler, file_path=config_input.filepath_tmp_s3)
    print(f"Total of keys: {len(df)}")
    df = df.loc[df["is_latest_in_month"] == False]
    keys_to_delete = df["s3_keys"].to_list()
    print(f"Total of keys to delete: {len(keys_to_delete)}")
    if len(keys_to_delete) > 0:
        for keys in keys_to_delete:
            print(f"Deleting key: {keys}")
            s3_handler.delete(file_path=keys)
    else:
        print("No keys to delete")
