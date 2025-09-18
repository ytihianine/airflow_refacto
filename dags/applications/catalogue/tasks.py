from typing import Callable
from airflow.decorators import task

from utils.file_handler import MinioFileHandler
from utils.df_utility import df_info
from utils.tasks.sql import (
    get_conn_from_s3_sqlite,
    get_data_from_s3_sqlite_file,
)
from utils.config.tasks import get_storage_rows

from dags.applications.catalogue import process


def create_task(
    selecteur: str,
    sqlite_file_s3_filepath: str,
    process_func: Callable = None,
):
    @task(task_id=selecteur)
    def _task(**context):
        nom_projet = context.get("params").get("nom_projet", None)
        if nom_projet is None:
            raise ValueError(
                "La variable nom_projet n'a pas été définie au niveau du DAG !"
            )
        # Hooks
        s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
        # Get config values related to the task
        row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
        grist_tbl_name = row_selecteur.loc[0, "nom_source"]

        # Get data of table
        conn = get_conn_from_s3_sqlite(
            sqlite_file_s3_filepath=sqlite_file_s3_filepath, s3_hook=s3_hook
        )
        df = get_data_from_s3_sqlite_file(
            grist_tbl_name=grist_tbl_name,
            sqlite_s3_filepath=sqlite_file_s3_filepath,
            sqlite_conn=conn,
        )

        if process_func is not None:
            df = process.clean_and_normalize_df(df=df)
            df_info(df=df, df_name=f"{grist_tbl_name} - Raw and normalized")
            df = process_func(df)
            df_info(df=df, df_name=f"{grist_tbl_name} - After processing")

        # Export to file
        s3_hook.load_bytes(
            bytes_data=df.to_parquet(path=None, index=False),
            key=row_selecteur.loc[0, "filepath_s3"] + ".parquet",
            replace=True,
        )

    return _task()


@task
def add_new_datasets(tbl_name: str, tmp_schema: str = "temporaire") -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # Hooks
    db_hook = PostgresHook(postgres_conn_id="db_data_store")

    # Scan new datasets
    schema_to_exclude = ", ".join(
        ["'pg_catalog'", "'information_schema'", "'public'", "'temporaire'"]
    )
    db_hook.run(
        sql=f"""INSERT INTO {tmp_schema}.tmp_{tbl_name} (schema_name, table_name)
            SELECT table_schema, table_name
            FROM information_schema.tables ist
            WHERE table_schema NOT IN ({schema_to_exclude})
            AND NOT EXISTS (
                SELECT 1
                FROM {tmp_schema}.tmp_{tbl_name} tdd
                WHERE tdd.schema_name = ist.table_schema
                AND tdd.table_name = ist.table_name
            );
        """
    )


@task
def sync_datasets(
    selecteur: str, prod_schema: str, sqlite_file_s3_filepath: str, **context
) -> None:
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # Hooks
    db_hook = PostgresHook(postgres_conn_id="db_data_store")
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

    # variables
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Get config values related to the task
    row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

    # Get data
    df_db = db_hook.get_pandas_df(
        sql=f"SELECT * FROM {prod_schema}.{row_selecteur.loc[0, "tbl_name"]}"
    )
    conn = get_conn_from_s3_sqlite(
        sqlite_file_s3_filepath=sqlite_file_s3_filepath, s3_hook=s3_hook
    )
    df_grist = process.normalize_df(
        get_data_from_s3_sqlite_file(
            grist_tbl_name=row_selecteur.loc[0, "nom_source"],
            sqlite_s3_filepath=sqlite_file_s3_filepath,
            sqlite_conn=conn,
        )
    )
    df_info(df=df_db, df_name="DB Datasets - Raw")
    df_info(df=df_grist, df_name="Grist Datasets - Raw")

    # Merge
    merged = pd.merge(
        df_db,
        df_grist,
        on=["schema_name", "table_name"],
        indicator=True,
        how="left",
        suffixes=("_db", "_grist"),
    )
    df_info(df=merged, df_name="Merged dataframes - Raw")

    # Export file


@task(task_id="update_grist")
def update_grist():
    pass


@task(task_id="update_db")
def update_db():
    pass
