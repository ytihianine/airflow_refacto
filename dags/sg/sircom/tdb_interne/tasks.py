from typing import Callable
from airflow.decorators import task

from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.file_handler import MinioFileHandler
from utils.dataframe import df_info
from utils.tasks.sql import get_conn_from_s3_sqlite, get_data_from_s3_sqlite_file
from utils.config.tasks import get_storage_rows

from dags.sg.sircom.tdb_interne import process


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

        import numpy as np

        # Hooks
        db_hook = PostgresHook(postgres_conn_id="db_data_store")
        # s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
        # Get config values related to the task
        row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
        grist_tbl_name = row_selecteur.loc[0, "nom_source"]
        db_tbl_name = row_selecteur.loc[0, "tbl_name"]

        # Get data of table
        conn = get_conn_from_s3_sqlite(sqlite_file_s3_filepath=sqlite_file_s3_filepath)
        df = get_data_from_s3_sqlite_file(
            table_name=grist_tbl_name,
            sqlite_s3_filepath=sqlite_file_s3_filepath,
            sqlite_conn=conn,
        )

        df_info(df=df, df_name=f"{grist_tbl_name} - Raw")
        df = process.clean_and_normalize_df(df=df)

        df = process_func(df)
        df = df.convert_dtypes()
        df = df.fillna(np.nan).replace([np.nan], [None])
        df_info(df=df, df_name=f"{grist_tbl_name} - After processing")

        # Insert into database
        db_hook.insert_rows(
            table=f"temporaire.tmp_{db_tbl_name}",
            rows=df.values.tolist(),
            target_fields=list(df.columns),
            commit_every=1000,
            executemany=False,
        )

    return _task()
