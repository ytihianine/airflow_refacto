from typing import Callable
from airflow.decorators import task

from utils.file_handler import MinioFileHandler
from utils.dataframe import df_info
from utils.tasks.sql import get_conn_from_s3_sqlite, get_data_from_s3_sqlite_file
from utils.config.tasks import get_storage_rows

from dags.sg.dsci.accompagnements_dsci import process


def create_task(
    selecteur: str,
    process_func: Callable = None,
):
    @task(task_id=selecteur)
    def _task(**context):
        nom_projet = context.get("params").get("nom_projet", None)
        sqlite_file_s3_filepath = context.get("params").get(
            "sqlite_file_s3_filepath", None
        )
        if nom_projet is None:
            raise ValueError(
                "La variable nom_projet n'a pas été définie au niveau du DAG !"
            )
        if sqlite_file_s3_filepath is None:
            raise ValueError(
                "La variable sqlite_file_s3_filepath n'a pas été définie au niveau du DAG !"
            )

        # Hooks
        s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
        # Get config values related to the task
        task_config = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
        grist_tbl_name = task_config.loc[0, "nom_source"]

        # Get data of table
        conn = get_conn_from_s3_sqlite(sqlite_file_s3_filepath=sqlite_file_s3_filepath)
        df = get_data_from_s3_sqlite_file(
            grist_tbl_name=grist_tbl_name,
            sqlite_s3_filepath=sqlite_file_s3_filepath,
            sqlite_conn=conn,
        )

        df = process.normalize_dataframe(df=df)
        df_info(df=df, df_name=f"{grist_tbl_name} - Source normalisée")
        df = process_func(df)
        df_info(df=df, df_name=f"{grist_tbl_name} - After processing")

        # Export
        s3_hook.load_bytes(
            bytes_data=df.to_parquet(path=None, index=False),
            key=task_config.loc[0, "filepath_tmp_s3"],
            replace=True,
        )

    return _task()
