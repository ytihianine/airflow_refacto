import sqlite3
from typing import Callable
from airflow.decorators import task
import pandas as pd

from utils.file_handler import MinioFileHandler

from utils.df_utility import df_info

from utils.tasks.sql import get_conn_from_s3_sqlite, get_storage_rows


def get_storage_rows_from_sqlite(
    nom_projet: str, connecteur: sqlite3.Connection
) -> pd.DataFrame:
    storage_paths = pd.read_sql_query(
        sql="""
            SELECT Projets.Projet, Selecteurs.Selecteur,
                Sources.nom_source,
                Storage.filename, Storage.local_tmp_dir, Storage.s3_bucket,
                Storage.s3_key, Storage.s3_tmp_key, Storage.db_tbl_name, Storage.tbl_order
            FROM Projets
                INNER JOIN Selecteurs ON Selecteurs.Projet = Projets.id
                INNER JOIN Sources ON Sources.Projet = Projets.id
                    AND Sources.Selecteur = Selecteurs.id
                INNER JOIN Storage ON Storage.Projet = Projets.id
                    AND Storage.Selecteur = Selecteurs.id
            WHERE Projets.Projet = :param_projet
                AND Selecteurs.Type_de_selecteur = "Source"
        """,
        con=connecteur,
        params={"param_projet": nom_projet},
    )

    # Adding columns
    storage_paths["filename_s3"] = storage_paths["filename"] + ".parquet"
    storage_paths["filename_local"] = storage_paths["filename"] + ".tsv"
    storage_paths["s3"] = storage_paths["s3_key"] + "/" + storage_paths["filename_s3"]
    storage_paths["local"] = (
        storage_paths["local_tmp_dir"] + "/" + storage_paths["filename_local"]
    )

    return storage_paths


def create_task(
    selecteur: str,
    process_func: Callable,
    sqlite_file_s3_filepath: str,
):
    @task(task_id=selecteur)
    def _task(**context):
        nom_projet = context.get("params").get("nom_projet", None)
        if nom_projet is None:
            raise ValueError(
                "La variable nom_projet n'a pas été définie au niveau du DAG !"
            )
        # Variables
        s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
        conn = get_conn_from_s3_sqlite(sqlite_file_s3_filepath=sqlite_file_s3_filepath)
        row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

        grist_tbl_name = row_selecteur.loc[0, "nom_source"]
        df = pd.read_sql_query(
            f"SELECT * FROM {grist_tbl_name}",
            con=conn,
        )

        # Removing all Grist internal Columns
        df = df.drop(df.filter(regex="^(grist|manual)").columns, axis=1)

        # Process Grist data
        df = process_func(df=df)
        df_info(df=df, df_name=f"{selecteur} - Après processing")

        # Send file to MiniIO tmp folder
        s3_filepath = row_selecteur.loc[0, "filepath_tmp_s3"]
        print(f"Exporting file to < {s3_filepath} >")
        s3_hook.load_bytes(
            bytes_data=df.to_parquet(path=None, index=False),  # convert df to bytes
            key=s3_filepath,
            replace=True,
        )

    return _task()
