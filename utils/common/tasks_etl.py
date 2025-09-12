"""
    Répertorie les principales tâches d'ETL génériques à utiliser dans les dags
"""

from typing import Callable
from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.file_handler import MinioFileHandler
from utils.common.config_func import get_storage_rows, get_cols_mapping
from utils.df_utility import df_info
from utils.common.config_func import (
    get_required_cols,
    format_cols_mapping,
)


def create_task_grist(
    selecteur: str,
    process_func: Callable = None,
):
    """
        Cas d'usage:
            extract: 1 table Grist
            transform: 1 fonction de processing (optionnelle)
            load: 1 fichier parquet généré sur s3
    """
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


@task(task_id="convert_oad_indic_to_parquet")
def convert_file_to_parquet(
    nom_projet: str,
    selecteur: str,
    process_func: Callable,
    **context
) -> None:
    # Variables

    # Hooks
    db_hook = PostgresHook(postgres_conn_id="db_data_store")
    db_conf_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

    # Config
    row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

    df_oad_indic = s3_hook.read_excel(
        file_name=row_selecteur.loc[0, "filepath_source_s3"]
    )

    # Cleaning df
    df_info(df=df_oad_indic, df_name="DF OAD indicateurs - INITIALISATION")
    df = process_func
    df_info(df=df_oad_indic, df_name="DF OAD indicateurs - After processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_oad_indic.to_parquet(path=None, index=False),
        key=row_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )
