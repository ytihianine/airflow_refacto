from typing import Callable
from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.file_handler import MinioFileHandler
from utils.common.config_func import (
    get_storage_rows,
    get_cols_mapping,
    format_cols_mapping,
)
from utils.df_utility import df_info

from dags.sg.snum.certificats_igc import process


def create_task_file(
    selecteur: str,
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
        s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")
        db_hook = PostgresHook(postgres_conn_id="db_depose_fichier")

        # Config
        config_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
        cols_to_rename = get_cols_mapping(
            nom_projet=nom_projet, db_hook=db_hook, selecteur=selecteur
        )
        cols_to_rename = format_cols_mapping(df_cols_map=cols_to_rename.copy())

        # Read data
        df = s3_hook.read_excel(file_name=config_selecteur.loc[0, "filepath_source_s3"])

        # Processing
        df_info(df=df, df_name=f"{selecteur} - Source")
        df = process_func(df, cols_to_rename)
        df_info(df=df, df_name=f"{selecteur} - Après processing")

        # Export
        s3_hook.load_bytes(
            bytes_data=df.to_parquet(path=None, index=False),
            key=config_selecteur.loc[0, "filepath_tmp_s3"],
            replace=True,
        )

    return _task()


@task_group
def source_files() -> None:
    agents = create_task_file(selecteur="agents", process_func=process.process_agents)
    aip = create_task_file(
        selecteur="aip",
        process_func=process.process_aip,
    )
    certificats = create_task_file(
        selecteur="certificats", process_func=process.process_certificats
    )
    igc = create_task_file(selecteur="igc", process_func=process.process_igc)

    # ordre des tâches
    chain([agents, aip, certificats, igc])


@task_group
def output_files() -> None:
    pass
