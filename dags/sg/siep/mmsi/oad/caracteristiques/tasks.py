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

from dags.sg.siep.mmsi.oad.caracteristiques import process


def create_task_oad_carac(
    selecteur: str,
    process_func: Callable,
):
    @task(task_id=selecteur)
    def _task(**context):
        # Variables
        nom_projet = context.get("params").get("nom_projet", None)
        if nom_projet is None:
            raise ValueError(
                "La variable nom_projet n'a pas été définie au niveau du DAG !"
            )
        # Hooks
        s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
        # Get config values related to the task
        row_oad_carac = get_storage_rows(nom_projet=nom_projet, selecteur="oad_carac")
        row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
        required_cols = get_required_cols(nom_projet=nom_projet, selecteur=selecteur)

        # Main part
        df = s3_hook.read_parquet(
            file_name=row_oad_carac.loc[0, "filepath_tmp_s3"],
            columns=required_cols["colname_dest"].to_list(),
        )

        df_info(df=df, df_name=f"DF {selecteur} - INITIALISATION")

        print("Start - Cleaning Process")
        df = process_func(df=df)
        print("End - Cleaning Process")
        df_info(df=df, df_name=f"DF {selecteur} - After Cleaning Process")

        # Export
        s3_hook.load_bytes(
            bytes_data=df.to_parquet(path=None, index=False),
            key=row_selecteur.loc[0, "filepath_tmp_s3"],
            replace=True,
        )

    return _task()


@task(task_id="convert_oad_caracteristique_to_parquet")
def convert_oad_caracteristique_to_parquet(nom_projet: str, selecteur: str) -> None:
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    db_conf_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

    # Main part
    df_oad = s3_hook.read_excel(
        file_name=row_selecteur.loc[
            0, "filepath_source_s3"
        ]  # , sheet_name="Source OAD"
    )

    df_info(df=df_oad, df_name="DF OAD - INITIALISATION")

    # Cleaning df
    cols_oad_caract = get_cols_mapping(
        nom_projet=nom_projet, db_hook=db_conf_hook, selecteur=selecteur
    )
    cols_oad_caract = format_cols_mapping(df_cols_map=cols_oad_caract.copy())
    df_oad = process.process_oad_file(df=df_oad, cols_mapping=cols_oad_caract)

    df_info(df=df_oad, df_name="DF OAD - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_oad.to_parquet(path=None, index=False),
        key=row_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task_group
def tasks_oad_caracteristiques():
    sites = create_task_oad_carac(
        selecteur="sites",
        process_func=process.process_sites,
    )
    biens = create_task_oad_carac(
        selecteur="biens",
        process_func=process.process_biens,
    )
    gestionnaires = create_task_oad_carac(
        selecteur="gestionnaires",
        process_func=process.process_gestionnaires,
    )
    biens_gestionnaires = create_task_oad_carac(
        selecteur="biens_gest",
        process_func=process.process_biens_gestionnaires,
    )
    biens_occupants = create_task_oad_carac(
        selecteur="biens_occupants",
        process_func=process.process_biens_occupants,
    )

    chain(
        [
            sites,
            biens,
            gestionnaires,
            biens_gestionnaires,
            biens_occupants,
        ],
    )
