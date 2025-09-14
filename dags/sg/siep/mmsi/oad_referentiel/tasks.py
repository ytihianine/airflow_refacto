from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.file_handler import MinioFileHandler
from utils.dataframe import df_info
from utils.config.tasks import (
    get_storage_rows,
    get_cols_mapping,
    format_cols_mapping,
)

from dags.sg.siep.mmsi.oad_referentiel.process import (
    process_typologie_bien,
)


@task(task_id="bien_typologie")
def bien_typologie(nom_projet: str) -> None:
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")
    db_conf_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    selecteur = "ref_typologie"
    storage_row = get_storage_rows(nom_projet=nom_projet, db_hook=db_conf_hook)
    df_ref_typologie = s3_hook.read_excel(
        file_name=storage_row.loc[0, "filepath_source_s3"],
    )

    df_info(
        df=df_ref_typologie, df_name="DF Référentiel bien typologie - INITIALISATION"
    )

    # Cleaning df
    df_cols_mapping = get_cols_mapping(nom_projet=nom_projet, db_hook=db_conf_hook)
    all_cols = format_cols_mapping(df_cols_map=df_cols_mapping, selecteur=selecteur)
    df_ref_typologie = process_typologie_bien(
        df=df_ref_typologie, cols_mapping=all_cols
    )

    df_info(
        df=df_ref_typologie, df_name="DF Référentiel bien typologie - After processing"
    )

    # Export processed data
    s3_hook.load_bytes(
        bytes_data=df_ref_typologie.to_parquet(path=None, index=False),
        key=storage_row.loc[0, "filepath_tmp_s3"],
        replace=True,
    )
