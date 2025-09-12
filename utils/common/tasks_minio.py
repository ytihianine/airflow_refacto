from airflow.decorators import task

from utils.file_handler import MinioFileHandler
from utils.common.config_func import get_storage_rows
from utils.df_utility import df_info
from utils.common.vars import paris_tz


@task
def copy_files_to_minio(
    bucket: str,
    source_key: str = None,
    dest_key: str = None,
    s3_conn_id: str = "minio_bucket_dsci",
    **context,
) -> None:
    # Varibles
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )

    # Define hook
    s3_hook = MinioFileHandler(connection_id=s3_conn_id, bucket="dsci")

    execution_date = context["dag_run"].execution_date.astimezone(paris_tz)
    curr_day = execution_date.strftime("%Y%m%d")
    curr_time = execution_date.strftime("%Hh%M")

    storage_rows = get_storage_rows(nom_projet=nom_projet)

    for row in storage_rows.itertuples():
        filename = row.filename
        source_key = row.filepath_tmp_s3
        dest_key = row.s3_key

        if filename == "" or filename is None:
            print(f"Filename undefined for selecteur <{row.selecteur}>. Skipping ...")
        else:
            s3_hook.copy_object(
                source_bucket_name=bucket,
                source_bucket_key=source_key,
                dest_bucket_name=bucket,
                dest_bucket_key=f"{dest_key}/{curr_day}/{curr_time}/{filename}",
            )


@task
def del_files_from_minio(
    bucket: str,
    s3_keys_to_del: list[str] = None,
    s3_conn_id: str = "minio_bucket_dsci",
    **context,
) -> None:
    s3_tmp_keys_to_del = []
    s3_source_keys_to_del = []

    # Define hook
    s3_hook = MinioFileHandler(connection_id=s3_conn_id, bucket=bucket)

    if s3_keys_to_del is None:
        nom_projet = context.get("params").get("nom_projet", None)
        if nom_projet is None:
            raise ValueError(
                "La variable nom_projet n'a pas été définie au niveau du DAG !"
            )

        # if no keys are specified, deleting temporary keys and source keys by default
        df_config_projet = get_storage_rows(nom_projet=nom_projet)
        df_info(
            df=df_config_projet,
            df_name=f"Config du projet: {nom_projet}",
            full_logs=False,
        )

        s3_tmp_keys_to_del = df_config_projet.loc[
            df_config_projet["filename"].notna()
            & (df_config_projet["filename"].str.strip() != ""),
            "filepath_tmp_s3",
        ].tolist()
        s3_source_keys_to_del = df_config_projet.loc[
            df_config_projet["nom_source"].notna()
            & (df_config_projet["nom_source"].str.strip() != ""),
            "filepath_source_s3",
        ].tolist()

    if len(s3_tmp_keys_to_del) > 0:
        print("Suppression des fichiers temporaires")
        s3_tmp_keys_to_del = [
            tmp_key
            for tmp_key in s3_tmp_keys_to_del
            if tmp_key != "" and tmp_key is not None
        ]
        s3_hook.delete_objects(bucket=bucket, keys=s3_tmp_keys_to_del)

    if len(s3_source_keys_to_del) > 0:
        print("Suppression des fichiers sources")
        s3_source_keys_to_del = [
            source_key
            for source_key in s3_source_keys_to_del
            if source_key != "" and source_key is not None
        ]
        s3_hook.delete_objects(bucket=bucket, keys=s3_source_keys_to_del)
