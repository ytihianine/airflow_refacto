from typing import Callable
from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain
from airflow.providers.postgres.hooks.postgres import PostgresHook


from utils.tasks.sql import get_conn_from_s3_sqlite, get_data_from_s3_sqlite_file
from utils.config.tasks import (
    get_storage_rows,
    get_cols_mapping,
    format_cols_mapping,
    get_required_cols,
)
from utils.dataframe import df_info

from dags.dge.carto_rem import process


def create_task_grist(
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


@task
def agent(
    selecteur: str, selecteur_carto_rem: str, selecteur_info_car: str, **context
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )

    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    config_carto_rem = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_carto_rem
    )
    config_info_car = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_info_car
    )
    required_cols = get_required_cols(nom_projet=nom_projet, selecteur=selecteur)

    # Read data
    df_carto_rem = s3_hook.read_parquet(
        file_name=config_carto_rem.loc[0, "filepath_tmp_s3"]
    )
    df_info_car = s3_hook.read_parquet(
        file_name=config_info_car.loc[0, "filepath_tmp_s3"]
    )

    # Processing
    df_info(df=df_carto_rem, df_name=f"{selecteur_carto_rem} - Source")
    df_info(df=df_info_car, df_name=f"{selecteur_info_car} - Source")
    df = process.process_agent(
        df_rem_carto=df_carto_rem,
        df_info_car=df_info_car,
        required_cols=required_cols["colname_dest"].to_list(),
    )
    df_info(df=df, df_name=f"{selecteur} - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df.to_parquet(path=None, index=False),
        key=config_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task
def agent_poste(
    selecteur: str,
    selecteur_agent: str,
    selecteur_carto_rem: str,
    selecteur_r4: str,
    **context,
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )

    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    config_agent = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur_agent)
    config_carto_rem = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_carto_rem
    )
    config_r4 = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur_r4)
    required_cols = get_required_cols(nom_projet=nom_projet, selecteur=selecteur)

    # Read data
    df_agent = s3_hook.read_parquet(file_name=config_agent.loc[0, "filepath_tmp_s3"])
    df_carto_rem = s3_hook.read_parquet(
        file_name=config_carto_rem.loc[0, "filepath_tmp_s3"]
    )
    df_r4 = s3_hook.read_parquet(file_name=config_r4.loc[0, "filepath_tmp_s3"])

    # Processing
    df_info(df=df_agent, df_name=f"{selecteur_agent} - Source")
    df_info(df=df_r4, df_name=f"{config_r4} - Source")
    df = process.process_agent_poste(
        df_agent=df_agent,
        df_carto_rem=df_carto_rem,
        df_r4=df_r4,
        required_cols=required_cols["colname_dest"].to_list(),
    )
    df_info(df=df, df_name=f"{selecteur} - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df.to_parquet(path=None, index=False),
        key=config_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task
def agent_remuneration(
    selecteur: str,
    selecteur_carto_rem: str,
    selecteur_agent_rem_variable: str,
    **context,
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )

    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    config_carto_rem = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_carto_rem
    )
    config_agent_rem_variable = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_agent_rem_variable
    )

    # Read data
    df_carto_rem = s3_hook.read_parquet(
        file_name=config_carto_rem.loc[0, "filepath_tmp_s3"]
    )
    df_agent_rem_variable = s3_hook.read_parquet(
        file_name=config_agent_rem_variable.loc[0, "filepath_tmp_s3"]
    )

    # Processing
    df_info(df=df_carto_rem, df_name=f"{selecteur_carto_rem} - Source")
    df_info(df=df_agent_rem_variable, df_name=f"{config_agent_rem_variable} - Source")
    df = process.process_agent_remuneration(
        df_rem_carto=df_carto_rem, df_agent_rem_variable=df_agent_rem_variable
    )
    df_info(df=df, df_name=f"{selecteur} - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df.to_parquet(path=None, index=False),
        key=config_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task_group
def source_files() -> None:
    agent_carto_rem = create_task_file(
        selecteur="agent_carto_rem", process_func=process.process_agent_carto_rem
    )
    agent_info_carriere = create_task_file(
        selecteur="agent_info_carriere",
        process_func=process.process_agent_info_carriere,
    )
    agent_r4 = create_task_file(
        selecteur="agent_r4", process_func=process.process_agent_r4
    )

    # ordre des tâches
    chain([agent_carto_rem, agent_info_carriere, agent_r4])


@task_group
def source_grist() -> None:
    agent_diplome = create_task_grist(
        selecteur="agent_diplome", process_func=process.process_agent_diplome
    )
    agent_revalorisation = create_task_grist(
        selecteur="agent_revalorisation",
        process_func=process.process_agent_revalorisation,
    )
    agent_contrat = create_task_grist(
        selecteur="agent_contrat", process_func=process.process_agent_contrat
    )
    agent_rem_variable = create_task_grist(
        selecteur="agent_rem_variable",
        process_func=process.process_agent_rem_variable,
    )

    # ordre des tâches
    chain([agent_diplome, agent_revalorisation, agent_contrat, agent_rem_variable])


@task_group
def output_files() -> None:

    # ordre des tâches
    chain(
        agent(
            selecteur="agent",
            selecteur_carto_rem="agent_carto_rem",
            selecteur_info_car="agent_info_carriere",
        ),
        [
            agent_poste(
                selecteur="agent_poste",
                selecteur_agent="agent",
                selecteur_carto_rem="agent_carto_rem",
                selecteur_r4="agent_r4",
            ),
            agent_remuneration(
                selecteur="agent_remuneration",
                selecteur_carto_rem="agent_carto_rem",
                selecteur_agent_rem_variable="agent_rem_variable",
            ),
        ],
    )


@task_group
def referentiels() -> None:
    ref_base_remuneration = create_task_grist(
        selecteur="ref_base_remuneration",
        process_func=process.process_ref_base_remuneration,
    )
    ref_base_revalorisation = create_task_grist(
        selecteur="ref_base_revalorisation",
        process_func=process.process_ref_base_revalorisation,
    )
    ref_experience_pro = create_task_grist(
        selecteur="ref_experience_pro", process_func=process.process_ref_experience_pro
    )
    ref_niveau_diplome = create_task_grist(
        selecteur="ref_niveau_diplome", process_func=process.process_ref_niveau_diplome
    )
    ref_valeur_point_indice = create_task_grist(
        selecteur="ref_valeur_point_indice",
        process_func=process.process_ref_valeur_point_indice,
    )

    # ordre des tâches
    chain(
        [
            ref_base_remuneration,
            ref_base_revalorisation,
            ref_experience_pro,
            ref_niveau_diplome,
            ref_valeur_point_indice,
        ]
    )
