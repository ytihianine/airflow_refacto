from airflow.decorators import task

from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.file_handler import MinioFileHandler
from utils.dataframe import df_info
from utils.config.tasks import (
    get_storage_rows,
    get_cols_mapping,
    format_cols_mapping,
)

from dags.sg.siep.mmsi.consommation_batiment import process


@task(task_id="convert_cons_mens_to_parquet")
def convert_cons_mens_to_parquet(selecteur: str, **context) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    db_conf_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

    # Main part
    df_cons_mens = s3_hook.read_excel(
        file_name=row_selecteur.loc[0, "filepath_source_s3"]
    )

    df_info(df=df_cons_mens, df_name=f"DF {selecteur} - INITIALISATION")

    # Cleaning df
    cols_oad_caract = get_cols_mapping(
        nom_projet=nom_projet, db_hook=db_conf_hook, selecteur=selecteur
    )
    cols_oad_caract = format_cols_mapping(df_cols_map=cols_oad_caract.copy())
    df_cons_mens = process.process_source_conso_mens(
        df=df_cons_mens, cols_mapping=cols_oad_caract
    )

    df_info(df=df_cons_mens, df_name=f"DF {selecteur} - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_cons_mens.to_parquet(path=None, index=False),
        key=row_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="informations_batiments")
def informations_batiments(selecteur: str, **context) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    db_conf_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    config_info_bat = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

    # Read files
    df_info_biens = s3_hook.read_excel(
        file_name=config_info_bat.loc[0, "filepath_source_s3"], sheet_name=1
    )
    colonnes_info_bat = get_cols_mapping(
        nom_projet=nom_projet, db_hook=db_conf_hook, selecteur=selecteur
    )
    colonnes_info_bat = format_cols_mapping(df_cols_map=colonnes_info_bat.copy())
    print(colonnes_info_bat)

    # Process
    df_info(df=df_info_biens, df_name=f"DF {selecteur} - Source")

    df_info_biens = process.process_source_bien_info_comp(
        df=df_info_biens, cols_mapping=colonnes_info_bat
    )
    df_info(df=df_info_biens, df_name=f"DF {selecteur} - Après processing")

    # Export file to s3
    s3_filepath = config_info_bat.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_info_biens.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )


@task(task_id="conso_mensuelles")
def conso_mensuelles(selecteur: str, selecteur_cons_mens_src: str, **context) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    config_conso_mens = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    config_conso_mens_source = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_cons_mens_src
    )

    # Read files
    df_conso_mensuelles = s3_hook.read_parquet(
        file_name=config_conso_mens_source.loc[0, "filepath_tmp_s3"],
        # columns=colonnes_conso_mens,
    )

    # Processing
    df_info(df=df_conso_mensuelles, df_name="DF conso mensuelle")
    df_conso_mensuelles = process.process_conso_mensuelles(df=df_conso_mensuelles)

    df_info(df=df_conso_mensuelles, df_name="DF conso mensuelle")

    # Export file to s3
    s3_filepath = config_conso_mens.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_conso_mensuelles.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )


@task(task_id="unpivot_conso_mens_corrigee")
def unpivot_conso_mens_corrigee(
    selecteur: str, selecteur_conso_mens: str, **context
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    # Get config
    config_cons_mens_unpivot = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )
    config_cons_mens = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_conso_mens
    )

    # Read data
    df_conso_mens = s3_hook.read_parquet(
        file_name=config_cons_mens.loc[0, "filepath_tmp_s3"],
    )

    # Processing
    df_conso_corr_unpivot = process.process_unpivot_conso_mens(
        df=df_conso_mens, use_conso_corrigee=True
    )
    df_info(df=df_conso_corr_unpivot, df_name=f"DF {selecteur} - Après processing")

    # Export file to s3
    s3_filepath = config_cons_mens_unpivot.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_conso_corr_unpivot.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )


@task(task_id="unpivot_conso_mens_brute")
def unpivot_conso_mens_brute(
    selecteur: str, selecteur_conso_mens: str, **context
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    # Config
    row_selecteur_cons_mens = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_conso_mens
    )
    row_selecteur_cons_mens_unpivot = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )

    # Read data
    df_conso_mens = s3_hook.read_parquet(
        file_name=row_selecteur_cons_mens.loc[0, "filepath_tmp_s3"],
    )

    # Process
    df_conso_corr_unpivot = process.process_unpivot_conso_mens(
        df=df_conso_mens, use_conso_corrigee=False
    )
    df_info(df=df_conso_corr_unpivot, df_name=f"DF {selecteur} - Après processing")

    # Export file to s3
    s3_filepath = row_selecteur_cons_mens_unpivot.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_conso_corr_unpivot.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )


@task(task_id="conso_annuelles")
def conso_annuelles(selecteur: str, selecteur_conso_mens: str, **context) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    row_selecteur_cons_mens = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_conso_mens
    )
    row_selecteur_cons_annuelle = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )

    # Read data
    df_conso_mens = s3_hook.read_parquet(
        file_name=row_selecteur_cons_mens.loc[0, "filepath_tmp_s3"],
    )
    # Processing
    df_info(df=df_conso_mens, df_name=f"DF {selecteur} - Source")
    df_conso_annuelle = process.process_conso_annuelle(df=df_conso_mens)
    df_info(df=df_conso_annuelle, df_name=f"DF {selecteur} - Après processing")

    # Export file to s3
    s3_filepath = row_selecteur_cons_annuelle.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_conso_annuelle.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )


@task(task_id="conso_statut_par_fluide")
def conso_statut_par_fluide(
    selecteur: str, selecteur_conso_annuelle: str, **context
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    row_selecteur_cons_mens = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_conso_annuelle
    )
    row_selecteur_cons_annuelle = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )

    # Read data
    df_conso_annuelle = s3_hook.read_parquet(
        file_name=row_selecteur_cons_mens.loc[0, "filepath_tmp_s3"],
    )
    # Processing
    df_info(df=df_conso_annuelle, df_name=f"DF {selecteur} - Source")
    df_conso_statut_par_fluide = process.process_conso_statut_par_fluide(
        df=df_conso_annuelle
    )
    df_info(df=df_conso_statut_par_fluide, df_name=f"DF {selecteur} - Après processing")

    # Export file to s3
    s3_filepath = row_selecteur_cons_annuelle.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_conso_statut_par_fluide.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )


@task(task_id="conso_avant_2019")
def conso_avant_2019(selecteur: str, selecteur_conso_annuelle: str, **context) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    row_selecteur_cons_avant_2019 = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )
    row_selecteur_cons_annuelle = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_conso_annuelle
    )

    # Read data
    df_conso_annuelle = s3_hook.read_parquet(
        file_name=row_selecteur_cons_annuelle.loc[0, "filepath_tmp_s3"],
    )
    # Processing
    df_info(df=df_conso_annuelle, df_name=f"DF {selecteur_conso_annuelle} - Source")
    df_conso_avant_2019 = process.process_conso_avant_2019(df=df_conso_annuelle)
    df_info(df=df_conso_avant_2019, df_name=f"DF {selecteur} - Après processing")

    # Export file to s3
    s3_filepath = row_selecteur_cons_avant_2019.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_conso_avant_2019.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )


@task(task_id="conso_statut_fluide_global")
def conso_statut_fluide_global(
    selecteur: str, selecteur_conso_statut_par_fluide: str, **context
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    row_selecteur_cons_mens = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_conso_statut_par_fluide
    )
    row_selecteur_cons_annuelle = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )

    # Read data
    df_conso_statut_par_fluide = s3_hook.read_parquet(
        file_name=row_selecteur_cons_mens.loc[0, "filepath_tmp_s3"],
    )
    # Processing
    df_info(
        df=df_conso_statut_par_fluide,
        df_name=f"DF {selecteur_conso_statut_par_fluide} - Source",
    )
    df_conso_statut_fluide_global = process.process_conso_statut_fluide_global(
        df=df_conso_statut_par_fluide
    )
    df_info(
        df=df_conso_statut_fluide_global, df_name=f"DF {selecteur} - Après processing"
    )

    # Export file to s3
    s3_filepath = row_selecteur_cons_annuelle.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_conso_statut_fluide_global.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )


@task(task_id="conso_statut_batiment")
def conso_statut_batiment(
    selecteur: str,
    selecteur_conso_statut_fluide_global: str,
    selecteur_conso_statut_avant_2019: str,
    **context,
) -> None:
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")

    # Get task configs
    config_statut_batiment = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )
    config_conso_statut_fluide_global = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_conso_statut_fluide_global
    )
    config_conso_statut_avant_2019 = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_conso_statut_avant_2019
    )

    # Read data
    df_conso_statut_fluide_global = s3_hook.read_parquet(
        file_name=config_conso_statut_fluide_global.loc[0, "filepath_tmp_s3"],
    )
    df_conso_avant_2019 = s3_hook.read_parquet(
        file_name=config_conso_statut_avant_2019.loc[0, "filepath_tmp_s3"],
    )

    # Processing
    df_info(
        df=df_conso_statut_fluide_global,
        df_name=f"DF {selecteur_conso_statut_fluide_global} - Source 1",
    )
    df_info(
        df=df_conso_avant_2019,
        df_name=f"DF {selecteur_conso_statut_avant_2019} - Source 2",
    )
    df_conso_statut_batiment = process.process_conso_statut_batiment(
        df_conso_statut_fluide_global=df_conso_statut_fluide_global,
        df_conso_avant_2019=df_conso_avant_2019,
    )
    df_info(df=df_conso_statut_batiment, df_name=f"DF {selecteur} - Après processing")

    # Export file to s3
    s3_filepath = config_statut_batiment.loc[0, "filepath_tmp_s3"]
    print(f"Exporting file to < {s3_filepath} >")
    s3_hook.load_bytes(
        bytes_data=df_conso_statut_batiment.to_parquet(
            path=None, index=False
        ),  # convert df to bytes
        key=s3_filepath,
        replace=True,
    )
