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

from dags.sg.siep.mmsi.oad.indicateurs import process


def create_task_oad_indic(
    selecteur: str, nom_projet: str = None, process_func: Callable = None
):
    @task(task_id=selecteur)
    def _task(**context):
        # Hooks
        s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
        # Get config values related to the task
        nom_projet = context.get("params").get("nom_projet", None)
        if nom_projet is None:
            raise ValueError(
                "La variable nom_projet n'a pas été définie au niveau du DAG !"
            )
        row_oad_indic = get_storage_rows(nom_projet=nom_projet, selecteur="oad_indic")
        row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
        required_cols = get_required_cols(nom_projet=nom_projet, selecteur=selecteur)
        config_bien = get_storage_rows(nom_projet=nom_projet, selecteur="biens")

        # Main part
        df = s3_hook.read_parquet(
            file_name=row_oad_indic.loc[0, "filepath_tmp_s3"],
            columns=required_cols["colname_dest"].to_list(),
        )
        df_bien = s3_hook.read_parquet(file_name=config_bien.loc[0, "filepath_tmp_s3"])

        df_info(df=df, df_name=f"DF {selecteur} - INITIALISATION")

        print("Start - Cleaning Process")
        df = process.filter_bien(df=df, df_bien=df_bien)
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


@task(task_id="convert_oad_indic_to_parquet")
def convert_oad_indic_to_parquet(
    nom_projet: str,
    selecteur: str,
) -> None:
    # Variables
    db_hook = PostgresHook(postgres_conn_id="db_data_store")
    db_conf_hook = PostgresHook(postgres_conn_id="db_depose_fichier")
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

    df_oad_indic = s3_hook.read_excel(
        file_name=row_selecteur.loc[0, "filepath_source_s3"]
    )

    df_info(df=df_oad_indic, df_name="DF OAD indicateurs - INITIALISATION")

    # Cleaning df
    cols_mapping = get_cols_mapping(
        nom_projet=nom_projet, db_hook=db_conf_hook, selecteur=selecteur
    )
    cols_oad_indic = format_cols_mapping(df_cols_map=cols_mapping)

    df_oad_indic = (
        df_oad_indic.set_axis(
            [" ".join(colname.split()) for colname in df_oad_indic.columns],
            axis="columns",
        )
        .rename(columns=cols_oad_indic, errors="raise")
        .dropna(subset=["code_bat_ter"])
    )
    df_oad_indic = df_oad_indic.drop_duplicates(
        subset=["code_bat_ter"], ignore_index=True
    )

    # Removing biens which are not presents in table bien
    biens = db_hook.get_pandas_df(sql="SELECT code_bat_ter FROM siep.bien;")
    biens = biens.loc[:, "code_bat_ter"].to_list()
    df_oad_indic = df_oad_indic[df_oad_indic["code_bat_ter"].isin(biens)]

    df_info(df=df_oad_indic, df_name="DF OAD indicateurs - After processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_oad_indic.to_parquet(path=None, index=False),
        key=row_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="strategie")
def task_strategie(selecteur: str, **context):
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    # Get config values related to the task
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    row_oad_carac = get_storage_rows(nom_projet=nom_projet, selecteur="oad_carac")
    row_oad_indic = get_storage_rows(nom_projet=nom_projet, selecteur="oad_indic")
    row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    config_bien = get_storage_rows(nom_projet=nom_projet, selecteur="biens")

    required_cols = get_required_cols(nom_projet=nom_projet, selecteur=selecteur)[
        "colname_dest"
    ]
    common = ["code_bat_ter"]
    required_cols_oad_carac = common + ["perimetre_spsi_initial", "perimetre_spsi_maj"]
    required_cols_oad_indic = common + [
        column for column in required_cols if column not in required_cols_oad_carac
    ]

    print(required_cols_oad_indic)
    print(required_cols_oad_carac)

    # Main part
    df_oad_carac = s3_hook.read_parquet(
        file_name=row_oad_carac.loc[0, "filepath_tmp_s3"],
        columns=required_cols_oad_carac,
    )
    df_oad_indic = s3_hook.read_parquet(
        file_name=row_oad_indic.loc[0, "filepath_tmp_s3"],
        columns=required_cols_oad_indic,
    )
    df_bien = s3_hook.read_parquet(file_name=config_bien.loc[0, "filepath_tmp_s3"])

    df_info(df=df_oad_carac, df_name=f"DF {selecteur}/OAD_CARAC - INITIALISATION")
    df_info(df=df_oad_indic, df_name=f"DF {selecteur}/OAD_INDIC - INITIALISATION")

    print("Start - Cleaning Process")
    df = process.process_strategie(df_oad_carac=df_oad_carac, df_oad_indic=df_oad_indic)
    df = process.filter_bien(df=df, df_bien=df_bien)
    print("End - Cleaning Process")
    df_info(df=df, df_name=f"DF {selecteur} - After Cleaning Process")

    # Export
    s3_hook.load_bytes(
        bytes_data=df.to_parquet(path=None, index=False),
        key=row_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="localisation")
def task_localisation(selecteur: str, **context):
    # Hooks
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci", bucket="dsci")
    # Get config values related to the task
    nom_projet = context.get("params").get("nom_projet", None)
    if nom_projet is None:
        raise ValueError(
            "La variable nom_projet n'a pas été définie au niveau du DAG !"
        )
    row_oad_carac = get_storage_rows(nom_projet=nom_projet, selecteur="oad_carac")
    row_oad_indic = get_storage_rows(nom_projet=nom_projet, selecteur="oad_indic")
    row_selecteur = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    config_bien = get_storage_rows(nom_projet=nom_projet, selecteur="biens")

    required_cols_oad_carac = get_required_cols(
        nom_projet=nom_projet, selecteur="localisation_carac"
    ).get("colname_dest", None)
    required_cols_oad_indic = get_required_cols(
        nom_projet=nom_projet, selecteur="localisation_indic"
    ).get("colname_dest", None)

    print(required_cols_oad_indic)
    print(required_cols_oad_carac)

    # Main part
    df_oad_carac = s3_hook.read_parquet(
        file_name=row_oad_carac.loc[0, "filepath_tmp_s3"],
        columns=required_cols_oad_carac,
    )
    df_oad_indic = s3_hook.read_parquet(
        file_name=row_oad_indic.loc[0, "filepath_tmp_s3"],
        columns=required_cols_oad_indic,
    )
    df_bien = s3_hook.read_parquet(file_name=config_bien.loc[0, "filepath_tmp_s3"])

    df_info(df=df_oad_carac, df_name=f"DF {selecteur}/OAD_CARAC - INITIALISATION")
    df_info(df=df_oad_indic, df_name=f"DF {selecteur}/OAD_INDIC - INITIALISATION")

    print("Start - Cleaning Process")
    df = process.process_localisation(
        df_oad_carac=df_oad_carac, df_oad_indic=df_oad_indic
    )
    df = process.filter_bien(df=df, df_bien=df_bien)
    print("End - Cleaning Process")
    df_info(df=df, df_name=f"DF {selecteur} - After Cleaning Process")

    # Export
    s3_hook.load_bytes(
        bytes_data=df.to_parquet(path=None, index=False),
        key=row_selecteur.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task_group
def tasks_oad_indicateurs():
    accessibilite = create_task_oad_indic(
        selecteur="accessibilite",
        process_func=process.process_accessibilite,
    )
    accessibilite_detail = create_task_oad_indic(
        selecteur="accessibilite_detail",
        process_func=process.process_accessibilite_detail,
    )
    bacs = create_task_oad_indic(
        selecteur="bacs",
        process_func=process.process_bacs,
    )
    bails = create_task_oad_indic(
        selecteur="bails",
        process_func=process.process_bails,
    )
    couts = create_task_oad_indic(
        selecteur="couts",
        process_func=process.process_couts,
    )
    deet_energie_ges = create_task_oad_indic(
        selecteur="deet_energie_ges",
        process_func=process.process_deet_energie,
    )
    etat_de_sante = create_task_oad_indic(
        selecteur="etat_de_sante",
        process_func=process.process_eds,
    )
    exploitation = create_task_oad_indic(
        selecteur="exploitation",
        process_func=process.process_exploitation,
    )
    note = create_task_oad_indic(
        selecteur="note",
        process_func=process.process_notes,
    )
    effectif = create_task_oad_indic(
        selecteur="effectif",
        process_func=process.process_effectif,
    )
    proprietaire = create_task_oad_indic(
        selecteur="proprietaire",
        process_func=process.process_proprietaire,
    )
    reglementation = create_task_oad_indic(
        selecteur="reglementation",
        process_func=process.process_reglementation,
    )
    surface = create_task_oad_indic(
        selecteur="surface",
        process_func=process.process_surface,
    )
    typologie = create_task_oad_indic(
        selecteur="typologie",
        process_func=process.process_typologie,
    )
    valeur = create_task_oad_indic(
        selecteur="valeur",
        process_func=process.process_valeur,
    )
    localisation = task_localisation(
        selecteur="localisation",
    )
    strategie = task_strategie(
        selecteur="strategie",
    )
    chain(
        [
            accessibilite,
            accessibilite_detail,
            bacs,
            bails,
            couts,
            deet_energie_ges,
            etat_de_sante,
            exploitation,
            localisation,
            note,
            effectif,
            proprietaire,
            reglementation,
            strategie,
            surface,
            typologie,
            valeur,
        ],
    )
