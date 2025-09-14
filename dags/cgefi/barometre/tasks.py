from typing import Callable
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils.file_handler import MinioFileHandler
from utils.config.tasks import (
    get_storage_rows,
    get_cols_mapping,
    format_cols_mapping,
)
from utils.dataframe import df_info

from dags.cgefi.barometre import process

SELECTEUR_BAROMETRE = "barometre"
SELECTEUR_ORGA_MERGE = "organisme_merge"


def create_task(
    selecteur: str,
    sqlite_file_s3_filepath: str,
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
        df = process_func(df)
        df_info(df=df, df_name=f"{selecteur} - Après processing")

        # Export
        s3_hook.load_bytes(
            bytes_data=df.to_parquet(path=None, index=False),
            key=config_selecteur.loc[0, "filepath_tmp_s3"],
            replace=True,
        )

    return _task()


@task(task_id="organisme")
def organisme(nom_projet: str, selecteur: str, selecteur_organisme_hc: str) -> None:
    # sigle -> HC = Hors corpus
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_orga = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    config_orga_hc = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_organisme_hc
    )
    config_orga_merge = get_storage_rows(
        nom_projet=nom_projet, selecteur=SELECTEUR_ORGA_MERGE
    )
    df_colnames = get_cols_mapping(nom_projet=nom_projet, selecteur=selecteur)
    colnames_mapping = format_cols_mapping(df_cols_map=df_colnames)

    # Read data
    df_orga = s3_hook.read_excel(file_name=config_orga.loc[0, "filepath_source_s3"])
    df_orga_hc = s3_hook.read_excel(
        file_name=config_orga_hc.loc[0, "filepath_source_s3"]
    )

    # Process
    df_info(df=df_orga, df_name=f"{selecteur} - Source")
    df_orga_hc = process.process_organisme_hors_corpus(
        df=df_orga_hc, cols_to_rename=colnames_mapping
    )
    df_orga = process.process_organisme(df=df_orga, cols_to_rename=colnames_mapping)
    df_orga = process.concat_df(list_df=[df_orga, df_orga_hc])
    df_info(
        df=df_orga,
        df_name=f"{SELECTEUR_BAROMETRE} + {selecteur} - Après processing",
    )
    df_orga, df_orga_merge = process.split_df_organisme(df_orga=df_orga)

    # Export
    s3_hook.load_bytes(
        bytes_data=df_orga.to_parquet(path=None, index=False),
        key=config_orga.loc[0, "filepath_tmp_s3"],
        replace=True,
    )
    s3_hook.load_bytes(
        bytes_data=df_orga_merge.to_parquet(path=None, index=False),
        key=config_orga_merge.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="cartographie")
def cartographie(nom_projet: str, selecteur: str) -> None:
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_orga_merge = get_storage_rows(
        nom_projet=nom_projet, selecteur=SELECTEUR_ORGA_MERGE
    )
    config_carto = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    df_colnames = get_cols_mapping(nom_projet=nom_projet, selecteur=selecteur)
    colnames_mapping = format_cols_mapping(df_cols_map=df_colnames)

    # Read data
    df_orga_merge = s3_hook.read_parquet(
        file_name=config_orga_merge.loc[0, "filepath_tmp_s3"],
    )
    df_carto = s3_hook.read_excel(
        file_name=config_carto.loc[0, "filepath_source_s3"],
    )

    # Process
    df_info(df=df_carto, df_name=f"{selecteur} - Source")
    df_info(df=df_orga_merge, df_name=f"{SELECTEUR_ORGA_MERGE} - Source")
    df_carto = process.process_cartographie(
        df=df_carto, df_orga_merge=df_orga_merge, cols_to_rename=colnames_mapping
    )
    df_info(df=df_carto, df_name="DF ORGANISME - Ajout cartographies")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_carto.to_parquet(path=None, index=False),
        key=config_carto.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="efc")
def efc(nom_projet: str, selecteur: str) -> None:
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_orga_merge = get_storage_rows(
        nom_projet=nom_projet, selecteur=SELECTEUR_ORGA_MERGE
    )
    config_efc = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)
    df_colnames = get_cols_mapping(nom_projet=nom_projet, selecteur=selecteur)
    colnames_mapping = format_cols_mapping(df_cols_map=df_colnames)

    # Read data
    df_orga_merge = s3_hook.read_parquet(
        file_name=config_orga_merge.loc[0, "filepath_tmp_s3"],
    )
    df_efc = s3_hook.read_excel(
        file_name=config_efc.loc[0, "filepath_source_s3"],
    )

    # Process
    df_info(df=df_efc, df_name=f"{selecteur} - Source")
    df_efc = process.process_efc(
        df=df_efc, df_orga_merge=df_orga_merge, cols_to_rename=colnames_mapping
    )
    df_info(df=df_efc, df_name=f"{selecteur} - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_efc.to_parquet(path=None, index=False),
        key=config_efc.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="recommandation")
def recommandation(nom_projet: str, selecteur: str) -> None:
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_recommandation = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur)

    # Read data
    df_reco = s3_hook.read_excel(
        file_name=config_recommandation.loc[0, "filepath_source_s3"],
    )
    df_colnames = get_cols_mapping(nom_projet=nom_projet, selecteur=selecteur)
    colnames_mapping = format_cols_mapping(df_cols_map=df_colnames)

    # Process
    df_info(df=df_reco, df_name=f"{selecteur} - Source")
    df_reco = process.process_recommandation(
        df=df_reco, cols_to_rename=colnames_mapping
    )
    df_info(df=df_reco, df_name=f"{selecteur} - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_reco.to_parquet(path=None, index=False),
        key=config_recommandation.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="fiche_signaletique")
def fiche_signaletique(nom_projet: str, selecteur: str) -> None:
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_fiche_signaletique = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )
    df_colnames = get_cols_mapping(nom_projet=nom_projet, selecteur=selecteur)
    colnames_mapping = format_cols_mapping(df_cols_map=df_colnames)

    # Read data
    df_fiches_signaletiques = s3_hook.read_excel(
        file_name=config_fiche_signaletique.loc[0, "filepath_source_s3"],
    )

    # Process
    df_info(df=df_fiches_signaletiques, df_name=f"{selecteur} - Source")
    df_fiches_signaletiques = process.process_fiches_signaletiques(
        df=df_fiches_signaletiques, cols_to_rename=colnames_mapping
    )
    df_info(df=df_fiches_signaletiques, df_name=f"{selecteur} - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_fiches_signaletiques.to_parquet(path=None, index=False),
        key=config_fiche_signaletique.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="rapport_hors_corpus")
def rapport_hors_corpus(nom_projet: str, selecteur: str) -> None:
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_rapport_hors_corpus = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur
    )

    # Read data
    df_hors_corpus = s3_hook.read_excel(
        file_name=config_rapport_hors_corpus.loc[0, "filepath_source_s3"],
    )

    # Process
    df_info(df=df_hors_corpus, df_name=f"{selecteur} - Source")
    df_hors_corpus = process.process_rapport_hors_corpus(df=df_hors_corpus)
    df_info(df=df_hors_corpus, df_name=f"{selecteur} - Après processing")

    # Export
    s3_hook.load_bytes(
        bytes_data=df_hors_corpus.to_parquet(path=None, index=False),
        key=config_rapport_hors_corpus.loc[0, "filepath_tmp_s3"],
        replace=True,
    )


@task(task_id="rapport_annuel")
def rapport_annuel(
    nom_projet: str, selecteur_rapport_annuel: str, selecteur_date_retour_attendue: str
) -> None:
    # Variables
    s3_hook = MinioFileHandler(connection_id="minio_bucket_dsci")

    # Config
    config_orga_merge = get_storage_rows(
        nom_projet=nom_projet, selecteur=SELECTEUR_ORGA_MERGE
    )
    config_rapport_annuel = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_rapport_annuel
    )
    df_colnames = get_cols_mapping(
        nom_projet=nom_projet, selecteur=selecteur_rapport_annuel
    )
    colnames_mapping_rap_an = format_cols_mapping(df_cols_map=df_colnames)

    config_date_retour_attendue = get_storage_rows(
        nom_projet=nom_projet, selecteur=selecteur_date_retour_attendue
    )
    df_colnames = get_cols_mapping(
        nom_projet=nom_projet, selecteur=selecteur_date_retour_attendue
    )
    colnames_mapping_date_ra = format_cols_mapping(df_cols_map=df_colnames)

    # Read data
    df_orga_merge = s3_hook.read_parquet(
        file_name=config_orga_merge.loc[0, "filepath_tmp_s3"],
    )
    df_rapport_annuel = s3_hook.read_excel(
        file_name=config_rapport_annuel.loc[0, "filepath_source_s3"],
    )
    df_date_retour_attendue = s3_hook.read_excel(
        file_name=config_date_retour_attendue.loc[0, "filepath_source_s3"],
    )

    # Process
    df_info(df=df_rapport_annuel, df_name=f"{selecteur_rapport_annuel} - Source")
    df_date_retour_attendue = process.process_date_retour_attendue(
        df=df_date_retour_attendue, cols_to_rename=colnames_mapping_date_ra
    )
    df_rapport_annuel = process.process_rapport_annuel(
        df=df_rapport_annuel,
        df_orga_merge=df_orga_merge,
        df_date_retour_attendu=df_date_retour_attendue,
        cols_to_rename=colnames_mapping_rap_an,
    )
    df_info(
        df=df_rapport_annuel, df_name=f"{selecteur_rapport_annuel} - Après processing"
    )

    # Export
    s3_hook.load_bytes(
        bytes_data=df_rapport_annuel.to_parquet(path=None, index=False),
        key=config_rapport_annuel.loc[0, "filepath_tmp_s3"],
        replace=True,
    )
