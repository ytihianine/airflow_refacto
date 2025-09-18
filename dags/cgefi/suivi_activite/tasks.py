from typing import Callable
from airflow.decorators import task, task_group
from airflow.models.baseoperator import chain

from utils.file_handler import MinioFileHandler
from utils.df_utility import df_info
from utils.common.tasks_sql import get_conn_from_s3_sqlite, get_data_from_s3_sqlite_file
from utils.common.config_func import get_storage_rows

from dags.cgefi.suivi_activite import process


def create_task(
    selecteur: str,
    selecteur_doc: str = "grist_doc",
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
        # doc_config = get_storage_rows(nom_projet=nom_projet, selecteur=selecteur_doc)
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


@task_group
def referentiels() -> None:
    ref_actif_type = create_task(
        selecteur="ref_actif_type", process_func=process.process_ref_actif_type
    )
    ref_actif_sous_type = create_task(
        selecteur="ref_actif_sous_type",
        process_func=process.process_ref_actif_sous_type,
    )
    ref_actions = create_task(
        selecteur="ref_actions", process_func=process.process_ref_actions
    )
    ref_formation_specialite = create_task(
        selecteur="ref_formation_specialite",
        process_func=process.process_ref_formation_specialite,
    )
    ref_passif_type = create_task(
        selecteur="ref_passif_type", process_func=process.process_ref_passif_type
    )
    ref_passif_sous_type = create_task(
        selecteur="ref_passif_sous_type",
        process_func=process.process_ref_passif_sous_type,
    )
    ref_secteur_professionnel = create_task(
        selecteur="ref_secteur_professionnel",
        process_func=process.process_ref_secteur_professionnel,
    )
    ref_type_de_frais = create_task(
        selecteur="ref_type_de_frais", process_func=process.process_ref_type_de_frais
    )
    ref_sous_type_de_frais = create_task(
        selecteur="ref_sous_type_de_frais",
        process_func=process.process_ref_sous_type_de_frais,
    )
    ref_type_organisme = create_task(
        selecteur="ref_type_organisme", process_func=process.process_ref_type_organisme
    )

    # Ordre des tâches
    chain(
        [
            ref_actif_type,
            ref_actif_sous_type,
            ref_actions,
            ref_formation_specialite,
            ref_passif_type,
            ref_passif_sous_type,
            ref_secteur_professionnel,
            ref_type_de_frais,
            ref_sous_type_de_frais,
            ref_type_organisme,
        ]
    )


@task_group
def processus_4() -> None:
    pass


@task_group
def processus_6() -> None:
    process_6_a01 = create_task(
        selecteur="process_6_a01", process_func=process.process_process_6_a01
    )
    process_6_b01 = create_task(
        selecteur="process_6_b01", process_func=process.process_process_6_b01
    )
    process_6_d01 = create_task(
        selecteur="process_6_d01", process_func=process.process_process_6_d01
    )
    process_6_e01 = create_task(
        selecteur="process_6_e01", process_func=process.process_process_6_e01
    )
    process_6_g02 = create_task(
        selecteur="process_6_g02", process_func=process.process_process_6_g02
    )
    process_6_g03 = create_task(
        selecteur="process_6_g03", process_func=process.process_process_6_g03
    )
    process_6_h01 = create_task(
        selecteur="process_6_h01", process_func=process.process_process_6_h01
    )

    # Ordre des tâches
    chain(
        [
            process_6_a01,
            process_6_b01,
            process_6_d01,
            process_6_e01,
            process_6_g02,
            process_6_g03,
            process_6_h01,
        ]
    )


@task_group
def processus_atpro() -> None:
    process_atpro_a01 = create_task(
        selecteur="process_atpro_a01", process_func=process.process_process_atpro_a01
    )
    process_atpro_f01 = create_task(
        selecteur="process_atpro_f01", process_func=process.process_process_atpro_f01
    )
    process_atpro_f02 = create_task(
        selecteur="process_atpro_f02", process_func=process.process_process_atpro_f02
    )
    process_atpro_g02 = create_task(
        selecteur="process_atpro_g02", process_func=process.process_process_atpro_g02
    )
    process_atpro_h01 = create_task(
        selecteur="process_atpro_h01", process_func=process.process_process_atpro_h01
    )
    process_atpro_j01 = create_task(
        selecteur="process_atpro_j01", process_func=process.process_process_atpro_j01
    )

    # Ordre des tâches
    chain(
        [
            process_atpro_a01,
            process_atpro_f01,
            process_atpro_f02,
            process_atpro_g02,
            process_atpro_h01,
            process_atpro_j01,
        ]
    )


@task_group
def informations_generales() -> None:
    controleur = create_task(
        selecteur="controleur", process_func=process.process_controleur
    )
    organisme = create_task(
        selecteur="organisme", process_func=process.process_organisme
    )
    organisme_type = create_task(
        selecteur="organisme_type", process_func=process.process_organisme_type
    )
    region_atpro = create_task(
        selecteur="region_atpro", process_func=process.process_region_atpro
    )

    # Ordre des tâches
    chain([controleur, organisme, organisme_type, region_atpro])
