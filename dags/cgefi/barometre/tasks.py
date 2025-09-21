from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.etl import create_file_etl_task, create_multi_files_input_etl_task

from dags.cgefi.barometre import process

SELECTEUR_BAROMETRE = "barometre"
SELECTEUR_ORGA_MERGE = "organisme_merge"


@task_group()
def source_files() -> None:
    cartographie = create_file_etl_task(
        selecteur="cartographie",
        process_func=process.process_cartographie,
    )
    efc = create_file_etl_task(
        selecteur="efc",
        process_func=process.process_efc,
    )
    recommandation = create_file_etl_task(
        selecteur="recommandation",
        process_func=process.process_recommandation,
    )
    fiche_signaletique = create_file_etl_task(
        selecteur="fiche_signaletique",
        process_func=process.process_fiches_signaletiques,
    )
    rapport_annuel = create_file_etl_task(
        selecteur="rapport_annuel",
        process_func=process.process_rapport_annuel,
    )
    organisme = create_file_etl_task(
        selecteur="organisme",
        process_func=process.process_organisme,
    )
    organisme_hc = create_file_etl_task(
        selecteur="organisme_hors_corpus",
        process_func=process.process_organisme_hors_corpus,
    )

    # ordre des tâches
    chain(
        [
            cartographie(),
            efc(),
            recommandation(),
            fiche_signaletique(),
            rapport_annuel(),
            organisme(),
            organisme_hc(),
        ]
    )
