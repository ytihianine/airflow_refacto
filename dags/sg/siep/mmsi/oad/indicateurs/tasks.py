from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.tasks.file import create_parquet_converter_task
from utils.tasks.etl import create_multi_files_input_etl_task

from dags.sg.siep.mmsi.oad.indicateurs import process


oad_indic_to_parquet = create_parquet_converter_task(
    selecteur="oad_indic",
    task_params={"task_id": "convert_oad_indicateur_to_parquet"},
    process_func=process.process_oad_indic,
)


@task_group
def tasks_oad_indicateurs():
    accessibilite = create_multi_files_input_etl_task(
        output_selecteur="accessibilite",
        input_selecteurs=["oad_indic"],
        process_func=process.process_accessibilite,
        use_required_cols=True,
    )
    accessibilite_detail = create_multi_files_input_etl_task(
        output_selecteur="accessibilite_detail",
        input_selecteurs=["oad_indic"],
        process_func=process.process_accessibilite_detail,
        use_required_cols=True,
    )
    bacs = create_multi_files_input_etl_task(
        output_selecteur="bacs",
        input_selecteurs=["oad_indic"],
        process_func=process.process_bacs,
        use_required_cols=True,
    )
    bails = create_multi_files_input_etl_task(
        output_selecteur="bails",
        input_selecteurs=["oad_indic"],
        process_func=process.process_bails,
        use_required_cols=True,
    )
    couts = create_multi_files_input_etl_task(
        output_selecteur="couts",
        input_selecteurs=["oad_indic"],
        process_func=process.process_couts,
        use_required_cols=True,
    )
    deet_energie_ges = create_multi_files_input_etl_task(
        output_selecteur="deet_energie_ges",
        input_selecteurs=["oad_indic"],
        process_func=process.process_deet_energie,
        use_required_cols=True,
    )
    etat_de_sante = create_multi_files_input_etl_task(
        output_selecteur="etat_de_sante",
        input_selecteurs=["oad_indic"],
        process_func=process.process_eds,
        use_required_cols=True,
    )
    exploitation = create_multi_files_input_etl_task(
        output_selecteur="exploitation",
        input_selecteurs=["oad_indic"],
        process_func=process.process_exploitation,
        use_required_cols=True,
    )
    note = create_multi_files_input_etl_task(
        output_selecteur="note",
        input_selecteurs=["oad_indic"],
        process_func=process.process_notes,
        use_required_cols=True,
    )
    effectif = create_multi_files_input_etl_task(
        output_selecteur="effectif",
        input_selecteurs=["oad_indic"],
        process_func=process.process_effectif,
        use_required_cols=True,
    )
    proprietaire = create_multi_files_input_etl_task(
        output_selecteur="proprietaire",
        input_selecteurs=["oad_indic"],
        process_func=process.process_proprietaire,
        use_required_cols=True,
    )
    reglementation = create_multi_files_input_etl_task(
        output_selecteur="reglementation",
        input_selecteurs=["oad_indic"],
        process_func=process.process_reglementation,
        use_required_cols=True,
    )
    surface = create_multi_files_input_etl_task(
        output_selecteur="surface",
        input_selecteurs=["oad_indic"],
        process_func=process.process_surface,
        use_required_cols=True,
    )
    typologie = create_multi_files_input_etl_task(
        output_selecteur="typologie",
        input_selecteurs=["oad_indic"],
        process_func=process.process_typologie,
        use_required_cols=True,
    )
    valeur = create_multi_files_input_etl_task(
        output_selecteur="valeur",
        input_selecteurs=["oad_indic"],
        process_func=process.process_valeur,
        use_required_cols=True,
    )
    localisation = create_multi_files_input_etl_task(
        output_selecteur="localisation",
        input_selecteurs=["oad_carac", "oad_indic", "biens"],
        process_func=process.process_localisation,
        use_required_cols=False,
    )
    strategie = create_multi_files_input_etl_task(
        output_selecteur="strategie",
        input_selecteurs=["oad_carac", "oad_indic", "biens"],
        process_func=process.process_strategie,
        use_required_cols=False,
    )
    chain(
        [
            accessibilite(),
            accessibilite_detail(),
            bacs(),
            bails(),
            couts(),
            deet_energie_ges(),
            etat_de_sante(),
            exploitation(),
            localisation(),
            note(),
            effectif(),
            proprietaire(),
            reglementation(),
            strategie(),
            surface(),
            typologie(),
            valeur(),
        ],
    )
