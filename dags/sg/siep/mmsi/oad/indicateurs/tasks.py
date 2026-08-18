from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.siep.mmsi.oad.indicateurs import process
from modules.common_tasks.etl import create_task
from modules.common_tasks.file import create_parquet_converter_task
from modules.types.dags import ETLStep, TaskConfig

oad_indic_to_parquet = create_parquet_converter_task(
    selecteur="oad_indic",
    task_params={"task_id": "convert_oad_indicateur_to_parquet"},
    process_func=process.process_oad_indic,
)


@task_group
def tasks_oad_indicateurs():
    oad_indic = create_task(
        task_config=TaskConfig(task_id="oad_indic"),
        output_selecteur="oad_indic",
        input_selecteurs=["oad_indic", "biens"],
        steps=[ETLStep(fn=process.filtrer_oad_indic, read_data=True)],
    )
    accessibilite = create_task(
        task_config=TaskConfig(task_id="accessibilite"),
        output_selecteur="accessibilite",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_accessibilite, read_data=True)],
    )
    accessibilite_detail = create_task(
        task_config=TaskConfig(task_id="accessibilite_detail"),
        output_selecteur="accessibilite_detail",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_accessibilite_detail, read_data=True)],
    )
    bacs = create_task(
        task_config=TaskConfig(task_id="bacs"),
        output_selecteur="bacs",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_bacs, read_data=True)],
    )
    bails = create_task(
        task_config=TaskConfig(task_id="bails"),
        output_selecteur="bails",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_bails, read_data=True)],
    )
    couts = create_task(
        task_config=TaskConfig(task_id="couts"),
        output_selecteur="couts",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_couts, read_data=True)],
    )
    deet_energie_ges = create_task(
        task_config=TaskConfig(task_id="deet_energie_ges"),
        output_selecteur="deet_energie_ges",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_deet_energie, read_data=True)],
    )
    etat_de_sante = create_task(
        task_config=TaskConfig(task_id="etat_de_sante"),
        output_selecteur="etat_de_sante",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_eds, read_data=True)],
    )
    exploitation = create_task(
        task_config=TaskConfig(task_id="exploitation"),
        output_selecteur="exploitation",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_exploitation, read_data=True)],
    )
    note = create_task(
        task_config=TaskConfig(task_id="note"),
        output_selecteur="note",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_notes, read_data=True)],
    )
    effectif = create_task(
        task_config=TaskConfig(task_id="effectif"),
        output_selecteur="effectif",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_effectif, read_data=True)],
    )
    proprietaire = create_task(
        task_config=TaskConfig(task_id="proprietaire"),
        output_selecteur="proprietaire",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_proprietaire, read_data=True)],
    )
    reglementation = create_task(
        task_config=TaskConfig(task_id="reglementation"),
        output_selecteur="reglementation",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_reglementation, read_data=True)],
    )
    surface = create_task(
        task_config=TaskConfig(task_id="surface"),
        output_selecteur="surface",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_surface, read_data=True)],
    )
    typologie = create_task(
        task_config=TaskConfig(task_id="typologie"),
        output_selecteur="typologie",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_typologie, read_data=True)],
    )
    valeur = create_task(
        task_config=TaskConfig(task_id="valeur"),
        output_selecteur="valeur",
        input_selecteurs=["oad_indic"],
        steps=[ETLStep(fn=process.process_valeur, read_data=True)],
    )
    localisation = create_task(
        task_config=TaskConfig(task_id="localisation"),
        output_selecteur="localisation",
        input_selecteurs=["oad_carac", "oad_indic", "biens"],
        steps=[ETLStep(fn=process.process_localisation, read_data=True)],
    )
    strategie = create_task(
        task_config=TaskConfig(task_id="strategie"),
        output_selecteur="strategie",
        input_selecteurs=["oad_carac", "oad_indic", "biens"],
        steps=[ETLStep(fn=process.process_strategie, read_data=True)],
    )

    chain(
        oad_indic(),
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
