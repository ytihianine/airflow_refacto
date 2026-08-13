from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.dge.carto_rem.grist import actions, process
from modules.common_tasks.etl import (
    create_task,
)
from modules.common_tasks.grist import generic_grist_processing
from modules.types.dags import ETLStep, TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy
from wrapt import partial


@task_group
def referentiels() -> None:
    ref_base_remuneration = ETLTask(
        task_config=TaskConfig(task_id="ref_base_remuneration"),
        target="ref_base_remuneration",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["base_remuneration"],
                ),
                input_key="ref_base_remuneration",
                output_key="ref_base_remuneration",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    ref_base_revalorisation = ETLTask(
        task_config=TaskConfig(task_id="ref_base_revalorisation"),
        target="ref_base_revalorisation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["base_revalorisation"],
                ),
                input_key="ref_base_revalorisation",
                output_key="ref_base_revalorisation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_niveau_diplome = ETLTask(
        task_config=TaskConfig(task_id="ref_niveau_diplome"),
        target="ref_niveau_diplome",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["niveau_diplome"],
                ),
                input_key="ref_niveau_diplome",
                output_key="ref_niveau_diplome",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_valeur_point_indice = ETLTask(
        task_config=TaskConfig(task_id="ref_valeur_point_indice"),
        target="ref_valeur_point_indice",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(generic_grist_processing, date_columns=["date_d_application"]),
                input_key="ref_valeur_point_indice",
                output_key="ref_valeur_point_indice",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_categorie_ecole = ETLTask(
        task_config=TaskConfig(task_id="ref_categorie_ecole"),
        target="ref_categorie_ecole",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(generic_grist_processing, txt_columns=["categorie_d_ecole"]),
                input_key="ref_categorie_ecole",
                output_key="ref_categorie_ecole",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_libelle_diplome = ETLTask(
        task_config=TaskConfig(task_id="ref_libelle_diplome"),
        target="ref_libelle_diplome",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "categorie_ecole": "id_categorie_ecole",
                        "niveau_diplome_associe": "id_niveau_diplome_associe",
                    },
                    txt_columns=["libelle_diplome"],
                    ref_columns=["id_categorie_ecole", "id_niveau_diplome_associe"],
                ),
                input_key="ref_libelle_diplome",
                output_key="ref_libelle_diplome",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    ref_position = ETLTask(
        task_config=TaskConfig(task_id="ref_position"),
        target="ref_position",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"niveau_diplome": "id_niveau_diplome"},
                    ref_columns=["id_niveau_diplome"],
                ),
                input_key="ref_position",
                output_key="ref_position",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_fonction_dge = ETLTask(
        task_config=TaskConfig(task_id="ref_fonction_dge"),
        target="ref_fonction_dge",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["fonction_dge", "fonction_dge_libelle_long"],
                ),
                input_key="ref_fonction_dge",
                output_key="ref_fonction_dge",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # ordre des tâches
    chain(
        [
            ref_base_remuneration.create_task(),
            ref_base_revalorisation.create_task(),
            ref_niveau_diplome.create_task(),
            ref_valeur_point_indice.create_task(),
            ref_categorie_ecole.create_task(),
            ref_libelle_diplome.create_task(),
            ref_position.create_task(),
            ref_fonction_dge.create_task(),
        ]
    )


@task_group
def source_grist() -> None:
    agent = ETLTask(
        task_config=TaskConfig(task_id="agent"),
        target="agent",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                ),
                input_key="agent",
                output_key="agent",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    agent_diplome = ETLTask(
        task_config=TaskConfig(task_id="agent_diplome"),
        target="agent_diplome",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "niveau_diplome_associe": "id_niveau_diplome_associe",
                        "categorie_d_ecole": "id_categorie_d_ecole",
                        "libelle_diplome": "id_libelle_diplome",
                    },
                    ref_columns=[
                        "id_libelle_diplome",
                        "id_niveau_diplome_associe",
                        "id_categorie_d_ecole",
                    ],
                ),
                input_key="agent_diplome",
                output_key="agent_diplome",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    agent_revalorisation = ETLTask(
        task_config=TaskConfig(task_id="agent_revalorisation"),
        target="agent_revalorisation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"base_revalorisation": "id_base_revalorisation"},
                    txt_columns=["historique"],
                    date_columns=["date_dernier_renouvellement", "date_derniere_revalorisation"],
                    ref_columns=["id_base_revalorisation"],
                    custom_fn=process.process_agent_revalorisation,
                ),
                input_key="agent_revalorisation",
                output_key="agent_revalorisation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    agent_revalorisation_proposition = ETLTask(
        task_config=TaskConfig(task_id="agent_revalorisation_proposition"),
        target="agent_revalorisation_proposition",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"base_revalorisation": "id_base_revalorisation"},
                    ref_columns=["id_base_revalorisation"],
                ),
                input_key="agent_revalorisation_proposition",
                output_key="agent_revalorisation_proposition",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    agent_contrat_complement = ETLTask(
        task_config=TaskConfig(task_id="agent_contrat_complement"),
        target="agent_contrat_complement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "date_d_entree_a_la_dge": "date_entree_dge",
                        "fonction_dge": "id_fonction_dge",
                        "duree_contrat_en_cours": "duree_contrat_en_cours_dge",
                        "duree_contrat_en_cours_auto": "duree_contrat_en_cours_auto_dge",
                    },
                    date_columns=[
                        "date_premier_contrat_mef",
                        "date_entree_dge",
                        "date_de_cdisation",
                    ],
                    ref_columns=["id_fonction_dge"],
                    custom_fn=process.process_agent_contrat_complement,
                ),
                input_key="agent_contrat_complement",
                output_key="agent_contrat_complement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    agent_remuneration_complement = ETLTask(
        task_config=TaskConfig(task_id="agent_remuneration_complement"),
        target="agent_remuneration_complement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "part_variable_collective": "plafond_part_variable_collective",
                        "base_remuneration": "id_base_remuneration",
                    },
                    txt_columns=["observations"],
                    ref_columns=["id_base_remuneration"],
                    custom_fn=process.process_agent_remuneration_complement,
                ),
                input_key="agent_remuneration_complement",
                output_key="agent_remuneration_complement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    agent_experience_pro = ETLTask(
        task_config=TaskConfig(task_id="agent_experience_pro"),
        target="agent_experience_pro",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"position_grille": "id_position_grille"},
                    ref_columns=["id_position_grille"],
                    custom_fn=process.process_agent_experience_pro,
                ),
                input_key="agent_experience_pro",
                output_key="agent_experience_pro",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # ordre des tâches
    chain(
        [
            agent.create_task(),
            agent_diplome.create_task(),
            agent_revalorisation.create_task(),
            agent_revalorisation_proposition.create_task(),
            agent_contrat_complement.create_task(),
            agent_remuneration_complement.create_task(),
            agent_experience_pro.create_task(),
        ]
    )


@task_group(group_id="load_to_grist")
def load_to_grist() -> None:
    get_agent_db = create_task(
        task_config=TaskConfig(task_id="get_agent_db"),
        output_selecteur="get_agent_db",
        steps=[ETLStep(fn=actions.get_agent_db, use_context=True)],
        export_output=True,
    )

    load_agent = create_task(
        task_config=TaskConfig(task_id="load_agent"),
        output_selecteur="load_agent",
        input_selecteurs=["get_agent_db", "agent"],
        steps=[
            ETLStep(
                fn=actions.load_agent,
                read_data=True,
                use_context=True,
                kwargs={"grist_doc_selecteur": "grist_doc"},
            )
        ],
        export_output=False,
    )

    chain(
        get_agent_db(),
        load_agent(),
    )
