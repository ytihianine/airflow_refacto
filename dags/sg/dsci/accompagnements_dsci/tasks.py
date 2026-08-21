from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.accompagnements_dsci import process
from modules.common_tasks.grist import generic_grist_processing
from modules.types.dags import TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy


# Création des tâches
@task_group
def referentiels() -> None:
    ref_bureau = ETLTask(
        task_config=TaskConfig(task_id="ref_bureau"),
        target="ref_bureau",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_ref_bureau,
                ),
                input_key="ref_bureau",
                output_key="ref_bureau",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_certification = ETLTask(
        task_config=TaskConfig(task_id="ref_certification"),
        target="ref_certification",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_ref_certification,
                ),
                input_key="ref_certification",
                output_key="ref_certification",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_competence_particuliere = ETLTask(
        task_config=TaskConfig(task_id="ref_competence_particuliere"),
        target="ref_competence_particuliere",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_ref_competence_particuliere,
                ),
                input_key="ref_competence_particuliere",
                output_key="ref_competence_particuliere",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_direction = ETLTask(
        task_config=TaskConfig(task_id="ref_direction"),
        target="ref_direction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_ref_direction,
                ),
                input_key="ref_direction",
                output_key="ref_direction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_profil_correspondant = ETLTask(
        task_config=TaskConfig(task_id="ref_profil_correspondant"),
        target="ref_profil_correspondant",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "id",
                        "profil_correspondant",
                        "intitule_long",
                        "created_by",
                        "updated_by",
                    ],
                    txt_columns=[
                        "profil_correspondant",
                        "intitule_long",
                        "created_by",
                        "updated_by",
                    ],
                    custom_fn=process.process_ref_profil_correspondant,
                ),
                input_key="ref_profil_correspondant",
                output_key="ref_profil_correspondant",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_qualite_service = ETLTask(
        task_config=TaskConfig(task_id="ref_qualite_service"),
        target="ref_qualite_service",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_ref_qualite_service,
                ),
                input_key="ref_qualite_service",
                output_key="ref_qualite_service",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_region = ETLTask(
        task_config=TaskConfig(task_id="ref_region"),
        target="ref_region",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_ref_region,
                ),
                input_key="ref_region",
                output_key="ref_region",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_semainier = ETLTask(
        task_config=TaskConfig(task_id="ref_semainier"),
        target="ref_semainier",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    date_columns=["date_semaine"],
                    custom_fn=process.process_ref_semainier,
                ),
                input_key="ref_semainier",
                output_key="ref_semainier",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_typologie_accompagnement = ETLTask(
        task_config=TaskConfig(task_id="ref_typologie_accompagnement"),
        target="ref_typologie_accompagnement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["typologie_accompagnement"],
                    custom_fn=process.process_ref_typologie_accompagnement,
                ),
                input_key="ref_typologie_accompagnement",
                output_key="ref_typologie_accompagnement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_pole = ETLTask(
        task_config=TaskConfig(task_id="ref_pole"),
        target="ref_pole",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"bureau": "id_bureau"},
                    custom_fn=process.process_ref_pole,
                ),
                input_key="ref_pole",
                output_key="ref_pole",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_type_accompagnement = ETLTask(
        task_config=TaskConfig(task_id="ref_type_accompagnement"),
        target="ref_type_accompagnement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"pole": "id_pole"},
                    ref_columns=["id_pole"],
                    custom_fn=process.process_ref_type_accompagnement,
                ),
                input_key="ref_type_accompagnement",
                output_key="ref_type_accompagnement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # Ordre des tâches
    chain(
        [
            ref_bureau.create_task(),
            ref_certification.create_task(),
            ref_competence_particuliere.create_task(),
            ref_direction.create_task(),
            ref_profil_correspondant.create_task(),
            ref_qualite_service.create_task(),
            ref_region.create_task(),
            ref_semainier.create_task(),
            ref_typologie_accompagnement.create_task(),
            ref_pole.create_task(),
            ref_type_accompagnement.create_task(),
        ],
    )


@task_group
def bilaterales() -> None:
    bilaterale = ETLTask(
        task_config=TaskConfig(task_id="bilaterale"),
        target="bilaterale",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"direction": "id_direction"},
                    ref_columns=["id_direction"],
                    date_columns=["date_de_rencontre"],
                    custom_fn=process.process_bilaterale,
                ),
                input_key="bilaterale",
                output_key="bilaterale",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    bilaterale_remontee = ETLTask(
        task_config=TaskConfig(task_id="bilaterale_remontee"),
        target="bilaterale_remontee",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "bilaterale": "id_bilaterale",
                        "bureau": "id_bureau",
                    },
                    txt_columns=["information_a_remonter"],
                    ref_columns=["id_bilaterale", "id_bureau"],
                    custom_fn=process.process_bilaterale_remontee,
                ),
                input_key="bilaterale_remontee",
                output_key="bilaterale_remontee",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Ordre des tâches
    chain([bilaterale.create_task(), bilaterale_remontee.create_task()])


@task_group
def correspondant() -> None:
    correspondant = ETLTask(
        task_config=TaskConfig(task_id="correspondant"),
        target="correspondant",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "id",
                        "mail",
                        "nom_complet",
                        "direction",
                        "entite",
                        "region",
                        "actif",
                        "promotion_fac",
                        "est_certifie_fac",
                        "actif_communaute_fac",
                        "direction_hors_mef",
                        "prenom",
                        "nom",
                        "date_debut_inactivite",
                    ],
                    cols_mapping={
                        "direction": "id_direction",
                        "region": "id_region",
                        "promotion_fac": "id_promotion_fac",
                    },
                    date_columns=["date_debut_inactivite"],
                    txt_columns=[
                        "mail",
                        "entite",
                        "direction_hors_mef",
                        "prenom",
                        "nom",
                        "nom_complet",
                    ],
                    ref_columns=["id_region", "id_direction", "id_promotion_fac"],
                    custom_fn=process.process_correspondant,
                ),
                input_key="correspondant",
                output_key="correspondant",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    correspondant_profil = ETLTask(
        task_config=TaskConfig(task_id="correspondant_profil"),
        target="correspondant_profil",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_correspondant",
                        "type_de_correspondant": "id_type_de_correspondant",
                    },
                    cols_to_keep=["id", "type_de_correspondant"],
                    ref_columns=["id_correspondant", "id_type_de_correspondant"],
                    custom_fn=process.process_correspondant_profil,
                ),
                input_key="correspondant_profil",
                output_key="correspondant_profil",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    correspondant_competence_particuliere = ETLTask(
        task_config=TaskConfig(task_id="correspondant_competence_particuliere"),
        target="correspondant_competence_particuliere",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_correspondant",
                        "competence_particuliere": "id_competence_particuliere",
                    },
                    cols_to_keep=["id", "competence_particuliere"],
                    ref_columns=["id_correspondant", "id_competence_particuliere"],
                    custom_fn=process.process_correspondant_competence_particuliere,
                ),
                input_key="correspondant_competence_particuliere",
                output_key="correspondant_competence_particuliere",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    correspondant_connaissance_communaute = ETLTask(
        task_config=TaskConfig(task_id="correspondant_connaissance_communaute"),
        target="correspondant_connaissance_communaute",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"id": "id_correspondant"},
                    cols_to_keep=["id", "connaissance_communaute"],
                    ref_columns=["id_correspondant"],
                    custom_fn=process.process_correspondant_connaissance_communaute,
                ),
                input_key="correspondant_connaissance_communaute",
                output_key="correspondant_connaissance_communaute",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # Ordre des tâches
    chain(
        [
            correspondant.create_task(),
            correspondant_profil.create_task(),
            correspondant_competence_particuliere.create_task(),
            correspondant_connaissance_communaute.create_task(),
        ]
    )


@task_group
def dsci() -> None:
    accompagnement_dsci = ETLTask(
        task_config=TaskConfig(task_id="accompagnement_dsci"),
        target="accompagnement_dsci",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "direction": "id_direction",
                        "prestataire": "recours_prestataire",
                    },
                    cols_to_keep=[
                        "id",
                        "annee",
                        "direction",
                        "service_bureau",
                        "sous_dir_bureau_",
                        "intitule_de_l_accompagnement",
                        "statut",
                        "prestataire",
                        "nom_du_prestataire",
                        "commentaires_complements",
                        "ressources_documentaires",
                        "debut_previsionnel_de_l_accompagnement",
                        "fin_previsionnelle_de_l_accompagnement",
                        "autres_participants",
                        "date_de_cloture_questionnaire",
                        "porteur_metier",
                    ],
                    txt_columns=[
                        "commentaires_complements",
                        "intitule_de_l_accompagnement",
                        "ressources_documentaires",
                        "service_bureau",
                        "sous_dir_bureau_",
                        "porteur_metier",
                    ],
                    date_columns=[
                        "debut_previsionnel_de_l_accompagnement",
                        "fin_previsionnelle_de_l_accompagnement",
                        "date_de_cloture_questionnaire",
                    ],
                    ref_columns=["id_direction"],
                    custom_fn=process.process_accompagnement_dsci,
                ),
                input_key="accompagnement_dsci",
                output_key="accompagnement_dsci",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    effectif_dsci = ETLTask(
        task_config=TaskConfig(task_id="effectif_dsci"),
        target="effectif_dsci",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"bureau": "id_bureau", "pole": "id_pole"},
                    cols_to_keep=[
                        "id",
                        "mail",
                        "bureau",
                        "pole",
                        "nom_complet",
                        "agent_present",
                        "fonction",
                        "absent_depuis",
                    ],
                    date_columns=["absent_depuis"],
                    ref_columns=["id_bureau", "id_pole"],
                    custom_fn=process.process_effectif_dsci,
                ),
                input_key="effectif_dsci",
                output_key="effectif_dsci",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    accompagnement_dsci_equipe = ETLTask(
        task_config=TaskConfig(task_id="accompagnement_dsci_equipe"),
        target="accompagnement_dsci_equipe",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_accompagnement",
                        "equipe_s_dsci": "id_equipe_s_dsci",
                    },
                    cols_to_keep=["id", "equipe_s_dsci"],
                    ref_columns=["id_accompagnement", "id_equipe_s_dsci"],
                    custom_fn=process.process_accompagnement_dsci_equipe,
                ),
                input_key="accompagnement_dsci_equipe",
                output_key="accompagnement_dsci_equipe",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    accompagnement_dsci_porteur = ETLTask(
        task_config=TaskConfig(task_id="accompagnement_dsci_porteur"),
        target="accompagnement_dsci_porteur",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_accompagnement",
                        "porteur_dsci": "id_porteur_dsci",
                    },
                    cols_to_keep=["id", "porteur_dsci"],
                    ref_columns=["id_accompagnement", "id_porteur_dsci"],
                    custom_fn=process.process_accompagnement_dsci_porteur,
                ),
                input_key="accompagnement_dsci_porteur",
                output_key="accompagnement_dsci_porteur",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    accompagnement_dsci_typologie = ETLTask(
        task_config=TaskConfig(task_id="accompagnement_dsci_typologie"),
        target="accompagnement_dsci_typologie",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_accompagnement",
                        "typologie": "id_typologie",
                    },
                    cols_to_keep=["id", "typologie"],
                    ref_columns=["id_accompagnement", "id_typologie"],
                    custom_fn=process.process_accompagnement_dsci_typologie,
                ),
                input_key="accompagnement_dsci_typologie",
                output_key="accompagnement_dsci_typologie",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Ordre des tâches
    chain(
        [
            accompagnement_dsci.create_task(),
            effectif_dsci.create_task(),
            accompagnement_dsci_equipe.create_task(),
            accompagnement_dsci_porteur.create_task(),
            accompagnement_dsci_typologie.create_task(),
        ]
    )


@task_group
def mission_innovation() -> None:
    accompagnement_mi = ETLTask(
        task_config=TaskConfig(task_id="accompagnement_mi"),
        target="accompagnement_mi",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "direction": "id_direction",
                        "pole": "id_pole",
                        "type_d_accompagnement": "id_type_d_accompagnement",
                    },
                    cols_to_keep=[
                        "id",
                        "intitule",
                        "direction",
                        "date_de_realisation",
                        "statut",
                        "pole",
                        "type_d_accompagnement",
                        "est_certifiant",
                        "places_max",
                        "nb_inscrits",
                        "places_restantes",
                        "est_ouvert_notation",
                        "informations_complementaires",
                    ],
                    txt_columns=["informations_complementaires"],
                    date_columns=["date_de_realisation"],
                    ref_columns=["id_direction", "id_pole", "id_type_d_accompagnement"],
                    custom_fn=process.process_accompagnement_mi,
                ),
                input_key="accompagnement_mi",
                output_key="accompagnement_mi",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    accompagnement_mi_satisfaction = ETLTask(
        task_config=TaskConfig(task_id="accompagnement_mi_satisfaction"),
        target="accompagnement_mi_satisfaction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "accompagnement": "id_accompagnement",
                        "type_d_accompagnement": "id_type_d_accompagnement",
                    },
                    cols_to_keep=[
                        "id",
                        "accompagnement",
                        "type_d_accompagnement",
                        "nombre_de_participants",
                        "nombre_de_reponses",
                        "taux_de_reponse",
                        "note_moyenne_de_satisfaction",
                        "unite",
                    ],
                    num_columns=[
                        "nombre_de_participants",
                        "nombre_de_reponses",
                        "taux_de_reponse",
                        "note_moyenne_de_satisfaction",
                    ],
                    ref_columns=["id_accompagnement", "id_type_d_accompagnement"],
                    custom_fn=process.process_accompagnement_mi_satisfaction,
                ),
                input_key="accompagnement_mi_satisfaction",
                output_key="accompagnement_mi_satisfaction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    animateur_interne = ETLTask(
        task_config=TaskConfig(task_id="animateur_interne"),
        target="animateur_interne",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "accompagnement": "id_accompagnement",
                        "animateur": "id_animateur",
                    },
                    ref_columns=["id_accompagnement", "id_animateur"],
                    custom_fn=process.process_animateur_interne,
                ),
                input_key="animateur_interne",
                output_key="animateur_interne",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    animateur_externe = ETLTask(
        task_config=TaskConfig(task_id="animateur_externe"),
        target="animateur_externe",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"accompagnement": "id_accompagnement"},
                    ref_columns=["id_accompagnement"],
                    custom_fn=process.process_animateur_externe,
                ),
                input_key="animateur_externe",
                output_key="animateur_externe",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    animateur_fac = ETLTask(
        task_config=TaskConfig(task_id="animateur_fac"),
        target="animateur_fac",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "accompagnement": "id_accompagnement",
                        "animateur": "id_animateur",
                    },
                    cols_to_keep=[
                        "id",
                        "accompagnement",
                        "animateur",
                    ],
                    ref_columns=["id_accompagnement", "id_animateur"],
                    custom_fn=process.process_animateur_fac,
                ),
                input_key="animateur_fac",
                output_key="animateur_fac",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    animateur_fac_certification = ETLTask(
        task_config=TaskConfig(task_id="animateur_fac_certification"),
        target="animateur_fac_certification",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_animateur_fac",
                        "certifications_souhaitees": "id_certifications_souhaitees",
                    },
                    cols_to_keep=["id", "certifications_souhaitees"],
                    ref_columns=["id_animateur_fac", "id_certifications_souhaitees"],
                    custom_fn=process.process_animateur_fac_certification,
                ),
                input_key="animateur_fac_certification",
                output_key="animateur_fac_certification",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    animateur_fac_certification_valide = ETLTask(
        task_config=TaskConfig(task_id="animateur_fac_certification_valide"),
        target="animateur_fac_certification_valide",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_animateur_fac",
                        "certifications_validees": "id_certifications_validees",
                    },
                    cols_to_keep=["id", "certifications_validees"],
                    ref_columns=["id_animateur_fac", "id_certifications_validees"],
                    custom_fn=process.process_animateur_fac_certification_valide,
                ),
                input_key="animateur_fac_certification_valide",
                output_key="animateur_fac_certification_valide",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    laboratoires_territoriaux = ETLTask(
        task_config=TaskConfig(task_id="laboratoires_territoriaux"),
        target="laboratoires_territoriaux",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"direction": "id_direction", "region": "id_region"},
                    ref_columns=["id_direction", "id_region"],
                    custom_fn=process.process_laboratoires_territoriaux,
                ),
                input_key="laboratoires_territoriaux",
                output_key="laboratoires_territoriaux",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    pleniere_quest_inscription = ETLTask(
        task_config=TaskConfig(task_id="pleniere_quest_inscription"),
        target="pleniere_quest_inscription",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "direction": "id_direction",
                        "id_accompagnement": "id_id_accompagnement",
                        "pleniere": "id_pleniere",
                    },
                    ref_columns=["id_direction", "id_id_accompagnement", "id_pleniere"],
                    custom_fn=process.process_pleniere_quest_inscription,
                ),
                input_key="pleniere_quest_inscription",
                output_key="pleniere_quest_inscription",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    pleniere_quest_satisfaction = ETLTask(
        task_config=TaskConfig(task_id="pleniere_quest_satisfaction"),
        target="pleniere_quest_satisfaction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=[
                        "mail",
                        "ce_que_j_ai_apprecie",
                        "ce_qui_peut_etre_ameliore",
                    ],
                    custom_fn=process.process_pleniere_quest_satisfaction,
                ),
                input_key="pleniere_quest_satisfaction",
                output_key="pleniere_quest_satisfaction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    passinnov_quest_inscription = ETLTask(
        task_config=TaskConfig(task_id="passinnov_quest_inscription"),
        target="passinnov_quest_inscription",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    custom_fn=process.process_passinnov_quest_inscription,
                ),
                input_key="passinnov_quest_inscription",
                output_key="passinnov_quest_inscription",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    passinnov_quest_satisfaction = ETLTask(
        task_config=TaskConfig(task_id="passinnov_quest_satisfaction"),
        target="passinnov_quest_satisfaction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "quest_passinnov": "id_quest_passinnov",
                        "id_passinnov": "id_id_passinnov",
                    },
                    ref_columns=["id_quest_passinnov", "id_id_passinnov"],
                    custom_fn=process.process_passinnov_quest_satisfaction,
                ),
                input_key="passinnov_quest_satisfaction",
                output_key="passinnov_quest_satisfaction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    formation_codev_quest_inscription = ETLTask(
        task_config=TaskConfig(task_id="formation_codev_quest_inscription"),
        target="formation_codev_quest_inscription",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "direction": "id_direction",
                        "id_accompagnement": "id_id_accompagnement",
                        "session_formation_codev": "id_session_formation_codev",
                    },
                    txt_columns=[
                        "formation_codev",
                        "experience_codev",
                        "details_experience",
                        "difficultes",
                        "attentes",
                    ],
                    ref_columns=[
                        "id_direction",
                        "id_id_accompagnement",
                        "id_session_formation_codev",
                    ],
                    custom_fn=process.process_formation_codev_quest_inscription,
                ),
                input_key="formation_codev_quest_inscription",
                output_key="formation_codev_quest_inscription",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    formation_fac_quest_satisfaction = ETLTask(
        task_config=TaskConfig(task_id="formation_fac_quest_satisfaction"),
        target="formation_fac_quest_satisfaction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "quest_formation": "id_quest_formation",
                        "promotion": "id_promotion",
                        "id_formation": "id_id_formation",
                    },
                    cols_to_keep=[
                        "id",
                        "quest_formation",
                        "mail",
                        "promotion",
                        "note_module_1",
                        "note_module_2",
                        "note_module_3",
                        "commentaire_m1",
                        "commentaire_m2",
                        "commentaire_m3",
                        "nps",
                        "utilite",
                        "besoin",
                        "id_formation",
                    ],
                    txt_columns=[
                        "commentaire_m1",
                        "commentaire_m2",
                        "commentaire_m3",
                        "utilite",
                        "besoin",
                    ],
                    ref_columns=[
                        "id_quest_formation",
                        "id_promotion",
                        "id_id_formation",
                    ],
                    custom_fn=process.process_formation_fac_quest_satisfaction,
                ),
                input_key="formation_fac_quest_satisfaction",
                output_key="formation_fac_quest_satisfaction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    formation_fac_envie_suite_quest_satisfaction = ETLTask(
        task_config=TaskConfig(task_id="formation_fac_envie_suite_quest_satisfaction"),
        target="formation_fac_envie_suite_quest_satisfaction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"id": "id_formation_fac"},
                    ref_columns=["id_formation_fac"],
                    cols_to_keep=["id", "envies_pour_la_suite"],
                    custom_fn=process.process_formation_fac_envie_suite_quest_satisfaction,
                ),
                input_key="formation_fac_envie_suite_quest_satisfaction",
                output_key="formation_fac_envie_suite_quest_satisfaction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    fac_hors_bercylab_quest_accompagnement = ETLTask(
        task_config=TaskConfig(task_id="fac_hors_bercylab_quest_accompagnement"),
        target="fac_hors_bercylab_quest_accompagnement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "direction": "id_direction",
                        "region": "id_region",
                        "facilitateur_1": "id_facilitateur_1",
                        "facilitateur_2": "id_facilitateur_2",
                        "facilitateur_3": "id_facilitateur_3",
                        "facilitateurs": "id_facilitateurs",
                    },
                    cols_to_keep=[
                        "id",
                        "intitule_de_l_accompagnement",
                        "direction",
                        "date_de_realisation",
                        "statut",
                        "synthese_de_l_accompagnement",
                        "region",
                        "facilitateur_1",
                        "facilitateur_2",
                        "facilitateur_3",
                    ],
                    date_columns=["date_de_realisation"],
                    txt_columns=[
                        "synthese_de_l_accompagnement",
                        "intitule_de_l_accompagnement",
                    ],
                    ref_columns=[
                        "id_direction",
                        "id_region",
                        "id_facilitateur_1",
                        "id_facilitateur_2",
                        "id_facilitateur_3",
                    ],
                    custom_fn=process.process_fac_hors_bercylab_quest_accompagnement,
                ),
                input_key="fac_hors_bercylab_quest_accompagnement",
                output_key="fac_hors_bercylab_quest_accompagnement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    fac_hors_bercylab_quest_type_accompagnement = ETLTask(
        task_config=TaskConfig(task_id="fac_hors_bercylab_quest_type_accompagnement"),
        target="fac_hors_bercylab_quest_type_accompagnement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"id": "id_formation_fac_hors_bercylab"},
                    cols_to_keep=["id", "type_d_accompagnement"],
                    ref_columns=["id_formation_fac_hors_bercylab"],
                    custom_fn=process.process_fac_hors_bercylab_quest_type_accompagnement,
                ),
                input_key="fac_hors_bercylab_quest_type_accompagnement",
                output_key="fac_hors_bercylab_quest_type_accompagnement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    fac_hors_bercylab_quest_accompagnement_participants = ETLTask(
        task_config=TaskConfig(task_id="fac_hors_bercylab_quest_accompagnement_participants"),
        target="fac_hors_bercylab_quest_accompagnement_participants",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"id": "id_formation_fac_hors_bercylab"},
                    cols_to_keep=["id", "participants"],
                    ref_columns=["id_formation_fac_hors_bercylab"],
                    custom_fn=process.process_fac_hors_bercylab_quest_accompagnement_participants,
                ),
                input_key="fac_hors_bercylab_quest_accompagnement_participants",
                output_key="fac_hors_bercylab_quest_accompagnement_participants",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    fac_hors_bercylab_quest_accompagnement_facilitateurs = ETLTask(
        task_config=TaskConfig(task_id="fac_hors_bercylab_quest_accompagnement_facilitateurs"),
        target="fac_hors_bercylab_quest_accompagnement_facilitateurs",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "id": "id_formation_fac_hors_bercylab",
                        "facilitateurs": "id_facilitateurs",
                    },
                    cols_to_keep=["id", "facilitateurs"],
                    ref_columns=["id_formation_fac_hors_bercylab", "id_facilitateurs"],
                    custom_fn=process.process_fac_hors_bercylab_quest_accompagnement_facilitateurs,
                ),
                input_key="fac_hors_bercylab_quest_accompagnement_facilitateurs",
                output_key="fac_hors_bercylab_quest_accompagnement_facilitateurs",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # Ordre des tâches
    chain(
        [
            accompagnement_mi.create_task(),
            accompagnement_mi_satisfaction.create_task(),
            animateur_interne.create_task(),
            animateur_externe.create_task(),
            animateur_fac.create_task(),
            animateur_fac_certification.create_task(),
            animateur_fac_certification_valide.create_task(),
            laboratoires_territoriaux.create_task(),
            pleniere_quest_inscription.create_task(),
            pleniere_quest_satisfaction.create_task(),
            passinnov_quest_inscription.create_task(),
            passinnov_quest_satisfaction.create_task(),
            formation_codev_quest_inscription.create_task(),
            formation_fac_quest_satisfaction.create_task(),
            formation_fac_envie_suite_quest_satisfaction.create_task(),
            fac_hors_bercylab_quest_accompagnement.create_task(),
            fac_hors_bercylab_quest_type_accompagnement.create_task(),
            fac_hors_bercylab_quest_accompagnement_participants.create_task(),
            fac_hors_bercylab_quest_accompagnement_facilitateurs.create_task(),
        ]
    )


@task_group
def conseil_interne() -> None:
    accompagnement_cci_opportunite = ETLTask(
        task_config=TaskConfig(task_id="accompagnement_cci_opportunite"),
        target="accompagnement_cci_opportunite",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"accompagnement": "id_accompagnement"},
                    date_columns=[
                        "date_de_reception",
                        "date_de_proposition_d_accompagnement",
                        "date_prise_de_decision",
                    ],
                    ref_columns=["id_accompagnement"],
                    custom_fn=process.process_accompagnement_cci_opportunite,
                ),
                input_key="accompagnement_cci_opportunite",
                output_key="accompagnement_cci_opportunite",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    charge_agent_cci = ETLTask(
        task_config=TaskConfig(task_id="charge_agent_cci"),
        target="charge_agent_cci",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "agent_e_": "id_agent_e_",
                        "semaine": "id_semaine",
                        "missions": "id_missions",
                    },
                    num_columns=["temps_passe", "taux_de_charge"],
                    ref_columns=["id_agent_e_", "id_semaine", "id_missions"],
                    custom_fn=process.process_charge_agent_cci,
                ),
                input_key="charge_agent_cci",
                output_key="charge_agent_cci",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    accompagnement_cci_quest_satisfaction = ETLTask(
        task_config=TaskConfig(task_id="accompagnement_cci_quest_satisfaction"),
        target="accompagnement_cci_quest_satisfaction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "formulaire_accompagnement": "id_formulaire_accompagnement",
                        "etape_de_cadrage": "id_etape_de_cadrage",
                        "aide_methodologique": "id_aide_methodologique",
                        "pilotage_et_suivi": "id_pilotage_et_suivi",
                        "respect_calendrier": "id_respect_calendrier",
                        "reactivite": "id_reactivite",
                        "adaptabilite": "id_adaptabilite",
                        "relationnel_client": "id_relationnel_client",
                        "qualite_des_livrables": "id_qualite_des_livrables",
                        "atteinte_objectifs": "id_atteinte_objectifs",
                        "accompagnement": "id_accompagnement",
                    },
                    ref_columns=[
                        "id_formulaire_accompagnement",
                        "id_etape_de_cadrage",
                        "id_aide_methodologique",
                        "id_pilotage_et_suivi",
                        "id_respect_calendrier",
                        "id_reactivite",
                        "id_adaptabilite",
                        "id_relationnel_client",
                        "id_qualite_des_livrables",
                        "id_atteinte_objectifs",
                        "id_accompagnement",
                    ],
                    custom_fn=process.process_accompagnement_cci_quest_satisfaction,
                ),
                input_key="accompagnement_cci_quest_satisfaction",
                output_key="accompagnement_cci_quest_satisfaction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # Ordre des tâches
    chain(
        [
            accompagnement_cci_opportunite.create_task(),
            charge_agent_cci.create_task(),
            accompagnement_cci_quest_satisfaction.create_task(),
        ]
    )
