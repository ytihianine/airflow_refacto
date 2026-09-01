from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.sg.dsci.experimentation_ia import process
from modules.common_tasks.grist import generic_grist_processing
from modules.types.dags import TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy

# Creation des taches


@task_group
def referentiels() -> None:

    ref_q1_direction = ETLTask(
        task_config=TaskConfig(task_id="ref_q1_direction"),
        target="ref_q1_direction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["direction"],
                    custom_fn=process.process_ref_q1_direction,
                ),
                input_key="ref_q1_direction",
                output_key="ref_q1_direction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q5_domaine = ETLTask(
        task_config=TaskConfig(task_id="ref_q5_domaine"),
        target="ref_q5_domaine",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["domaine"],
                    custom_fn=process.process_ref_q5_domaine,
                ),
                input_key="ref_q5_domaine",
                output_key="ref_q5_domaine",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q6_niveau_utilisation = ETLTask(
        task_config=TaskConfig(task_id="ref_q6_niveau_utilisation"),
        target="ref_q6_niveau_utilisation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["id", "niveau_d_appropriation"],
                    txt_columns=["niveau_d_appropriation"],
                    custom_fn=process.process_ref_q6_niveau_utilisation,
                ),
                input_key="ref_q6_niveau_utilisation",
                output_key="ref_q6_niveau_utilisation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q9_cas_usage = ETLTask(
        task_config=TaskConfig(task_id="ref_q9_cas_usage"),
        target="ref_q9_cas_usage",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["cas_d_usage"],
                    custom_fn=process.process_ref_q9_cas_usage,
                ),
                input_key="ref_q9_cas_usage",
                output_key="ref_q9_cas_usage",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # ==============================
    # referentiels du questionnaire2
    # ==============================
    ref_q28_raisons_perte = ETLTask(
        task_config=TaskConfig(task_id="ref_q28_raisons_perte"),
        target="ref_q28_raisons_perte",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["raisons"],
                    custom_fn=process.process_ref_q28_raisons_perte,
                ),
                input_key="ref_q28_raisons_perte",
                output_key="ref_q28_raisons_perte",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q25_impact_observe = ETLTask(
        task_config=TaskConfig(task_id="ref_q25_impact_observe"),
        target="ref_q25_impact_observe",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["observation"],
                    custom_fn=process.process_ref_q25_impact_observe,
                ),
                input_key="ref_q25_impact_observe",
                output_key="ref_q25_impact_observe",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q24_impact_identifie = ETLTask(
        task_config=TaskConfig(task_id="ref_q24_impact_identifie"),
        target="ref_q24_impact_identifie",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["impacts"],
                    custom_fn=process.process_ref_q24_impact_identifie,
                ),
                input_key="ref_q24_impact_identifie",
                output_key="ref_q24_impact_identifie",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q23_taux_correction = ETLTask(
        task_config=TaskConfig(task_id="ref_q23_taux_correction"),
        target="ref_q23_taux_correction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["id", "taux_de_correction"],
                    txt_columns=["taux_de_correction"],
                    custom_fn=process.process_ref_q23_taux_correction,
                ),
                input_key="ref_q23_taux_correction",
                output_key="ref_q23_taux_correction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q22_typologie_erreurs = ETLTask(
        task_config=TaskConfig(task_id="ref_q22_typologie_erreurs"),
        target="ref_q22_typologie_erreurs",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["erreurs"],
                    custom_fn=process.process_ref_q22_typologie_erreurs,
                ),
                input_key="ref_q22_typologie_erreurs",
                output_key="ref_q22_typologie_erreurs",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q20_autres_ia = ETLTask(
        task_config=TaskConfig(task_id="ref_q20_autres_ia"),
        target="ref_q20_autres_ia",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["comparaisons"],
                    custom_fn=process.process_ref_q20_autres_ia,
                ),
                input_key="ref_q20_autres_ia",
                output_key="ref_q20_autres_ia",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q16_taches = ETLTask(
        task_config=TaskConfig(task_id="ref_q16_taches"),
        target="ref_q16_taches",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["taches"],
                    custom_fn=process.process_ref_q16_taches,
                ),
                input_key="ref_q16_taches",
                output_key="ref_q16_taches",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q14_evolution_craintes = ETLTask(
        task_config=TaskConfig(task_id="ref_q14_evolution_craintes"),
        target="ref_q14_evolution_craintes",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["evolutions"],
                    custom_fn=process.process_ref_q14_evolution_craintes,
                ),
                input_key="ref_q14_evolution_craintes",
                output_key="ref_q14_evolution_craintes",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q13_facteurs_progression = ETLTask(
        task_config=TaskConfig(task_id="ref_q13_facteurs_progression"),
        target="ref_q13_facteurs_progression",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["facteurs"],
                    custom_fn=process.process_ref_q13_facteurs_progression,
                ),
                input_key="ref_q13_facteurs_progression",
                output_key="ref_q13_facteurs_progression",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q10_principaux_freins = ETLTask(
        task_config=TaskConfig(task_id="ref_q10_principaux_freins"),
        target="ref_q10_principaux_freins",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["freins"],
                    custom_fn=process.process_ref_q10_principaux_freins,
                ),
                input_key="ref_q10_principaux_freins",
                output_key="ref_q10_principaux_freins",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q6_participation_programme = ETLTask(
        task_config=TaskConfig(task_id="ref_q6_participation_programme"),
        target="ref_q6_participation_programme",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["participation"],
                    custom_fn=process.process_ref_q6_participation_programme,
                ),
                input_key="ref_q6_participation_programme",
                output_key="ref_q6_participation_programme",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q5_formation_suivie = ETLTask(
        task_config=TaskConfig(task_id="ref_q5_formation_suivie"),
        target="ref_q5_formation_suivie",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["formation_suivie"],
                    custom_fn=process.process_ref_q5_formation_suivie,
                ),
                input_key="ref_q5_formation_suivie",
                output_key="ref_q5_formation_suivie",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q3_niveau_2 = ETLTask(
        task_config=TaskConfig(task_id="ref_q3_niveau_2"),
        target="ref_q3_niveau_2",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["niveau"],
                    custom_fn=process.process_ref_q3_niveau,
                ),
                input_key="ref_q3_niveau_2",
                output_key="ref_q3_niveau_2",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q7_accords = ETLTask(
        task_config=TaskConfig(task_id="ref_q7_accords"),
        target="ref_q7_accords",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["reponses"],
                    custom_fn=process.process_ref_q7_accords,
                ),
                input_key="ref_q7_accords",
                output_key="ref_q7_accords",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # ==============================
    # referentiels du questionnaire2_bis
    # ==============================
    ref_raisons_non_utilisation = ETLTask(
        task_config=TaskConfig(task_id="ref_raisons_non_utilisation"),
        target="ref_raisons_non_utilisation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["raisons"],
                    custom_fn=process.process_ref_raisons_non_utilisation,
                ),
                input_key="ref_raisons_non_utilisation",
                output_key="ref_raisons_non_utilisation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # ==============================
    # Référentiels du questionnaire_3
    # ==============================
    ref_q6_formation_suivie = ETLTask(
        task_config=TaskConfig(task_id="ref_q6_formation_suivie"),
        target="ref_q6_formation_suivie",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["formation"],
                    custom_fn=process.process_ref_q6_formation_suivie,
                ),
                input_key="ref_q6_formation_suivie",
                output_key="ref_q6_formation_suivie",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q7_particip_programme = ETLTask(
        task_config=TaskConfig(task_id="ref_q7_particip_programme"),
        target="ref_q7_particip_programme",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["participation"],
                    custom_fn=process.process_ref_q7_particip_programme,
                ),
                input_key="ref_q7_particip_programme",
                output_key="ref_q7_particip_programme",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q8_raisons_non_participation = ETLTask(
        task_config=TaskConfig(task_id="ref_q8_raisons_non_participation"),
        target="ref_q8_raisons_non_participation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["raisons"],
                    custom_fn=process.process_ref_q8_raisons_non_participation,
                ),
                input_key="ref_q8_raisons_non_participation",
                output_key="ref_q8_raisons_non_participation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q11_leviers_progressions = ETLTask(
        task_config=TaskConfig(task_id="ref_q11_leviers_progressions"),
        target="ref_q11_leviers_progressions",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["leviers"],
                    custom_fn=process.process_ref_q11_leviers_progressions,
                ),
                input_key="ref_q11_leviers_progressions",
                output_key="ref_q11_leviers_progressions",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q12_impacts_taches_pro = ETLTask(
        task_config=TaskConfig(task_id="ref_q12_impacts_taches_pro"),
        target="ref_q12_impacts_taches_pro",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["impacts"],
                    custom_fn=process.process_ref_q12_impacts_taches_pro,
                ),
                input_key="ref_q12_impacts_taches_pro",
                output_key="ref_q12_impacts_taches_pro",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q14_taches_rebarbativ = ETLTask(
        task_config=TaskConfig(task_id="ref_q14_taches_rebarbativ"),
        target="ref_q14_taches_rebarbativ",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["taches_rebarbatives"],
                    custom_fn=process.process_ref_q14_taches_rebarbativ,
                ),
                input_key="ref_q14_taches_rebarbativ",
                output_key="ref_q14_taches_rebarbativ",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q17_autres_outils = ETLTask(
        task_config=TaskConfig(task_id="ref_q17_autres_outils"),
        target="ref_q17_autres_outils",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["id", "autres_outils"],
                    txt_columns=["autres_outils"],
                    custom_fn=process.process_ref_q17_autres_outils,
                ),
                input_key="ref_q17_autres_outils",
                output_key="ref_q17_autres_outils",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q17_satisfaction_autre_outil = ETLTask(
        task_config=TaskConfig(task_id="ref_q17_satisfaction_autre_outil"),
        target="ref_q17_satisfaction_autre_outil",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["id", "satisfaction_autres_outils"],
                    txt_columns=["satisfaction_autres_outils"],
                    custom_fn=process.process_ref_q17_satisfaction_autre_outil,
                ),
                input_key="ref_q17_satisfaction_autre_outil",
                output_key="ref_q17_satisfaction_autre_outil",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q18_comparaisons = ETLTask(
        task_config=TaskConfig(task_id="ref_q18_comparaisons"),
        target="ref_q18_comparaisons",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["comparaisons"],
                    custom_fn=process.process_ref_q18_comparaisons,
                ),
                input_key="ref_q18_comparaisons",
                output_key="ref_q18_comparaisons",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q19_fonctionnalites = ETLTask(
        task_config=TaskConfig(task_id="ref_q19_fonctionnalites"),
        target="ref_q19_fonctionnalites",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["fonctionnalites"],
                    custom_fn=process.process_ref_q19_fonctionnalites,
                ),
                input_key="ref_q19_fonctionnalites",
                output_key="ref_q19_fonctionnalites",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q21_risques_identifies = ETLTask(
        task_config=TaskConfig(task_id="ref_q21_risques_identifies"),
        target="ref_q21_risques_identifies",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["risques"],
                    custom_fn=process.process_ref_q21_risques_identifies,
                ),
                input_key="ref_q21_risques_identifies",
                output_key="ref_q21_risques_identifies",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    ref_q25_besoins = ETLTask(
        task_config=TaskConfig(task_id="ref_q25_besoins"),
        target="ref_q25_besoins",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["besoins"],
                    custom_fn=process.process_ref_q25_besoins,
                ),
                input_key="ref_q25_besoins",
                output_key="ref_q25_besoins",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # Ordre des tâches
    chain(
        [
            ref_q1_direction.create_task(),
            ref_q5_domaine.create_task(),
            ref_q6_niveau_utilisation.create_task(),
            ref_q9_cas_usage.create_task(),
            ref_q28_raisons_perte.create_task(),
            ref_q25_impact_observe.create_task(),
            ref_q24_impact_identifie.create_task(),
            ref_q23_taux_correction.create_task(),
            ref_q22_typologie_erreurs.create_task(),
            ref_q20_autres_ia.create_task(),
            ref_q16_taches.create_task(),
            ref_q14_evolution_craintes.create_task(),
            ref_q13_facteurs_progression.create_task(),
            ref_q10_principaux_freins.create_task(),
            ref_q6_participation_programme.create_task(),
            ref_q5_formation_suivie.create_task(),
            ref_q3_niveau_2.create_task(),
            ref_q7_accords.create_task(),
            ref_raisons_non_utilisation.create_task(),
            ref_q6_formation_suivie.create_task(),
            ref_q7_particip_programme.create_task(),
            ref_q8_raisons_non_participation.create_task(),
            ref_q11_leviers_progressions.create_task(),
            ref_q12_impacts_taches_pro.create_task(),
            ref_q14_taches_rebarbativ.create_task(),
            ref_q17_autres_outils.create_task(),
            ref_q17_satisfaction_autre_outil.create_task(),
            ref_q18_comparaisons.create_task(),
            ref_q19_fonctionnalites.create_task(),
            ref_q21_risques_identifies.create_task(),
            ref_q25_besoins.create_task(),
        ]
    )


@task_group()
def repartition() -> None:
    quota_par_entite = ETLTask(
        task_config=TaskConfig(task_id="quota_par_entite"),
        target="quota_par_entite",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "experimentation_demarree",
                        "entite",
                        "nbre_d_acces_previsionnels",
                        "nb_acces_demande",
                        "code",
                        "nbre_de_connexion_effective_au_05_03_2026",
                        "nb_de_reponses_au_questionnaire",
                        "nb_reponse_q2",
                        "nb_reponse_q3",
                        "relance_dsci",
                        "appel_a_candidature_dsci",
                        "referent_ia",
                        "courriel",
                    ],
                    cols_mapping={"nbre_de_connexion_effective_au_05_03_2026": "nbre_connexion_effective"},
                    txt_columns=[
                        "code",
                        "relance_dsci",
                        "appel_a_candidature_dsci",
                        "referent_ia",
                        "courriel",
                    ],
                    num_columns=[
                        "nbre_d_acces_previsionnels",
                        "nbre_connexion_effective",
                    ],
                    custom_fn=process.process_quota_par_entite,
                ),
                input_key="quota_par_entite",
                output_key="quota_par_entite",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Ordre des tâches
    chain([quota_par_entite.create_task()])


@task_group
def suivi_experimentateurs() -> None:
    experimentateurs = ETLTask(
        task_config=TaskConfig(task_id="experimentateurs"),
        target="experimentateurs",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "no_id",
                        "entite",
                        "parti",
                        "courriel",
                        "courriel_corrige",
                        "connecte_",
                        "reponse_au_questionnaire_1",
                        "reponse_au_questionnaire_2",
                        "reponse_au_questionnaire_3",
                    ],
                    txt_columns=[
                        "no_id",
                        "parti",
                        "courriel",
                        "courriel_corrige",
                    ],
                    custom_fn=process.process_experimentateurs,
                ),
                input_key="experimentateurs",
                output_key="experimentateurs",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Ordre des tâches
    chain([experimentateurs.create_task()])


@task_group
def suivi_questionnaire_1() -> None:
    questionnaire_1 = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_1"),
        target="questionnaire_1",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "no_id",
                        "direction",
                        "tranche_age",
                        "categorie_emploi",
                        "statut",
                        "domaine_professionnel",
                        "metier",
                        "situation_d_encadrement",
                        "autres_experimentateurs",
                        "niveau_d_utilisation_ia",
                        "usage_ia_perso_avant_expe",
                        "usage_ia_pro_avant_expe",
                        "craintes_usage_ia_pro",
                        "raisons_des_craintes",
                        "attentes_experimentation",
                        "autres_cas_usage_transverse",
                        "cas_d_usage_metier",
                        "formation_suivie_usage_ia_",
                        "autre_formation_suivie",
                        "autre_besoin_accompagnement",
                        "besoin_acculturation_encadrement",
                    ],
                    cols_mapping={
                        "direction": "id_direction",
                        "domaine_professionnel": "id_domaine_professionnel",
                        "niveau_d_utilisation_ia": "id_niveau_d_utilisation_ia",
                    },
                    txt_columns=[
                        "no_id",
                        "metier",
                        "raisons_des_craintes",
                        "attentes_experimentation",
                        "cas_d_usage_metier",
                        "autres_cas_usage_transverse",
                        "autre_formation_suivie",
                        "autre_besoin_accompagnement",
                    ],
                    ref_columns=[
                        "id_direction",
                        "id_domaine_professionnel",
                        "id_niveau_d_utilisation_ia",
                    ],
                    custom_fn=process.process_questionnaire_1,
                ),
                input_key="questionnaire_1",
                output_key="questionnaire_1",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_1_cas_usage = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_1_cas_usage"),
        target="questionnaire_1_cas_usage",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "cas_d_usage_envisages"],
                    cols_mapping={
                        "cas_d_usage_envisages": "id_cas_d_usage_envisages",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_1_cas_usage,
                ),
                input_key="questionnaire_1_cas_usage",
                output_key="questionnaire_1_cas_usage",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_1_besoins_accompagnement = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_1_besoins_accompagnement"),
        target="questionnaire_1_besoins_accompagnement",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "besoin_accompagnement"],
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_1_besoins_accompagnement,
                ),
                input_key="questionnaire_1_besoins_accompagnement",
                output_key="questionnaire_1_besoins_accompagnement",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # Ordre de tâches
    chain(
        [
            questionnaire_1.create_task(),
            questionnaire_1_cas_usage.create_task(),
            questionnaire_1_besoins_accompagnement.create_task(),
        ]
    )


@task_group
def suivi_questionnaire_2() -> None:
    questionnaire_2 = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2"),
        target="questionnaire_2",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "no_id",
                        "autres_types_d_interactions",
                        "niveau_d_usage_ia_post_expe_",
                        "frequence_d_usage_assistant_ia",
                        "autres_formation_ia",
                        "raison_non_participation_rdv",
                        "autre_besoin_accompagnement",
                        "apprentissage_assistant_ia_ressenti_",
                        "difficultes_techniques_rencontrees2",
                        "autres_difficultes",
                        "autres_taches_realisees",
                        "autres_freins",
                        "recommandation_collegues_mef",
                        "sensation_montee_en_competences",
                        "autres_sources_de_progression",
                        "evolution_des_craintes_initiales",
                        "utilite_metier_mef",
                        "decouverte_d_usages_inattendus",
                        "les_usages_inattendus",
                        "mode_de_decouverte_usages",
                        "autre_mode_de_decouverte",
                        "diminution_d_usage_ia_non_souveraines",
                        "comparaison_autres_ia",
                        "frequence_des_erreurs",
                        "autres_types_d_erreurs",
                        "cas_usage_principal_teste",
                        "temps_economise_par_semaine",
                        "cu1_nombre_echanges_moyens_affinage_reponse",
                        "taux_moyen_de_correction_rep_assistant",
                        "pertinence_assistant_ia",
                        "commentaires",
                        "deuxieme_cas_d_usage_teste",
                        "cu2_temps_economise_par_semaine",
                        "cu2_nombre_echanges_moyens",
                        "cu2_taux_moyen_de_correction_assistant",
                        "cu2_pertinence_assistant_ia",
                        "commentaires2",
                        "troisieme_cas_d_usage",
                        "cu3_temps_economise_par_semaine",
                        "cu3_nombre_echanges_moyens_affinage_reponse",
                        "cu3_taux_moyen_de_correction_assistant",
                        "cu3_pertinence_assistant_ia",
                        "commentaires3",
                        "autres_impacts_identifies",
                        "autres_impacts_observes",
                        "impact_sur_le_temps_de_travail",
                        "estimation_globale_gain_de_temps",
                        "raisons_perte_de_temps",
                        "autres_raisons",
                        "ia_favorise_relations_humaines_",
                    ],
                    cols_mapping={
                        "niveau_d_usage_ia_post_expe_": "id_niveau_d_usage_ia_post_expe_",
                        "recommandation_collegues_mef": "id_recommandation_collegues_mef",
                        "sensation_montee_en_competences": "id_sensation_montee_en_competences",
                        "evolution_des_craintes_initiales": "id_evolution_des_craintes_initiales",
                        "utilite_metier_mef": "id_utilite_metier_mef",
                        "diminution_d_usage_ia_non_souveraines": "id_diminution_d_usage_ia_non_souveraines",
                        "comparaison_autres_ia": "id_comparaison_autres_ia",
                        "taux_moyen_de_correction_rep_assistant": "id_taux_moyen_de_correction_rep_assistant",
                        "cu2_taux_moyen_de_correction_assistant": "id_cu2_taux_moyen_de_correction_rep_assistant",
                        "cu3_taux_moyen_de_correction_assistant": "id_cu3_taux_moyen_de_correction_rep_assistant",
                        "raisons_perte_de_temps": "id_raisons_perte_de_temps",
                        "ia_favorise_relations_humaines_": "id_ia_favorise_relations_humaines_",
                    },
                    txt_columns=[
                        "no_id",
                        "autres_types_d_interactions",
                        "autres_formation_ia",
                        "raison_non_participation_rdv",
                        "autre_besoin_accompagnement",
                        "autres_difficultes",
                        "autres_freins",
                        "autres_sources_de_progression",
                        "autres_taches_realisees",
                        "les_usages_inattendus",
                        "mode_de_decouverte_usages",
                        "autre_mode_de_decouverte",
                        "autres_types_d_erreurs",
                        "cas_usage_principal_teste",
                        "commentaires",
                        "deuxieme_cas_d_usage_teste",
                        "commentaires2",
                        "troisieme_cas_d_usage",
                        "commentaires3",
                        "autres_impacts_identifies",
                        "autres_impacts_observes",
                        "autres_raisons",
                    ],
                    ref_columns=[
                        "id_niveau_d_usage_ia_post_expe_",
                        "id_recommandation_collegues_mef",
                        "id_sensation_montee_en_competences",
                        "id_evolution_des_craintes_initiales",
                        "id_utilite_metier_mef",
                        "id_diminution_d_usage_ia_non_souveraines",
                        "id_comparaison_autres_ia",
                        "id_taux_moyen_de_correction_rep_assistant",
                        "id_cu2_taux_moyen_de_correction_rep_assistant",
                        "id_cu3_taux_moyen_de_correction_rep_assistant",
                        "id_raisons_perte_de_temps",
                        "id_ia_favorise_relations_humaines_",
                    ],
                    custom_fn=process.process_questionnaire_2,
                ),
                input_key="questionnaire_2",
                output_key="questionnaire_2",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_typologie_interaction = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_typologie_interaction"),
        target="questionnaire_2_typologie_interaction",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "types_d_interactions_mef"],
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_2_typologie_interaction,
                ),
                input_key="questionnaire_2_typologie_interaction",
                output_key="questionnaire_2_typologie_interaction",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_formation_suivie = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_formation_suivie"),
        target="questionnaire_2_formation_suivie",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={
                        "formation_ia_suivie_post_expe_": "id_formation_ia_suivie_post_expe_",
                    },
                    cols_to_keep=["no_id", "formation_ia_suivie_post_expe_"],
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_2_formation_suivie,
                ),
                input_key="questionnaire_2_formation_suivie",
                output_key="questionnaire_2_formation_suivie",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_participation = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_participation"),
        target="questionnaire_2_participation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "participation_programme_rdv"],
                    cols_mapping={
                        "participation_programme_rdv": "id_participation_programme_rdv",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_2_participation,
                ),
                input_key="questionnaire_2_participation",
                output_key="questionnaire_2_participation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_freins = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_freins"),
        target="questionnaire_2_freins",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "freins_a_l_utilisation"],
                    cols_mapping={
                        "freins_a_l_utilisation": "id_freins_a_l_utilisation",
                    },
                    custom_fn=process.process_questionnaire_2_freins,
                ),
                input_key="questionnaire_2_freins",
                output_key="questionnaire_2_freins",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_facteurs_progression = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_facteurs_progression"),
        target="questionnaire_2_facteurs_progression",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "facteurs_de_progression"],
                    cols_mapping={
                        "facteurs_de_progression": "id_facteurs_de_progression",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_2_facteurs_progression,
                ),
                input_key="questionnaire_2_facteurs_progression",
                output_key="questionnaire_2_facteurs_progression",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_taches = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_taches"),
        target="questionnaire_2_taches",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "taches_realisees_avec_ia"],
                    cols_mapping={
                        "taches_realisees_avec_ia": "id_taches_realisees_avec_ia",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_2_taches,
                ),
                input_key="questionnaire_2_taches",
                output_key="questionnaire_2_taches",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_typologie_erreurs = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_typologie_erreurs"),
        target="questionnaire_2_typologie_erreurs",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "types_d_erreurs_frequentes2"],
                    cols_mapping={
                        "types_d_erreurs_frequentes2": "id_types_d_erreurs_frequentes2",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_2_typologie_erreurs,
                ),
                input_key="questionnaire_2_typologie_erreurs",
                output_key="questionnaire_2_typologie_erreurs",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_impact_observe = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_impact_observe"),
        target="questionnaire_2_impact_observe",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "observations_des_impacts"],
                    cols_mapping={
                        "observations_des_impacts": "id_observations_des_impacts",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_2_impact_observe,
                ),
                input_key="questionnaire_2_impact_observe",
                output_key="questionnaire_2_impact_observe",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_impact_identifie = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_impact_identifie"),
        target="questionnaire_2_impact_identifie",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "impacts_identifies_au_travail"],
                    cols_mapping={
                        "impacts_identifies_au_travail": "id_impacts_identifies_au_travail",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_2_impact_identifie,
                ),
                input_key="questionnaire_2_impact_identifie",
                output_key="questionnaire_2_impact_identifie",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )

    # Ordre des tâches
    chain(
        [
            questionnaire_2.create_task(),
            questionnaire_2_typologie_interaction.create_task(),
            questionnaire_2_formation_suivie.create_task(),
            questionnaire_2_participation.create_task(),
            questionnaire_2_freins.create_task(),
            questionnaire_2_facteurs_progression.create_task(),
            questionnaire_2_taches.create_task(),
            questionnaire_2_typologie_erreurs.create_task(),
            questionnaire_2_impact_observe.create_task(),
            questionnaire_2_impact_identifie.create_task(),
        ]
    )


@task_group
def suivi_questionnaire_2_bis() -> None:
    questionnaire_2_bis = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_bis"),
        target="questionnaire_2_bis",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "courriel",
                        "avez_vous_deja_utilise_l_assistant_ia_",
                        "autres_raisons",
                        "ajouter_quelque_chose",
                    ],
                    txt_columns=["courriel", "autres_raisons", "ajouter_quelque_chose"],
                    custom_fn=process.process_questionnaire_2_bis,
                ),
                input_key="questionnaire_2_bis",
                output_key="questionnaire_2_bis",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_2_bis_raisons_non_utilisation = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_2_bis_raisons_non_utilisation"),
        target="questionnaire_2_bis_raisons_non_utilisation",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["courriel", "raisons_non_utilisation_assistant_ia"],
                    cols_mapping={
                        "raisons_non_utilisation_assistant_ia": "id_raisons_non_utilisation",
                    },
                    txt_columns=["courriel"],
                    custom_fn=process.process_questionnaire_2_bis_raisons_non_utilisation,
                ),
                input_key="questionnaire_2_bis_raisons_non_utilisation",
                output_key="questionnaire_2_bis_raisons_non_utilisation",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Ordre des tâches
    chain(
        [
            questionnaire_2_bis.create_task(),
            questionnaire_2_bis_raisons_non_utilisation.create_task(),
        ]
    )


@task_group
def suivi_questionnaire_3() -> None:
    questionnaire_3 = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_3"),
        target="questionnaire_3",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=[
                        "no_id",
                        "temps_fonction_exercee",
                        "genre",
                        "frequence_utilisation",
                        "evolution_usage",
                        "quelles_raisons_facons",
                        "raisons_non_participation",
                        "autres",
                        "evaluation_niveau_acculturation",
                        "evolution_sentiment",
                        "autres_leviers",
                        "impacts_taches_pro",
                        "temps_gagnes",
                        "impacts_taches_rebarbatives",
                        "impact_perception",
                        "sentiment_de_fierte",
                        "experimentation_interne",
                        "autres_outils",
                        "satisfaction_autre_outil",
                        "comparaison_autres_ia",
                        "autres_fonctionnalites",
                        "utilisation_moindre",
                        "autres_risques",
                        "recommandations",
                        "etre_ambassadeur",
                        "bonnes_pratiques",
                        "autres_besoins_importants",
                        "autres_besoins_moindres",
                        "ameliorations",
                        "aspects_a_ameliorer",
                        "interface",
                        "contenu",
                        "connexions",
                        "autre_retour_libre",
                        "retours_libres",
                    ],
                    cols_mapping={
                        "raisons_non_participation": "id_raisons_non_participation",
                        "impacts_taches_pro": "id_impacts_taches_pro",
                        "impacts_taches_rebarbatives": "id_impacts_taches_rebarbatives",
                        "autres_outils": "id_autres_outils",
                        "satisfaction_autre_outil": "id_satisfaction_autre_outil",
                        "comparaison_autres_ia": "id_comparaison_autres_ia",
                    },
                    txt_columns=[
                        "no_id",
                        "quelles_raisons_facons",
                        "autres",
                        "autres_leviers",
                        "autres_fonctionnalites",
                        "autres_risques",
                        "bonnes_pratiques",
                        "autres_besoins_importants",
                        "autres_besoins_moindres",
                        "ameliorations",
                        "aspects_a_ameliorer",
                        "interface",
                        "contenu",
                        "connexions",
                        "autre_retour_libre",
                        "retours_libres",
                    ],
                    ref_columns=[
                        "id_raisons_non_participation",
                        "id_impacts_taches_pro",
                        "id_impacts_taches_rebarbatives",
                        "id_autres_outils",
                        "id_satisfaction_autre_outil",
                        "id_comparaison_autres_ia",
                    ],
                    custom_fn=process.process_questionnaire_3,
                ),
                input_key="questionnaire_3",
                output_key="questionnaire_3",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_3_formation_suivie = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_3"),
        target="questionnaire_3_formation_suivie",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "formation_suivie"],
                    cols_mapping={"formation_suivie": "id_formation_suivie"},
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_3_formation_suivie,
                ),
                input_key="questionnaire_3_formation_suivie",
                output_key="questionnaire_3_formation_suivie",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_3_programme_rdv = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_3_programme_rdv"),
        target="questionnaire_3_programme_rdv",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "programme_de_rdv"],
                    cols_mapping={"programme_de_rdv": "id_programme_de_rdv"},
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_3_programme_rdv,
                ),
                input_key="questionnaire_3_programme_rdv",
                output_key="questionnaire_3_programme_rdv",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_3_leviers_progression = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_3_leviers_progression"),
        target="questionnaire_3_leviers_progression",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "leviers_progression"],
                    cols_mapping={
                        "leviers_progression": "id_leviers_progression",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_3_leviers_progression,
                ),
                input_key="questionnaire_3_leviers_progression",
                output_key="questionnaire_3_leviers_progression",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_3_fonctionnalites = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_3_fonctionnalites"),
        target="questionnaire_3_fonctionnalites",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "fonctionnalites"],
                    cols_mapping={"fonctionnalites": "id_fonctionnalites"},
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_3_fonctionnalites,
                ),
                input_key="questionnaire_3_fonctionnalites",
                output_key="questionnaire_3_fonctionnalites",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_3_risques_identifies = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_3_risques_identifies"),
        target="questionnaire_3_risques_identifies",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "risques_identifies"],
                    cols_mapping={
                        "risques_identifies": "id_risques_identifies",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_3_risques_identifies,
                ),
                input_key="questionnaire_3_risques_identifies",
                output_key="questionnaire_3_risques_identifies",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_3_besoins_prioritaires = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_3_besoins_prioritaires"),
        target="questionnaire_3_besoins_prioritaires",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "besoins_prioritaires"],
                    cols_mapping={
                        "besoins_prioritaires": "id_besoins_prioritaires",
                    },
                    txt_columns=[
                        "no_id",
                    ],
                    custom_fn=process.process_questionnaire_3_besoins_prioritaires,
                ),
                input_key="questionnaire_3_besoins_prioritaires",
                output_key="questionnaire_3_besoins_prioritaires",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    questionnaire_3_besoins_moindres = ETLTask(
        task_config=TaskConfig(task_id="questionnaire_3_besoins_moindres"),
        target="questionnaire_3_besoins_moindres",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_to_keep=["no_id", "besoins_moindres"],
                    cols_mapping={"besoins_moindres": "id_besoins_moindres"},
                    custom_fn=process.process_questionnaire_3_besoins_moindres,
                ),
                input_key="questionnaire_3_besoins_moindres",
                output_key="questionnaire_3_besoins_moindres",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=True,
    )
    # Ordre des tâches
    chain(
        [
            questionnaire_3.create_task(),
            questionnaire_3_formation_suivie.create_task(),
            questionnaire_3_programme_rdv.create_task(),
            questionnaire_3_leviers_progression.create_task(),
            questionnaire_3_fonctionnalites.create_task(),
            questionnaire_3_risques_identifies.create_task(),
            questionnaire_3_besoins_prioritaires.create_task(),
            questionnaire_3_besoins_moindres.create_task(),
        ]
    )
