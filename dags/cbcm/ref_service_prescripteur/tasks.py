from functools import partial

from airflow.sdk import task_group
from airflow.sdk.bases.operator import chain
from dags.cbcm.ref_service_prescripteur import actions, process
from modules.common_tasks.etl import create_task
from modules.common_tasks.grist import generic_grist_processing
from modules.types.dags import ETLStep, TaskConfig
from modules.types.readers import GristReaderStrategy
from modules.types.tasks import ETLTask, SingleInputStep
from modules.types.writers import FileWriterStrategy


@task_group(group_id="grist_source")
def grist_source() -> None:
    ref_prog = ETLTask(
        task_config=TaskConfig(task_id="ref_prog"),
        target="ref_prog",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["prog"],
                    custom_fn=process.process_ref_prog,
                ),
                input_key="ref_prog",
                output_key="ref_prog",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    ref_bop = ETLTask(
        task_config=TaskConfig(task_id="ref_bop"),
        target="ref_bop",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["bop"],
                    ref_columns=["prog"],
                    custom_fn=process.process_ref_bop,
                ),
                input_key="ref_bop",
                output_key="ref_bop",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    ref_uo = ETLTask(
        task_config=TaskConfig(task_id="ref_uo"),
        target="ref_uo",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["uo"],
                    ref_columns=["prog", "bop"],
                    custom_fn=process.process_ref_uo,
                ),
                input_key="ref_uo",
                output_key="ref_uo",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    ref_cc = ETLTask(
        task_config=TaskConfig(task_id="ref_cc"),
        target="ref_cc",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["cc"],
                    ref_columns=["prog", "bop", "uo"],
                    custom_fn=process.process_ref_cc,
                ),
                input_key="ref_cc",
                output_key="ref_cc",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    ref_sp_pilotage = ETLTask(
        task_config=TaskConfig(task_id="ref_sp_pilotage"),
        target="ref_sp_pilotage",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["service_prescripteur"],
                    custom_fn=process.process_ref_sp_pilotage,
                ),
                input_key="ref_sp_pilotage",
                output_key="ref_sp_pilotage",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    ref_sp_choisi = ETLTask(
        task_config=TaskConfig(task_id="ref_sp_choisi"),
        target="ref_sp_choisi",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["service_prescripteur", "mail"],
                    custom_fn=process.process_ref_sp_choisi,
                ),
                input_key="ref_sp_choisi",
                output_key="ref_sp_choisi",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    ref_sdep = ETLTask(
        task_config=TaskConfig(task_id="ref_sdep"),
        target="ref_sdep",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    txt_columns=["service_depense"],
                    custom_fn=process.process_ref_sdep,
                ),
                input_key="ref_sdep",
                output_key="ref_sdep",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    sp = ETLTask(
        task_config=TaskConfig(task_id="sp"),
        target="sp",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"centre_de_cout": "centre_cout"},
                    txt_columns=[
                        "centre_financier",
                        "centre_cout",
                        "couple_cf_cc",
                        "observation",
                    ],
                    date_columns=["date_derniere_maj"],
                    ref_columns=[
                        "service_prescripteur_pilotage_",
                        "service_depense",
                        "service_prescripteur_choisi_selon_cf_cc",
                        "designation_prog",
                        "designation_bop",
                        "designation_uo",
                        "designation_cc",
                    ],
                    custom_fn=process.process_sp,
                ),
                input_key="sp",
                output_key="sp",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    # Services prescripteurs renseignés manuellement
    delai_global_paiement_sp_manuel = ETLTask(
        task_config=TaskConfig(task_id="delai_global_paiement_sp_manuel"),
        target="delai_global_paiement_sp_manuel",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"service_prescripteur": "id_service_prescripteur"},
                    ref_columns=["id_service_prescripteur"],
                    custom_fn=process.process_delai_global_paiement_sp_manuel,
                ),
                input_key="delai_global_paiement_sp_manuel",
                output_key="delai_global_paiement_sp_manuel",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    demande_achat_sp_manuel = ETLTask(
        task_config=TaskConfig(task_id="demande_achat_sp_manuel"),
        target="demande_achat_sp_manuel",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"service_prescripteur": "id_service_prescripteur"},
                    ref_columns=["id_service_prescripteur"],
                    custom_fn=process.process_demande_achat_sp_manuel,
                ),
                input_key="demande_achat_sp_manuel",
                output_key="demande_achat_sp_manuel",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    demande_paiement_sp_manuel = ETLTask(
        task_config=TaskConfig(task_id="demande_paiement_sp_manuel"),
        target="demande_paiement_sp_manuel",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"service_prescripteur": "id_service_prescripteur"},
                    ref_columns=["id_service_prescripteur"],
                    custom_fn=process.process_demande_paiement_sp_manuel,
                ),
                input_key="demande_paiement_sp_manuel",
                output_key="demande_paiement_sp_manuel",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )
    engagement_juridique_sp_manuel = ETLTask(
        task_config=TaskConfig(task_id="engagement_juridique_sp_manuel"),
        target="engagement_juridique_sp_manuel",
        reader=GristReaderStrategy(),
        steps=[
            SingleInputStep(
                fn=partial(
                    generic_grist_processing,
                    cols_mapping={"service_prescripteur": "id_service_prescripteur"},
                    ref_columns=["id_service_prescripteur"],
                    custom_fn=process.process_engagement_juridique_sp_manuel,
                ),
                input_key="engagement_juridique_sp_manuel",
                output_key="engagement_juridique_sp_manuel",
            )
        ],
        writers=[FileWriterStrategy()],
        add_metadata=False,
    )

    chain(
        [
            ref_prog.create_task(),
            ref_bop.create_task(),
            ref_uo.create_task(),
            ref_cc.create_task(),
            ref_sdep.create_task(),
            ref_sp_choisi.create_task(),
            ref_sp_pilotage.create_task(),
            sp.create_task(),
            delai_global_paiement_sp_manuel.create_task(),
            demande_achat_sp_manuel.create_task(),
            demande_paiement_sp_manuel.create_task(),
            engagement_juridique_sp_manuel.create_task(),
        ]
    )


@task_group(group_id="fetch_from_db")
def fetch_from_db() -> None:
    get_all_cf_cc = create_task(
        task_config=TaskConfig(task_id="get_all_cf_cc"),
        output_selecteur="get_all_cf_cc",
        steps=[
            ETLStep(
                fn=actions.get_all_cf_cc,
            )
        ],
    )
    get_demande_achat = create_task(
        task_config=TaskConfig(task_id="get_demande_achat"),
        output_selecteur="get_demande_achat",
        steps=[
            ETLStep(
                fn=actions.get_demande_achat,
            )
        ],
    )
    get_demande_paiement_complet = create_task(
        task_config=TaskConfig(task_id="get_demande_paiement_complet"),
        output_selecteur="get_demande_paiement_complet",
        steps=[
            ETLStep(
                fn=actions.get_demande_paiement_complet,
            )
        ],
    )
    get_delai_global_paiement = create_task(
        task_config=TaskConfig(task_id="get_delai_global_paiement"),
        output_selecteur="get_delai_global_paiement",
        steps=[
            ETLStep(
                fn=actions.get_delai_global_paiement,
            )
        ],
    )

    get_engagement_juridique = create_task(
        task_config=TaskConfig(task_id="get_engagement_juridique"),
        output_selecteur="get_engagement_juridique",
        steps=[
            ETLStep(
                fn=actions.get_engagement_juridique,
            )
        ],
    )

    chain(
        [
            get_all_cf_cc(),
            get_demande_achat(),
            get_demande_paiement_complet(),
            get_delai_global_paiement(),
            get_engagement_juridique(),
        ]
    )


@task_group(group_id="load_to_grist")
def load_to_grist() -> None:
    load_new_cf_cc = create_task(
        task_config=TaskConfig(task_id="load_new_cf_cc"),
        output_selecteur="load_new_cf_cc",
        input_selecteurs=["get_all_cf_cc", "sp"],
        steps=[ETLStep(fn=actions.load_new_cf_cc, read_data=True)],
        export_output=False,
    )

    load_demande_achat = create_task(
        task_config=TaskConfig(task_id="load_demande_achat"),
        output_selecteur="load_demande_achat",
        input_selecteurs=["get_demande_achat", "demande_achat_sp_manuel"],
        steps=[
            ETLStep(
                fn=actions.load_demande_achat,
                read_data=True,
            )
        ],
        export_output=False,
    )

    load_demande_paiement_complet = create_task(
        task_config=TaskConfig(task_id="load_demande_paiement_complet"),
        output_selecteur="load_demande_paiement_complet",
        input_selecteurs=[
            "get_demande_paiement_complet",
            "demande_paiement_sp_manuel",
        ],
        steps=[
            ETLStep(
                fn=actions.load_demande_paiement_complet,
                read_data=True,
            )
        ],
        export_output=False,
    )

    load_delai_global_paiement = create_task(
        task_config=TaskConfig(task_id="load_delai_global_paiement"),
        output_selecteur="load_delai_global_paiement",
        input_selecteurs=[
            "get_delai_global_paiement",
            "delai_global_paiement_sp_manuel",
        ],
        steps=[
            ETLStep(
                fn=actions.load_delai_global_paiement,
                read_data=True,
            )
        ],
        export_output=False,
    )

    load_engagement_juridique = create_task(
        task_config=TaskConfig(task_id="load_engagement_juridique"),
        output_selecteur="load_engagement_juridique",
        input_selecteurs=["get_engagement_juridique", "engagement_juridique_sp_manuel"],
        steps=[
            ETLStep(
                fn=actions.load_engagement_juridique,
                read_data=True,
            )
        ],
        export_output=False,
    )

    chain(
        [
            load_new_cf_cc(),
            load_demande_achat(),
            load_demande_paiement_complet(),
            load_delai_global_paiement(),
            load_engagement_juridique(),
        ]
    )
