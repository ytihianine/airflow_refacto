from airflow.decorators import dag, task_group
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from utils.tasks.sql import (
    # get_project_config,
    get_tbl_names_from_postgresql,
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
)
from utils.tasks.grist import download_grist_doc_to_s3
from utils.config.tasks import get_storage_rows

from dags.sg.dsci.accompagnements_dsci.tasks import create_task
from dags.sg.dsci.accompagnements_dsci import process

LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/dsci/carte_identite_mef?ref_type=heads"  # noqa
LINK_DOC_DATA = (
    "https://grist.numerique.gouv.fr/o/catalogue/k9LvttaYoxe6/catalogage-MEF"
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


@dag(
    default_args=default_args,
    schedule_interval="*/5 8-13,14-19 * * 1-5",
    catchup=False,
    dag_id="accompagnements_dsci",
    params={
        "nom_projet": "Accompagnements DSCI",
        "sqlite_file_s3_filepath": "sg/dsci/accompagnements_dsci/accompagnements_dsci.db",
        "mail": {
            "enable": False,
            "To": ["yanis.tihianine@finances.gouv.fr"],
            "CC": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
)
def accompagnements_dsci_dag():
    # Variables
    nom_projet = "Accompagnements DSCI"
    sqlite_file_s3_filepath = "sg/dsci/accompagnements_dsci/accompagnements_dsci.db"
    # Database
    prod_schema = "activite_dsci"

    # Création des tâches
    @task_group
    def referentiels() -> None:
        ref_bureau = create_task(
            selecteur="ref_bureau", process_func=process.process_ref_bureau
        )
        ref_certification = create_task(
            selecteur="ref_certification",
            process_func=process.process_ref_certification,
        )
        ref_competence_particuliere = create_task(
            selecteur="ref_competence_particuliere",
            process_func=process.process_ref_competence_particuliere,
        )
        ref_direction = create_task(
            selecteur="ref_direction", process_func=process.process_ref_direction
        )
        ref_profil_correspondant = create_task(
            selecteur="ref_profil_correspondant",
            process_func=process.process_ref_profil_correspondant,
        )
        ref_promotion_fac = create_task(
            selecteur="ref_promotion_fac",
            process_func=process.process_ref_promotion_fac,
        )
        ref_qualite_service = create_task(
            selecteur="ref_qualite_service",
            process_func=process.process_ref_qualite_service,
        )
        ref_region = create_task(
            selecteur="ref_region", process_func=process.process_ref_region
        )
        ref_semainier = create_task(
            selecteur="ref_semainier", process_func=process.process_ref_semainier
        )
        ref_typologie_accompagnement = create_task(
            selecteur="ref_typologie_accompagnement",
            process_func=process.process_ref_typologie_accompagnement,
        )
        ref_pole = create_task(
            selecteur="ref_pole", process_func=process.process_ref_pole
        )
        ref_type_accompagnement = create_task(
            selecteur="ref_type_accompagnement",
            process_func=process.process_ref_type_accompagnement,
        )

        # Ordre des tâches
        chain(
            [
                ref_bureau,
                ref_certification,
                ref_competence_particuliere,
                ref_direction,
                ref_profil_correspondant,
                ref_promotion_fac,
                ref_qualite_service,
                ref_region,
                ref_semainier,
                ref_typologie_accompagnement,
                ref_pole,
                ref_type_accompagnement,
            ],
        )

    @task_group
    def bilaterales() -> None:
        struc_bilaterales = create_task(
            selecteur="struc_bilaterales",
            process_func=process.process_struc_bilaterales,
        )
        struc_bilaterale_remontee = create_task(
            selecteur="struc_bilaterale_remontee",
            process_func=process.process_struc_bilaterale_remontee,
        )
        # Ordre des tâches
        chain([struc_bilaterales, struc_bilaterale_remontee])

    @task_group
    def correspondant() -> None:
        struc_correspondant = create_task(
            selecteur="struc_correspondant",
            process_func=process.process_struc_correspondant,
        )
        struc_correspondant_profil = create_task(
            selecteur="struc_correspondant_profil",
            process_func=process.process_struc_correspondant_profil,
        )
        # struc_correspondant_certification = create_task(
        #     selecteur="struc_correspondant_certification",
        #     process_func=process.process_struc_correspondant_certification,
        # )

        # Ordre des tâches
        chain([struc_correspondant, struc_correspondant_profil])

    @task_group
    def mission_innovation() -> None:
        struc_accompagnement_mi = create_task(
            selecteur="struc_accompagnement_mi",
            process_func=process.process_struc_accompagnement_mi,
        )
        struc_accompagnement_mi_satisfaction = create_task(
            selecteur="struc_accompagnement_mi_satisfaction",
            process_func=process.process_struc_accompagnement_mi_satisfaction,
        )

        # Ordre des tâches
        chain([struc_accompagnement_mi, struc_accompagnement_mi_satisfaction])

    # Ordre des tâches
    chain(
        # get_project_config(),
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema, tbl_names_task_id="get_tbl_names_from_postgresql"
        ),
        download_grist_doc_to_s3(
            workspace_id="dsci",
            doc_id_key="grist_doc_id_accompagnements_dsci",
            s3_filepath=sqlite_file_s3_filepath,
        ),
        [referentiels(), bilaterales(), correspondant(), mission_innovation()],
        import_file_to_db.partial(keep_file_id_col=True).expand(
            storage_row=get_storage_rows(nom_projet=nom_projet).to_dict("records")
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema, tbl_names_task_id="get_tbl_names_from_postgresql"
        ),
    )


accompagnements_dsci_dag()
