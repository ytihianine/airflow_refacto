from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from utils.tasks.sql import (
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
    delete_tmp_tables,
)
from utils.tasks.grist import download_grist_doc_to_s3
from utils.config.tasks import get_projet_config

from dags.sg.dsci.accompagnements_dsci.tasks import (
    validate_params,
    referentiels,
    bilaterales,
    correspondant,
    mission_innovation,
)

# Variables
nom_projet = "Accompagnements DSCI"
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
        "nom_projet": nom_projet,
        "db": {
            "prod_schema": "activite_dsci",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": ["yanis.tihianine@finances.gouv.fr"],
            "cc": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
)
def accompagnements_dsci_dag():

    # Ordre des tâches
    chain(
        validate_params(),
        download_grist_doc_to_s3(
            selecteur="grist_doc",
            workspace_id="dsci",
            doc_id_key="grist_doc_id_accompagnements_dsci",
        ),
        [referentiels(), bilaterales(), correspondant(), mission_innovation()],
        create_tmp_tables(),
        import_file_to_db.partial(keep_file_id_col=True).expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        delete_tmp_tables(),
    )


accompagnements_dsci_dag()
