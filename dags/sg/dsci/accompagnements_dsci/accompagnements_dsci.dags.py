from datetime import timedelta
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
from utils.config.dag_params import create_dag_params, create_default_args

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


@dag(
    dag_id="accompagnements_dsci",
    schedule_interval="*/5 8-13,14-19 * * 1-5",
    default_args=create_default_args(retries=1, retry_delay=timedelta(seconds=30)),
    catchup=False,
    params=create_dag_params(
        nom_projet=nom_projet,
        prod_schema="activite_dsci",
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
        mail_enable=False,
    ),
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
