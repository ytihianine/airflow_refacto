from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from datetime import timedelta

from utils.mails.mails import make_mail_func_callback, MailStatus
from utils.tasks.sql import (
    get_tbl_names_from_postgresql,
    create_tmp_tables,
    import_file_to_db,
    copy_tmp_table_to_real_table,
)
from utils.config.tasks import get_storage_rows
from utils.tasks.grist import download_grist_doc_to_s3

from dags.cgefi.suivi_activite.tasks import (
    referentiels,
    informations_generales,
    processus_4,
    processus_6,
    processus_atpro,
)

# Mails
To = []  # ["brigitte.lekime@finances.gouv.fr", "yanis.tihianine@finances.gouv.fr"]
CC = ["labo-data@finances.gouv.fr"]
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
    "retry_delay": timedelta(seconds=10),
}


# Définition du DAG
@dag(
    "cgefi_suivi_activite",
    schedule_interval="*/5 * * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["CGEFI", "ACTIVITE", "POC", "TABLEAU DE BORD"],
    description="""Pipeline qui récupère les nouvelles données dans Grist
        pour actualiser le tableau de bord de suivi d'activité du CGEFI""",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "nom_projet": "Emploi et formation",
        "sqlite_file_s3_filepath": "cgefi/suivi_activite/suivi_activite.db",
        "mail": {
            "enable": False,
            "To": To,
            "CC": CC,
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DATA,
        },
    },
    on_failure_callback=make_mail_func_callback(mail_statut=MailStatus.ERROR),
)
def suivi_activite():
    # Variables
    nom_projet = "Emploi et formation"
    sqlite_file_s3_filepath = "cgefi/suivi_activite/suivi_activite.db"
    # Database
    prod_schema = "cgefi_poc"

    """ Task definition """

    chain(
        # projet_config,
        download_grist_doc_to_s3(
            workspace_id="dsci-cgefi",
            doc_id_key="grist_doc_id_cgefi_suivi_activite",
            s3_filepath=sqlite_file_s3_filepath,
        ),
        get_tbl_names_from_postgresql(),
        create_tmp_tables(
            prod_schema=prod_schema,
            tbl_names_task_id="get_tbl_names_from_postgresql",
        ),
        [
            referentiels(),
            informations_generales(),
            processus_4(),
            processus_6(),
            processus_atpro(),
        ],
        import_file_to_db.partial(keep_file_id_col=True).expand(
            storage_row=get_storage_rows(nom_projet=nom_projet).to_dict("records")
        ),
        copy_tmp_table_to_real_table(
            prod_schema=prod_schema, tbl_names_task_id="get_tbl_names_from_postgresql"
        ),
    )


suivi_activite()
