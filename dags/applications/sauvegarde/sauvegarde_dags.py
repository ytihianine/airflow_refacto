from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from utils.mails.mails import make_mail_func_callback, MailStatus

from dags.applications.sauvegarde.tasks import (
    bearer_token,
    get_dashboard_ids_and_titles,
    get_dashboard_export,
    # export_user_roles,
)


link_documentation_pipeline = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/transverse/sauvegarde?ref_type=heads"  # noqa
link_documentation_donnees = ""  # noqa


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# Définition du DAG
@dag(
    "sauvegarde-tdb",
    schedule="@daily",
    max_active_runs=1,
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "SAUVEGARDE", "CHARTSGOUV"],
    description="Pipeline de des tableaux de bord Chartsgouv.",
    params={
        "nom_projet": "Sauvegarde tableaux de bords",
        "mail": {
            "enable": True,
            "to": ["yanis.tihianine@finances.gouv.fr"],
            "CC": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": link_documentation_pipeline,
            "lien_donnees": link_documentation_donnees,
        },
    },
    on_failure_callback=make_mail_func_callback(
        mail_statut=MailStatus.ERROR,
    ),
    on_success_callback=make_mail_func_callback(mail_statut=MailStatus.SUCCESS),
    default_args=default_args,
)
def sauvegarde_pipeline():
    dashboard_ids_and_titles = get_dashboard_ids_and_titles()
    dashboard_export = get_dashboard_export.expand(
        dashboard_id_title=dashboard_ids_and_titles
    )

    # Ordre des tâches
    chain(
        bearer_token(),
        dashboard_ids_and_titles,
        dashboard_export,
        # export_user_roles(
        #     s3_file_handler=MINIO_FILE_HANDLER
        # )
    )


sauvegarde_pipeline()
