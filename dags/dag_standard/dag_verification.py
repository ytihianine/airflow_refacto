from typing import Any


from pprint import pprint

from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

# from infra.mails.sender import create_airflow_callback, MailStatus
from infra.mails.default_smtp import (
    send_mail,
    create_airflow_callback,
    MailStatus,
    MailMessage,
)

from utils.tasks.sql import create_projet_snapshot, get_projet_snapshot


LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/consommation_batiment?ref_type=heads"  # noqa
LINK_DOC_DONNEES = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa


default_args: dict[str, Any] = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(n=1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


# Définition du DAG
@dag(
    dag_id="dag_verification",
    schedule_interval=None,
    max_active_runs=1,
    max_consecutive_failed_dag_runs=1,
    catchup=False,
    tags=["SG", "Vérification"],
    description="Dag de vérification.",  # noqa
    default_args=default_args,
    params={
        "nom_projet": "Projet test",
        "db": {
            "prod_schema": "dsci",
            "tmp_schema": "temporaire",
        },
        "mail": {
            "enable": False,
            "to": ["yanis.tihianine@finances.gouv.fr"],
            "cc": ["labo-data@finances.gouv.fr"],
        },
        "docs": {
            "lien_pipeline": LINK_DOC_PIPELINE,
            "lien_donnees": LINK_DOC_DONNEES,
        },
    },
    on_failure_callback=create_airflow_callback(mail_status=MailStatus.ERROR),
)
def dag_verification() -> None:
    @task
    def print_context(**context) -> None:
        pprint(object=context)
        pprint(object=context["dag"].__dict__)
        pprint(object=context["ti"].__dict__)
        pprint(
            object=context["ti"].xcom_pull(
                key="snapshot_id", task_ids="get_projet_snapshot"
            )
        )

    @task
    def send_simple_mail(**context) -> None:
        mail_message = MailMessage(
            to=["yanis.tihianine@finances.gouv.fr"],
            cc=["yanis.tihianine@finances.gouv.fr"],
            subject="Simple test depuis Airflow",
            html_content="Test réussi !",
        )
        send_mail(mail_message=mail_message)

    @task
    def send_error_mail(**context) -> None:
        mail_task = create_airflow_callback(mail_status=MailStatus.ERROR)
        mail_task()

    @task
    def send_success_mail(**context) -> None:
        mail_task = create_airflow_callback(mail_status=MailStatus.SUCCESS)
        mail_task()

    # Ordre des tâches
    chain(
        [
            create_projet_snapshot(),
            get_projet_snapshot(),
            print_context(),
            send_simple_mail(),
            send_error_mail(),
            send_success_mail(),
        ],
    )


dag_verification()
