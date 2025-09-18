"""
    Contient la définition du DAG qui se base sur des fichiers déposés par l'agent
"""

from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

from dags.templates.dag_grist.tasks import task_1, task_2

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
    "template_dag_grist",
    schedule_interval="*/20 * * * 1-5",
    max_active_runs=1,
    catchup=False,
    tags=["TEMPLATE"],
    description="""Pipeline de templating DAG avec Grist""",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
    params={
        "send_mail": True,
    },
)
def template_dag_grist():
    link_task = EmptyOperator(task_id="link_task")

    # Ordre des tâches
    chain(task_1(), task_2(), link_task)


template_dag_grist()
