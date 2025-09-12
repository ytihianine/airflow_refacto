"""
    Contient la définition du DAG qui se base sur des documents Grist
"""

from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator


from dags.templates.dag_file.tasks import task_1, task_2


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=20),
}


# Définition du DAG
@dag(
    "template_file_dag",
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=["TEMPLATE"],
    description="""Pipeline de templating DAG avec les Fichiers""",
    max_consecutive_failed_dag_runs=1,
    default_args=default_args,
)
def template_file_dag():
    """
    Dag
    """
    link_task = EmptyOperator(task_id="link_task")

    # Ordre des tâches
    chain(task_1(), task_2(), link_task)


template_file_dag()
