"""
    Contient toutes les tâches du DAG
"""

from airflow.decorators import task


from dags.templates.dag_grist.process import process_fn_1, process_fn_2


@task(task_id="task_1")
def task_1() -> None:
    pass


@task(task_id="task_2")
def task_2() -> None:
    pass
