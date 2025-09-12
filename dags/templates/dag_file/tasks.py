"""
    Contient toutes les tâches du DAG
"""

from airflow.decorators import task


@task(task_id="task_1")
def task_1() -> None:
    pass


@task(task_id="task_2")
def task_2() -> None:
    pass
