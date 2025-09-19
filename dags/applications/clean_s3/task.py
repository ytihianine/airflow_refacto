from airflow.decorators import task_group
from airflow.models.baseoperator import chain

from utils.config.types import (
    KEY_NOM_PROJET,
    KEY_MAIL,
    KEY_MAIL_ENABLE,
    KEY_MAIL_TO,
    KEY_MAIL_CC,
    KEY_DOCS,
    KEY_DOCS_LIEN_PIPELINE,
    KEY_DOCS_LIEN_DONNEES,
)
from utils.tasks.etl import create_action_etl_task
from utils.tasks.validation import create_validate_params_task

from dags.applications.clean_s3.actions import list_keys, process_keys, delete_old_keys


validate_params = create_validate_params_task(
    required_paths=[
        KEY_NOM_PROJET,
        KEY_MAIL,
        KEY_MAIL_ENABLE,
        KEY_MAIL_TO,
        KEY_MAIL_CC,
        KEY_DOCS,
        KEY_DOCS_LIEN_PIPELINE,
        KEY_DOCS_LIEN_DONNEES,
    ],
    require_truthy=None,
    task_id="validate_dag_params",
)


@task_group(group_id="clean_s3")
def clean_s3_task_group() -> None:
    get_keys = create_action_etl_task(
        task_id="list_keys",
        action_func=list_keys,
        action_kwargs={"selecteur": "list_keys"},
    )
    filter_keys = create_action_etl_task(
        task_id="process_keys",
        action_func=process_keys,
        action_kwargs={
            "input_selecteur": "list_keys",
            "output_selecteur": "process_keys",
        },
    )
    delete_keys = create_action_etl_task(
        task_id="delete_old_keys",
        action_func=delete_old_keys,
        action_kwargs={"input_selecteur": "delete_old_keys"},
    )

    chain(get_keys(), filter_keys(), delete_keys())
