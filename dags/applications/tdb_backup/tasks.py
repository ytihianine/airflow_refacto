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
)
from utils.tasks.etl import create_action_etl_task
from utils.tasks.validation import create_validate_params_task

from dags.applications.tdb_backup import actions


validate_params = create_validate_params_task(
    required_paths=[
        KEY_NOM_PROJET,
        KEY_MAIL,
        KEY_MAIL_ENABLE,
        KEY_MAIL_TO,
        KEY_MAIL_CC,
        KEY_DOCS,
        KEY_DOCS_LIEN_PIPELINE,
    ],
    require_truthy=None,
    task_id="validate_dag_params",
)


@task_group(group_id="tdb_backup")
def tdb_backup_task_group() -> None:
    """Task group to backup dashboards and user roles from Chartsgouv (Superset)."""
    access_token = create_action_etl_task(
        task_id="access_token",
        action_func=actions.get_bearer_token,
    )
    dashboards_info = create_action_etl_task(
        task_id="dashboards_info", action_func=actions.get_dashboard_ids_and_titles
    )
    export_dashboards = create_action_etl_task(
        task_id="export_dashboards", action_func=actions.get_dashboard_export
    )

    chain(
        access_token(),
        dashboards_info.expand(action_kwargs={"access_token": access_token}),
        export_dashboards.expand(
            action_kwargs={
                "access_token": access_token,
                "dashboards_info": dashboards_info,
            }
        ),
    )
