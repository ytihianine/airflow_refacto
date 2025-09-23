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

from dags.applications.db_backup import actions


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


dump_databases = create_action_etl_task(
    task_id="db_backup",
    action_func=actions.create_dump_files,
    action_kwargs={"nom_projet": "Sauvegarde databases"},
)
