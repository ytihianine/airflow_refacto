from utils.tasks.validation import create_validate_params_task
from utils.config.types import ALL_KEYS
from utils.tasks.etl import create_file_etl_task

from dags.sg.siep.mmsi.oad_referentiel import process


validate_params = create_validate_params_task(
    required_paths=ALL_KEYS,
    require_truthy=None,
    task_id="validate_dag_params",
)


bien_typologie = create_file_etl_task(
    selecteur="ref_typologie",
    process_func=process.process_typologie_bien,
)
