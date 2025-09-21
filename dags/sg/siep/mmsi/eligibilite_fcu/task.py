from utils.tasks.etl import create_action_to_file_etl_task

from dags.sg.siep.mmsi.eligibilite_fcu.actions import eligibilite_fcu


eligibilite_fcu_to_file = create_action_to_file_etl_task(
    output_selecteur="fcu",
    task_id="eligibilite_fcu_to_file",
    action_func=eligibilite_fcu,
)
