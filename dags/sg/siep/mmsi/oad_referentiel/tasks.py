from dags.sg.siep.mmsi.oad_referentiel import process
from utils.tasks.etl import create_file_etl_task


bien_typologie = create_file_etl_task(
    selecteur="bien_typologie",
    process_func=process.process_typologie_bien,
)
