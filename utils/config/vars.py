""" Répertorie les variables communes à toutes les pipelines """

import os
import pytz
from functools import lru_cache


@lru_cache(maxsize=1)
def get_root_folder() -> str:
    """Get root folder based on environment."""
    base_folder = os.getenv("AIRFLOW_HOME")
    if base_folder is None:
        return "/home/onyxia/work/airflow-demo"
    return os.path.join(base_folder, "dags", "repo")


NO_PROCESS_MSG = "No complementary actions needed ! Skipping ..."

ENV_VAR = os.environ.copy()

""" Configuration du proxy """
PROXY = "172.16.0.53:3128"
AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0"
)

""" TimeZone """
paris_tz = pytz.timezone("Europe/Paris")

""" MinIO """
TMP_KEY = "tmp"

# DEFAULT VARIABLES
DEFAULT_SMTP_CONN_ID = "smtp_nubonyxia"
DEFAULT_TMP_SCHEMA = "temporaire"
DEFAULT_PG_DATA_CONN_ID = "db_data_store"
DEFAULT_PG_CONFIG_CONN_ID = "db_depose_fichier"
DEFAULT_S3_CONN_ID = "minio_bucket_dsci"
DEFAULT_S3_BUCKET = "dsci"
