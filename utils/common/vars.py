""" Répertorie les variables communes à toutes les pipelines """

import os
import pytz


def get_root_folder():
    """Get root folder based on environnement"""
    # Check if attribute is already present => lazy initialisation
    if not hasattr(get_root_folder, "_base_folder"):
        base_folder = os.getenv("AIRFLOW_HOME", None)
        if base_folder is None:
            get_root_folder._base_folder = "/home/onyxia/work/airflow-demo"
        else:
            get_root_folder._base_folder = os.path.join(
                base_folder,
                "dags",
                "repo",
            )
    return get_root_folder._base_folder


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
DEFAULT_TMP_SCHEMA = "temporaire"
DEFAULT_PG_CONN_ID = "db_data_store"
DEFAULT_S3_CONN_ID = "minio_bucket_dsci"
