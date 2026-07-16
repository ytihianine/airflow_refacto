"""Répertorie les variables communes à toutes les pipelines"""

import logging
import os
import sys
from functools import lru_cache

import pytz


@lru_cache(maxsize=1)
def get_root_folder() -> str:
    """Get root folder based on environment."""
    base_folder = os.getenv("AIRFLOW_HOME")
    if base_folder is None:
        return "/home/onyxia/work/airflow-demo"
    return os.path.join(base_folder, "dags", "repo")


# Custom logger configuration for scripts - Airflow has its own logger
custom_logger = logging.Logger(name=__name__, level=logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)  # Handler pour afficher les logs dans la console
formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(fmt=formatter)
custom_logger.addHandler(hdlr=handler)


ENV_VAR = os.environ.copy()

# HTTP Proxy Configuration
PROXY = ENV_VAR.get("AIRFLOW_PROXY", None)
AGENT = ENV_VAR.get("AIRFLOW_USER_AGENT", None)

# Timezone configuration
PARIS_TZ = pytz.timezone(zone="Europe/Paris")

# DEFAULT VARIABLES
DEFAULT_SMTP_CONN_ID = "smtp_nubonyxia"
DEFAULT_MAIL_CC = ["labo-data@finances.gouv.fr", "yanis.tihianine@finances.gouv.fr"]
DEFAULT_TMP_SCHEMA = "temporaire"
DEFAULT_PG_DATA_CONN_ID = "db_data_store"
DEFAULT_S3_CONN_ID = "minio_bucket_dsci"
DEFAULT_S3_BUCKET = "dsci"

# Catalog POLARIS
DEFAULT_POLARIS_HOST = "https://polaris-catalog.lab.incubateur.finances.rie.gouv.fr"
DEFAULT_POLARIS_CATALOG = "cat_data_store"
DEFAULT_POLARIS_CLIENT_ID = ENV_VAR.get("AIRFLOW_POLARIS_CLIENT_ID", "root")
DEFAULT_POLARIS_CLIENT_SECRET = ENV_VAR.get("AIRFLOW_POLARIS_CLIENT_SECRET", "root")

# S3
DEFAULT_S3_ENDPOINT_URL = "https://minio.lab.incubateur.finances.rie.gouv.fr"

# Trino
DEFAULT_TRINO_HOST = "trino-mef-sg.lab.incubateur.finances.rie.gouv.fr"

# Grist
DEFAULT_GRIST_HOST = "https://grist.numerique.gouv.fr"

# Processing messages
NO_PROCESS_MSG = "No complementary actions needed ! Skipping ..."

# Feature flags messages
FF_MAIL_DISABLED_MSG = "Mail feature flag is disabled ! Skipping mail sending ..."
FF_S3_DISABLED_MSG = "S3 feature flag is disabled ! Skipping S3 operations ..."
FF_DB_DISABLED_MSG = "Database feature flag is disabled ! Skipping database operations ..."
FF_CONVERT_DISABLED_MSG = (
    "File conversion to parquet feature flag is disabled ! Skipping File conversion operations ..."
)
FF_DOWNLOAD_GRIST_DOC_DISABLED_MSG = (
    "Download Grist Doc feature flag is disabled ! Skipping Grist document download ..."
)
