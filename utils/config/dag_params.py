from datetime import timedelta
from airflow.utils.dates import days_ago
from typing import Optional

DEFAULT_OWNER = "airflow"
DEFAULT_EMAIL_TO = ["yanis.tihianine@finances.gouv.fr"]
DEFAULT_EMAIL_CC = ["labo-data@finances.gouv.fr"]
DEFAULT_TMP_SCHEMA = "temporaire"


def create_default_args(
    retries: int = 0, retry_delay: Optional[timedelta] = None, **kwargs
) -> dict:
    """Create standard default_args for DAGs."""
    args = {
        "owner": DEFAULT_OWNER,
        "depends_on_past": False,
        "start_date": days_ago(1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": retries,
    }
    if retry_delay:
        args["retry_delay"] = retry_delay
    args.update(kwargs)
    return args
