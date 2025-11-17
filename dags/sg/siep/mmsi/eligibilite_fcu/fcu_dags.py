from datetime import timedelta
from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago

from infra.mails.default_smtp import create_airflow_callback, MailStatus
from utils.config.dag_params import create_dag_params, create_default_args
from utils.config.tasks import get_projet_config
from utils.tasks.sql import (
    create_tmp_tables,
    copy_tmp_table_to_real_table,
    import_file_to_db,
)

from utils.tasks.s3 import (
    copy_s3_files,
    del_s3_files,
)

from dags.sg.siep.mmsi.eligibilite_fcu.task import eligibilite_fcu_to_file


nom_projet = "France Chaleur Urbaine (FCU)"
LINK_DOC_PIPELINE = "https://forge.dgfip.finances.rie.gouv.fr/sg/dsci/lt/airflow-demo/-/tree/main/dags/sg/siep/mmsi/eligibilite_fcu?ref_type=heads"  # noqa
LINK_DOC_DATA = "https://catalogue-des-donnees.lab.incubateur.finances.rie.gouv.fr/app/dataset?datasetId=49"  # noqa


# Définition du DAG
@dag(
    "eligibilite_fcu",
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    tags=["SG", "SIEP", "PRODUCTION", "BATIMENT", "FCU"],
    description="Récupérer pour chaque bâtiment de l'OAD son éligibilité au réseau Franche Chaleur Urbaine (FCU)",  # noqa
    max_consecutive_failed_dag_runs=1,
    default_args=create_default_args(retries=1, retry_delay=timedelta(seconds=20)),
    params=create_dag_params(
        nom_projet=nom_projet,
        prod_schema="siep",
        lien_pipeline=LINK_DOC_PIPELINE,
        lien_donnees=LINK_DOC_DATA,
        mail_enable=False,
    ),
    on_success_callback=create_airflow_callback(mail_status=MailStatus.SUCCESS),
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def eligibilite_fcu_dag():

    chain(
        eligibilite_fcu_to_file(),
        create_tmp_tables(),
        import_file_to_db.expand(
            selecteur_config=get_projet_config(nom_projet=nom_projet)
        ),
        copy_tmp_table_to_real_table(),
        copy_s3_files(bucket="dsci"),
        del_s3_files(bucket="dsci"),
    )


eligibilite_fcu_dag()
