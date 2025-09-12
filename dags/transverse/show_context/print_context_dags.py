from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from datetime import timedelta
from pprint import pprint
import pytz

from utils.common.tasks_sql import get_project_config
from utils.mails.mails import make_mail_func_callback, MailStatus

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

link_documentation_pipeline = "Non-défini"
link_documentation_donnees = "Non-défini"


# Définition du DAG
@dag(
    "liste-des-variables-contexte",
    schedule="@once",
    max_active_runs=1,
    catchup=False,
    tags=["SG", "DSCI", "PRODUCTION", "INFO"],
    description="Liste les variables disponibles dans le contexte d'un DAG",
    default_args=default_args,
    params={
        "nom_projet": "Projet test",
        "mail": {
            "enable": False,
            "To": ["yanis.tihianine@finances.gouv.fr"],
            "CC": [""],
        },
        "docs": {
            "lien_pipeline": link_documentation_pipeline,
            "lien_donnees": link_documentation_donnees,
        },
    },
    on_success_callback=make_mail_func_callback(mail_statut=MailStatus.SUCCESS),
)
def liste_contexte_var():
    @task
    def print_context(**context):
        pprint(context)
        pprint(context.dag)

    @task
    def my_task(**context):
        execution_date = context["dag_run"].execution_date
        print(execution_date)
        print("avant timezone", execution_date.strftime("%Y-%m-%d %H:%M:%S"))
        # Convert to Paris Timezone
        paris_tz = pytz.timezone("Europe/Paris")
        execution_date_paris = execution_date.astimezone(paris_tz)

        # Format without timezone info
        formatted_time = execution_date_paris.strftime(
            "%Y-%m-%d %H:%M:%S"
        )  # Ensures output like "14:00:00"
        print("apres timezone", formatted_time)

    @task
    def print_conf_var(**context):
        send_mail = context.get("params").get("send_mail")
        print(f"send_mail value: {send_mail}")

    @task
    def mail_success(**context):
        mail_func_success = make_mail_func_callback(
            mail_statut=MailStatus.SUCCESS,
        )
        mail_func_success(context=context)

    @task
    def mail_start(**context):
        mail_func_start = make_mail_func_callback(
            mail_statut=MailStatus.START,
        )
        mail_func_start(context=context)

    @task
    def mail_error(**context):
        mail_func_error = make_mail_func_callback(
            mail_statut=MailStatus.ERROR,
        )
        mail_func_error(context=context)

    projet_config = get_project_config()

    chain(
        projet_config,
        print_context(),
        my_task(),
        print_conf_var(),
        [mail_success(), mail_start(), mail_error()],
    )


# Exécution du DAG
liste_contexte_var()
