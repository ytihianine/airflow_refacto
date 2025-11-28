from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.models import Variable

from infra.mails.default_smtp import create_airflow_callback, MailStatus
from dags.sg.dsci.hooks.process import create_ics_file

from utils.config.vars import DEFAULT_GRIST_HOST

# Mails
to = ["yanis.tihianine@finances.gouv.fr"]
cc = []

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


@dag(
    dag_id="hooks_check",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    params={
        "mail": {
            "enable": False,
            "to": to,
            "cc": cc,
        },
        "link_documentation_pipeline": "",
        "link_documentation_donnees": "",
    },
    on_failure_callback=create_airflow_callback(
        mail_status=MailStatus.ERROR,
    ),
)
def hooks_check():
    @task(task_id="mail_hook")
    def mail_hook() -> None:
        from airflow.providers.smtp.hooks.smtp import SmtpHook
        from infra.http_client.adapters import HttpxAPIClient
        from infra.grist.client import GristAPI
        from utils.config.vars import PROXY, AGENT

        nubo_smtp = SmtpHook(smtp_conn_id="smtp_nubonyxia")
        httpx_internet_client = HttpxAPIClient(proxy=PROXY, user_agent=AGENT)
        grist_api = GristAPI(
            api_client=httpx_internet_client,
            base_url=DEFAULT_GRIST_HOST,
            workspace_id="docs",
            doc_id="biWSFf9kY2xrsF9WaSTTGG",
            api_token=Variable.get("grist_secret_key"),
        )

        data_reunion = grist_api.get_df_from_records(tbl_name="Reunion")
        print(data_reunion)
        print(nubo_smtp)
        with nubo_smtp as smtp:
            for row in data_reunion.itertuples():
                ics_filepath = create_ics_file(data_row=row)
                print(f"ICS file created at < {ics_filepath} >")
                smtp.send_email_smtp(
                    to=to,
                    subject="AirflowTest",
                    html_content="Ceci est un message automatique",
                    files=[ics_filepath],
                )

    """ Tasks order"""
    chain(mail_hook())


hooks_check()
