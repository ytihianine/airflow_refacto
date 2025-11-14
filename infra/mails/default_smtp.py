from airflow.utils.email import send_email


def send_success_mail(context: dict) -> None:
    dag_run = context.get("dag_run")

    msg = "DAG ran successfully"

    subject = f"DAG {dag_run} has completed"

    send_email(
        conn_id="smtp_nubonyxia",
        to=["yanis.tihianine@finances.gouv.fr"],
        subject=subject,
        html_content=msg,
    )
