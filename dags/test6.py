from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

with DAG("demo6",
    start_date=datetime(2023, 1, 1),
    on_success_callback=[
        send_slack_notification(
            text="The DAG {{ dag.dag_id }} succeeded",
            channel="#general",
            username="Airflow",
        )
    ],
):
    BashOperator(
        task_id="mytask",
        on_failure_callback=[
            send_slack_notification(
                text="The task {{ ti.task_id }} failed",
                channel="#general",
                username="Airflow",
            )
        ],
        bash_command="fail",
    )