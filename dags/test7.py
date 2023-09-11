from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from pendulum import datetime, duration
from airflow import DAG
# Airflow Operators are templates for tasks and encompass the logic that your DAG will actually execute.
# To learn more about operators, see: https://registry.astronomer.io/.

# DAG and task decorators for interfacing with the TaskFlow API
from airflow.decorators import dag, task, task_group

# A function that sets sequential dependencies between tasks including lists of tasks
from airflow.models.baseoperator import chain

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
import smtplib
# Used to label node edges in the Airflow UI
from airflow.utils.edgemodifier import Label
import os
from datetime import datetime, timedelta
import time
# Used to determine the day of the week
from airflow.utils.weekday import WeekDay
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential 
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import json
from pandas import json_normalize
from airflow.utils.task_group import TaskGroup
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator,DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from astro.files import File
from airflow import Dataset
import sys
import time

from datetime import datetime, timedelta
from typing import Optional

from airflow.decorators import dag, task
from airflow.models import TaskInstance
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.configuration import conf

SLACK_WEBHOOK_URL='a'


def alert_slack_channel(context: dict):

    last_task: Optional[TaskInstance] = context.get('task_instance')
    dag_name = last_task.dag_id
    task_name = last_task.task_id
    print(last_task.task_id)
    print(last_task.dag_id)
    error_message = context.get('exception') or context.get('reason')
    execution_date = context.get('execution_date')
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    # print(ti.log_url)
    # print(ti.task_id)
    
    base_url_needed= conf.get("webserver", "base_url")
    if not base_url_needed.find("localhost")>0:    
        my_regular_var= base_url_needed
        base_url= my_regular_var[:my_regular_var.rfind('/')]
        extras=my_regular_var[my_regular_var.rfind('/')+1:]
    else:
        base_url=base_url_needed
    file_and_link_template = "<{log_url}|{name}>"
    failed_tis = [file_and_link_template.format(log_url=ti.log_url, name=ti.task_id)
                  for ti in task_instances
                  if ti.state == 'failed']
    title = f':red_circle: Dag: *{dag_name}* has failed, with ({len(failed_tis)}) failed tasks'
    msg_parts = {
        'Execution date': execution_date,
        'Failed Tasks': ', '.join(failed_tis),
        'Error': error_message
    }
    msg = "\n".join([title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]).strip()
    msg=msg.replace(base_url,base_url_needed)
    hook =SlackWebhookHook(slack_webhook_conn_id='slack_conn')
    hook.client.send(text=msg)
    return


with DAG ('demo7',start_date=datetime(2023, 1, 1),
    schedule="@daily",
    default_view="graph",
    catchup=False,
    on_failure_callback=alert_slack_channel
) as dag:

    def func_print_statement(ti):
        print("Hello World!")
        a=10
        #from airflow.models import Variable
        #my_regular_var = Variable.get("base_url")
        #my_regular_var= os.environ["base_url"]
        from airflow.configuration import conf
        base_url_needed= conf.get("webserver", "base_url")
        if not base_url_needed.find("localhost")>0:    
            my_regular_var= base_url_needed
            base_url= my_regular_var[:my_regular_var.rfind('/')]
            extras=my_regular_var[my_regular_var.rfind('/')+1:]
            print(base_url)
            print(extras)
            log_url_old= ti.log_url
            log_url_final= log_url_old.replace(base_url,base_url_needed)
            # print(log_url_old= context["dag_run"])
        else:
            log_url_final=ti.log_url
            extras="a"
        print("base url is: ",base_url_needed)
        print("extras url is: ",extras)
        print("log_url is: ",log_url_final)
        b='10'+a

    print_statement = PythonOperator(
        task_id="print_statement",
        python_callable=func_print_statement
    )

    load_b = BashOperator(
        task_id='load_b',
        bash_command='fail',
        on_failure_callback=alert_slack_channel
    )
    print_statement >> load_b