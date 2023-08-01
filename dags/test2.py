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
import json
from pandas import json_normalize
from airflow.utils.task_group import TaskGroup
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator,DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook


DBT_CLOUD_CONN_ID = "dbt_conn"
JOB_ID = "388957"

with DAG ('demos',start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    # Default settings applied to all tasks within the DAG; can be overwritten at the task level.
    default_args={
        "owner": "akash",  # Defines the value of the "owner" column in the DAG view of the Airflow UI
        "retries": 1,  # If a task fails, it will retry 2 times.
        "retry_delay": duration(
            seconds=20
        ),
        'trigger_rule': 'all_success' 
    },
    default_view="graph",
    catchup=False
) as dag:

    with TaskGroup(group_id='dbt_job') as dbt_job:

        def func_check_job(job_id):
            """
            Retrieves the last run for a given dbt Cloud job and checks
            to see if the job is not currently running.
            """
            hook = DbtCloudHook(DBT_CLOUD_CONN_ID)
            #runs = hook.list_job_runs(job_definition_id=job_id, order_by="-id")
            runs = hook.list_job_runs(job_definition_id=job_id)
            print('the run info is as below', runs[0].text)
            print('the run info is as follows', [run for run in runs[0].json()])
            latest_run = runs[0].json()["data"][0]
            return DbtCloudJobRunStatus.is_terminal(latest_run["status"])

        trigger_job = DbtCloudRunJobOperator(
            task_id="trigger_dbt_cloud_job",
            dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
            job_id=JOB_ID,
            check_interval=30,
            timeout=3600,
        )

        
        def func_get_job_run(ti):
            hook = DbtCloudHook(DBT_CLOUD_CONN_ID)
            runid=ti.xcom_pull(key='return_value')
            a=hook.get_job_run_status(run_id=runid)
            if a==10:
                ti.xcom_push(key="dbt_job_run_status",value="SUCCESS")
            else:
                ti.xcom_push(key="dbt_job_run_status",value="NOT SUCCESS")
            return 
        
        check_job = PythonOperator(
            task_id='check_job',
            python_callable= func_check_job,
            trigger_rule='none_failed', 
            op_kwargs={'job_id':JOB_ID,'dbt_cloud_conn_id':DBT_CLOUD_CONN_ID}
        )

        get_job_run = PythonOperator(
            task_id='get_job_run',
            python_callable= func_get_job_run,
            trigger_rule='none_failed' 
        )
        check_job >> trigger_job >> get_job_run