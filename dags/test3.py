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



with DAG ('demos2',start_date=datetime(2023, 1, 1),
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

    # def t2_error_task(context):
    #     print('on failure: run logs but how')



    # def do_python_stuff_success():
    #     print("do_python_stuff_success")


    # def do_python_stuff():
    #     a='10'
    #     b=77
    #     print(a+b)
    #     print('Python stuff')


    # t1_task = PythonOperator(
    #     task_id='my_operator_t1',
    #     python_callable=do_python_stuff,
    #     on_failure_callback=t2_error_task,
    #     dag=dag
    # )

    # t3_task_success = PythonOperator(
    #     task_id='my_operator_t3',
    #     python_callable=do_python_stuff_success,
    #     dag=dag
    # )

    def func_insert_query(ti):
        snowflake_query= [
        """call create_update_daily_view_employee('2023-06-24')"""]
        ti.xcom_push(key='snow_query',value="""call create_update_daily_view_employee('2023-06-24')""")
        



    insert_query = PythonOperator(
        task_id='insert_query_to_meta',
        python_callable = func_insert_query
    )

    snowflake_query= [
        """call create_update_daily_view_employee('2023-06-24')"""
    ]
    create_view =SnowflakeOperator(
        task_id="create_view",
        sql="{{ti.xcom_pull(key='snow_query')}}",
        snowflake_conn_id="snowflake"
    )

    insert_query >> create_view