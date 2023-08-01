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


with DAG ('test',start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    # Default settings applied to all tasks within the DAG; can be overwritten at the task level.
    default_args={
        "owner": "akash",  # Defines the value of the "owner" column in the DAG view of the Airflow UI
        "retries": 0,  # If a task fails, it will retry 2 times.
        "retry_delay": duration(
            seconds=20
        ), 
    },
    default_view="graph",
    catchup=False
) as dag:
    

    def func_send_email_alert(ti,smtp_server,smtp_port,username,password,to_email):
        prev_iterator_id=ti.xcom_pull(key="decider_iterator")
        prev_task_id=ti.xcom_pull(key="metadata")[prev_iterator_id]
        xcom_value= ti.xcom_pull(task_ids=prev_task_id) 
        if xcom_value['success']=='1':
            date=str(datetime.now().date())
            subject="The Pipeline: P1001 run failed as of date: {date}"
            text += "The error received in the Pipeline run is as follows: "
            text+= xcom_value['error_message']
        else:
            date=str(datetime.now().date())
            subject="The Pipeline: P1001 run was successful as of date: {date}"
            text += "No error received "

        s = smtplib.SMTP(smtp_server, int(smtp_port))
        s.starttls()
        s.login(username, password)

        message = 'Subject: {}\n\n{}'.format(subject, text)
        s.sendmail(username,to_email, message)
        s.quit()

    
    def func_store_log(date):
        hook =PostgresHook(postgres_conn_id='postgres_neon_tech')
        hook.copy_expert(
            sql= "COPY logs.process_logs FROM stdin WITH(FORMAT CSV)",
            filename= '/tmp/processed_log_{date}.csv'
        )
    def func_process_log(ti):
        prev_iterator_id=ti.xcom_pull(key="decider_iterator")
        prev_task_id=ti.xcom_pull(key="metadata")[prev_iterator_id] 
        ti.xcom_push(key="decider_iterator",value=prev_iterator_id+1)
        xcom_value= ti.xcom_pull(task_ids=prev_task_id) 
        date=datetime.now()
        processed_log =json_normalize({
            'pipeline_name': 'P1001',
            'success': xcom_value['success'],
            'error_message': xcom_value['error_message'],
            'date': date
            })
        processed_log.to_csv('/tmp/processed_log_{date}.csv',index=None,header=False)
        func_store_log(date)
        
    with TaskGroup(group_id="log_details") as log_details:
        process_log = PythonOperator(
        task_id='process_log',
        python_callable= func_process_log
        )
        send_email_alert= PythonOperator(
            task_id='send_email_alert',
            python_callable = func_send_email_alert,
            op_kwargs={'smtp_server':os.environ['GMAIL_SMTP_SERVER'],'smtp_port':os.environ['GMAIL_SMTP_PORT'],'username':os.environ['GMAIL_USERNAME'],'password':os.environ['GMAIL_PASSWORD'],'to_email':os.environ['EMAIL_TO_USER']}
        )

        process_log >> send_email_alert


    def func_insert_metadata(ti):
        ti.xcom_push(key="metadata",value={1: {"decider_task": "files_exists_decider"},2: {"decider_task": "adf_load_adls_to_landing_success_decider"}}      
        )
        ti.xcom_push(key="decider_iterator",value=0)
        
    insert_metadata = PythonOperator(
        task_id='insert_metadata',
        python_callable = func_insert_metadata
    )
    def func_check_folder_and_files(connstr,container,date):
        print(container)
        print(date)
        blob_service_client=BlobServiceClient.from_connection_string(connstr)
        container_client = blob_service_client.get_container_client(container)
        myblob_date_files = container_client.list_blobs(name_starts_with="employeedata/"+date+"/")
        
        blob_list=[]

        for s in myblob_date_files:
            blob_list.append(s)
            break
        if len(blob_list) > 0:
            print(blob_list)
            return {'success':'1','error_message':None}
        else:
            path=str("employeedata/")
            full_path=str("raw/salesdata/")+date+"/"
            error_message="Either the folder: "+date+" does not exist at "+container+"/"+path+" Or there are no files at location: "+full_path
            return {'success':'0','error_message':error_message}
    

    check_folder_and_files =  PythonOperator(
        task_id='check_folder_and_files',
        python_callable = func_check_folder_and_files,
        op_kwargs={'connstr':os.environ['AZURE_BLOB_CONN_STR'],'container':os.environ['AZURE_CONTAINER_NAME'],'date':'2023-06-24'}
    )

    def func_files_exists_decider(ti):
        xcom_value= ti.xcom_pull(task_ids="check_folder_and_files")
        prev_iterator_id=ti.xcom_pull(key="decider_iterator")
        ti.xcom_push(key="decider_iterator",value=prev_iterator_id+1)
        if xcom_value['success']=='1':
            return "log_details.process_log"
        else:
            return "fail_1"

    
    fail_c1 = BashOperator(
        task_id='fail_1',
        bash_command='sleep 10'
    )
    files_exists_decider = BranchPythonOperator(
        task_id='files_exists_decider',
        python_callable= func_files_exists_decider
    )


    insert_metadata >> check_folder_and_files >> files_exists_decider 
    files_exists_decider >>  log_details
    files_exists_decider >>  fail_c1
    