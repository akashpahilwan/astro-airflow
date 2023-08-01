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


DBT_CLOUD_CONN_ID = "dbt_conn"
JOB_ID = "388957"

with DAG ('process_data',start_date=datetime(2023, 1, 1),
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
    

    def func_send_email_alert(ti,smtp_server,smtp_port,username,password,to_email):
        prev_iterator_id=ti.xcom_pull(key="decider_iterator")-1
        prev_task_id=ti.xcom_pull(key="metadata")[prev_iterator_id]['check_task'] 
        xcom_value= ti.xcom_pull(task_ids=prev_task_id) 
        print("prev_iterator_id: ",prev_iterator_id)
        print("prev_task_id: ",prev_task_id)
        print("xcom_value: ",xcom_value)
        if xcom_value['success']=='0':
            date=str(datetime.now().date())
            subject="The Pipeline: P1001 run failed as of date: "+date
            text = "The error received in the Pipeline run is as follows: "
            text += xcom_value['error_message']
        else:
            date=str(datetime.now().date())
            subject="The Pipeline: P1001 run was successful as of date: "+date
            text = "No error received"

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
        prev_task_id=ti.xcom_pull(key="metadata")[prev_iterator_id]['check_task'] 
        print("prev_iterator_id: ",prev_iterator_id)
        print("prev_task_id: ",prev_task_id)
        ti.xcom_push(key="decider_iterator",value=prev_iterator_id+1)
        xcom_value= ti.xcom_pull(task_ids=prev_task_id) 
        print("xcom_value: ",xcom_value)
        date=datetime.now()
        processed_log =json_normalize({
            'pipeline_name': 'P1001',
            'success': xcom_value['success'],
            'error_message': xcom_value['error_message'],
            'date': date
            })
        processed_log.to_csv('/tmp/processed_log_{date}.csv',index=None,header=False)
        func_store_log(date)
        
    with TaskGroup(group_id='log_details') as log_details:
        process_log = PythonOperator(
        task_id='process_log',
        python_callable= func_process_log,
        trigger_rule='none_failed' 
        )
        send_email_alert= PythonOperator(
            task_id='send_email_alert',
            python_callable = func_send_email_alert,
            op_kwargs={'smtp_server':os.environ['GMAIL_SMTP_SERVER'],'smtp_port':os.environ['GMAIL_SMTP_PORT'],'username':os.environ['GMAIL_USERNAME'],'password':os.environ['GMAIL_PASSWORD'],'to_email':os.environ['EMAIL_TO_USER']}
        )

        process_log >> send_email_alert


    def func_insert_metadata(ti):
        ti.xcom_push(key="metadata",
                     value=
                     {
                         1: {"check_task": "check_folder_and_files"},
                         2: {"check_task": "check_copy_adls_to_landing_pipeline_run_status"},
                         3: {"check_task": "get_job_run"}
                         }      
        )
        ti.xcom_push(key="decider_iterator",value=0)
        
    insert_metadata = PythonOperator(
        task_id='insert_metadata',
        python_callable = func_insert_metadata
    )
    def func_check_folder_and_files(connstr,container,date):
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
            full_path=str("raw/employeedata/")+date+"/"
            error_message="Either the folder: "+date+" does not exist at "+container+"/"+path+" Or there are no files at location: "+full_path
            return {'success':'0','error_message':error_message}
    t1= 'echo `hostname`'
    
    def print_activity_run_details(activity_run):
        """Print activity run details."""
        if activity_run.status == 'Succeeded':
            return {"success": '1',"error_message": None}
        else:
            return {"success": '0',"error_message": activity_run.error['message']}
    def func_check_copy_adls_to_landing_pipeline_run_status(ti,subscription_id,rg_name,df_name,client_id,client_secret,tenant_id):
        print(subscription_id, 'subscription+id')
        credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id) 
        resource_client = ResourceManagementClient(credentials, subscription_id)
        adf_client = DataFactoryManagementClient(credentials, subscription_id)
        run_id= ti.xcom_pull(key='run_id',task_ids='copy_data_from_adls_to_landing')
        pipeline_run = adf_client.pipeline_runs.get(
            rg_name, df_name, run_id)
        filter_params = RunFilterParameters(
            last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
        query_response = adf_client.activity_runs.query_by_pipeline_run(
            rg_name, df_name, pipeline_run.run_id, filter_params)
        return print_activity_run_details(query_response.value[0])

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
            return "copy_data_from_adls_to_landing"
        else:
            return "log_details.process_log"

    # pass_c = BashOperator(
    #     task_id='pass',
    #     bash_command='sleep 10'
    # )
    # fail_c = BashOperator(
    #     task_id='fail',
    #     bash_command='sleep 10 '
    # )
    # pass_c1 = BashOperator(
    #     task_id='pass_1',
    #     bash_command='sleep 10'
    # )
    # fail_c1 = BashOperator(
    #     task_id='fail_1',
    #     bash_command='sleep 10'
    # )
    files_exists_decider = BranchPythonOperator(
        task_id='files_exists_decider',
        python_callable= func_files_exists_decider,
        depends_on_past=False,
        wait_for_downstream=False
    )
    copy_data_from_adls_to_landing = AzureDataFactoryRunPipelineOperator(
        task_id="copy_data_from_adls_to_landing",
        pipeline_name="pipeline1",
        parameters={"dataset_name":"employeedata","ssdate": "2023-06-24"},
        azure_data_factory_conn_id="azure_data_factory",
        trigger_rule='all_success' 
    )
    def decider_12(ti):
        xcom_value= ti.xcom_pull(task_ids="check_path_for_files") 
        if xcom_value['success']=='1':
            return "pass_1"
        else:
            return "fail_1"
        
    def func_adf_load_adls_to_landing_success_decider(ti):
        xcom_value= ti.xcom_pull(task_ids="check_copy_adls_to_landing_pipeline_run_status") 
        prev_iterator_id=ti.xcom_pull(key="decider_iterator")
        ti.xcom_push(key="decider_iterator",value=prev_iterator_id+1)
        if xcom_value['success']=='1':
            return "insert_query"
        else:
            return "log_details.process_log"
    check_copy_adls_to_landing_pipeline_run_status =PythonOperator(
        task_id='check_copy_adls_to_landing_pipeline_run_status',
        python_callable = func_check_copy_adls_to_landing_pipeline_run_status,
        op_kwargs=    {'subscription_id': os.environ['AZURE_DATA_FACTORY_SUBSCRIPTION_ID'],
                        'rg_name': os.environ['AZURE_RESOURCE_GROUP_NAME'],
                        'df_name': os.environ['AZURE_DATA_FACTORY_NAME'],
                        'client_id': os.environ['AZURE_DATA_FACTORY_CLIENT_ID'],
                        'client_secret': os.environ['AZURE_DATA_FACTORY_CLIENT_SECRET'],
                        'tenant_id':os.environ['AZURE_DATA_FACTORY_TENANT_ID']
                        },
        trigger_rule='none_skipped' 
    )
    adf_load_adls_to_landing_success_decider = BranchPythonOperator(
        task_id='adf_load_adls_to_landing_success_decider',
        python_callable= func_adf_load_adls_to_landing_success_decider
    )
    def func_insert_query(ti):
        snowflake_query= [
        """call create_update_daily_view_employee('2023-06-24')"""]
        ti.xcom_push(key='snow_query',value="""call create_update_daily_view_employee('2023-06-24')""")
        
    insert_query = PythonOperator(
        task_id='insert_query',
        python_callable = func_insert_query
    )

    create_view =SnowflakeOperator(
        task_id="create_view",
        sql="{{ti.xcom_pull(key='snow_query')}}",
        snowflake_conn_id="snowflake"
    )
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
                return {"success":1,"error_message":"No Errors"}  
            else:
                return {"success":0,"error_message":"Please check the logs in DBT Cloud for Runid: {runid}"}    
        
        check_job = PythonOperator(
            task_id='check_job',
            python_callable= func_check_job,
            trigger_rule='all_success', 
            op_kwargs={'job_id':JOB_ID,'dbt_cloud_conn_id':DBT_CLOUD_CONN_ID}
        )

        get_job_run = PythonOperator(
            task_id='get_job_run',
            python_callable= func_get_job_run,
            trigger_rule='none_skipped' 
        )
        check_job >> trigger_job >> get_job_run
    

    insert_metadata >> check_folder_and_files >> files_exists_decider 
    files_exists_decider >>  log_details
    files_exists_decider >>  copy_data_from_adls_to_landing >> check_copy_adls_to_landing_pipeline_run_status >> adf_load_adls_to_landing_success_decider
    adf_load_adls_to_landing_success_decider >> log_details 
    adf_load_adls_to_landing_success_decider >> insert_query >> create_view >> check_job
    get_job_run >> log_details