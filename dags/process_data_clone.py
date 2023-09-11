#functional imports
import os
from datetime import datetime, timedelta
import time
import smtplib
import json
from pandas import json_normalize

#airflow related imports and dependencies with other tools
#from pendulum import datetime, duration
from pendulum import duration
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.weekday import WeekDay
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

#airflow related imports and dependencies with other tools : Please note utils needs to be installed first -> present in requirements.txt
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator,DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

#airflow related imports for Azure connectivity: blob, adf pipeline trigger run, result of pipeline run
from azure.storage.blob import BlobServiceClient
from azure.identity import ClientSecretCredential 
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *

from airflow.configuration import conf

#dbt connection id and job id : needs to be parameterized
DBT_CLOUD_CONN_ID = "dbt_conn"
JOB_ID = "403919"
run_date='2023-06-24'
run_pipeline='pipeline1'
run_dataset='employeedata'

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


#dag definition

"""
This dag is created to run the end to end etl pipeline flow for Dimention table. 
If this is parameterized, same can be used for other dimention tables consdering there is different adf pipeline,stored proc,dbt job for each dimention.
Pleae note we are not using taskflow api syntax for creating dag as we are also learning airflow. We are using native traditional airflow syntax.  

"""
with DAG ('process_data_cloned',start_date=datetime(2023, 1, 1),
    max_active_runs=1,
    schedule="@daily",
    # Default settings applied to all tasks within the DAG; can be overwritten at the task level.
    default_args={
        "owner": "akash",  # Defines the value of the "owner" column in the DAG view of the Airflow UI
        "retries": 1,  # If a task fails, it will retry 1 times.
        "retry_delay": duration(
            seconds=10 
        ),
        'trigger_rule': 'all_success'  # trigger rule defines why a task gets triggered, on which condition. By default, all tasks have the same trigger rule all_success set which means, if all parents of a task succeed, then the task gets triggered.
    },
    default_view="graph",
    on_failure_callback=alert_slack_channel,
    catchup=False
) as dag:
    

    """
    Alerting mechanism definition is as below:
    1. func_store_log and func_process_log : stores the error logs in postgres database defined -> table logs.process_logs
    2. func_send_email_alert : send email alerts if a task we defined fails due to some issues.

    We created a task group called "log_details "which combines the above two tasks and trigger whenever required(in case of task failures)
    The concept of task group is similar to sub-dags. As sub-dags are going to be depreciated in future version of airflow, we used task groups. 

    Please note: The logs are not sent for every activity which gets failed, currently logs are configured only for the tasks where we expect failures due to functional issues.
    For any techincal failures like task(decider) failed due to connectivity issue, seperately logging needs to be enabled.
    This needs to be enaled when we actually deploy airflow in production to avoid any issues.

    """
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
        

    #This is the task group created for logging details in case of pipeline success or failures    
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

    """
    This a first task in the pipeline, which creates a metadata dict with name "metadata". 
    This metadata dict is pushed to airflow xcom. Def: XComs (short for “cross-communications”) are a mechanism that let Tasks talk to each other, as by default Tasks are entirely isolated and may be running on entirely different machines.
    The reason of using this is:
    We have a taskgroup for logging called : log_details.
    This "log_details" is referenced multiple times in the same dag at different parts of pipeline failure.
    There is no easier of saying for that task  to say which task output we need to refer in case its called due to failure.
    Hence, I created this metadata mechanism. 
    There can be multiple different ways we can go around this. This method is just easier to track failures for me. 

    """
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

    """
    This task check if the files we want for that day are present in the provided location or not. 
    We are considering client location adls where client will dump the data at EOD.
    We will go through that location at scheduled time and check if atleast one file exist in the path.
    """
    def func_check_folder_and_files(connstr,container,date):
        a='11'
        b='11'+111
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
    
    check_folder_and_files =  PythonOperator(
        task_id='check_folder_and_files',
        python_callable = func_check_folder_and_files,
        op_kwargs={'connstr':os.environ['AZURE_BLOB_CONN_STR'],'container':os.environ['AZURE_CONTAINER_NAME'],'date':run_date}
    )    

    """
    This function checks the output of above task and decides whether the pipeline needs to run ahead or stop and log the details.
    """
    def func_files_exists_decider(ti):
        xcom_value= ti.xcom_pull(task_ids="check_folder_and_files")
        prev_iterator_id=ti.xcom_pull(key="decider_iterator")
        ti.xcom_push(key="decider_iterator",value=prev_iterator_id+1)
        if xcom_value['success']=='1':
            return "copy_data_from_adls_to_landing"
        else:
            return "log_details.process_log"

    files_exists_decider = BranchPythonOperator(
        task_id='files_exists_decider',
        python_callable= func_files_exists_decider,
        depends_on_past=False,
        wait_for_downstream=False
        #trigger_rule='none_skipped'
    )

    """
    This task is ran after confirming we have atleast one file to process.
    This task is created to run adf pipeline to copy the files from client adls to our landing zone adls.
    The parameters passed are dataset name and date for which the data needs to be copied.
    This task outputs the runid of the pipeline and we use that runid in next task to check the status of the pipeline run.

    """
    copy_data_from_adls_to_landing = AzureDataFactoryRunPipelineOperator(
        task_id="copy_data_from_adls_to_landing",
        pipeline_name=run_pipeline,
        parameters={"dataset_name":run_dataset,"date": run_date},
        azure_data_factory_conn_id="azure_data_factory",
        trigger_rule='all_success' 
    )

    """
    This task checks if the adf copy acitvity is success or failed and outputs the success or failure message.

    Please note this task needs to run irrespective of prev task is success or failed. Hence the trigger rule defined as "none_skipped" but default value is "all_success"
    """
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
        
    """
    This task checks the output of above task and decides whether the pipeline needs to run ahead or stop and log the details.
    """
    def func_adf_load_adls_to_landing_success_decider(ti):
        xcom_value= ti.xcom_pull(task_ids="check_copy_adls_to_landing_pipeline_run_status") 
        prev_iterator_id=ti.xcom_pull(key="decider_iterator")
        ti.xcom_push(key="decider_iterator",value=prev_iterator_id+1)
        if xcom_value['success']=='1':
            return "insert_query"
        else:
            return "log_details.process_log"

    adf_load_adls_to_landing_success_decider = BranchPythonOperator(
        task_id='adf_load_adls_to_landing_success_decider',
        python_callable= func_adf_load_adls_to_landing_success_decider
    )

    """
    This task insert a query into xcom to be used for next task: insert_query
    """
    def func_insert_query(ti):
        snowflake_query= [
        """call create_update_daily_view_employee('{run_date})"""]
        ti.xcom_push(key='snow_query',value="call create_update_daily_view_employee('"+run_date+"')")
        
    insert_query = PythonOperator(
        task_id='insert_query',
        python_callable = func_insert_query
    )

    """
    This task runs the above added query in xcom.
    This calls the stored proc configured in snowflake for updating the view for only the data we need to transformed and copied to dimention table.
    """
    create_view =SnowflakeOperator(
        task_id="create_view",
        sql="{{ti.xcom_pull(key='snow_query')}}",
        snowflake_conn_id="snowflake"
    )

    """
    This taskgroup triggers a dbt job created for a specific dimention table.
    This dbt job has two commands.
    1. Tests the source table: The view we created above.
    2. trigger the dbt sql script to implement merge statement.
    
    Also check if the dbt job run is success or failure and sends the notification accordingly.
    """

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
                return {"success":0,"error_message":"Please check the logs in DBT Cloud for Runid: "+runid}    
        
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
    

    insert_metadata >> check_folder_and_files >> files_exists_decider >> [log_details,copy_data_from_adls_to_landing]
    copy_data_from_adls_to_landing >> check_copy_adls_to_landing_pipeline_run_status >> adf_load_adls_to_landing_success_decider >> [log_details,insert_query]
    insert_query >> create_view >> check_job
    get_job_run >> log_details