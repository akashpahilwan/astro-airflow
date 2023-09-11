use database airflow;
use schema landing;

CREATE or replace STAGE my_azure_stage 
URL = '<azure_url>'
CREDENTIALS = (AZURE_SAS_TOKEN = '<azure_sas_token>');


list @my_azure_stage/employeedata;
show stages;