{{
     config(
        materialized = 'incremental',
        unique_key='empid',
        merge_update_columns = [ 'employee_name', 'positionin', 'state','zip','sex','dateofbirth','maritaldesc','citizendesc','employmentstatus','department', 'managername','recruitmentsource', 'performanceScore', 'filename','loaded_on','extracted_on'],
        transient=False,
        schema="analytics" 
    )   
}}


{% set refresh= var('refresh_type') %}


with employees as (
    {% if refresh=='daily'  %}
        select * from {{ source('landing_tables', 'VIEW_STG_EMPLOYEE_FILTERED_DAILY') }}
    {% else %}
        select * from {{ source('landing_tables', 'VIEW_STG_EMPLOYEE') }}
    {% endif %}
)

select * from employees