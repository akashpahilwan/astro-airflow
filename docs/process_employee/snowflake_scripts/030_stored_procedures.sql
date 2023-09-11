------------------------------------------------------
Stored Procedure to create view for Daily Refresh
------------------------------------------------------

CREATE OR REPLACE PROCEDURE AIRFLOW.LANDING.CREATE_UPDATE_DAILY_VIEW_EMPLOYEE("DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'declare 
    script_external_table varchar;
    script_view_employee varchar;

begin
    --refresh External table --auto refresh not available for aws 
    create or replace external table ext_employee 
    with location = @my_azure_stage/employeedata/ file_format = my_csv_format auto_refresh = false;

    --create view for daily updates
    --drop view if exists view_stg_employee_filtered_daily;
    script_view_employee :=''create or replace view view_stg_employee_filtered_daily
    as
    select 
        value:c1::varchar as employee_name,	
        value:c2::int as empid,	
        value:c3::varchar as positionin,	
        value:c4::varchar as state,	
        value:c5::varchar as zip,	
        value:c6::varchar as sex,	
        value:c7::varchar as dateofbirth,	
        value:c8::varchar as maritaldesc,	
        value:c9::varchar as citizendesc,	
        value:c10::varchar as employmentstatus,	
        value:c11::varchar as department,	
        value:c12::varchar as managername,	
        value:c13::varchar as recruitmentsource,	
        value:c14::varchar as performanceScore,
        value:c15::varchar as filename,
        value:c16::varchar as extracted_on,
        value:c17::varchar as loaded_on
        from ext_employee where value:c16::varchar= \\'''' || :date || ''\\'''';
    execute immediate :script_view_employee;
    return script_view_employee;
end';

------------------------------------------------------
Stored Procedure to create view for Full Refresh
------------------------------------------------------

CREATE OR REPLACE PROCEDURE AIRFLOW.LANDING.CREATE_UPDATE_VIEW_EMPLOYEE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 'declare 
    script_external_table varchar;
    script_view_employee varchar;

begin
    --refresh External table --auto refresh not available for aws 
    create or replace external table ext_employee 
    with location = @my_azure_stage/employeedata/ file_format = my_csv_format auto_refresh = false;

    --create view for daily updates
    --drop view if exists view_stg_employee;
    script_view_employee :=''create or replace view AIRFLOW.LANDING.VIEW_STG_EMPLOYEE(
        	EMPLOYEE_NAME,
        	EMPID,
        	POSITIONIN,
        	STATE,
        	ZIP,
        	SEX,
        	DATEOFBIRTH,
        	MARITALDESC,
        	CITIZENDESC,
        	EMPLOYMENTSTATUS,
        	DEPARTMENT,
        	MANAGERNAME,
        	RECRUITMENTSOURCE,
        	PERFORMANCESCORE,
        	FILENAME,
        	EXTRACTED_ON,
        	LOADED_ON
            ) as
                select * exclude (rnk)  from(
                select 
                    value:c1::varchar as employee_name,	
                    value:c2::int as empid,	
                    value:c3::varchar as positionin,	
                    value:c4::varchar as state,	
                    value:c5::varchar as zip,	
                    value:c6::varchar as sex,	
                    value:c7::varchar as dateofbirth,	
                    value:c8::varchar as maritaldesc,	
                    value:c9::varchar as citizendesc,	
                    value:c10::varchar as employmentstatus,	
                    value:c11::varchar as department,	
                    value:c12::varchar as managername,	
                    value:c13::varchar as recruitmentsource,	
                    value:c14::varchar as performanceScore,
                    value:c15::varchar as filename,
                    value:c16::varchar as extracted_on,
                    value:c17::varchar as loaded_on,
                    row_number() over(partition by value:c2::int order by value:c16::varchar desc) as rnk
                    from ext_employee
                    )where rnk=1;'';
    execute immediate :script_view_employee;
    return script_view_employee;
end';