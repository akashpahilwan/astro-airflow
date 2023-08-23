Overview
========

This is the very first project dag  created for a demo architecture for end to end orchestration using Airflow. <br>


Project Contents
================

Different tools/services Integration 
-------------------------
As airflow is a orchestration tool, we tested it with different tool used for different purposes listed below:
1. Azure Data Factory : Used for ingesting the data from client location(sftp/ftp/adls) to a alds path where we can access this data.
2. Snowflake : Used for datawarehousing purpose.
3. DBT: Used for testing and Transformation purpose.
4. Postgres: Used to store log data based on if dag run fails.

Along with these components logging,scheduling and altering will be managed by Airflow.

Project Architecture
-------------------------
![ETL Process](images/ETL_Architecture.png)

Final DAG
-------------------------
