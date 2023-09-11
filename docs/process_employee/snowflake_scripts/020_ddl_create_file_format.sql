use database airflow;
use schema landing;

create or replace file format my_csv_format
  type = csv
  field_delimiter = ','
  skip_header = 1
  null_if = ('NULL', 'null')
  empty_field_as_null = true,
  field_optionally_enclosed_by='"'

desc file format my_csv_format;