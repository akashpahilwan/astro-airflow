CREATE TABLE IF NOT EXISTS logs.process_logs
(
    pipeline_name character varying(10),
    success character varying(1),
    error_message character varying(255),
    date character varying(50)
)