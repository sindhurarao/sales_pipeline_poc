CREATE TABLE ingestion_audit
(
    run_id STRING,
    Source STRING,
    Target STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_ingested BIGINT,
    error_message STRING
    )
USING DELTA;