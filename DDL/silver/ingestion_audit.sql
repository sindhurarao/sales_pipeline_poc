CREATE TABLE silver_ingestion_audit
(
    run_id STRING,
    source STRING,
    target STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    records_ingested BIGINT,
    error_message STRING
    )
USING DELTA;