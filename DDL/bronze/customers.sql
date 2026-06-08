CREATE TABLE IF NOT EXISTS bronze_customers
(
    customer_id STRING,
    name STRING,
    email STRING,
    phone STRING,
    address STRING,
    segment STRING,
    country STRING,
    city STRING,
    state STRING,
    postal_code STRING,
    region STRING,
    ingestion_timestamp TIMESTAMP,
    source_file_name STRING,
    load_date DATE,
    run_id STRING
)
USING DELTA
PARTITIONED BY (load_date);

OPTIMIZE bronze_customers;