CREATE TABLE IF NOT EXISTS bronze_products
(
    product_id STRING,
    category STRING,
    subcategory STRING,
    product_name STRING,
    state STRING,
    price_per_product DOUBLE,
    ingestion_timestamp TIMESTAMP,
    source_file_name STRING,
    load_date DATE,
    run_id STRING
)
USING DELTA
PARTITIONED BY (load_date);

OPTIMIZE bronze_products;