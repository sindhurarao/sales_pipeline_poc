CREATE TABLE IF NOT EXISTS bronze_orders
(
    row_id STRING,
    order_id STRING,
    order_date STRING,
    ship_date STRING,
    ship_mode STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    price DOUBLE,
    discount DOUBLE,
    profit DOUBLE,
    ingestion_timestamp TIMESTAMP,
    source_file_name STRING,
    load_date DATE,
    run_id STRING,
    _rescued_data STRING
)
USING DELTA
PARTITIONED BY (load_date);

OPTIMIZE bronze_orders
ZORDER BY (order_id, customer_id, product_id);
