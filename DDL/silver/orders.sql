CREATE TABLE silver_orders
(
    order_key BIGINT GENERATED ALWAYS AS IDENTITY,
    order_id STRING,
    row_id STRING,
    order_date DATE,
    order_year INT,
    order_month INT,
    order_quarter INT,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    price DOUBLE,
    discount DOUBLE,
    profit DOUBLE,
    ship_date DATE,
    ship_mode STRING,
    load_date DATE,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (order_year);

OPTIMIZE silver_orders
ZORDER BY (order_id, customer_id, product_id);