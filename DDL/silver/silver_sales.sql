CREATE TABLE silver_sales
(
    sales_key BIGINT GENERATED ALWAYS AS IDENTITY,
    order_id STRING,
    row_id STRING,
    order_date DATE,
    order_year INT,
    order_month INT,
    order_quarter INT,
    customer_id STRING,
    customer_name STRING,
    product_id STRING,
    category STRING,
    subcategory STRING,
    product_name STRING,
    quantity INT,
    sales_amount DOUBLE,
    discount DOUBLE,
    profit DOUBLE,
    load_date DATE
)
USING DELTA
PARTITIONED BY (order_year);

OPTIMIZE silver_sales
ZORDER BY (order_year, customer_id, product_id);