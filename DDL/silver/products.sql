CREATE TABLE silver_dim_product
(
    product_id STRING,
    category STRING,
    sub_category STRING,
    product_name STRING,
    standard_price DOUBLE,
    effective_date DATE,
    expiry_date DATE,
    current_flag BOOLEAN
)
USING DELTA;