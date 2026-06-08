CREATE TABLE silver_dim_customer
(
    customer_id STRING,
    customer_name STRING,
    segment STRING,
    country STRING,
    region STRING,
    state STRING,
    city STRING,
    postal_code STRING,
    effective_date DATE,
    expiry_date DATE,
    current_flag BOOLEAN
)
USING DELTA;