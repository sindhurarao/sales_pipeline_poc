CREATE TABLE silver_dim_product
(
    product_id STRING,
    category STRING,
    subcategory STRING,
    product_name STRING,
    standard_price DOUBLE
)
USING DELTA;