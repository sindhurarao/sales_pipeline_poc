CREATE MATERIALIZED VIEW gold_profit_by_year_category_customer
USING DELTA
AS
SELECT
    order_year,
    category AS product_category,
    subcategory AS product_sub_category,
    customer_id,
    customer_name,
    SUM(profit) AS total_profit
FROM silver_sales
where is_active = true
GROUP BY
    order_year,
    category,
    subcategory,
    customer_id,
    customer_name;