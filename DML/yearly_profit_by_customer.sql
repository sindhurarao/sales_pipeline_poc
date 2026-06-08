SELECT
    customer_id,
    customer_name,
    order_year,
    SUM(profit) AS total_profit
FROM dev.silver_sales
GROUP BY customer_id, customer_name, order_year
ORDER BY customer_name, order_year;