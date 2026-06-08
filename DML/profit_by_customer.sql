SELECT
    customer_id,
    customer_name,
    SUM(profit) AS total_profit
FROM dev.silver_sales
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC;