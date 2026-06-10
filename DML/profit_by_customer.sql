SELECT
    customer_id,
    customer_name,
    SUM(total_profit) AS total_profit
FROM dev.gold_profit_by_year_category_customer
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC;