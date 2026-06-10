SELECT
    customer_id,
    customer_name,
    order_year,
    SUM(total_profit) AS total_profit
FROM dev.gold.gold_profit_by_year_category_customer
GROUP BY customer_id, customer_name, order_year
ORDER BY customer_name, order_year;