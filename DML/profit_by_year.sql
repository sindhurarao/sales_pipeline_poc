SELECT
    order_year,
    SUM(total_profit) AS total_profit
FROM dev.gold_profit_by_year_category_customer
GROUP BY order_year
ORDER BY order_year;