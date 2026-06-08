SELECT
    order_year,
    SUM(profit) AS total_profit
FROM dev.silver_sales
GROUP BY order_year
ORDER BY order_year;