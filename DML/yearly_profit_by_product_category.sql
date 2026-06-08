SELECT
    order_year,
    category AS product_category,
    SUM(profit) AS total_profit
FROM dev.silver_sales
GROUP BY order_year, category
ORDER BY order_year, category;