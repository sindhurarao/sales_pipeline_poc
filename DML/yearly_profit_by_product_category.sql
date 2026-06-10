SELECT
    order_year,
    product_category,
    SUM(total_profit) AS total_profit
FROM dev.gold.gold_profit_by_year_category_customer
GROUP BY order_year, product_category
ORDER BY order_year, product_category;