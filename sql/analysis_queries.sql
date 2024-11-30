-- 1. Detailed Analysis of Revenue, Profit, and Discount by Customer Segment, Product, and Region
SELECT ds.segment, dp.category, dp.sub_category, dl.region, 
       SUM(fs.sale_price) AS total_revenue, 
       SUM(fs.profit) AS total_profit, 
       AVG(fs.discount_percent) AS avg_discount
FROM fact_sales fs
JOIN dim_segment ds ON fs.segment_id = ds.segment_id
JOIN dim_product dp ON fs.product_id = dp.product_id
JOIN dim_location dl ON fs.location_id = dl.location_id
GROUP BY ds.segment, dp.category, dp.sub_category, dl.region
ORDER BY total_revenue DESC;


-- 2. Best-Selling Products in Each Region and Their Total Revenue
WITH product_sales AS (
    SELECT dl.region, dp.product_id, dp.category, dp.sub_category, 
           SUM(fs.quantity) AS total_quantity,
           SUM(fs.sale_price) AS total_revenue
    FROM fact_sales fs
    JOIN dim_location dl ON fs.location_id = dl.location_id
    JOIN dim_product dp ON fs.product_id = dp.product_id
    GROUP BY dl.region, dp.product_id, dp.category, dp.sub_category
),
max_sales AS (
    SELECT region, MAX(total_quantity) AS max_quantity
    FROM product_sales
    GROUP BY region
)
SELECT ps.region, ps.product_id, ps.category, ps.sub_category, 
       ps.total_quantity, ps.total_revenue
FROM product_sales ps
JOIN max_sales ms ON ps.region = ms.region AND ps.total_quantity = ms.max_quantity
ORDER BY ps.region, ps.total_revenue DESC;


-- 3. Identifying Months with Decreased Revenue Compared to the Previous Month in the Same Year
SELECT current_month.year, current_month.month, 
       current_month.revenue AS current_month_revenue, 
       previous_month.revenue AS previous_month_revenue
FROM (
    SELECT dd.year, dd.month, SUM(fs.sale_price) AS revenue
    FROM fact_sales fs
    JOIN dim_date dd ON fs.order_date = dd.order_date
    GROUP BY dd.year, dd.month
) current_month
LEFT JOIN (
    SELECT dd.year, dd.month, SUM(fs.sale_price) AS revenue
    FROM fact_sales fs
    JOIN dim_date dd ON fs.order_date = dd.order_date
    GROUP BY dd.year, dd.month
) previous_month
ON current_month.year = previous_month.year 
   AND current_month.month = previous_month.month + 1
WHERE current_month.revenue < previous_month.revenue
ORDER BY current_month.year, current_month.month, current_month.revenue;



-- 4. Which Customer Segments Contribute the Most to Lifetime Value?
SELECT ds.segment, 
       COUNT(DISTINCT fs.order_id) AS total_orders,
       SUM(fs.sale_price) AS total_revenue,
       SUM(fs.sale_price) / COUNT(DISTINCT fs.order_id) AS average_order_value,
       COUNT(DISTINCT fs.order_id) * (SUM(fs.sale_price) / COUNT(DISTINCT fs.order_id)) AS lifetime_value
FROM fact_sales fs
JOIN dim_segment ds ON fs.segment_id = ds.segment_id
GROUP BY ds.segment
ORDER BY lifetime_value DESC;


-- 5. Predicting Profit for 2024 Based on the Growth Trend in Previous Years
WITH yearly_profit AS (
    SELECT dd.year, SUM(fs.profit) AS total_profit
    FROM fact_sales fs
    JOIN dim_date dd ON fs.order_date = dd.order_date
    GROUP BY dd.year
)
SELECT year, total_profit,
       LAG(total_profit) OVER (ORDER BY year) AS previous_profit,
       (total_profit - LAG(total_profit) OVER (ORDER BY year)) * 100.0 / 
       LAG(total_profit) OVER (ORDER BY year) AS growth_rate
FROM yearly_profit;

