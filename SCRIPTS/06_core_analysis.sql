 -- ============================================================
-- AdventureWorks Analytics | Core Analysis
-- Tool: SQL Server (Azure SQL Edge via Docker)
-- ============================================================
-- What this does:
--   The main analysis layer. Covers magnitude, ranking, trends
--   over time, cumulative growth, and year-over-year performance.
-- ============================================================


-- ============================================================
-- MAGNITUDE ANALYSIS
-- Quantifying the business — who's buying, what's selling,
-- where the money is coming from. Grouped by key dimensions.
-- ============================================================

-- total customers by country
SELECT
    country,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC;

-- total customers by gender
SELECT
    gender,
    COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC;

-- total products by category
SELECT
    category,
    COUNT(product_key) AS total_products
FROM gold.dim_products
GROUP BY category
ORDER BY total_products DESC;

-- average product cost by category
SELECT
    category,
    AVG(cost) AS avg_cost
FROM gold.dim_products
GROUP BY category
ORDER BY avg_cost DESC;

-- total revenue by category
SELECT
    p.category,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- total revenue by customer
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;

-- quantity sold by country
SELECT
    c.country,
    SUM(f.quantity) AS total_sold_items
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sold_items DESC;

-- revenue-to-cost ratio by subcategory
-- high revenue doesn't always mean high efficiency — this shows
-- which subcategories actually make the most per unit of cost
SELECT
    p.subcategory,
    SUM(f.sales_amount)                         AS total_revenue,
    SUM(p.cost * f.quantity)                    AS total_cost,
    ROUND(
        CAST(SUM(f.sales_amount) AS FLOAT) /
        NULLIF(SUM(p.cost * f.quantity), 0)
    , 2)                                        AS revenue_to_cost_ratio
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.subcategory
ORDER BY revenue_to_cost_ratio DESC;


-- ============================================================
-- RANKING ANALYSIS
-- Top and bottom performers. Using both TOP N for simplicity
-- and window functions where rank-based filtering is needed.
-- ============================================================

-- top 5 products by revenue (simple)
SELECT TOP 5
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;

-- top 5 products by revenue (window function — easier to filter/extend)
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,
        RANK() OVER (ORDER BY SUM(f.sales_amount) DESC) AS rank_products
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.product_name
) ranked_products
WHERE rank_products <= 5;

-- bottom 5 products by revenue
SELECT TOP 5
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue;

-- top 10 customers by revenue
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;

-- 3 customers with the fewest orders
SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
GROUP BY
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders;


-- ============================================================
-- CHANGE OVER TIME
-- Monthly and yearly trends in sales, customers, and quantity.
-- Starts with acquisition trend to understand how the customer
-- base itself grew before looking at revenue trends.
-- ============================================================

-- how many new customers were acquired each year?
-- uses each customer's first ever order as their acquisition date
-- yoy_growth_pct shows whether acquisition is accelerating or slowing
WITH first_orders AS (
    SELECT
        customer_key,
        MIN(order_date) AS first_order_date
    FROM gold.fact_sales
    GROUP BY customer_key
),
yearly_acquisition AS (
    SELECT
        YEAR(first_order_date) AS acquisition_year,
        COUNT(customer_key)    AS new_customers
    FROM first_orders
    GROUP BY YEAR(first_order_date)
)
SELECT
    acquisition_year,
    new_customers,
    LAG(new_customers) OVER (ORDER BY acquisition_year)  AS prev_year_customers,
    new_customers - LAG(new_customers) OVER (ORDER BY acquisition_year) AS yoy_change,
    CASE
        WHEN LAG(new_customers) OVER (ORDER BY acquisition_year) IS NULL THEN NULL
        ELSE ROUND(
            (new_customers - LAG(new_customers) OVER (ORDER BY acquisition_year)) * 100.0 /
            NULLIF(LAG(new_customers) OVER (ORDER BY acquisition_year), 0)
        , 2)
    END AS yoy_growth_pct
FROM yearly_acquisition
ORDER BY acquisition_year;

-- monthly sales trend — year + month as separate columns
SELECT
    YEAR(order_date)                AS order_year,
    MONTH(order_date)               AS order_month,
    SUM(sales_amount)               AS total_sales,
    COUNT(DISTINCT customer_key)    AS total_customers,
    SUM(quantity)                   AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);

/*
-- using DATETRUNC — cleaner for charting tools
SELECT
    DATETRUNC(month, order_date)        AS order_date,
    SUM(sales_amount)                   AS total_sales,
    COUNT(DISTINCT customer_key)        AS total_customers,
    SUM(quantity)                       AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date);

running on Azure SQL Edge via Docker, so DATETRUNC is not available; year truncation handled using YEAR() instead. 
*/

SELECT
    YEAR(order_date)                    AS order_year,
    SUM(sales_amount)                   AS total_sales,
    COUNT(DISTINCT customer_key)        AS total_customers,
    SUM(quantity)                       AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date);        

/*
-- using FORMAT — readable labels for reports
SELECT
    FORMAT(order_date, 'yyyy-MMM')      AS order_date,
    SUM(sales_amount)                   AS total_sales,
    COUNT(DISTINCT customer_key)        AS total_customers,
    SUM(quantity)                       AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM');


running on Azure SQL Edge via Docker, so FORMAT is not available; FORMAT handled using CONCAT() instead. 
*/

SELECT
    CONCAT(YEAR(order_date), '-', RIGHT('0' + CAST(MONTH(order_date) AS VARCHAR), 2)) AS order_date,
    SUM(sales_amount)               AS total_sales,
    COUNT(DISTINCT customer_key)    AS total_customers,
    SUM(quantity)                   AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);


-- ============================================================
-- CUMULATIVE ANALYSIS
-- Running totals and moving averages to track growth over time.
-- Useful for spotting whether the business is consistently
-- growing or just having good isolated periods.
-- ============================================================

/*
SELECT
    order_date,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
    AVG(avg_price)   OVER (ORDER BY order_date) AS moving_avg_price
FROM (
    SELECT
        DATETRUNC(year, order_date) AS order_date,
        SUM(sales_amount)           AS total_sales,
        AVG(price)                  AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(year, order_date)
) t;

running on Azure SQL Edge via Docker, so DATETRUNC is not available; year truncation handled using YEAR() instead. 
*/

SELECT
    order_year,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_year) AS running_total_sales,
    AVG(avg_price)   OVER (ORDER BY order_year) AS moving_avg_price
FROM (
    SELECT
        YEAR(order_date)    AS order_year,
        SUM(sales_amount)   AS total_sales,
        AVG(price)          AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY YEAR(order_date)
) t;


-- ============================================================
-- PERFORMANCE ANALYSIS
-- Year-over-year product performance. Each product compared
-- against its own historical average and previous year.
-- Extended to also compare products within their own category
-- and to surface shipping speed differences by country.
-- ============================================================

-- yearly product performance vs its own average and previous year
WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date)  AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY
        YEAR(f.order_date),
        p.product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name)                                     AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name)                     AS diff_avg,
    CASE
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year)                 AS py_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
    CASE
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No Change'
    END AS py_change
FROM yearly_product_sales
ORDER BY product_name, order_year;

-- how does each product perform vs the average of its own category?
-- the above query benchmarks against overall average — this one is more
-- targeted: it spots underperformers dragging down an otherwise strong category
WITH product_sales AS (
    SELECT
        p.category,
        p.product_name,
        SUM(f.sales_amount) AS total_revenue
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY p.category, p.product_name
)
SELECT
    category,
    product_name,
    total_revenue,
    AVG(total_revenue) OVER (PARTITION BY category)                     AS category_avg_revenue,
    total_revenue - AVG(total_revenue) OVER (PARTITION BY category)     AS diff_from_category_avg,
    CASE
        WHEN total_revenue > AVG(total_revenue) OVER (PARTITION BY category) THEN 'Above Category Avg'
        WHEN total_revenue < AVG(total_revenue) OVER (PARTITION BY category) THEN 'Below Category Avg'
        ELSE 'At Category Avg'
    END AS performance_vs_category
FROM product_sales
ORDER BY category, total_revenue DESC;

-- average shipping time by country
-- flags whether certain regions are being served slower than others
SELECT
    c.country,
    COUNT(DISTINCT f.order_number)                              AS total_orders,
    AVG(DATEDIFF(day, f.order_date, f.shipping_date))          AS avg_days_to_ship,
    MIN(DATEDIFF(day, f.order_date, f.shipping_date))          AS fastest_ship,
    MAX(DATEDIFF(day, f.order_date, f.shipping_date))          AS slowest_ship
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
WHERE f.order_date    IS NOT NULL
  AND f.shipping_date IS NOT NULL
GROUP BY c.country
ORDER BY avg_days_to_ship;
