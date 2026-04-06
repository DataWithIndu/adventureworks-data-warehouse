-- ============================================================
-- AdventureWorks Analytics | Segmentation
-- Tool: SQL Server (Azure SQL Edge via Docker)
-- ============================================================
-- What this does:
--   Groups data into meaningful segments for targeted insight.
--   Covers product cost tiers, customer value segments,
--   demographic spending patterns, and category contribution
--   to overall sales.
-- ============================================================


-- ============================================================
-- DATA SEGMENTATION
-- Grouping products and customers into meaningful buckets.
-- Makes it easier to target the right segments with the right
-- strategy instead of treating everyone the same.
-- ============================================================

-- product cost segments — how many products fall in each price tier?
WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE
            WHEN cost < 100                THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500  THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;

-- customer value segments
-- VIP     : 12+ months history AND total spend > 5000
-- Regular : 12+ months history AND total spend <= 5000
-- New     : less than 12 months of order history
WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(f.sales_amount)                               AS total_spending,
        MIN(order_date)                                   AS first_order,
        MAX(order_date)                                   AS last_order,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM (
    SELECT
        customer_key,
        CASE
            WHEN lifespan >= 12 AND total_spending > 5000  THEN 'VIP'
            WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spending
) segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;

-- which high-value customers have gone quiet?
-- uses the most recent order year in the dataset as the reference point
-- so the logic stays valid regardless of when the data was last refreshed
-- a 2+ year gap from a previously high-spending customer is a churn signal
WITH customer_yearly AS (
    SELECT
        f.customer_key,
        YEAR(f.order_date)  AS order_year,
        SUM(f.sales_amount) AS yearly_spend
    FROM gold.fact_sales f
    WHERE f.order_date IS NOT NULL
    GROUP BY f.customer_key, YEAR(f.order_date)
),
customer_summary AS (
    SELECT
        customer_key,
        MAX(order_year)             AS last_active_year,
        SUM(yearly_spend)           AS total_spend,
        COUNT(DISTINCT order_year)  AS active_years,
        MAX(MAX(order_year)) OVER() AS latest_year_in_data
    FROM customer_yearly
    GROUP BY customer_key
)
SELECT
    cs.customer_key,
    c.first_name,
    c.last_name,
    cs.total_spend,
    cs.active_years,
    cs.last_active_year,
    cs.latest_year_in_data,
    CASE
        WHEN cs.total_spend > 5000
             AND cs.latest_year_in_data - cs.last_active_year >= 2 THEN 'High Value - Churned'
        WHEN cs.total_spend > 5000
             AND cs.latest_year_in_data - cs.last_active_year < 2  THEN 'High Value - Active'
        ELSE 'Standard'
    END AS retention_status
FROM customer_summary cs
LEFT JOIN gold.dim_customers c
    ON c.customer_key = cs.customer_key
ORDER BY cs.total_spend DESC;

-- do spending habits differ by gender or marital status?
-- the demographic columns in dim_customers exist but were never
-- used in the segmentation above — this fills that gap
SELECT
    c.gender,
    c.marital_status,
    COUNT(DISTINCT c.customer_key)               AS total_customers,
    SUM(f.sales_amount)                          AS total_revenue,
    ROUND(AVG(CAST(f.sales_amount AS FLOAT)), 2) AS avg_order_value,
    ROUND(
        CAST(SUM(f.sales_amount) AS FLOAT) /
        NULLIF(COUNT(DISTINCT c.customer_key), 0)
    , 2)                                         AS avg_revenue_per_customer
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
WHERE c.gender != 'n/a'
GROUP BY c.gender, c.marital_status
ORDER BY avg_revenue_per_customer DESC;


-- ============================================================
-- PART TO WHOLE ANALYSIS
-- Which categories drive the most overall revenue?
-- Shows each category's share of total sales.
-- ============================================================

WITH category_sales AS (
    SELECT
        p.category,
        SUM(f.sales_amount) AS total_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.category
)
SELECT
    category,
    total_sales,
    SUM(total_sales) OVER ()                                                 AS overall_sales,
    ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2)  AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;
