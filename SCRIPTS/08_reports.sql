-- ============================================================
-- AdventureWorks Analytics | Reports
-- Tool: SQL Server (Azure SQL Edge via Docker)
-- ============================================================
-- What this does:
--   Two consolidated reporting views that pull everything together.
--   report_customers : full customer profile with segments and KPIs
--   report_products  : full product profile with segments and KPIs
--
--   Built as views so they can be queried any time without
--   rerunning the underlying logic.
-- ============================================================


-- ============================================================
-- CUSTOMER REPORT
-- Consolidates customer metrics, segments, and KPIs in one view.
-- Segments  : VIP / Regular / New based on spend and tenure
-- Age groups : Under 20 / 20-29 / 30-39 / 40-49 / 50+
-- Retention  : High Value Active / High Value Churned / Standard
-- KPIs       : recency, average order value, average monthly spend
-- ============================================================

IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS

WITH base_query AS (
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name)  AS customer_name,
        DATEDIFF(year, c.birthdate, GETDATE())   AS age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON c.customer_key = f.customer_key
    WHERE order_date IS NOT NULL
),
customer_aggregation AS (
    SELECT
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number)                      AS total_orders,
        SUM(sales_amount)                                 AS total_sales,
        SUM(quantity)                                     AS total_quantity,
        COUNT(DISTINCT product_key)                       AS total_products,
        MAX(order_date)                                   AS last_order_date,
        MIN(order_date)                                   AS first_order_date,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    FROM base_query
    GROUP BY
        customer_key,
        customer_number,
        customer_name,
        age
),
latest_year AS (
    SELECT MAX(YEAR(order_date)) AS latest_year_in_data
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
)
SELECT
    ca.customer_key,
    ca.customer_number,
    ca.customer_name,
    ca.age,
    CASE
        WHEN ca.age < 20                 THEN 'Under 20'
        WHEN ca.age BETWEEN 20 AND 29    THEN '20-29'
        WHEN ca.age BETWEEN 30 AND 39    THEN '30-39'
        WHEN ca.age BETWEEN 40 AND 49    THEN '40-49'
        ELSE '50 and above'
    END AS age_group,
    CASE
        WHEN ca.lifespan >= 12 AND ca.total_sales > 5000  THEN 'VIP'
        WHEN ca.lifespan >= 12 AND ca.total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    -- retention status flags high-value customers who have gone quiet
    CASE
        WHEN ca.total_sales > 5000
             AND ly.latest_year_in_data - YEAR(ca.last_order_date) >= 2 THEN 'High Value - Churned'
        WHEN ca.total_sales > 5000
             AND ly.latest_year_in_data - YEAR(ca.last_order_date) < 2  THEN 'High Value - Active'
        ELSE 'Standard'
    END AS retention_status,
    ca.last_order_date,
    DATEDIFF(month, ca.last_order_date, GETDATE()) AS recency,
    ca.total_orders,
    ca.total_sales,
    ca.total_quantity,
    ca.total_products,
    ca.lifespan,
    CASE
        WHEN ca.total_orders = 0 THEN 0
        ELSE ca.total_sales / ca.total_orders
    END AS avg_order_value,
    CASE
        WHEN ca.lifespan = 0 THEN ca.total_sales
        ELSE ca.total_sales / ca.lifespan
    END AS avg_monthly_spend
FROM customer_aggregation ca
CROSS JOIN latest_year ly;
GO


-- ============================================================
-- PRODUCT REPORT
-- Consolidates product metrics, segments, and KPIs in one view.
-- Segments  : High-Performer / Mid-Range / Low-Performer by revenue
-- Category  : flags whether product is above or below category avg
-- KPIs      : recency, revenue-to-cost ratio, avg order revenue,
--             avg monthly revenue
-- ============================================================

IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW gold.report_products;
GO

CREATE VIEW gold.report_products AS

WITH base_query AS (
    SELECT
        f.order_number,
        f.order_date,
        f.customer_key,
        f.sales_amount,
        f.quantity,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.cost
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL
),
product_aggregations AS (
    SELECT
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date))        AS lifespan,
        MAX(order_date)                                          AS last_sale_date,
        COUNT(DISTINCT order_number)                             AS total_orders,
        COUNT(DISTINCT customer_key)                             AS total_customers,
        SUM(sales_amount)                                        AS total_sales,
        SUM(quantity)                                            AS total_quantity,
        SUM(cost * quantity)                                     AS total_cost,
        ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
    FROM base_query
    GROUP BY
        product_key,
        product_name,
        category,
        subcategory,
        cost
)
SELECT
    product_key,
    product_name,
    category,
    subcategory,
    cost,
    last_sale_date,
    DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
    CASE
        WHEN total_sales > 50000  THEN 'High-Performer'
        WHEN total_sales >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    -- how does this product compare to others in its own category?
    CASE
        WHEN total_sales > AVG(total_sales) OVER (PARTITION BY category) THEN 'Above Category Avg'
        WHEN total_sales < AVG(total_sales) OVER (PARTITION BY category) THEN 'Below Category Avg'
        ELSE 'At Category Avg'
    END AS performance_vs_category,
    -- revenue generated per unit of cost — higher means more efficient
    ROUND(
        CAST(total_sales AS FLOAT) / NULLIF(total_cost, 0)
    , 2) AS revenue_to_cost_ratio,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE total_sales / total_orders
    END AS avg_order_revenue,
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales / lifespan
    END AS avg_monthly_revenue
FROM product_aggregations;
GO


-- SELECT * FROM gold.report_customers;
-- SELECT * FROM gold.report_products;
