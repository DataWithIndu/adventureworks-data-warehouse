-- ============================================================
-- AdventureWorks Analytics | Exploration
-- Tool: SQL Server (Azure SQL Edge via Docker)
-- ============================================================
-- What this does:
--   Gets familiar with the data before doing any real analysis.
--   Covers database structure, dimension values, date boundaries,
--   and high-level business metrics in one place.
-- ============================================================


-- ============================================================
-- DATABASE STRUCTURE
-- What tables exist and what columns do they have.
-- ============================================================

-- all tables in the database
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES;

-- columns for dim_customers
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';

-- columns for dim_products
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_products';

-- columns for fact_sales
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_sales';


-- ============================================================
-- DIMENSION EXPLORATION
-- Unique values across key dimension columns.
-- Useful for spotting unexpected values before analysis.
-- ============================================================

-- unique countries customers come from
SELECT DISTINCT
    country
FROM gold.dim_customers
ORDER BY country;

-- full product hierarchy: category > subcategory > product
SELECT DISTINCT
    category,
    subcategory,
    product_name
FROM gold.dim_products
ORDER BY category, subcategory, product_name;


-- ============================================================
-- DATE RANGE
-- Understanding the time boundaries of the data.
-- ============================================================

-- first and last order date, and how many months of data we have
SELECT
    MIN(order_date)                                     AS first_order_date,
    MAX(order_date)                                     AS last_order_date,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date))   AS order_range_months
FROM gold.fact_sales;

-- age range of customers based on birthdate
SELECT
    MIN(birthdate)                              AS oldest_birthdate,
    DATEDIFF(YEAR, MIN(birthdate), GETDATE())   AS oldest_age,
    MAX(birthdate)                              AS youngest_birthdate,
    DATEDIFF(YEAR, MAX(birthdate), GETDATE())   AS youngest_age
FROM gold.dim_customers;


-- ============================================================
-- KEY METRICS
-- High level numbers to get a feel for the business before
-- diving into detailed analysis.
-- ============================================================

-- individual metric checks
SELECT SUM(sales_amount)            AS total_sales          FROM gold.fact_sales;
SELECT SUM(quantity)                AS total_quantity        FROM gold.fact_sales;
SELECT AVG(price)                   AS avg_price             FROM gold.fact_sales;
SELECT COUNT(DISTINCT order_number) AS total_orders          FROM gold.fact_sales;
SELECT COUNT(product_name)          AS total_products        FROM gold.dim_products;
SELECT COUNT(customer_key)          AS total_customers       FROM gold.dim_customers;

-- customers who have actually placed an order (vs all registered customers)
SELECT COUNT(DISTINCT customer_key) AS customers_with_orders FROM gold.fact_sales;

-- all key metrics in one summary view
SELECT 'Total Sales' AS measure_name,     SUM(sales_amount)   AS measure_value         FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity',                  SUM(quantity)                                FROM gold.fact_sales
UNION ALL
SELECT 'Average Price',                   AVG(price)                                   FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders',                    COUNT(DISTINCT order_number)                 FROM gold.fact_sales
UNION ALL
SELECT 'Total Products',                  COUNT(DISTINCT product_name)                 FROM gold.dim_products
UNION ALL
SELECT 'Total Customers',                 COUNT(customer_key)                          FROM gold.dim_customers;
