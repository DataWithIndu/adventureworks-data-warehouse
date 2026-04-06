# AdventureWorks Data Warehouse

SQL Server (Azure SQL Edge via Docker) | Data Warehousing | DataWithIndu

---

## What is this project about?

I wanted to go beyond writing queries on a flat CSV and actually build something that looks like what data teams work with in the real world. So instead of just analyzing data, I built the pipeline that prepares it first -- a full medallion architecture (Bronze, Silver, Gold) on top of the AdventureWorks dataset, and then ran a full business analysis on top of that covering revenue trends, customer segmentation, product performance, and churn detection.

The idea was to understand what it actually takes to go from raw, messy source data to something a business can confidently make decisions from. Turns out it involves a lot of cleaning, a few frustrating join issues, and some genuinely interesting findings at the end.

---

## About the Data

AdventureWorks is Microsoft's official sample database. I used the Data Warehouse version, which simulates a bicycle company selling bikes, accessories, and clothing across multiple countries.

Dataset link: https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver17&tabs=ssms

The data comes from two source systems -- a CRM and an ERP -- across six CSV files. The analysis covers December 2010 to January 2014, roughly 37 months of transaction history.

---

## Setup

SQL Server running via Azure SQL Edge on Docker, with VS Code as the editor. CSVs had to be converted to pipe-separated format before loading because BULK INSERT and commas in text fields do not get along. Files were copied into the container at `/tmp/` using `docker cp` before running the load procedures.

---

## Project Structure

The project is split into 8 files, organized by what they do.

**01_setup.sql** -- Creates the DataWarehouse database and three schemas (bronze, silver, gold). Running this drops and recreates everything, so it is a one-time thing.

**02_bronze.sql** -- Creates all six raw tables and loads data from the CSVs using BULK INSERT. No transformations here, data goes in exactly as it came from the source. The load procedure logs time taken per table and catches errors if something goes wrong.

**03_silver.sql** -- This is where all the cleaning happens. Coded values get expanded, duplicates get removed, dates stored as integers get converted to actual dates, bad data gets flagged or nulled out. A full list of what was cleaned is in the Data Cleaning section below.

**04_gold.sql** -- Three views that form the star schema: `dim_customers`, `dim_products`, and `fact_sales`. This is the layer everything downstream queries from.

**05_exploration.sql** -- Getting familiar with the data before analysis. Database structure, unique dimension values, date boundaries, and high-level business metrics.

**06_core_analysis.sql** -- The main analysis. Revenue by category, product rankings, customer rankings, monthly and yearly trends, cumulative growth, and year-over-year product performance.

**07_segmentation.sql** -- Grouping things into meaningful buckets. Product cost tiers, customer value segments, churn signals for high-value customers, and demographic spending patterns.

**08_reports.sql** -- Two consolidated reporting views (`report_customers` and `report_products`) that pull everything together in one place so you do not have to run ten queries to answer one business question.

---

## Data Cleaning Notes

The silver layer handles all of this before any analysis runs.

- Gender and marital status were stored as single letters (`M`, `F`, `S`) -- expanded to readable labels
- Customer records had duplicates across the same ID -- kept the most recent record using `ROW_NUMBER()`
- Dates were stored as 8-digit integers in the source (like `20130415`) -- converted to proper DATE type
- Sales amounts were sometimes negative or inconsistent with quantity x price -- recalculated where needed
- Some birthdates were in the future -- nulled out as clearly bad data
- Country codes were inconsistent (`US`, `USA`, `DE`) -- standardized to full country names
- ERP customer IDs had a `NAS` prefix that did not exist in CRM -- stripped before joining
- A `dwh_create_date` column was added to every silver table for audit tracking

One thing that could not be fully resolved: 7 products in `dim_products` show NULL for category and subcategory. The join between `crm_prd_info` and `erp_px_cat_g1v2` on `cat_id` does not find a match for these products. They show up in the data, they just cannot be categorized. Flagged rather than dropped.

---

## Key Findings

18,484 customers, 295 products, 27,659 orders, $29.4M in total revenue. Every single registered customer placed at least one order -- no dead accounts, 100% activation.

---

**The business is basically a bike company and nothing else**

Bikes drive 96.46% of total revenue ($28.3M). Accessories contribute 2.39% ($700K) and Clothing 1.16% ($340K). That is the entire revenue breakdown. If anything disrupts bike sales -- supply chain, pricing, demand -- there is almost no cushion from the other categories. The diversification exists on paper but not in the numbers.

---

**High revenue and high efficiency are not the same thing**

This was the most interesting finding. When I calculated revenue-to-cost ratio by subcategory, the ranking looked nothing like the revenue ranking.

| Subcategory | Total Revenue | Revenue-to-Cost Ratio |
|-------------|--------------|----------------------|
| Socks | $5,112 | 3.00x |
| Fenders | $46,662 | 2.75x |
| Tires and Tubes | $244,634 | 2.70x |
| Mountain Bikes | $9,952,254 | 1.78x |
| Touring Bikes | $3,844,580 | 1.61x |
| Road Bikes | $14,519,438 | 1.57x |

Road Bikes bring in the most revenue of any subcategory but return only $1.57 for every $1 of cost. Socks bring in almost nothing but return $3.00 per $1 of cost. A business optimizing for revenue and a business optimizing for margin efficiency would make completely different decisions here.

---

**The entire top-5 product list is one product family**

All five highest-revenue products are Mountain-200 variants -- Black and Silver, in sizes 38, 42, and 46 -- each generating between $1.29M and $1.37M. The top-line revenue story is one product family in a few different specs. Any disruption to Mountain-200 hits the whole business hard.

The bottom-5 by revenue are Racing Socks and small consumables, ranging from $2,430 to $7,440. Easy to write off -- but Socks have the best revenue-to-cost ratio in the catalog at 3.0x. Low revenue is not the same as low value.

---

**2013 was a completely different year**

| Year | New Customers | YoY Change |
|------|--------------|------------|
| 2010 | 14 | — |
| 2011 | 2,216 | +15,628% |
| 2012 | 3,225 | +45.5% |
| 2013 | 12,521 | +288.3% |
| 2014 | 506 | -96.0% |

New customer acquisition in 2013 was nearly 4x the 2012 number. Something big happened that year -- a campaign, a new market, a product launch -- the data does not say what but the spike is undeniable. The 2014 number looks like a crash but the data only runs through January 2014, so that is one month, not a full year.

Annual revenue followed the same pattern: $7.1M in 2011, $5.8M in 2012 (actually a dip), then $16.3M in 2013. The 2012 dip is also worth noting -- revenue fell before it surged, which suggests something changed in the product or pricing mix that year.

---

**Average selling price has been falling the whole time**

| Year | Moving Avg Price |
|------|-----------------|
| 2010 | $3,101 |
| 2011 | $3,146 |
| 2012 | $2,670 |
| 2013 | $2,080 |
| 2014 | $1,668 |

Every year, customers paid less on average. Revenue still grew in 2013 because volume increased massively, but the price trend is going the other direction. That is either a deliberate strategy to make products more accessible or pricing pressure -- either way it is something a business would want to understand and not just accept.

---

**80% of the customer base never moved past "New"**

| Segment | Customers | Share |
|---------|-----------|-------|
| New | 14,631 | 79% |
| Regular | 2,198 | 12% |
| VIP | 1,655 | 9% |

VIP = 12+ months of order history and total spend above $5,000. Regular = 12+ months but spend at or below $5,000. New = everyone else.

The acquisition funnel clearly works. But most customers buy once or twice and never reach the spending or tenure level to move up a segment. Growing the VIP share even a few percentage points would have an outsized revenue impact -- these are the customers who keep coming back and spend the most when they do.

---

**Single female customers give the best return per person**

| Gender | Marital Status | Avg Revenue per Customer |
|--------|---------------|--------------------------|
| Female | Single | $1,719.80 |
| Male | Single | $1,620.88 |
| Female | Married | $1,533.55 |
| Male | Married | $1,503.34 |

Single customers outspend married ones regardless of gender. If a campaign has a limited budget and needs to target the segment that gives the most revenue per person, single female customers are the answer. Married males are the largest group but the lowest average spenders.

---

**Shipping time is a data quality flag, not a finding**

Every single order ships in exactly 7 days to every country, with fastest and slowest both also at 7. No variation across 27,659 orders and 6 different countries. This almost certainly means the shipping date was auto-populated in the source system rather than actually recorded. Calling this out as a limitation rather than presenting it as a real operational insight.

---

**The report views exist so you do not have to rerun everything**

`report_customers` has customer segment, age group, retention status, recency, average order value, and average monthly spend all in one place. One query, no joins needed.

`report_products` has product segment (High/Mid/Low Performer), performance vs. category average, revenue-to-cost ratio, and average monthly revenue in one place. A product flagged as Low-Performer, Below Category Average, and low revenue-to-cost ratio is a clean discontinuation signal. A High-Performer Above Category Average is one you protect.

The point of these views is that a non-technical stakeholder can get to actionable answers fast without rebuilding the logic every time.

---

## SQL Concepts Used

- CTEs (including multi-layer and chained CTEs)
- Window Functions:
  - `ROW_NUMBER()` -- deduplication in silver layer and surrogate key generation in gold views
  - `RANK()` -- product and customer revenue rankings
  - `LAG()` -- year-over-year change in customer acquisition and product sales
  - `LEAD()` -- deriving product end dates from the next version's start date
  - `SUM() OVER()` -- running total of sales across years
  - `AVG() OVER(PARTITION BY ...)` -- moving average price and category-level benchmarking
  - `MAX(MAX()) OVER()` -- dynamic latest-year reference for churn detection without hardcoding a date
- Stored Procedures with `TRY/CATCH` error handling and per-table batch logging
- Views -- gold layer star schema and the two reporting views in the analytics layer
- `CROSS JOIN` -- used in `report_customers` to make a scalar CTE (latest year in data) available across all rows without repeating a subquery in every column
- `BULK INSERT` with pipe delimiter for CSV loading
- `NULLIF` -- divide-by-zero protection throughout the analytics layer
- `COALESCE` -- multi-source fallback logic in `dim_customers` for gender (CRM primary, ERP fallback)
- `DATEDIFF` -- lifespan in months, recency, age calculation, and shipping day gaps
- `GETDATE()` -- age calculation and recency in the report views
- `COUNT(DISTINCT ...)` -- unique order counts, unique customers per product, unique products per customer
- `BETWEEN` -- product cost tier bucketing in segmentation
- `YEAR()` and `MONTH()` instead of `DATETRUNC` (not available on Azure SQL Edge)
- `CONCAT()` with `RIGHT()` and `CAST()` instead of `FORMAT()` (also not available on Azure SQL Edge) -- used to build `YYYY-MM` date labels for the monthly trend output
- `CAST`, `CASE WHEN`, String Functions, Aggregate Functions, JOINs

---

Built by Indu Sharma
github.com/DataWithIndu
