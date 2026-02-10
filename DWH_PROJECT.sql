DROP DATABASE IF EXISTS datawarehousedwh;
CREATE DATABASE datawarehousedwh;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

---------------------------------------------------------------
-- 1. CRM Customers - bronze.crm_cust_info
---------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id             numeric,
    cst_key            text,
    cst_firstname      text,
    cst_lastname       text,
    cst_marital_status text,
    cst_gndr           text,
    cst_create_date    text
);


COPY bronze.crm_cust_info
FROM '/Users/gangarajukonimeti/DWH/CRM/cust_info.csv'
CSV HEADER;

SELECT *
FROM bronze.crm_cust_info WHERE cst_firstname = 'Jon';

---------------------------------------------------------------
-- 2. CRM Products - bronze.crm_prd_info
---------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id        numeric,
    prd_key       text,
    prd_nm        text,
    prd_cost      text,
    prd_line      text,
    prd_start_dt  text,
    prd_end_dt    text
);


COPY bronze.crm_prd_info
FROM '/Users/gangarajukonimeti/DWH/CRM/prd_info.csv'
CSV HEADER;

SELECT *
FROM bronze.crm_prd_info WHERE prd_nm = 'Sport-100 Helmet- Red';

---------------------------------------------------------------
-- 3. CRM Sales Details - bronze.crm_sales_details
---------------------------------------------------------------
DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   text,
    sls_prd_key   text,
    sls_cust_id   numeric,
    sls_order_dt  numeric,
    sls_ship_dt   numeric,
    sls_due_dt    numeric,
    sls_sales     numeric,
    sls_quantity  numeric,
    sls_price     numeric
);


COPY bronze.crm_sales_details
FROM '/Users/gangarajukonimeti/DWH/CRM/sales_details.csv'
CSV HEADER;

SELECT *
FROM bronze.crm_sales_details WHERE sls_prd_key = 'BK-M82S-44';

---------------------------------------------------------------
-- 4. ERP Customer Demographics - bronze.erp_cust_az12
---------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid   text,
    bdate text,
    gen   text
);


COPY bronze.erp_cust_az12
FROM '/Users/gangarajukonimeti/DWH/ERP/CUST_AZ12.csv'
CSV HEADER;

---------------------------------------------------------------
-- 5. ERP Customer Location - bronze.erp_loc_a101
---------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid   text,
    cntry text
);


COPY bronze.erp_loc_a101
FROM '/Users/gangarajukonimeti/DWH/ERP/LOC_A101.csv'
CSV HEADER;

---------------------------------------------------------------
-- 6. ERP Product Categories - bronze.erp_px_cat_g1v2
---------------------------------------------------------------
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           text,
    cat          text,
    subcat       text,
    maintenance  text
);


COPY bronze.erp_px_cat_g1v2
FROM '/Users/gangarajukonimeti/DWH/ERP/PX_CAT_G1V2.csv'
CSV HEADER;



---------------------------------------------------------------
-- 1. Silver CRM Customers - silver.crm_cust_info
---------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info AS
SELECT
    cst_id::int AS cst_id,
    trim(cst_key) AS cst_key,
    trim(cst_firstname) AS cst_firstname,
    trim(cst_lastname)AS cst_lastname,
    trim(cst_marital_status)AS cst_marital_status,
    CASE
        WHEN upper(trim(cst_gndr)) = 'M' THEN 'Male'
        WHEN upper(trim(cst_gndr)) = 'F' THEN 'Female'
        WHEN cst_gndr IS NULL OR trim(cst_gndr) = '' THEN 'Unknown'
        ELSE cst_gndr
    END AS cst_gndr,
    to_date(cst_create_date, 'YYYY-MM-DD')                AS cst_create_date,
    -- normalized key to join with ERP
    trim(cst_key)                                         AS cust_key_std
FROM bronze.crm_cust_info;

SELECT * FROM bronze.crm_cust_info;

SELECT * FROM silver.crm_cust_info ;
---------------------------------------------------------------
-- 2. Silver CRM Products - silver.crm_prd_info
---------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info AS
SELECT
    prd_id::int                                           AS prd_id,
    trim(prd_key)                                         AS prd_key,
    trim(prd_nm)                                          AS prd_nm,
    NULLIF(trim(prd_cost), '')::numeric(12,2)             AS prd_cost,
    trim(prd_line)                                        AS prd_line,
    to_date(prd_start_dt, 'YYYY-MM-DD')                   AS prd_start_dt,
    to_date(prd_end_dt,   'YYYY-MM-DD')                   AS prd_end_dt,

    -- for joining with PX_CAT_G1V2
    concat(
        split_part(prd_key, '-', 1), '_',  -- e.g. 'AC'
        split_part(prd_key, '-', 2)        -- e.g. 'HE'
    )                                      AS category_id,

    -- for joining with sales_details.sls_prd_key
    concat(
        split_part(prd_key, '-', 3), '-',  -- 'BK'
        split_part(prd_key, '-', 4), '-',  -- 'R93R'
        split_part(prd_key, '-', 5)        -- '62'
    )                                      AS prd_sls_key
FROM bronze.crm_prd_info;

SELECT * FROM silver.crm_prd_info;

---------------------------------------------------------------
-- 3. Silver CRM Sales Details - silver.crm_sales_details
---------------------------------------------------------------
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details AS
SELECT
    trim(sls_ord_num)                                 AS sls_ord_num,
    trim(sls_prd_key)                                 AS sls_prd_key,
    sls_cust_id::int                                  AS sls_cust_id,
    to_date(sls_order_dt::text, 'YYYYMMDD')           AS order_date,
    to_date(sls_ship_dt::text,  'YYYYMMDD')           AS ship_date,
    to_date(sls_due_dt::text,   'YYYYMMDD')           AS due_date,
    sls_sales::numeric(12,2)                          AS sls_sales,
    sls_quantity::int                                 AS sls_quantity,
    sls_price::numeric(12,2)                          AS sls_price
FROM bronze.crm_sales_details;

SELECT * FROM silver.crm_sales_details;

SELECT * FROM bronze.crm_sales_details;

---------------------------------------------------------------
-- 4. Silver ERP Customer Demographics - silver.erp_cust_az12
---------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 AS
SELECT
    cid                                                   AS raw_cid,
    -- Standardized customer key to match CRM cst_key
    replace(
        CASE
            WHEN cid LIKE 'NAS%' THEN substring(cid FROM 4)   -- drop 'NAS'
            ELSE cid
        END,
        '-', ''
    )                                                   AS cust_key_std,
    CASE
        WHEN bdate IS NULL OR trim(bdate) = '' THEN NULL
        ELSE to_date(bdate, 'YYYY-MM-DD')
    END                                                 AS bdate,
    CASE
        WHEN gen IS NULL OR trim(gen) = '' THEN 'Unknown'
        ELSE gen
    END                                                 AS gen
FROM bronze.erp_cust_az12;


SELECT * FROM bronze.erp_cust_az12;

SELECT * FROM silver.erp_cust_az12;
---------------------------------------------------------------
-- 5. Silver ERP Customer Location - silver.erp_loc_a101
---------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 AS
SELECT
    cid                                                   AS raw_cid,
    replace(trim(cid), '-', '')                           AS cust_key_std,
    trim(cntry)                                           AS cntry
FROM bronze.erp_loc_a101;

SELECT * FROM bronze.erp_loc_a101;

SELECT * FROM silver.erp_loc_a101;

---------------------------------------------------------------
-- 6. Silver ERP Product Category - silver.erp_px_cat_g1v2
---------------------------------------------------------------
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 AS
SELECT
    trim(id)          AS id,
    trim(cat)         AS cat,
    trim(subcat)      AS subcat,
    trim(maintenance) AS maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM bronze.erp_px_cat_g1v2;

SELECT * FROM silver.erp_px_cat_g1v2;


---------------------------------------------------------------
-- 1. Dimension: Customers - gold.dim_customers
---------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_customers;
CREATE TABLE gold.dim_customers AS
SELECT
    row_number() OVER (ORDER BY c.cst_id)                   AS customer_key,
    c.cst_id                                                AS customer_id,
    c.cst_key                                               AS customer_code,
    c.cst_firstname                                         AS first_name,
    c.cst_lastname                                          AS last_name,
    l.cntry                                                 AS country,
    c.cst_marital_status                                    AS marital_status,
    -- Prefer CRM gender if not 'Unknown', else ERP, else 'Unknown'
    COALESCE(
        NULLIF(c.cst_gndr, 'Unknown'),
        NULLIF(e.gen, 'Unknown'),
        'Unknown'
    )                                                       AS gender,
    e.bdate                                                 AS birth_date,
    c.cst_create_date                                       AS create_date
FROM silver.crm_cust_info c
LEFT JOIN silver.erp_cust_az12 e
    ON c.cust_key_std = e.cust_key_std
LEFT JOIN silver.erp_loc_a101 l
    ON c.cust_key_std = l.cust_key_std;

CREATE INDEX idx_dim_customers_customer_id
    ON gold.dim_customers (customer_id);

SELECT * FROM gold.dim_customers;

---------------------------------------------------------------
-- 2. Dimension: Products - gold.dim_products
---------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_products;
CREATE TABLE gold.dim_products AS
SELECT
    row_number() OVER (ORDER BY p.prd_id)                  AS product_key,
    p.prd_id                                               AS product_id,
    p.prd_key                                              AS product_code,
    p.prd_sls_key                                          AS product_sls_key, -- to join fact
    p.prd_nm                                               AS product_name,
    p.prd_cost                                             AS product_cost,
    p.prd_line                                             AS product_line,
    p.prd_start_dt                                         AS start_date,
    p.prd_end_dt                                           AS end_date,
    x.cat                                                  AS category,
    x.subcat                                               AS subcategory,
    x.maintenance                                          AS maintenance_flag
FROM silver.crm_prd_info p
LEFT JOIN silver.erp_px_cat_g1v2 x
    ON p.category_id = x.id;

CREATE INDEX idx_dim_products_sls_key
    ON gold.dim_products (product_sls_key);

CREATE INDEX idx_dim_products_product_id
    ON gold.dim_products (product_id);

SELECT * FROM gold.dim_products;

---------------------------------------------------------------
-- 3. Dimension: Date - gold.dim_date
---------------------------------------------------------------
DROP TABLE IF EXISTS gold.dim_date;
CREATE TABLE gold.dim_date AS
WITH all_dates AS (
    SELECT DISTINCT order_date AS dt FROM silver.crm_sales_details
    UNION
    SELECT DISTINCT ship_date  AS dt FROM silver.crm_sales_details
    UNION
    SELECT DISTINCT due_date   AS dt FROM silver.crm_sales_details
)
SELECT
    CAST(to_char(dt, 'YYYYMMDD') AS int)                   AS date_key,
    dt                                                     AS date_value,
    EXTRACT(YEAR  FROM dt)::int                            AS year,
    EXTRACT(QUARTER FROM dt)::int                          AS quarter,
    EXTRACT(MONTH FROM dt)::int                            AS month,
    to_char(dt, 'Month')                                   AS month_name,
    EXTRACT(DAY   FROM dt)::int                            AS day,
    EXTRACT(DOW   FROM dt)::int                            AS day_of_week,
    to_char(dt, 'Day')                                     AS day_name,
    CASE WHEN EXTRACT(DOW FROM dt) IN (6,0) THEN 'Weekend'
         ELSE 'Weekday' END                                AS weekday_weekend
FROM all_dates;

ALTER TABLE gold.dim_date
    ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_key);

CREATE INDEX idx_dim_date_date_value
    ON gold.dim_date (date_value);

SELECT * FROM gold.dim_date;

---------------------------------------------------------------
-- 4. Fact: Sales - gold.fact_sales
---------------------------------------------------------------
DROP TABLE IF EXISTS gold.fact_sales;
CREATE TABLE gold.fact_sales AS
SELECT
    row_number() OVER (ORDER BY s.sls_ord_num, s.sls_prd_key, s.sls_cust_id)
                                                            AS sales_key,
    s.sls_ord_num                                           AS order_number,

    d_order.date_key                                        AS order_date_key,
    d_ship.date_key                                         AS ship_date_key,
    d_due.date_key                                          AS due_date_key,

    c.customer_key                                          AS customer_key,
    p.product_key                                           AS product_key,

    s.sls_sales                                             AS sales_amount,
    s.sls_quantity                                          AS quantity,
    s.sls_price                                             AS price
FROM silver.crm_sales_details s
JOIN gold.dim_customers c
    ON c.customer_id = s.sls_cust_id
JOIN gold.dim_products p
    ON p.product_sls_key = s.sls_prd_key
LEFT JOIN gold.dim_date d_order
    ON d_order.date_value = s.order_date
LEFT JOIN gold.dim_date d_ship
    ON d_ship.date_value = s.ship_date
LEFT JOIN gold.dim_date d_due
    ON d_due.date_value = s.due_date;

CREATE INDEX idx_fact_sales_customer_key
    ON gold.fact_sales (customer_key);

CREATE INDEX idx_fact_sales_product_key
    ON gold.fact_sales (product_key);

CREATE INDEX idx_fact_sales_order_date_key
    ON gold.fact_sales (order_date_key);

SELECT * FROM gold.fact_sales;

/* ============================================================
   OLAP / ANALYTICAL QUERIES
   ============================================================ */

---------------------------------------------------------------
-- Q1: Sales by Year / Month (ROLLUP)
---------------------------------------------------------------
-- This shows total sales per year, per month, and grand total.
SELECT
    d.year,
    d.month,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_date d
    ON f.order_date_key = d.date_key
GROUP BY ROLLUP (d.year, d.month)
ORDER BY d.year, d.month;

---------------------------------------------------------------
-- Q2: Sales by Country and Gender (CUBE)
---------------------------------------------------------------
-- This shows combinations of country, gender and their subtotals.
SELECT
    c.country,
    c.gender,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
GROUP BY CUBE (c.country, c.gender)
HAVING c.country IS NOT NULL AND c.country <> '' AND c.gender IS NOT NULL AND c.gender <> ''
ORDER BY c.country, c.gender;


---------------------------------------------------------------
-- Q3: Top 10 Customers by Sales
---------------------------------------------------------------
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.first_name, c.last_name, c.country
ORDER BY total_sales DESC
LIMIT 10;

---------------------------------------------------------------
-- Q4: Product Category Performance
---------------------------------------------------------------
SELECT
    p.category,
    p.subcategory,
    SUM(f.sales_amount)AS total_sales,
    SUM(f.quantity)AS total_quantity,
    AVG(f.price)AS avg_unit_price
FROM gold.fact_sales f
JOIN gold.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY total_sales DESC;

---------------------------------------------------------------
-- Q5: Sales Trend by Month + Running Total
---------------------------------------------------------------
SELECT
    d.year,
    d.month,
    SUM(f.sales_amount)AS monthly_sales,
    SUM(SUM(f.sales_amount)) OVER (
        ORDER BY d.year, d.month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )AS running_total
FROM gold.fact_sales f
JOIN gold.dim_date d
    ON f.order_date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

---------------------------------------------------------------
-- Q6: Simple RFM-style Customer Segmentation
---------------------------------------------------------------
WITH customer_stats AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.first_name,
        c.last_name,
        MAX(d.date_value)                   AS last_order_date,
        COUNT(DISTINCT f.order_number)      AS frequency,
        SUM(f.sales_amount)                 AS monetary
    FROM gold.fact_sales f
    JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    JOIN gold.dim_date d
        ON f.order_date_key = d.date_key
    GROUP BY c.customer_key, c.customer_id, c.first_name, c.last_name
)
SELECT
    *,
    CASE
        WHEN monetary >= 5000 THEN 'High value'
        WHEN monetary >= 2000 THEN 'Medium value'
        ELSE 'Low value'
    END AS value_segment
FROM customer_stats
ORDER BY customer_id desc;

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    SUM(f.sales_amount) AS total_sales,
    CASE 
        WHEN SUM(f.sales_amount) >= 10000 THEN 'High Value'
        WHEN SUM(f.sales_amount) >= 5000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS monetary_segment
FROM gold.fact_sales f
JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.first_name, c.last_name, c.country
ORDER BY c.customer_id ASC;







