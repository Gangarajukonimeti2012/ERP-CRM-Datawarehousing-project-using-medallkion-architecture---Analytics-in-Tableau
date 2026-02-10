DROP DATABASE IF EXISTS datawarehouse;

-- Recreate the database
CREATE DATABASE datawarehouse;

-- Connect to the newly created database
-- datawarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

/* ============================================================================
   BRONZE LAYER – RAW DATA INGESTION
   ----------------------------------------------------------------------------
   Purpose:
   - Store raw data exactly as received from CRM and ERP systems.
   - No transformations, no cleaning, no business logic.
   - Acts as the single source of truth for audit & reprocessing.
   - Ensures reproducibility of the pipeline.

   Key Responsibilities:
   ✓ Ingest data from multiple source systems (CRM, ERP).
   ✓ Maintain raw structure to avoid data loss.
   ✓ Load CSV files using COPY into Bronze schema tables.

   In this project:
   - CRM provides customer, product, and sales data.
   - ERP provides customer birthdates, gender, and location.
   ============================================================================ */


DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);


DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);


DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);


DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);


DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);


DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);



TRUNCATE bronze.crm_cust_info;
COPY bronze.crm_cust_info FROM '/Users/gangarajukonimeti/DWH/CRM/cust_info.csv' CSV HEADER;

TRUNCATE bronze.crm_prd_info;
COPY bronze.crm_prd_info FROM '/Users/gangarajukonimeti/DWH/CRM/prd_info.csv' CSV HEADER;

TRUNCATE bronze.crm_sales_details;
COPY bronze.crm_sales_details FROM '/Users/gangarajukonimeti/DWH/CRM/sales_details.csv' CSV HEADER;

TRUNCATE bronze.erp_cust_az12;
COPY bronze.erp_cust_az12 FROM '/Users/gangarajukonimeti/DWH/ERP/CUST_AZ12.csv' CSV HEADER;

TRUNCATE bronze.erp_loc_a101;
COPY bronze.erp_loc_a101 FROM '/Users/gangarajukonimeti/DWH/ERP/LOC_A101.csv' CSV HEADER;

TRUNCATE bronze.erp_px_cat_g1v2;
COPY bronze.erp_px_cat_g1v2 FROM '/Users/gangarajukonimeti/DWH/ERP/PX_CAT_G1V2.csv' CSV HEADER;

-- CRM Customer OLTP Query
SELECT cst_id, cst_firstname, cst_lastname, cst_marital_status, cst_gndr
FROM bronze.crm_cust_info
ORDER BY cst_id
LIMIT 20;

-- CRM Sales OLTP Query
SELECT sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_sales
FROM bronze.crm_sales_details
WHERE sls_order_dt = '20230101'
LIMIT 20;

-- ERP Customer Location OLTP Query
SELECT cid, cntry FROM bronze.erp_loc_a101 LIMIT 20;


-- Raw CRM Customers
SELECT *  FROM bronze.crm_cust_info LIMIT 20;

-- Raw CRM Products
SELECT * FROM bronze.crm_prd_info LIMIT 20;

-- Raw CRM Sales
SELECT * FROM bronze.crm_sales_details LIMIT 20;

-- Raw ERP Data
SELECT * FROM bronze.erp_cust_az12 LIMIT 20;
SELECT * FROM bronze.erp_loc_a101 LIMIT 20;

/* ============================================================================
   SILVER LAYER – DATA CLEANING & STANDARDIZATION
   ----------------------------------------------------------------------------
   Purpose:
   - Clean and standardize raw data to make it analytics-ready.
   - Fix inconsistent formats (dates, gender, marital status).
   - Remove nulls, duplicates, invalid values.
   - Convert raw fields into meaningful business attributes.

   Key Responsibilities:
   ✓ Clean text fields (TRIM, UPPER).
   ✓ Standardize categorical values (e.g., Male/Female).
   ✓ Convert dates (YYYYMMDD → DATE).
   ✓ Merge CRM + ERP attributes at the cleaned level.
   ✓ Enforce data quality rules.

   In this project:
   - CRM customer data is cleaned and standardized.
   - ERP location, gender, and birthdates are validated.
   - Sales dates are safely converted from integers.
   ============================================================================ */

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info AS
SELECT DISTINCT
    cst_id,
    cst_key,
    TRIM(cst_firstname)   AS cst_firstname,
    TRIM(cst_lastname)    AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'Unknown'
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'Unknown'
    END AS cst_gndr,
    cst_create_date
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL;

-- silver.crm_prd_info (cleaned product)
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info AS
SELECT
    prd_id,
    prd_key,
    prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    TRIM(prd_line) AS prd_line,
    prd_start_dt,
    prd_end_dt
FROM bronze.crm_prd_info;

-- silver.crm_sales_details (cleaned sales)
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details AS
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    -- convert YYYYMMDD text to date; will error if values are malformed. If needed, pre-clean source.
    CASE WHEN LENGTH(TRIM(sls_order_dt::text)) = 8 THEN TO_DATE(TRIM(sls_order_dt::text), 'YYYYMMDD') ELSE NULL END AS sls_order_dt,
    CASE WHEN LENGTH(TRIM(sls_ship_dt::text)) = 8  THEN TO_DATE(TRIM(sls_ship_dt::text), 'YYYYMMDD') ELSE NULL END AS sls_ship_dt,
    CASE WHEN LENGTH(TRIM(sls_due_dt::text)) = 8   THEN TO_DATE(TRIM(sls_due_dt::text), 'YYYYMMDD') ELSE NULL END AS sls_due_dt,
    ABS(COALESCE(sls_sales, 0))  AS sls_sales,
    ABS(COALESCE(sls_quantity, 0)) AS sls_quantity,
    CASE
        WHEN sls_price IS NULL THEN NULL
        ELSE ABS(sls_price)
    END AS sls_price
FROM bronze.crm_sales_details;

-- silver.erp_cust_az12 (cleaned ERP customer)
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 AS
SELECT
    cid,
    CASE 
        WHEN bdate IS NULL 
             OR TRIM(bdate::text) = '' 
             OR bdate::text = '0000-00-00'
        THEN NULL
        ELSE bdate::text
    END AS bdate,
    CASE
        WHEN UPPER(TRIM(gen)) LIKE 'M%' THEN 'Male'
        WHEN UPPER(TRIM(gen)) LIKE 'F%' THEN 'Female'
        ELSE 'Unknown'
    END AS gen
FROM bronze.erp_cust_az12;

-- silver.erp_loc_a101 (cleaned location)
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 AS
SELECT
    cid,
    CASE
        WHEN UPPER(TRIM(cntry)) IN ('US','USA','UNITED STATES') THEN 'United States'
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) = '' THEN 'Unknown'
        ELSE cntry
    END AS cntry
FROM bronze.erp_loc_a101;

-- silver.erp_px_cat_g1v2 (copy)
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 AS
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2;


-- Clean CRM Customers
SELECT * FROM silver.crm_cust_info ORDER BY cst_id LIMIT 20;

-- Clean CRM Products
SELECT * FROM silver.crm_prd_info ORDER BY prd_id LIMIT 20;

-- Clean CRM Sales
SELECT * FROM silver.crm_sales_details ORDER BY sls_ord_num LIMIT 20;

-- Clean ERP Customers
SELECT * FROM silver.erp_cust_az12 LIMIT 20;

-- Clean ERP Locations
SELECT * FROM silver.erp_loc_a101 LIMIT 20;

/* ============================================================================
   GOLD LAYER – STAR SCHEMA (DIMENSIONS & FACTS)
   ----------------------------------------------------------------------------
   Purpose:
   - Build business-friendly data models for reporting (Power BI, Tableau, SQL).
   - Create Dimensional Models: Dimensions + Fact Tables.
   - Combine Silver layer tables into a full analytics-ready warehouse.

   Key Responsibilities:
   ✓ Create surrogate keys for dimensions (ROW_NUMBER()).
   ✓ Build dimension tables (Customers, Products, Product Category).
   ✓ Build fact table (Sales).
   ✓ Ensure all joins follow star schema principles.
   ✓ Expose simplified views for BI and analytics teams.

   In this project:
   - dim_customers: unified CRM + ERP customer profile.
   - dim_products: product master + attributes.
   - fact_sales: sales transactions with product/customer keys.

   Analysts and BI tools read only from this GOLD layer.
   ============================================================================ */

DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY c.cst_id) AS customer_key,
    c.cst_id AS customer_id,
    c.cst_key AS customer_number,
    c.cst_firstname AS first_name,
    c.cst_lastname  AS last_name,
    l.cntry         AS country,
    c.cst_marital_status AS marital_status,
    COALESCE(NULLIF(c.cst_gndr,'Unknown'), e.gen, 'Unknown') AS gender,
    e.bdate         AS birth_date,
    c.cst_create_date AS create_date
FROM silver.crm_cust_info c
LEFT JOIN silver.erp_cust_az12 e ON c.cst_key = e.cid
LEFT JOIN silver.erp_loc_a101 l ON c.cst_key = l.cid;

-- dim_products
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pr.prd_id) AS product_key,
    pr.prd_id AS product_id,
    pr.prd_key AS product_number,
    pr.prd_nm AS product_name,
    pr.prd_cost AS product_cost,
    pr.prd_line AS product_line
FROM silver.crm_prd_info pr
WHERE pr.prd_key IS NOT NULL;

-- fact_sales
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT
    s.sls_ord_num AS order_number,
    p.product_key,
    c.customer_key,
    s.sls_cust_id AS customer_id,
    s.sls_order_dt AS order_date,
    s.sls_ship_dt  AS shipping_date,
    s.sls_due_dt   AS due_date,
    s.sls_sales    AS sales_amount,
    s.sls_quantity AS quantity,
    s.sls_price    AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_products p ON p.product_number = s.sls_prd_key
LEFT JOIN gold.dim_customers c ON c.customer_id = s.sls_cust_id;


-- DIM CUSTOMERS
SELECT * FROM gold.dim_customers ORDER BY customer_key;

-- DIM PRODUCTS
SELECT * FROM gold.dim_products ORDER BY product_key;

-- FACT SALES
SELECT * FROM gold.fact_sales ORDER BY order_number;

/* ============================================================================
   ANALYTICS / OLAP LAYER – BUSINESS METRICS & KPIs
   ----------------------------------------------------------------------------
   Purpose:
   - Provide ready-to-use business insights for dashboards & reports.
   - Perform aggregations such as monthly sales, top customers, etc.
   - Enable fast querying using precomputed analytical views.

   Key Responsibilities:
   ✓ Monthly sales trends (fact_sales_monthly)
   ✓ Top customers by revenue (top_customers)
   ✓ Sales by country (sales_by_country)
   ✓ Unified KPI report using UNION ALL

   ============================================================================ */

DROP VIEW IF EXISTS gold.fact_sales_monthly;
CREATE VIEW gold.fact_sales_monthly AS
SELECT
    DATE_TRUNC('month', order_date) AS month,
    product_key,
    customer_key,
    SUM(sales_amount) AS total_sales,
    SUM(quantity) AS total_quantity,
    AVG(price) AS avg_price
FROM gold.fact_sales
GROUP BY 1,2,3;

-- Monthly Sales (OLAP)
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(sales_amount) AS total_sales FROM gold.fact_sales 
	GROUP BY 1
	ORDER BY 1;


DROP VIEW IF EXISTS gold.top_customers;
CREATE VIEW gold.top_customers AS
SELECT
    cu.customer_key,
    cu.first_name || ' ' || cu.last_name AS customer_name,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity
FROM gold.fact_sales f
JOIN gold.dim_customers cu ON cu.customer_key = f.customer_key
GROUP BY 1,2
ORDER BY total_sales DESC
LIMIT 10;

-- Top Customers (OLAP)
SELECT 
    customer_key,
    SUM(sales_amount) AS total_sales
FROM gold.fact_sales
GROUP BY customer_key
ORDER BY total_sales DESC;

DROP VIEW IF EXISTS gold.sales_by_country;
CREATE VIEW gold.sales_by_country AS
SELECT
    cu.country,
    SUM(f.sales_amount) AS total_sales,
    SUM(f.quantity) AS total_quantity,
    AVG(f.price) AS avg_price
FROM gold.fact_sales f
JOIN gold.dim_customers cu ON cu.customer_key = f.customer_key
GROUP BY cu.country
ORDER BY total_sales DESC;

-- Sales by Country (OLAP)
SELECT 
    c.country,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c 
  ON c.customer_key = f.customer_key
GROUP BY c.country
ORDER BY total_sales DESC;

-- UNION 
-- Generate a Report that shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers;

SELECT 
    TABLE_CATALOG, 
    TABLE_SCHEMA, 
    TABLE_NAME, 
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES;

-- Retrieve all columns for a specific table (dim_customers)
SELECT 
    COLUMN_NAME, 
    DATA_TYPE, 
    IS_NULLABLE, 
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';




