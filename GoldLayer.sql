CREATE OR REPLACE PROCEDURE gold.load_gold()
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    rows_count        INTEGER;
    start_time        TIMESTAMP;
    end_time          TIMESTAMP;
    interval_diff     INTERVAL;
    hours             INTEGER;
    minutes           INTEGER;
    seconds           INTEGER;
    milliseconds      INTEGER;
    batch_start_time  TIMESTAMP;
    batch_end_time    TIMESTAMP;
BEGIN

    RAISE NOTICE '==================================================';
    RAISE NOTICE '=========== LOADING GOLD LAYER ===================';
    RAISE NOTICE '==================================================';
    RAISE NOTICE '';
    batch_start_time := NOW();

    -- =============================
    -- >> Create dim_customers view
    -- =============================
    RAISE NOTICE 'Loading gold.dim_customers';
    start_time := NOW();

    DROP VIEW IF EXISTS gold.dim_customers;
    CREATE VIEW gold.dim_customers AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name,
        la.cntry AS country,
        ci.cst_marital_status AS marital_status,
        CASE
            WHEN ci.cst_gndr != 'Unknow' THEN ci.cst_gndr
            ELSE COALESCE(ca.gen, 'Unknown') 
        END AS gender,
        ca.bdate AS birth_date,
        ci.cst_create_date AS create_date
    FROM
        silver.crm_cust_info AS ci
        LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.cid
        LEFT JOIN silver.erp_loc_a101 AS la ON ci.cst_key = la.cid;

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'gold.dim_customers: % rows affected', rows_count;

    end_time := NOW();
    interval_diff := end_time - start_time;
    hours := EXTRACT(HOUR FROM interval_diff);
    minutes := EXTRACT(MINUTE FROM interval_diff);
    seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
    milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '';

    -- =============================
    -- >> Create dim_products view
    -- =============================
    RAISE NOTICE 'Loading gold.dim_products';
    start_time := NOW();

    DROP VIEW IF EXISTS gold.dim_products;
    CREATE VIEW gold.dim_products AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY prd_start_dt, pn.sls_prd_key) AS product_key,
        pn.prd_id AS product_id,
        pn.sls_prd_key AS product_number,
        pn.prd_nm AS product_name,  
        pn.cat_id AS category_id,
        pc.cat AS category,
        pc.subcat AS subcategory,
        pc.maintenance AS maintenance,
        pn.prd_cost AS product_cost,
        pn.prd_line AS product_line,
        pn.prd_start_dt AS start_dt
    FROM
        silver.crm_prd_info AS pn
        LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pn.cat_id = pc.id
    WHERE
        pn.prd_end_dt IS NULL;

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'gold.dim_products: % rows affected', rows_count;

    end_time := NOW();
    interval_diff := end_time - start_time;
    hours := EXTRACT(HOUR FROM interval_diff);
    minutes := EXTRACT(MINUTE FROM interval_diff);
    seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
    milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '';

    -- =============================
    -- >> Create fact_sales view
    -- =============================
    RAISE NOTICE 'Loading gold.fact_sales';
    start_time := NOW();

    DROP VIEW IF EXISTS gold.fact_sales;
    CREATE VIEW gold.fact_sales AS
    SELECT
        sd.sls_ord_num AS order_number,
        pr.product_key,
        cu.customer_key,
        sd.sls_cust_id AS customer_id,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt AS shipping_date,
        sd.sls_due_dt AS due_date,
        sd.sls_sales AS sales_amount,
        sd.sls_quantity AS quantity, 
        sd.sls_price AS price
    FROM
        silver.crm_sales_details AS sd
        LEFT JOIN gold.dim_products AS pr ON pr.product_number = sd.sls_prd_key
        LEFT JOIN gold.dim_customers AS cu ON cu.customer_id = sd.sls_cust_id;

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'gold.fact_sales: % rows affected', rows_count;

    end_time := NOW();
    interval_diff := end_time - start_time;
    hours := EXTRACT(HOUR FROM interval_diff);
    minutes := EXTRACT(MINUTE FROM interval_diff);
    seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
    milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '';

    -- =============================
    -- Batch completion
    -- =============================
    batch_end_time := NOW();
    interval_diff := batch_end_time - batch_start_time;
    hours := EXTRACT(HOUR FROM interval_diff);
    minutes := EXTRACT(MINUTE FROM interval_diff);
    seconds := EXTRACT(SECOND FROM interval_diff)::INTEGER;
    milliseconds := EXTRACT(MILLISECONDS FROM interval_diff)::INTEGER % 1000;
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'GOLD LAYER LOADING COMPLETED';
    RAISE NOTICE 'Total Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '---------------------------------------------------';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE '--- ERROR OCCURRED DURING GOLD LAYER LOAD ---------';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE 'Error Detail: %', COALESCE(PG_EXCEPTION_DETAIL, 'N/A');
        RAISE NOTICE 'Error Hint: %', COALESCE(PG_EXCEPTION_HINT, 'N/A');
        RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE '';
        ROLLBACK;

END;
$BODY$;

CALL gold.load_gold();

SELECT * FROM gold.dim_customers LIMIT 10;
SELECT * FROM gold.dim_products LIMIT 10;
SELECT * FROM gold.fact_sales LIMIT 10;
