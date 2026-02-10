CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $BODY$
DECLARE
    rows_count        INTEGER;
    start_time        timestamptz;
    end_time          timestamptz;
    interval_diff     INTERVAL;
    total_ms          bigint;
    hours             INTEGER;
    minutes           INTEGER;
    seconds           INTEGER;
    milliseconds      INTEGER;
    batch_start_time  timestamptz;
    batch_end_time    timestamptz;
BEGIN

    RAISE NOTICE '==================================================';
    RAISE NOTICE '=========== LOADING SILVER LAYER =================';
    RAISE NOTICE '==================================================';
    RAISE NOTICE '';

    batch_start_time := clock_timestamp();
    RAISE NOTICE 'Starting silver.load_silver procedure';
    RAISE NOTICE '';

    --------------------------------------------------------------------------
    -- CRM: crm_cust_info
    --------------------------------------------------------------------------
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '------------- Loading CRM Tables ------------------';
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '';

    RAISE NOTICE '----------';
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncate Table and insert into silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info(
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'Unknown'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'Unknown'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id 
                ORDER BY cst_create_date DESC 
            ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'crm_cust_info: % rows affected', rows_count;

    end_time := clock_timestamp();
    interval_diff := end_time - start_time;
    total_ms := floor(EXTRACT(EPOCH FROM interval_diff) * 1000)::bigint;
    hours := (total_ms / 3600000)::int;
    minutes := ((total_ms % 3600000) / 60000)::int;
    seconds := ((total_ms % 60000) / 1000)::int;
    milliseconds := (total_ms % 1000)::int;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '----------';
    RAISE NOTICE '';

    --------------------------------------------------------------------------
    -- CRM: crm_prd_info
    --------------------------------------------------------------------------
    RAISE NOTICE '----------';
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncate Table and insert into silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        sls_prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS sls_prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'Unknown'
        END AS prd_line,
        prd_start_dt,
        LEAD(prd_start_dt) OVER ( PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
    FROM 
        bronze.crm_prd_info;

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'crm_prd_info: % rows affected', rows_count;

    end_time := clock_timestamp();
    interval_diff := end_time - start_time;
    total_ms := floor(EXTRACT(EPOCH FROM interval_diff) * 1000)::bigint;
    hours := (total_ms / 3600000)::int;
    minutes := ((total_ms % 3600000) / 60000)::int;
    seconds := ((total_ms % 60000) / 1000)::int;
    milliseconds := (total_ms % 1000)::int;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '----------';
    RAISE NOTICE '';

    --------------------------------------------------------------------------
    -- CRM: crm_sales_details
    --------------------------------------------------------------------------
    RAISE NOTICE '----------';
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncate Table and insert into silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE 
            WHEN sls_order_dt = 0 
                 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) != 8 THEN NULL
            ELSE TO_DATE(CAST(sls_order_dt AS VARCHAR), 'YYYYMMDD')
        END AS sls_order_dt,
        CASE 
            WHEN sls_ship_dt = 0 
                 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) != 8 THEN NULL
            ELSE TO_DATE(CAST(sls_ship_dt AS VARCHAR), 'YYYYMMDD')
        END AS sls_ship_dt,
        CASE 
            WHEN sls_due_dt = 0 
                 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) != 8 THEN NULL
            ELSE TO_DATE(CAST(sls_due_dt AS VARCHAR), 'YYYYMMDD')
        END AS sls_due_dt,
        CASE
            WHEN sls_sales IS NULL THEN ABS(sls_quantity) * ABS(COALESCE(sls_price,0)) 
            WHEN sls_sales < 0 THEN ABS(sls_sales)
            WHEN sls_sales = 0 THEN ABS(sls_quantity) * ABS(COALESCE(sls_price,0)) 
            WHEN ABS(sls_quantity) * ABS(sls_price) != sls_sales THEN ABS(sls_quantity) * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,
        ABS(sls_quantity) AS sls_quantity,
        CASE 
            WHEN sls_price < 0 THEN ABS(sls_price)
            WHEN sls_price = 0 THEN NULLIF(sls_price, 0)
            WHEN sls_price IS NULL THEN CASE WHEN sls_quantity = 0 THEN NULL ELSE sls_sales / ABS(sls_quantity) END
            ELSE sls_price
        END AS sls_price
    FROM 
        bronze.crm_sales_details
    WHERE
        (sls_order_dt IS NULL OR sls_ship_dt IS NULL OR sls_due_dt IS NULL) -- keep a defensive check to avoid wrong comparisons
        OR ( (sls_order_dt <= sls_ship_dt OR sls_ship_dt IS NULL) 
             AND (sls_order_dt <= sls_due_dt OR sls_due_dt IS NULL)
             AND sls_quantity != 0
             AND COALESCE(sls_price,0) != 0
           );

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'crm_sales_details: % rows affected', rows_count;

    end_time := clock_timestamp();
    interval_diff := end_time - start_time;
    total_ms := floor(EXTRACT(EPOCH FROM interval_diff) * 1000)::bigint;
    hours := (total_ms / 3600000)::int;
    minutes := ((total_ms % 3600000) / 60000)::int;
    seconds := ((total_ms % 60000) / 1000)::int;
    milliseconds := (total_ms % 1000)::int;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '----------';
    RAISE NOTICE '';

    --------------------------------------------------------------------------
    -- ERP: erp_cust_az12
    --------------------------------------------------------------------------
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '------------- Loading ERP Tables ------------------';
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '';

    RAISE NOTICE '----------';
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncate Table and insert into silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > TO_DATE('2025-03-02', 'YYYY-MM-DD') THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'Unknown'
        END AS gen
    FROM 
        bronze.erp_cust_az12;

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'erp_cust_az12: % rows affected', rows_count;

    end_time := clock_timestamp();
    interval_diff := end_time - start_time;
    total_ms := floor(EXTRACT(EPOCH FROM interval_diff) * 1000)::bigint;
    hours := (total_ms / 3600000)::int;
    minutes := ((total_ms % 3600000) / 60000)::int;
    seconds := ((total_ms % 60000) / 1000)::int;
    milliseconds := (total_ms % 1000)::int;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '----------';
    RAISE NOTICE '';

    --------------------------------------------------------------------------
    -- ERP: erp_loc_a101
    --------------------------------------------------------------------------
    RAISE NOTICE '----------';
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncate Table and insert into silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN UPPER(TRIM(cntry)) IN ('USA', 'US', 'UNITED STATES') THEN 'United States'
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN 'Unknown'
            ELSE cntry
        END AS cntry
    FROM
        bronze.erp_loc_a101;

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'erp_loc_a101: % rows affected', rows_count;

    end_time := clock_timestamp();
    interval_diff := end_time - start_time;
    total_ms := floor(EXTRACT(EPOCH FROM interval_diff) * 1000)::bigint;
    hours := (total_ms / 3600000)::int;
    minutes := ((total_ms % 3600000) / 60000)::int;
    seconds := ((total_ms % 60000) / 1000)::int;
    milliseconds := (total_ms % 1000)::int;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '----------';
    RAISE NOTICE '';

    --------------------------------------------------------------------------
    -- ERP: erp_px_cat_g1v2
    --------------------------------------------------------------------------
    RAISE NOTICE '----------';
    start_time := clock_timestamp();

    RAISE NOTICE '>> Truncate Table and insert into silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM
        bronze.erp_px_cat_g1v2;

    GET DIAGNOSTICS rows_count = ROW_COUNT;
    RAISE NOTICE 'erp_px_cat_g1v2: % rows affected', rows_count;

    end_time := clock_timestamp();
    interval_diff := end_time - start_time;
    total_ms := floor(EXTRACT(EPOCH FROM interval_diff) * 1000)::bigint;
    hours := (total_ms / 3600000)::int;
    minutes := ((total_ms % 3600000) / 60000)::int;
    seconds := ((total_ms % 60000) / 1000)::int;
    milliseconds := (total_ms % 1000)::int;
    RAISE NOTICE 'Load Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '----------';
    RAISE NOTICE '';

    -- Completed
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'silver.load_silver procedure completed';
    batch_end_time := clock_timestamp();
    interval_diff := batch_end_time - batch_start_time;
    total_ms := floor(EXTRACT(EPOCH FROM interval_diff) * 1000)::bigint;
    hours := (total_ms / 3600000)::int;
    minutes := ((total_ms % 3600000) / 60000)::int;
    seconds := ((total_ms % 60000) / 1000)::int;
    milliseconds := (total_ms % 1000)::int;
    RAISE NOTICE 'Silver Layer Loading Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE '--- ERROR OCCURRED DURING LOADING SILVER LAYER -----';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
        RAISE NOTICE 'Error Detail: %', COALESCE(PG_EXCEPTION_DETAIL, 'N/A');
        RAISE NOTICE 'Error Hint: %', COALESCE(PG_EXCEPTION_HINT, 'N/A');
        RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE '';
        -- Re-raise so the caller (psql/pgAdmin) sees the error as well
        RAISE;
END;
$BODY$;

BEGIN;
CALL silver.load_silver();
-- if everything ok COMMIT; on error ROLLBACK;
COMMIT;

SELECT * FROM silver.crm_cust_info LIMIT 10;
