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

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $BODY$
DECLARE
    batch_start_time timestamptz;
    batch_end_time timestamptz;
    start_time timestamptz;
    end_time timestamptz;
    interval_diff interval;
    total_ms bigint;
    hours int;
    minutes int;
    seconds int;
    milliseconds int;
    rows_count bigint;
BEGIN
    RAISE NOTICE '==================================================';
    RAISE NOTICE '=========== LOADING BRONZE LAYER =================';
    RAISE NOTICE '==================================================';
    RAISE NOTICE '';

    batch_start_time := clock_timestamp();
    RAISE NOTICE 'Starting bronze.load_bronze procedure';
    RAISE NOTICE '';

    ------------------------------------------------------------
    -- CRM tables
    ------------------------------------------------------------
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '------------- Loading CRM Tables ------------------';
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '';

    -- crm_cust_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_cust_info;
    EXECUTE format(
        $$COPY bronze.crm_cust_info FROM PROGRAM %L WITH (FORMAT csv, DELIMITER ',', HEADER)$$,
        'cat /Users/gangarajukonimeti/DWH/CRM/cust_info.csv'
    );
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
    RAISE NOTICE '';

    -- crm_prd_info
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_prd_info;
    EXECUTE format(
        $$COPY bronze.crm_prd_info FROM PROGRAM %L WITH (FORMAT csv, DELIMITER ',', HEADER)$$,
        'cat /Users/gangarajukonimeti/DWH/CRM/prd_info.csv'
    );
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
    RAISE NOTICE '';

    -- crm_sales_details
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.crm_sales_details;
    EXECUTE format(
        $$COPY bronze.crm_sales_details FROM PROGRAM %L WITH (FORMAT csv, DELIMITER ',', HEADER)$$,
        'cat /Users/gangarajukonimeti/DWH/CRM/sales_details.csv'
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
    RAISE NOTICE '';

    ------------------------------------------------------------
    -- ERP tables
    ------------------------------------------------------------
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '------------- Loading ERP Tables ------------------';
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '';

    -- erp_cust_az12
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_cust_az12;
    EXECUTE format(
        $$COPY bronze.erp_cust_az12 FROM PROGRAM %L WITH (FORMAT csv, DELIMITER ',', HEADER)$$,
        'cat /Users/gangarajukonimeti/DWH/ERP/CUST_AZ12.csv'
    );
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
    RAISE NOTICE '';

    -- erp_loc_a101
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_loc_a101;
    EXECUTE format(
        $$COPY bronze.erp_loc_a101 FROM PROGRAM %L WITH (FORMAT csv, DELIMITER ',', HEADER)$$,
        'cat /Users/gangarajukonimeti/DWH/ERP/LOC_A101.csv'
    );
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
    RAISE NOTICE '';

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    EXECUTE format(
        $$COPY bronze.erp_px_cat_g1v2 FROM PROGRAM %L WITH (FORMAT csv, DELIMITER ',', HEADER)$$,
        'cat /Users/gangarajukonimeti/DWH/ERP/PX_CAT_G1V2.csv'
    );
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
    RAISE NOTICE '';

    -- Completed
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE 'bronze.load_bronze procedure completed';
    batch_end_time := clock_timestamp();
    interval_diff := batch_end_time - batch_start_time;
    total_ms := floor(EXTRACT(EPOCH FROM interval_diff) * 1000)::bigint;
    hours := (total_ms / 3600000)::int;
    minutes := ((total_ms % 3600000) / 60000)::int;
    seconds := ((total_ms % 60000) / 1000)::int;
    milliseconds := (total_ms % 1000)::int;
    RAISE NOTICE 'Bronze Layer Loading Duration: % hours, % minutes, % seconds, % milliseconds', hours, minutes, seconds, milliseconds;
    RAISE NOTICE '---------------------------------------------------';
    RAISE NOTICE '';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE '---- ERROR OCCURRED DURING LOADING BRONZE LAYER -----';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
        RAISE NOTICE 'Error Detail: %', COALESCE(PG_EXCEPTION_DETAIL, 'N/A');
        RAISE NOTICE 'Error Hint: %', COALESCE(PG_EXCEPTION_HINT, 'N/A');
        RAISE NOTICE '----------------------------------------------------';
        RAISE NOTICE '';
        -- re-raise so the caller (psql/pgAdmin) also sees the error, if desired
        RAISE;
END;
$BODY$;

CALL bronze.load_bronze();

SELECT * FROM bronze.crm_cust_info LIMIT 10;
