
-- 🚀 LOAD DATA INFILE — Keyword Explanation (Short Notes)

-- 🔹 MAIN COMMAND
-- LOAD DATA INFILE 'file_path'
-- 👉 Bulk loads CSV file into MySQL table (FASTEST method)

-- 🔹 TARGET TABLE
-- INTO TABLE bronze_crm_cust_info
-- 👉 Specifies where data will be loaded

-- 🔹 FILE FORMAT
-- FIELDS TERMINATED BY ','
-- 👉 Column separator (comma-separated values)

-- ENCLOSED BY '"'
-- 👉 Handles values inside quotes ("John Doe")

-- LINES TERMINATED BY '\n'
-- 👉 Each row ends with newline

-- IGNORE 1 ROWS
-- 👉 Skips header row (column names)

-- 🔹 USER VARIABLES (MOST IMPORTANT)
-- (
--  @cst_id,
--  @cst_key,
--  @cst_firstname,
--  @cst_lastname,
--  @cst_marital_status,
--  @cst_gndr,
--  @cst_create_date,
--  @extra
-- )

-- 👉 Reads CSV data into TEMP variables (not directly into table)

-- WHY IMPORTANT?
-- - Prevents errors from dirty data
-- - Handles missing values
-- - Avoids datatype issues

-- 🔹 EXTRA VARIABLE (🔥 KEY TRICK)
-- @extra

-- 👉 Absorbs:
-- - Extra columns
-- - Broken rows
-- - Column mismatch

-- 👉 Prevents:
-- Error 1261 (column count mismatch)

-- 🔹 SET CLAUSE (DATA MAPPING)
-- Maps variables → actual table columns

-- 🔹 NULLIF FUNCTION
-- NULLIF(@value, '')

-- 👉 Converts:
-- '' (empty string) → NULL

-- 👉 Prevents:
-- - datatype errors
-- - incorrect values

-- 🎯 FINAL FLOW
-- CSV File → @variables → SET mapping → Table (Bronze)

-- 🔥 KEY IDEA
-- @variables = safety layer
-- @extra = error absorber
-- SET = transformation
-- NULLIF = clean empty values

-- 🎯 INTERVIEW LINE
-- "User variables handle dirty data, @extra prevents column mismatch errors,
-- and SET clause maps and cleans data before inserting into the table."









  

/*

======================================================================================================
DDL Script: Create Bronze Tables
======================================================================================================

Script Purpose:
      This script creates tables in the 'bronze' schema, dropping existing tables if already exist. 
      Run this script to re-define the DDL structure of 'bronze' tables

=======================================================================================================

*/




	-- =========================
	-- CRM CUSTOMER INFO
	-- =========================
	TRUNCATE TABLE bronze_crm_cust_info;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/cust_info.csv'
	INTO TABLE bronze_crm_cust_info
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS
	(
	 @cst_id,
	 @cst_key,
	 @cst_firstname,
	 @cst_lastname,
	 @cst_marital_status,
	 @cst_gndr,
	 @cst_create_date,
	 @extra
	)
	SET 
	 cst_id             = NULLIF(@cst_id, ''),
	 cst_key            = NULLIF(@cst_key, ''),
	 cst_firstname      = NULLIF(@cst_firstname, ''),
	 cst_lastname       = NULLIF(@cst_lastname, ''),
	 cst_marital_status = NULLIF(@cst_marital_status, ''),
	 cst_gndr           = NULLIF(@cst_gndr, ''),
	 cst_create_date    = NULLIF(@cst_create_date, '');

	-- =========================
	-- PRODUCT INFO
	-- =========================
	TRUNCATE TABLE bronze_crm_prd_info;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/prd_info.csv'
	INTO TABLE bronze_crm_prd_info
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS
	(
	 @prd_id,
	 @prd_key,
	 @prd_nm,
	 @prd_cost,
	 @prd_line,
	 @prd_start_dt,
	 @prd_end_dt,
	 @extra
	)
	SET 
	 prd_id       = NULLIF(@prd_id, ''),
	 prd_key      = NULLIF(@prd_key, ''),
	 prd_nm       = NULLIF(@prd_nm, ''),
	 prd_cost     = NULLIF(@prd_cost, ''),
	 prd_line     = NULLIF(@prd_line, ''),
	 prd_start_dt = NULLIF(@prd_start_dt, ''),
	 prd_end_dt   = NULLIF(@prd_end_dt, '');

	-- =========================
	-- SALES DETAILS
	-- =========================
    
    
    
    			 
			-- | Problem          | Solution                                |
			-- | ---------------- | --------------------------------------- |
			-- | Extra columns    | `@extra1, @extra2, @extra3` absorb them |
			-- | Truncation error | Avoided                                 |
			-- | Dirty rows       | Still loaded                            |
			-- | Column mismatch  | Fixed                                   |
    
    
    
	TRUNCATE TABLE bronze_crm_sales_details;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_crm/sales_details.csv'
	INTO TABLE bronze_crm_sales_details
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS
	(
	 @sls_ord_num,
	 @sls_prd_key,
	 @sls_cust_id,
	 @sls_order_dt,
	 @sls_ship_dt,
	 @sls_due_dt,
	 @sls_sales,
	 @extra1,
	 @extra2,
	 @extra3
	)
	SET 
	 sls_ord_num = NULLIF(@sls_ord_num, ''),
	 sls_prd_key = NULLIF(@sls_prd_key, ''),
	 sls_cust_id = NULLIF(@sls_cust_id, ''),
	 sls_order_dt = NULLIF(@sls_order_dt, ''),
	 sls_ship_dt = NULLIF(@sls_ship_dt, ''),
	 sls_due_dt = NULLIF(@sls_due_dt, ''),
	 sls_sales   = NULLIF(@sls_sales, '');

	-- =========================
	-- ERP CUSTOMER AZ12
	-- =========================
	TRUNCATE TABLE bronze_erp_cust_az12;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/CUST_AZ12.csv'
	INTO TABLE bronze_erp_cust_az12
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS
	(
	 @cid,
	 @bdate,
	 @gen,
	 @extra
	)
	SET 
	 cid   = NULLIF(@cid, ''),
	 bdate = NULLIF(@bdate, ''),
	 gen   = NULLIF(@gen, '');

	-- =========================
	-- ERP LOCATION
	-- =========================
	TRUNCATE TABLE bronze_erp_loc_a101;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/LOC_A101.csv'
	INTO TABLE bronze_erp_loc_a101
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS
	(
	 @cid,
	 @cntry,
	 @extra
	)
	SET 
	 cid    = NULLIF(@cid, ''),
	 cntry  = NULLIF(@cntry, '');

	-- =========================
	-- ERP PRODUCT CATEGORY
	-- =========================
	TRUNCATE TABLE bronze_erp_px_cat_g1v2;

	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/source_erp/PX_CAT_G1V2.csv'
	INTO TABLE bronze_erp_px_cat_g1v2
	FIELDS TERMINATED BY ','
	ENCLOSED BY '"'
	LINES TERMINATED BY '\n'
	IGNORE 1 ROWS
	(
	 @id,
	 @cat,
	 @subcat,
	 @maintenance,
	 @extra
	)
	SET 
	 id          = NULLIF(@id, ''),
	 cat         = NULLIF(@cat, ''),
	 subcat      = NULLIF(@subcat, ''),
	 maintenance = NULLIF(@maintenance, '');







DROP PROCEDURE IF EXISTS run_bronze_layer;

DELIMITER $$

CREATE PROCEDURE run_bronze_layer()
BEGIN

    DECLARE err_msg TEXT;
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- Get actual error message
        GET DIAGNOSTICS CONDITION 1
            err_msg = MESSAGE_TEXT;

        -- Print error
        SELECT err_msg;
    END;



    SET start_time = NOW();
    SELECT '=================================';
    SELECT 'Bronze load started' AS status;
    SELECT '=================================';
     
	SELECT '---------------------------';
	SELECT 'Loading CRM table' AS status;
    SELECT '---------------------------';
	SELECT COUNT(*) FROM bronze_crm_cust_info;
    SELECT COUNT(*) FROM bronze_crm_prd_info;
    SELECT COUNT(*) FROM bronze_crm_sales_details;
    
	SELECT '---------------------------';
	SELECT 'Loading ERP table' AS status;
    SELECT '---------------------------';
    SELECT COUNT(*) FROM bronze_erp_cust_az12;
    SELECT COUNT(*) FROM bronze_erp_loc_a101;
    SELECT COUNT(*) FROM bronze_erp_px_cat_g1v2;

    
    
	-- =========================
    -- END TIME
    -- =========================
    SET end_time = NOW();

    SELECT '=================================' AS line;
    SELECT 'Bronze load completed' AS status;
    SELECT end_time AS end_time;
    SELECT TIMESTAMPDIFF(SECOND, start_time, end_time) AS duration_seconds;
    SELECT '=================================' AS line;


    
END $$

DELIMITER ;














