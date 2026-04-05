/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/



DROP PROCEDURE datawarehouse.run_silver_layer;


DELIMITER $$

CREATE PROCEDURE run_silver_layer()
BEGIN

    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;
    
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		DECLARE err_msg TEXT;
		DECLARE err_code INT;

		-- ✅ Get error details
		GET DIAGNOSTICS CONDITION 1
			err_msg = MESSAGE_TEXT,
			err_code = MYSQL_ERRNO;

		SELECT '=====================================================';
		SELECT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
		SELECT CONCAT('Error Message: ', err_msg);
		SELECT CONCAT('Error Code: ', err_code);
		SELECT '=====================================================';
	END;
    
    
    
    SET batch_start_time = CURRENT_TIMESTAMP;
     
	SELECT "==============================================================";
    SELECT 'Loading Silver Level';
    SELECT "==============================================================";
    
    SELECT '---------------------------------------------------------------';
    SELECT 'Loading CRM Table';
    SELECT '---------------------------------------------------------------';
    
    -- --------------------------------
    -- Loading silver_crm_cust_info
    -- --------------------------------
    
            SET start_time = CURRENT_TIMESTAMP;
			SELECT '>> Truncating Table: silver_crm_cust_info';
			TRUNCATE TABLE datawarehouse.silver_crm_cust_info;
			SELECT '>> Inserting Data Into: silver_crm_cust_info';
			INSERT INTO datawarehouse.silver_crm_cust_info (
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
					   WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					   WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					   ELSE 'n/a'
				   END AS cst_gndr,
				   CASE 
					   WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					   WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					   ELSE 'n/a'
				   END AS cst_gndr,
				   CASE 
					   WHEN TRIM(cst_create_date) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
					   THEN STR_TO_DATE(TRIM(cst_create_date), '%Y-%m-%d')
					   ELSE NULL
				   END AS cst_create_date
			FROM (
			   SELECT *,
				   ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
			   FROM datawarehouse.bronze_crm_cust_info
			) t
			WHERE t.flag_last = 1 AND t.cst_id IS NOT NULL;
            SET end_time = CURRENT_TIMESTAMP;
            SELECT CONCAT( '>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), 'Second');
            SELECT '>> -------------';



    -- ----------------------------------------
    -- Loading silver_crm_prd_info
    -- ----------------------------------------
    
            SET start_time = CURRENT_TIMESTAMP;
			SELECT '>> Truncating Table: silver_crm_prd_info';
			TRUNCATE TABLE datawarehouse.silver_crm_prd_info;
			SELECT '>> Inserting Data Into: silver_crm_prd_info';
			INSERT INTO datawarehouse.silver_crm_prd_info (
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT 
				  prd_id,
				  REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
				  SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
				  prd_nm,
				  COALESCE(prd_cost, 0) AS prd_cost,
				  CASE UPPER(TRIM(prd_line))
					   WHEN 'R' THEN 'Road'
					   WHEN 'M' THEN 'Mountain'
					   WHEN 'S' THEN 'Other Sales'
					   WHEN 'T' THEN 'Touring'
					   ELSE 'n/a'
				  END AS prd_line,
				  CAST(prd_start_dt AS DATE) AS prd_start_dt,
				  CAST(DATE_SUB(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) AS DATE) AS prd_end_dt
			FROM datawarehouse.bronze_crm_prd_info;
            SET end_time = CURRENT_TIMESTAMP;
            SELECT CONCAT( '>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), 'Second');
            SELECT '>> -------------';
     
     
      -- ----------------------------------------
    -- Loading silver_crm_sales_details
    -- ----------------------------------------
    
            SET start_time = CURRENT_TIMESTAMP;
			SELECT '>> Truncating Table: silver_crm_sales_details';
			TRUNCATE TABLE datawarehouse.silver_crm_sales_details;
			SELECT '>> Inserting Data Into: silver_crm_sales_details';
			INSERT INTO datawarehouse.silver_crm_sales_details (
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
					WHEN sls_order_dt IS NULL OR sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 
						THEN NULL
					ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
				END AS sls_order_dt,
				CASE 
					WHEN sls_ship_dt IS NULL OR sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 
						THEN NULL
					ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
				END AS sls_ship_dt,
				CASE 
					WHEN sls_due_dt IS NULL OR sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 
						THEN NULL
					ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
				END AS sls_due_dt,
				CASE 
					WHEN sls_sales IS NULL 
						 OR sls_sales <= 0 
						 OR sls_sales != sls_quantity * ABS(
							 CASE 
								 WHEN sls_price IS NULL OR sls_price <= 0
								 THEN sls_sales / NULLIF(sls_quantity, 0)
								 ELSE sls_price
							 END
						 )
					THEN sls_quantity * ABS(
							 CASE 
								 WHEN sls_price IS NULL OR sls_price <= 0
								 THEN sls_sales / NULLIF(sls_quantity, 0)
								 ELSE sls_price
							 END
						 )
					ELSE sls_sales
				END AS fixed_sales,
				sls_quantity,
				CASE 
					WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price
				END AS sls_price
			FROM datawarehouse.bronze_crm_sales_details;
            SET end_time = CURRENT_TIMESTAMP;
            SELECT CONCAT( '>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), 'Second');
            SELECT '>> -------------';
            



    SELECT '---------------------------------------------------------------';
    SELECT 'Loading ERP Table';
    SELECT '---------------------------------------------------------------';
    	
    
	-- ----------------------------------------
    -- Loading silver_erp_cust_az12
    -- ----------------------------------------
    
            SET start_time = CURRENT_TIMESTAMP;
			SELECT '>> Truncating Table: silver_erp_cust_az12';
			TRUNCATE TABLE datawarehouse.silver_erp_cust_az12;
			SELECT '>> Inserting Data Into: silver_erp_cust_az12';
			INSERT INTO datawarehouse.silver_erp_cust_az12(
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
					   WHEN bdate > CURRENT_DATE THEN NULL
					   ELSE bdate
				   END AS bdate,
				   CASE  
					  WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F', 'FEMALE') THEN 'Female'
					  WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M', 'MALE') THEN 'Male'
					  ELSE 'n/a'
				   END AS gen
			FROM datawarehouse.bronze_erp_cust_az12;			
            SET end_time = CURRENT_TIMESTAMP;
            SELECT CONCAT( '>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), 'Second');
            SELECT '>> -------------';



    
	-- ----------------------------------------
    -- Loading silver_erp_loc_a101
    -- ----------------------------------------
    
            SET start_time = CURRENT_TIMESTAMP;
			SELECT '>> Truncating Table: silver_erp_loc_a101';
			TRUNCATE TABLE datawarehouse.silver_erp_loc_a101;
			SELECT '>> Inserting Data Into: silver_erp_loc_a101';
			INSERT INTO datawarehouse.silver_erp_loc_a101(
				  cid,
				  cntry
			)
			SELECT 
				  CASE 
					  WHEN cid LIKE 'AW-%' THEN REPLACE(cid, '-', '')
					  ELSE cid
				  END AS cid,
				  CASE 
					  WHEN TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', '')) IN ('US', 'USA') THEN 'United State'
					  WHEN TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', '')) = 'DE' THEN 'Germany'
					  WHEN TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', '')) = '' OR cntry IS NULL THEN 'n/a'
					  ELSE TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''))
				  END AS cntry
			FROM datawarehouse.bronze_erp_loc_a101;			
            SET end_time = CURRENT_TIMESTAMP;
            SELECT CONCAT( '>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), 'Second');
            SELECT '>> -------------';


    
	
    -- ----------------------------------------
    -- Loading silver_erp_px_cat_g1v2
    -- ----------------------------------------
    
            SET start_time = CURRENT_TIMESTAMP;
			SELECT '>> Truncating Table: silver_erp_px_cat_g1v2';
			TRUNCATE TABLE datawarehouse.silver_erp_px_cat_g1v2;
			SELECT '>> Inserting Data Into: silver_erp_px_cat_g1v2';
			INSERT INTO datawarehouse.silver_erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance
			)
			SELECT 
				  id,
				  cat,
				  subcat,
				  CASE
					  WHEN UPPER((REPLACE(REPLACE(maintenance, '\r', ''), '\n', '')))='YES' THEN 'Yes'
					  WHEN UPPER((REPLACE(REPLACE(maintenance, '\r', ''), '\n', '')))='NO' THEN 'No'
					  ELSE 'n/a'
				  END AS maintenance
			FROM datawarehouse.bronze_erp_px_cat_g1v2;

			SELECT *
			FROM datawarehouse.bronze_erp_px_cat_g1v2;	
            SET end_time = CURRENT_TIMESTAMP;
            SELECT CONCAT( '>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), 'Second');
            SELECT '>> -------------';
            
		    SET batch_end_time = CURRENT_TIMESTAMP;
            SELECT '===============================================';
            SELECT 'Loading Silver Layer is Completed';
            SELECT CONCAT( '>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), 'Second');
            SELECT '===============================================';
  


END $$

DELIMITER ;




