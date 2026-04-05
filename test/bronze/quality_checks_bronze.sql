


-- =====================================================
-- Find Quality Issue in Bronze Layer
-- =====================================================



-- =========
-- CRM
-- =========





-- =================================
-- Table : bronze_crm_cust_info
-- =================================

-- -------------------------------------------------
-- Check For Nulls and Duplicate in Primary Key
-- Expectation: No Result
-- -------------------------------------------------
SELECT
     cst_id,
     COUNT(*) AS no_count
FROM datawarehouse.bronze_crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-----------------------------
-- Check For Unwanted Space
-- Expectation: No Results
-----------------------------
SELECT
      cst_id,
      cst_key,
      cst_firstname,
      cst_lastname,
      cst_marital_status,
      cst_gndr,
      cst_create_date
FROM datawarehouse.bronze_crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);
-- chech one by one for every column



----------------------------------
-- Check Null OR Negative Number
-- Expectation: No Results
----------------------------------
SELECT prd_cost
FROM datawarehouse.bronze_crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

----------------------------------------
-- Data Standerization and Consistency
----------------------------------------
SELECT DISTINCT cst_gndr
FROM datawarehouse.bronze_crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM datawarehouse.bronze_crm_cust_info;







-- =================================
-- Table : bronze_crm_prd_info
-- =================================

-- -------------------------------------------------
-- Check For Nulls and Duplicate in Primary Key
-- Expectation: No Result
-- -------------------------------------------------
SELECT 
       prd_id,
       COUNT(*) AS no_count
FROM datawarehouse.bronze_crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-----------------------------
-- Check For Unwanted Space
-- Expectation: No Results
-----------------------------
SELECT 
       prd_id,
       prd_key,
       prd_nm,
       prd_line,
       prd_start_dt,
       prd_end_dt
FROM datawarehouse.bronze_crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
-- chech one by one for every column

----------------------------------------
-- Data Standerization and Consistency
----------------------------------------
SELECT DISTINCT prd_line
FROM datawarehouse.bronze_crm_prd_info;

----------------------------------
-- Check for invalid date order
----------------------------------
SELECT *,
       DATE_SUB(LEAD(prd_start_dt) OVER w, INTERVAL 1 DAY) AS prd_end_dt_test
FROM datawarehouse.bronze_crm_prd_info
WHERE prd_end_dt < prd_start_dt
WINDOW w AS (PARTITION BY prd_key ORDER BY prd_start_dt);









-- ===============================================
-- check problem in bronze_crm_sales_details
-- ===============================================
SELECT 
	  sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
FROM datawarehouse.bronze_crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM datawarehouse.silver_crm_cust_info);

SELECT 
	  sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
FROM datawarehouse.bronze_crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM datawarehouse.silver_crm_prd_info);


SELECT 
	  sls_ord_num,
      sls_prd_key,
      sls_cust_id,
      sls_order_dt,
      sls_ship_dt,
      sls_due_dt,
      sls_sales,
      sls_quantity,
      sls_price
FROM datawarehouse.bronze_crm_sales_details;

SELECT cst_id FROM datawarehouse.silver_crm_cust_info;
SELECT prd_key FROM datawarehouse.silver_crm_prd_info;

-------------------------
-- Check for invalid date
--------------------------
SELECT 
      NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM datawarehouse.bronze_crm_sales_details
WHERE sls_order_dt = 0
      OR LENGTH(sls_order_dt) != 8
      OR sls_order_dt > 20500101
      OR sls_order_dt < 19000101;

SELECT 
      NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM datawarehouse.bronze_crm_sales_details
WHERE sls_ship_dt = 0
      OR LENGTH(sls_ship_dt) != 8
      OR sls_ship_dt > 20500101
      OR sls_ship_dt < 19000101;
      
SELECT 
      NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM datawarehouse.bronze_crm_sales_details
WHERE sls_due_dt = 0
      OR LENGTH(sls_due_dt) != 8
      OR sls_due_dt > 20500101
      OR sls_due_dt < 19000101;
      
----------------------------------
-- Check for invalid date order
----------------------------------
SELECT *
FROM datawarehouse.bronze_crm_sales_details
WHERE sls_order_dt > sls_ship_dt
      OR sls_order_dt > sls_due_dt;

-- ----------------------------------------------------------
-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Value must not be NULL, zero, or negative.
-- --------------------------------------------------------------------
-- CONVERSION RULE:
-- If Sales is negative, or null, derive it using Quantity and Price
-- If Price is zero or null, calculate it using Sales and Quantity
-- if Price is negative, convert it to a positive values
-- --------------------------------------------------------------------

SELECT DISTINCT 
	  sls_sales AS old_sls_sales,
	  sls_quantity,
      sls_price AS old_sls_price,
      CASE 
           WHEN  sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			   THEN sls_quantity * ABS(sls_price)
	       ELSE sls_sales
	  END AS sls_sales,
      CASE 
           WHEN sls_price <= 0 OR sls_price IS NULL 
                THEN sls_sales / NULLIF(sls_quantity, 0)
		   ELSE sls_price
	  END AS sls_price 
FROM datawarehouse.bronze_crm_sales_details
WHERE sls_sales != (sls_quantity * sls_price)
      OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
      OR sls_sales <= 0 OR  sls_quantity <= 0 OR sls_price <= 0
ORDER BY old_sls_sales, sls_quantity, old_sls_price;
      
SELECT *
FROM datawarehouse.bronze_crm_sales_details;








-- =========
-- ERP
-- =========


-- ===================================================
-- check problem in bronze_erp_cust_az12
-- ===================================================
SELECT 
       CASE 
           WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
           ELSE cid
	   END AS cid,
       bdate,
       gen
FROM datawarehouse.bronze_erp_cust_az12
WHERE  CASE 
           WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
           ELSE cid
	   END  NOT IN (SELECT DISTINCT cst_key FROM datawarehouse.silver_crm_cust_info);

SELECT *
FROM datawarehouse.silver_crm_cust_info;

-- -------------------------------
-- Identify out of range date
-- -------------------------------
SELECT bdate
FROM datawarehouse.bronze_erp_cust_az12
WHERE bdate < '1910-01-01' OR bdate > CURRENT_DATE;

-- -----------------------------------------
-- Data Standerrization and Consistency
-- -----------------------------------------
SELECT DISTINCT TRIM(gen)
FROM datawarehouse.bronze_erp_cust_az12;

-- 'M\r' ≠ 'M'
-- 'Female\n' ≠ 'FEMALE'

SELECT DISTINCT
      gen AS old_gen, 
      CASE  
          WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F', 'FEMALE') THEN 'Female'
          WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M', 'MALE') THEN 'Male'
          ELSE 'n/a'
	  END AS gen
FROM datawarehouse.bronze_erp_cust_az12;




-- ===================================================
-- check problem in bronze_erp_loc_a101
-- ===================================================
SELECT *
FROM datawarehouse.bronze_erp_loc_a101
WHERE cid NOT IN (SELECT DISTINCT cst_key FROM datawarehouse.silver_crm_cust_info);

SELECT DISTINCT cst_key FROM datawarehouse.silver_crm_cust_info;

SELECT 
      CASE 
          WHEN cid LIKE 'AW-%' THEN REPLACE(cid, '-', '')
          ELSE cid
	  END AS cid
FROM datawarehouse.bronze_erp_loc_a101
WHERE CASE 
          WHEN cid LIKE 'AW-%' THEN REPLACE(cid, '-', '')
          ELSE cid
	  END  NOT IN (SELECT DISTINCT cst_key FROM datawarehouse.silver_crm_cust_info);
      
-- ----------------------------------------
-- Data Standerization and Consistency
-- ----------------------------------------
SELECT DISTINCT cntry
FROM datawarehouse.bronze_erp_loc_a101;

SELECT DISTINCT 
      CASE 
          WHEN TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', '')) IN ('US', 'USA') THEN 'United State'
          WHEN TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', '')) = 'DE' THEN 'Germany'
          WHEN TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', '')) = '' OR cntry IS NULL THEN 'n/a'
          ELSE TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''))
	  END AS cntry
FROM datawarehouse.bronze_erp_loc_a101;








-- ===================================================
-- check problem in bronze_erp_px_cat_g1v2
-- ===================================================
SELECT id
FROM datawarehouse.bronze_erp_px_cat_g1v2
WHERE id NOT IN (SELECT DISTINCT cat_id FROM datawarehouse.silver_crm_prd_info);

-- -----------------------
-- Check Unwanted Space
-- -----------------------
SELECT id
FROM datawarehouse.bronze_erp_px_cat_g1v2
WHERE id != TRIM(id);

SELECT cat
FROM datawarehouse.bronze_erp_px_cat_g1v2
WHERE cat != TRIM(cat);


SELECT subcat
FROM datawarehouse.bronze_erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);

SELECT maintenance
FROM datawarehouse.bronze_erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance);

-- ------------------------------------------
-- Data Standerization and Consistency
-- ------------------------------------------

SELECT DISTINCT cat
FROM datawarehouse.bronze_erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM datawarehouse.bronze_erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM datawarehouse.bronze_erp_px_cat_g1v2;

SELECT DISTINCT
      CASE
          WHEN UPPER((REPLACE(REPLACE(maintenance, '\r', ''), '\n', '')))='YES' THEN 'Yes'
          WHEN UPPER((REPLACE(REPLACE(maintenance, '\r', ''), '\n', '')))='NO' THEN 'No'
          ELSE 'n/a'
	  END AS maintenance
FROM datawarehouse.bronze_erp_px_cat_g1v2;

