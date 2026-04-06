
/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


-- ===================================================================================================
-- Create Dimension : gold_dim_customers
-- ===================================================================================================

CREATE OR REPLACE VIEW gold_dim_customers AS
SELECT
      ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
      ci.cst_id AS customer_id,
      ci.cst_key AS customer_number,
      ci.cst_firstname AS first_name,
      ci.cst_lastname AS last_name,
	  la.cntry AS country,
      ci.cst_marital_status AS marital_status,
      CASE 
          WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the Master for gender info
          ELSE COALESCE(ca.gen, 'n/a')
	  END AS gender,
	  ca.bdate AS birthdate,
      ci.cst_create_date AS create_date
FROM datawarehouse.silver_crm_cust_info ci
LEFT JOIN datawarehouse.silver_erp_cust_az12 ca
        ON ci.cst_key = ca.cid
LEFT JOIN datawarehouse.silver_erp_loc_a101 la
	    ON ci.cst_key = la.cid;










-- ===================================================================================================
-- Create Dimension : gold_dim_products
-- ===================================================================================================

CREATE OR REPLACE VIEW gold_dim_products AS
SELECT 
       ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
       pn.prd_id AS product_id,
       pn.prd_key AS product_number ,
       pn.prd_nm AS product_name,
       pn.cat_id AS category_id,
       pc.cat AS category,
       pc.subcat AS subcategory,
       pc.maintenance,
       pn.prd_cost AS cost,
       pn.prd_line AS product_line,
       pn.prd_start_dt AS start_date
FROM datawarehouse.silver_crm_prd_info pn
LEFT JOIN datawarehouse.silver_erp_px_cat_g1v2 pc
       ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data










-- ===================================================================================================
-- Create Fact : gold_fact_sales
-- ===================================================================================================

CREATE OR REPLACE VIEW gold_fact_sales AS
SELECT 
      sls_ord_num AS order_number,
      pr.product_key,
      cu.customer_key,
      sls_order_dt AS orde_dater,
      sls_ship_dt AS shipping_date,
      sls_due_dt AS due_date,
      sls_sales AS sales_amount,
      sls_quantity AS quantity,
      sls_price AS price
FROM datawarehouse.silver_crm_sales_details sd
LEFT JOIN datawarehouse.gold_dim_products pr
       ON sd.sls_prd_key = pr.product_number
LEFT JOIN datawarehouse.gold_dim_customers cu
       ON sd.sls_cust_id = cu.customer_id;

