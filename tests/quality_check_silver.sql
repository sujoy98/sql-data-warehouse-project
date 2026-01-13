/*
======================================================================
Quality Checks
======================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
======================================================================
*/


-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- Check for unwanted spaces
-- Expectation: No Result
SELECT
	px_id,
	px_subcat,
	px_maintenance
FROM bronze.erp_px_cat_G1V2
WHERE px_id != TRIM(px_id) OR px_subcat != TRIM(px_subcat) OR px_maintenance != TRIM(px_maintenance)

-- Check for NULLs or Negative Numbers
-- Expectation: No Result
SELECT
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT px_maintenance
FROM bronze.erp_px_cat_G1V2

-- Check for Indalid Date Orders
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Identify Out-of-Range Dated
SELECT DISTINCT
	cst_bdate
FROM silver.erp_cust_AZ12
WHERE cst_bdate < '1920-01-01' OR cst_bdate > GETDATE()

-- 
SELECT
	NULLIF(sls_due_dt, 0) AS [sls_order_dt]
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
	OR LEN(sls_due_dt) != 8
	OR sls_due_dt > 20500101
	OR sls_due_dt	< 19990101
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- Check for unwanted spaces
-- Expectation: No Result
SELECT
	px_id,
	px_subcat,
	px_maintenance
FROM bronze.erp_px_cat_G1V2
WHERE px_id != TRIM(px_id) OR px_subcat != TRIM(px_subcat) OR px_maintenance != TRIM(px_maintenance)

-- Check for NULLs or Negative Numbers
-- Expectation: No Result
SELECT
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT px_maintenance
FROM bronze.erp_px_cat_G1V2

-- Check for Indalid Date Orders
SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- Identify Out-of-Range Dated
SELECT DISTINCT
	cst_bdate
FROM silver.erp_cust_AZ12
WHERE cst_bdate < '1920-01-01' OR cst_bdate > GETDATE()

-- 
SELECT
	NULLIF(sls_due_dt, 0) AS [sls_order_dt]
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
	OR LEN(sls_due_dt) != 8
	OR sls_due_dt > 20500101
	OR sls_due_dt	< 19990101

