/*
======================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================================

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
    EXEC silver.load_silver;
======================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @silver_Start_Time DATETIME, @silver_End_Time DATETIME, @start_time DATETIME, @end_time DATETIME;
	SET @silver_Start_Time = GETDATE();
	BEGIN TRY
		PRINT '================================================================';
		PRINT 'Loading Silver Layer';
		PRINT '================================================================';

		PRINT '-----------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------------';
	
	-- silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting into silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS [cst_firstname],
			TRIM(cst_lastname) AS [cst_firstname],
			(CASE UPPER(TRIM(cst_material_status))
				WHEN 'M' THEN 'Married'
				WHEN 'S' THEN 'Single'
				ELSE 'N/A'
			END) AS [cst_material_status], -- Normalize maritial status valeus to readable format
			(CASE UPPER(TRIM(cst_gndr))
				WHEN 'F' THEN 'Female' 
				WHEN 'M' THEN 'Male'
				ELSE 'N/A'
			END) AS [cst_gndr],-- Normalize gender values to readable format
			cst_create_date
		FROM(
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		)T 
		WHERE flag_last = 1;-- Select the most recent record per customer
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

		--===================================================================================================
		--===================================================================================================

	-- silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting into silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
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

			-- DERIVED COLUMNS
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS [cat_id],
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS [prd_key],

			prd_nm,

			-- NULL HANDLING
			ISNULL(prd_cost, 0) AS prd_cost,

			-- DATA NORMALISATION
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
		
				-- NULL HANDLING
				ELSE 'N/A'	
			END AS [prd_line],

			CAST(prd_start_dt AS DATE) AS [prd_start_dt],

			-- DATA ENRICHMENT
			CAST(DATEADD(DAY, -1, LEAD(prd_start_dt, 1) OVER (
				PARTITION BY prd_key ORDER BY prd_start_dt))AS DATE) AS [prd_end_dt]
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';


		--===================================================================================================
		--===================================================================================================
	-- silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting into silver.sales_details'
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
	 
			 -- sls_order_dt
			 -- Handling invalid data and typecasting
			 (CASE WHEN sls_order_dt =  0 OR LEN(sls_order_dt) !=8 THEN NULL
				-- casting integer to a date "int>varchar>date"
				ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
			 END) AS [sls_order_dt],
	 
			 -- sls_ship_dt
			 (CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			 END) AS [sls_ship_dt],

			 -- sls_due_dt
			 (CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			 END) AS [sls_due_dt],
	
			-- sls_sales
					/* RULES: for sls_sales,sls_quantity,sls_price <AFTER_consulting_DATA_source>
					1. If Sales is negative, zero, or null, derive it using Quantity and Price
					2. If Price is zero or null, calculate it using Sales and Quantity
					3. If Price is negetive, convert it to a positive value
					*/
			-- Handling the missing data and invalid data by deriving a new column from the existing column
			CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != (ABS(sls_quantity) * ABS(sls_price))
				THEN (ABS(sls_quantity) * ABS(sls_price))
				ELSE sls_sales
			END AS [sls_sales],

			/* sls_sales -> similar logic without ABS() but failing in certain scenarions getting -ve values
			need to implement DQ framework as per online help
				CASE WHEN sls_sales <=0 OR sls_sales IS NULL
					THEN (sls_quantity * sls_price)
					ELSE sls_sales
				END) AS [sls_sales],
	
				CASE WHEN sls_price <=0 OR sls_price IS NULL
					THEN (sls_sales/sls_quantity)
					ELSE sls_price
				END) AS [sls_price]
			*/
	 
			 sls_quantity,
	 
			-- sls_price
			-- Handling the missing data and invalid data by deriving a new column from the existing column
			CASE WHEN sls_price IS NULL OR sls_price <=0
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS [sls_price]
		FROM bronze.crm_sales_details;
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

		--===================================================================================================
		--===================================================================================================
		PRINT '-----------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------------------';
	-- silver.erp_cust_AZ12
		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.erp_cust_AZ12';
		TRUNCATE TABLE silver.erp_cust_AZ12;
		PRINT '>> Inserting into silver.erp_cust_AZ12'
		INSERT INTO silver.erp_cust_AZ12 (
			cst_cid,
			cst_bdate,
			cst_gen
		)
		select

			-- handled invalid values
			(CASE WHEN cst_cid LIKE 'NAS%' THEN SUBSTRING(cst_cid, 4, LEN(cst_cid))
				ELSE cst_cid
			END) AS [cst_cid],

			-- handled invalid values
			-- Chekcing data quality if the date is greater than todays date, as age cant be in future
			(CASE WHEN cst_bdate > GETDATE() THEN NULL
				ELSE cst_bdate
			END) AS [cst_bdate],

			-- Data Normalisation, also handles the missing values
			(CASE 
				WHEN cst_gen = '' OR cst_gen IS NULL THEN 'N/A'
				WHEN cst_gen = 'F' OR cst_gen = 'f' THEN 'Female'
				WHEN cst_gen = 'M' OR cst_gen = 'm' THEN 'Male'
				ELSE cst_gen
			END) AS [cst_gen]
		FROM bronze.erp_cust_AZ12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

		--===================================================================================================
		--===================================================================================================
	-- silver.erp_loc_A101	
		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.erp_loc_A101';
		TRUNCATE TABLE silver.erp_loc_A101;
		PRINT '>> Inserting into silver.erp_loc_A101'
		INSERT INTO silver.erp_loc_A101 (
			loc_cid,
			loc_cntry
		)
		SELECT
			-- Removed invalid values
			REPLACE(loc_cid, '-', '') AS [loc_cid],

			-- Performed data normalisation, as well handled missing missing values
			(CASE 
				WHEN TRIM(loc_cntry) IS NULL OR loc_cntry = '' THEN 'N/A'
				WHEN TRIM(loc_cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(loc_cntry) IN ('US', 'USA')  THEN 'United States'
				WHEN TRIM(loc_cntry) = 'UK' THEN 'United Kingdom'
				ELSE TRIM(loc_cntry)
			END) AS loc_cntry
		FROM bronze.erp_loc_A101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

		--===================================================================================================
		--===================================================================================================
	-- silver.erp_px_cat_G1V2
		SET @start_time = GETDATE();
		PRINT '>> Truncating table silver.erp_px_cat_G1V2';
		TRUNCATE TABLE silver.erp_px_cat_G1V2;
		PRINT '>> Inserting into silver.erp_px_cat_G1V2'
		INSERT INTO silver.erp_px_cat_G1V2 (
			px_id,
			px_cat,
			px_subcat,
			px_maintenance
		)
		SELECT * FROM bronze.erp_px_cat_G1V2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '============================================================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================================================';
	END CATCH
	SET @silver_End_Time = GETDATE();
	PRINT '============================================================================';
	PRINT 'Loading Silver layer is Complete';
	PRINT '>> Silver Layer Running time :' + CAST(DATEDIFF(SECOND, @silver_Start_Time, @silver_End_Time)AS NVARCHAR) + ' Seconds';
	PRINT '============================================================================';
END
