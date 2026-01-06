/*
======================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
======================================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
======================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @bronze_Start_Time DATETIME, @bronze_End_Time DATETIME, @start_time DATETIME, @end_time DATETIME;
	SET @bronze_Start_Time = GETDATE();
	BEGIN TRY
		PRINT '================================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================================';

		PRINT '-----------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------------';

-- bronze.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Sujoy\Documents\Projects\datasets\source_crm\cust_info.csv'
		WITH (
		-- Telling SQL the data starts from the 2nd row from the CSV
			FIRSTROW = 2,

		-- separator/delimitor
			FIELDTERMINATOR = ',',

		-- To improve the performance, telling as soon as the data is getting loded lock the table
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

-- bronze.crm_prd_info
		SET @start_time = GETDATE(); 
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Sujoy\Documents\Projects\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

-- bronze.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Sujoy\Documents\Projects\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

-- bronze.erp_loc_A101
		SET @start_time = GETDATE();
		PRINT '-----------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------------------';
	
		PRINT '>> Truncating Table: bronze.erp_loc_A101';
		TRUNCATE TABLE bronze.erp_loc_A101;
		PRINT '>> Inserting Data into: bronze.erp_loc_A101';
		BULK INSERT bronze.erp_loc_A101
		FROM 'C:\Users\Sujoy\Documents\Projects\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

-- bronze.erp_cust_AZ12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_AZ12';
		TRUNCATE TABLE bronze.erp_cust_AZ12;
		PRINT '>> Inserting Data into: bronze.erp_cust_AZ12';
		BULK INSERT bronze.erp_cust_AZ12
		FROM 'C:\Users\Sujoy\Documents\Projects\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';

-- bronze.erp_px_cat_G1V2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_G1V2';
		TRUNCATE TABLE bronze.erp_px_cat_G1V2;
		PRINT '>> Inserting Data into: bronze.erp_px_cat_G1V2';
		BULK INSERT bronze.erp_px_cat_G1V2
		FROM 'C:\Users\Sujoy\Documents\Projects\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds'
		PRINT '-----------------------------------------------------------------';
	END TRY
	BEGIN CATCH
		PRINT '============================================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '============================================================================';
	END CATCH
	SET @bronze_End_Time = GETDATE();
	PRINT '============================================================================';
	PRINT 'Loading Bronze layer is Complete';
	PRINT '>> Bronze Layer Running time :' + CAST(DATEDIFF(SECOND, @bronze_Start_Time, @bronze_End_Time)AS NVARCHAR) + ' Seconds';
	PRINT '============================================================================';
END


