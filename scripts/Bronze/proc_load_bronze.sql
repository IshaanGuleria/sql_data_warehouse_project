/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================*/
create or alter procedure bronze.load_bronze as
begin 

	begin try
	BEGIN TRANSACTION;

	IF OBJECT_ID('bronze.crm_cust_info') IS NOT NULL
		Truncate table bronze.crm_cust_info;

			PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables crm_cust_info';
			PRINT '------------------------------------------------';

	bulk insert bronze.crm_cust_info from 
	'C:\Users\ESHAAN\OneDrive\Desktop\project_1\sql_data_warehouse_project\datasets\source_crm\cust_info.csv'
	with(
	Firstrow = 2,
	fieldterminator=',',
	tablock)


	IF OBJECT_ID('bronze.crm_prd_info') IS NOT NULL
		Truncate table bronze.crm_prd_info;
	
			PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables crm_prd_info';
			PRINT '------------------------------------------------';

	bulk insert bronze.crm_prd_info from 
	'C:\Users\ESHAAN\OneDrive\Desktop\project_1\sql_data_warehouse_project\datasets\source_crm\prd_info.csv'
	with(
	Firstrow = 2,
	fieldterminator=',',
	tablock)
	
	IF OBJECT_ID('bronze.crm_sales_details') IS NOT NULL
		Truncate table bronze.crm_sales_details;
	
		    PRINT '------------------------------------------------';
			PRINT 'Loading CRM Tables crm_sales_details';
			PRINT '------------------------------------------------';
	
	bulk insert bronze.crm_sales_details from 
	'C:\Users\ESHAAN\OneDrive\Desktop\project_1\sql_data_warehouse_project\datasets\source_crm\sales_details.csv'
	with(
	Firstrow = 2,
	fieldterminator=',',
	tablock)
	

			PRINT '************************************************';
			PRINT 'Loading erp Tables';
			PRINT '************************************************';


	IF OBJECT_ID('bronze.erp_cust_az12') IS NOT NULL
		Truncate table bronze.erp_cust_az12;
	
			PRINT '------------------------------------------------';
			PRINT 'Loading erp Tables erp_cust_az12 ';
			PRINT '------------------------------------------------';	
		

	bulk insert bronze.erp_cust_az12 from 
	'C:\Users\ESHAAN\OneDrive\Desktop\project_1\sql_data_warehouse_project\datasets\source_erp\CUST_AZ12.csv'
	with(
	Firstrow = 2,
	fieldterminator=',',
	tablock)

	IF OBJECT_ID('bronze.erp_loc_a101') IS NOT NULL
			Truncate table bronze.erp_loc_a101;
			
			PRINT '------------------------------------------------';
			PRINT 'Loading erp Tables erp_loc_a101';
			PRINT '------------------------------------------------';

	bulk insert bronze.erp_loc_a101 from 
	'C:\Users\ESHAAN\OneDrive\Desktop\project_1\sql_data_warehouse_project\datasets\source_erp\LOC_A101.csv'
	with(
	Firstrow = 2,
	fieldterminator=',',
	tablock)
	IF OBJECT_ID('bronze.erp_px_cat_g1v2') IS NOT NULL
		Truncate table bronze.erp_px_cat_g1v2;
			
			PRINT '------------------------------------------------';
			PRINT 'Loading erp Tables erp_px_cat_g1v2';
			PRINT '------------------------------------------------';
			
	bulk insert bronze.erp_px_cat_g1v2 from 
	'C:\Users\ESHAAN\OneDrive\Desktop\project_1\sql_data_warehouse_project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
	Firstrow = 2,
	fieldterminator=',',
	tablock)
	COMMIT TRANSACTION;
	print '========================'
	print 'Data loaded successfully'
	print '========================'

	end try
	begin catch
	ROLLBACK TRANSACTION;
	print 'Error Occured during loading the bronze layer'
	end catch
end
