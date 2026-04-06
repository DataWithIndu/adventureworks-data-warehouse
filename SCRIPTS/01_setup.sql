-- ============================================================
-- AdventureWorks Data Warehouse | Setup
-- Tool: SQL Server (Azure SQL Edge via Docker)
-- ============================================================
-- What this does:
--   Creates the DataWarehouse database from scratch and sets up
--   three schemas: bronze, silver, and gold.
--   Re-running this script will drop and recreate the database,
--   so all existing data will be lost. Run this only once at setup.
-- ============================================================

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
