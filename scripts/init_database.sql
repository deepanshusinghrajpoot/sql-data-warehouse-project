
/*

===============================================================================
Create Database and Schemas
===============================================================================

Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated.


*/


-- Drop and recreate the 'DataWarehouse' database
CREATE DATABASE IF NOT EXISTS DataWarehouse;
