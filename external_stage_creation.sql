------------------------------------------------------------
-- 01. CREATE DATABASE AND SCHEMA
-- This creates a place to store all your tables and objects.
------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS PACIFIC_DB;
USE DATABASE PACIFIC_DB;

CREATE SCHEMA IF NOT EXISTS PACIFIC_SCHEMA;
USE SCHEMA PACIFIC_SCHEMA;

------------------------------------------------------------
-- 02. CREATE EXTERNAL STAGE (ADLS STORAGE)
-- A stage is like a connection to your cloud storage.
-- Here we connect Snowflake to your Azure Data Lake folder.
-- The SAS token gives Snowflake permission to read the files.
------------------------------------------------------------
CREATE OR REPLACE STAGE adls_stage_sas
  URL='azure://pacificretailstgpp.blob.core.windows.net/landing/'
  CREDENTIALS = (
    AZURE_SAS_TOKEN='?sp=rl&st=2026-01-16T00:35:47Z&se=2026-01-16T08:50:47Z&spr=https&sv=2024-11-04&sr=c&sig=CEfCAHFBuBpEQINpW3vVWUGHQoPxUJuhWhE59x3H6cs%3D'
  );

-- This shows all files inside the stage (optional check)
LIST @adls_stage_sas;

------------------------------------------------------------
-- 03. BRONZE LAYER: RAW CUSTOMER DATA (CSV)
-- We create a raw table to store the CSV data exactly as it is.
------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER_TABLE (
  col1 STRING,
  col2 STRING,
  col3 STRING,
  col4 STRING,
  col5 STRING,
  col6 STRING,
  col7 STRING,
  col8 STRING,
  col9 STRING
);

-- Load the CSV files from ADLS into the CUSTOMER_TABLE
COPY INTO CUSTOMER_TABLE
FROM @adls_stage_sas/Customer/
FILE_FORMAT = (
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  SKIP_HEADER = 1   -- skip the header row
);

-- Quick check to confirm data loaded
SELECT * FROM CUSTOMER_TABLE LIMIT 10;

------------------------------------------------------------
-- 04. BRONZE LAYER: RAW ORDER DATA (PARQUET)
-- Parquet files are loaded into a VARIANT column.
-- VARIANT stores semi-structured data (like JSON).
------------------------------------------------------------
CREATE OR REPLACE TABLE ORDER_TABLE (
  data VARIANT
);

COPY INTO ORDER_TABLE
FROM @adls_stage_sas/Order/
FILE_FORMAT = (TYPE = 'PARQUET');

-- Quick check to see raw Parquet data
SELECT data FROM ORDER_TABLE LIMIT 10;

------------------------------------------------------------
-- 05. BRONZE LAYER: RAW PRODUCT DATA (JSON)
-- JSON files are also stored in a VARIANT column.
------------------------------------------------------------
CREATE OR REPLACE TABLE PRODUCT_TABLE (
  data VARIANT
);

COPY INTO PRODUCT_TABLE
FROM @adls_stage_sas/Product/
FILE_FORMAT = (TYPE = 'JSON');

-- Quick check to see raw JSON data
SELECT data FROM PRODUCT_TABLE LIMIT 10;
