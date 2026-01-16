------------------------------------------------------------
-- 03. GOLD LAYER (STAR SCHEMA)
-- The Gold layer is used for reporting and dashboards.
-- We create dimension tables and a fact table.
-- These tables are clean, simple, and optimized for BI tools.
------------------------------------------------------------

USE DATABASE PACIFIC_DB;
USE SCHEMA PACIFIC_SCHEMA;

------------------------------------------------------------
-- 1. DIM_CUSTOMER
-- This table stores customer information.
-- It is a lookup table for customer details.
------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_CUSTOMER AS
SELECT
    col1 AS customer_id,
    col2 AS first_name,
    col3 AS last_name,
    col4 AS email,
    col5 AS phone,
    col6 AS address,
    col7 AS city,
    col8 AS state,
    col9 AS country
FROM CUSTOMER_TABLE;

-- Optional check
SELECT * FROM DIM_CUSTOMER LIMIT 10;

------------------------------------------------------------
-- 2. DIM_PRODUCT
-- This table stores product details.
-- It is a lookup table for product information.
------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_PRODUCT AS
SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    rating,
    stock_quantity,
    is_active
FROM PRODUCT_CLEAN;

-- Optional check
SELECT * FROM DIM_PRODUCT LIMIT 10;

------------------------------------------------------------
-- 3. FACT_SALES
-- This is the main fact table.
-- It stores all sales transactions.
-- It links to DIM_CUSTOMER and DIM_PRODUCT using IDs.
------------------------------------------------------------

CREATE OR REPLACE TABLE FACT_SALES AS
SELECT
    o.transaction_id,
    o.transaction_date,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.total_amount,
    o.payment_method,
    o.store_type
FROM ORDER_CLEAN o;

-- Optional check
SELECT * FROM FACT_SALES LIMIT 10;

------------------------------------------------------------
-- END OF GOLD LAYER
-- Your data is now ready for dashboards and analytics.
------------------------------------------------------------
