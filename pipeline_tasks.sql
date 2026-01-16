------------------------------------------------------------
-- 04. PIPELINE AUTOMATION (STREAMS + MERGE + TASKS)
-- This file creates the automation layer.
-- Streams track new or changed data in Bronze tables.
-- MERGE statements update Silver tables.
-- Tasks run the MERGE automatically every morning.
------------------------------------------------------------

USE DATABASE PACIFIC_DB;
USE SCHEMA PACIFIC_SCHEMA;

------------------------------------------------------------
-- 1. CREATE STREAMS
-- Streams watch the Bronze tables and capture new or changed rows.
------------------------------------------------------------

-- Stream for Customer CSV data
CREATE OR REPLACE STREAM CUSTOMER_STREAM
ON TABLE CUSTOMER_TABLE;

-- Stream for Order Parquet data
CREATE OR REPLACE STREAM ORDER_STREAM
ON TABLE ORDER_TABLE;

-- Stream for Product JSON data
CREATE OR REPLACE STREAM PRODUCT_STREAM
ON TABLE PRODUCT_TABLE;

------------------------------------------------------------
-- 2. MERGE STATEMENTS (MANUAL FIRST RUN)
-- These MERGE commands update the Silver tables using the streams.
-- Run each MERGE once manually before enabling the tasks.
------------------------------------------------------------

-------------------------
-- CUSTOMER MERGE
-------------------------
MERGE INTO CUSTOMER_TABLE t
USING CUSTOMER_STREAM s
ON t.col1 = s.col1
WHEN MATCHED THEN UPDATE SET
    t.col2 = s.col2,
    t.col3 = s.col3,
    t.col4 = s.col4,
    t.col5 = s.col5,
    t.col6 = s.col6,
    t.col7 = s.col7,
    t.col8 = s.col8,
    t.col9 = s.col9
WHEN NOT MATCHED THEN INSERT VALUES
    (s.col1, s.col2, s.col3, s.col4, s.col5, s.col6, s.col7, s.col8, s.col9);

-------------------------
-- ORDER MERGE
-------------------------
MERGE INTO ORDER_CLEAN t
USING (
    SELECT
        data:transaction_id::STRING AS transaction_id,
        data:transaction_date::DATE AS transaction_date,
        data:customer_id::NUMBER AS customer_id,
        data:product_id::NUMBER AS product_id,
        data:quantity::NUMBER AS quantity,
        data:total_amount::FLOAT AS total_amount,
        data:payment_method::STRING AS payment_method,
        data:store_type::STRING AS store_type
    FROM ORDER_STREAM
) s
ON t.transaction_id = s.transaction_id
WHEN MATCHED THEN UPDATE SET
    t.transaction_date = s.transaction_date,
    t.customer_id = s.customer_id,
    t.product_id = s.product_id,
    t.quantity = s.quantity,
    t.total_amount = s.total_amount,
    t.payment_method = s.payment_method,
    t.store_type = s.store_type
WHEN NOT MATCHED THEN INSERT VALUES
    (s.transaction_id, s.transaction_date, s.customer_id, s.product_id, s.quantity, s.total_amount, s.payment_method, s.store_type);

-------------------------
-- PRODUCT MERGE
-------------------------
MERGE INTO PRODUCT_CLEAN t
USING (
    SELECT
        value:product_id::NUMBER AS product_id,
        value:name::STRING AS product_name,
        value:category::STRING AS category,
        value:brand::STRING AS brand,
        value:price::FLOAT AS price,
        value:rating::FLOAT AS rating,
        value:stock_quantity::NUMBER AS stock_quantity,
        value:is_active::BOOLEAN AS is_active
    FROM PRODUCT_STREAM,
    LATERAL FLATTEN(input => data)
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
    t.product_name = s.product_name,
    t.category = s.category,
    t.brand = s.brand,
    t.price = s.price,
    t.rating = s.rating,
    t.stock_quantity = s.stock_quantity,
    t.is_active = s.is_active
WHEN NOT MATCHED THEN INSERT VALUES
    (s.product_id, s.product_name, s.category, s.brand, s.price, s.rating, s.stock_quantity, s.is_active);

------------------------------------------------------------
-- 3. CREATE TASKS (AUTOMATION)
-- Tasks run the MERGE statements automatically every morning.
------------------------------------------------------------

-------------------------
-- CUSTOMER TASK (4 AM)
-------------------------
CREATE OR REPLACE TASK CUSTOMER_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 4 * * * America/Chicago'
AS
MERGE INTO CUSTOMER_TABLE t
USING CUSTOMER_STREAM s
ON t.col1 = s.col1
WHEN MATCHED THEN UPDATE SET
    t.col2 = s.col2,
    t.col3 = s.col3,
    t.col4 = s.col4,
    t.col5 = s.col5,
    t.col6 = s.col6,
    t.col7 = s.col7,
    t.col8 = s.col8,
    t.col9 = s.col9
WHEN NOT MATCHED THEN INSERT VALUES
    (s.col1, s.col2, s.col3, s.col4, s.col5, s.col6, s.col7, s.col8, s.col9);

-------------------------
-- ORDER TASK (5 AM)
-------------------------
CREATE OR REPLACE TASK ORDER_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 5 * * * America/Chicago'
AS
MERGE INTO ORDER_CLEAN t
USING (
    SELECT
        data:transaction_id::STRING AS transaction_id,
        data:transaction_date::DATE AS transaction_date,
        data:customer_id::NUMBER AS customer_id,
        data:product_id::NUMBER AS product_id,
        data:quantity::NUMBER AS quantity,
        data:total_amount::FLOAT AS total_amount,
        data:payment_method::STRING AS payment_method,
        data:store_type::STRING AS store_type
    FROM ORDER_STREAM
) s
ON t.transaction_id = s.transaction_id
WHEN MATCHED THEN UPDATE SET
    t.transaction_date = s.transaction_date,
    t.customer_id = s.customer_id,
    t.product_id = s.product_id,
    t.quantity = s.quantity,
    t.total_amount = s.total_amount,
    t.payment_method = s.payment_method,
    t.store_type = s.store_type
WHEN NOT MATCHED THEN INSERT VALUES
    (s.transaction_id, s.transaction_date, s.customer_id, s.product_id, s.quantity, s.total_amount, s.payment_method, s.store_type);

-------------------------
-- PRODUCT TASK (4 AM)
-------------------------
CREATE OR REPLACE TASK PRODUCT_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 0 4 * * * America/Chicago'
AS
MERGE INTO PRODUCT_CLEAN t
USING (
    SELECT
        value:product_id::NUMBER AS product_id,
        value:name::STRING AS product_name,
        value:category::STRING AS category,
        value:brand::STRING AS brand,
        value:price::FLOAT AS price,
        value:rating::FLOAT AS rating,
        value:stock_quantity::NUMBER AS stock_quantity,
        value:is_active::BOOLEAN AS is_active
    FROM PRODUCT_STREAM,
    LATERAL FLATTEN(input => data)
) s
ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
    t.product_name = s.product_name,
    t.category = s.category,
    t.brand = s.brand,
    t.price = s.price,
    t.rating = s.rating,
    t.stock_quantity = s.stock_quantity,
    t.is_active = s.is_active
WHEN NOT MATCHED THEN INSERT VALUES
    (s.product_id, s.product_name, s.category, s.brand, s.price, s.rating, s.stock_quantity, s.is_active);

------------------------------------------------------------
-- 4. ACTIVATE ALL TASKS
------------------------------------------------------------
ALTER TASK CUSTOMER_TASK RESUME;
ALTER TASK ORDER_TASK RESUME;
ALTER TASK PRODUCT_TASK RESUME;
