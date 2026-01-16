------------------------------------------------------------
-- 02. SILVER LAYER TRANSFORMATIONS
-- In the Silver layer, we clean and structure the raw data.
-- We convert semi-structured data (JSON, Parquet) into
-- clean relational tables that are easy to query.
------------------------------------------------------------

USE DATABASE PACIFIC_DB;
USE SCHEMA PACIFIC_SCHEMA;

------------------------------------------------------------
-- 1. CUSTOMER TABLE (CSV)
-- The customer CSV is already clean and structured.
-- No transformation is needed here.
-- We simply keep CUSTOMER_TABLE as our Silver table.
------------------------------------------------------------

-- Optional check
SELECT * FROM CUSTOMER_TABLE LIMIT 10;

------------------------------------------------------------
-- 2. ORDER TABLE (PARQUET → CLEAN TABLE)
-- The ORDER_TABLE contains Parquet data stored in a VARIANT column.
-- We extract each field and convert it into proper data types.
-- This creates a clean, structured ORDER_CLEAN table.
------------------------------------------------------------

CREATE OR REPLACE TABLE ORDER_CLEAN AS
SELECT
    data:customer_id::NUMBER          AS customer_id,
    data:payment_method::STRING       AS payment_method,
    data:product_id::NUMBER           AS product_id,
    data:quantity::NUMBER             AS quantity,
    data:store_type::STRING           AS store_type,
    data:total_amount::FLOAT          AS total_amount,
    data:transaction_date::DATE       AS transaction_date,
    data:transaction_id::STRING       AS transaction_id
FROM ORDER_TABLE;

-- Optional check
SELECT * FROM ORDER_CLEAN LIMIT 10;

------------------------------------------------------------
-- 3. PRODUCT TABLE (JSON ARRAY → CLEAN TABLE)
-- The PRODUCT_TABLE contains JSON inside an array.
-- We use LATERAL FLATTEN to extract each product object.
-- Then we convert each field into proper data types.
------------------------------------------------------------

CREATE OR REPLACE TABLE PRODUCT_CLEAN AS
SELECT
    value:product_id::NUMBER        AS product_id,
    value:name::STRING              AS product_name,
    value:category::STRING          AS category,
    value:brand::STRING             AS brand,
    value:price::FLOAT              AS price,
    value:rating::FLOAT             AS rating,
    value:stock_quantity::NUMBER    AS stock_quantity,
    value:is_active::BOOLEAN        AS is_active
FROM PRODUCT_TABLE,
LATERAL FLATTEN(input => data);

-- Optional check
SELECT * FROM PRODUCT_CLEAN LIMIT 10;

------------------------------------------------------------
-- END OF SILVER LAYER
-- At this point, all raw Bronze data is cleaned and structured.
-- Next step: Gold layer (star schema).
------------------------------------------------------------
