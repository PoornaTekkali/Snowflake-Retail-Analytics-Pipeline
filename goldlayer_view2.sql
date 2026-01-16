USE DATABASE PACIFIC_DB;
USE SCHEMA PACIFIC_SCHEMA;

------------------------------------------------------------
-- GOLD VIEW 2: CUSTOMER_PRODUCT_AFFINITY_VIEW
-- This view shows customer buying behavior.
-- It tells which customer buys which product,
-- how many times they bought it,
-- and the time gap between first and last purchase.
------------------------------------------------------------

CREATE OR REPLACE VIEW CUSTOMER_PRODUCT_AFFINITY_VIEW AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.state,
    c.country,

    p.product_id,
    p.product_name,
    p.category,
    p.brand,

    COUNT(f.transaction_id) AS total_transactions,   -- number of purchases
    SUM(f.quantity) AS total_quantity,               -- total items bought
    MIN(f.transaction_date) AS first_purchase_date,  -- first time customer bought this product
    MAX(f.transaction_date) AS last_purchase_date,   -- last time customer bought this product
    DATEDIFF('day',
             MIN(f.transaction_date),
             MAX(f.transaction_date)) AS days_between_purchases

FROM FACT_SALES f
LEFT JOIN DIM_CUSTOMER c
    ON f.customer_id = c.customer_id
LEFT JOIN DIM_PRODUCT p
    ON f.product_id = p.product_id

GROUP BY
    c.customer_id, c.first_name, c.last_name, c.email,
    c.city, c.state, c.country,
    p.product_id, p.product_name, p.category, p.brand

ORDER BY
    c.customer_id,
    p.product_id;

    SELECT * FROM CUSTOMER_PRODUCT_AFFINITY_VIEW LIMIT 20;
