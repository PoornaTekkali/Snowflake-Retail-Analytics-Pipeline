-- Set the correct database and schema
USE DATABASE PACIFIC_DB;
USE SCHEMA PACIFIC_SCHEMA;

------------------------------------------------------------
-- GOLD VIEW: DAILY_SALES_VIEW
-- This view summarizes sales for each day.
-- It shows daily revenue, quantity sold, number of orders,
-- and average order value.
------------------------------------------------------------

CREATE OR REPLACE VIEW DAILY_SALES_VIEW AS
SELECT
    transaction_date,
    SUM(total_amount) AS daily_revenue,          -- total money earned that day
    SUM(quantity) AS total_quantity_sold,        -- total items sold
    COUNT(transaction_id) AS total_transactions, -- number of orders
    AVG(total_amount) AS avg_order_value         -- average order size
FROM FACT_SALES
GROUP BY transaction_date
ORDER BY transaction_date;

SELECT * FROM DAILY_SALES_VIEW LIMIT 10;
