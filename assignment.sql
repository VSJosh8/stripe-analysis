----- *** CREATE TABLES FROM RAW CSV *** -----
--CREATE TABLE customer (
--    USER_ID TEXT,
--    customer_id TEXT,
--    CREATED TEXT,
--    signup_time TEXT,
--    last_login TEXT,
--    email TEXT,
--    born_year TEXT,
--    gender TEXT,
--    country TEXT,
--    platform TEXT
--);
--
--CREATE TABLE charge (
--    customer_id TEXT,
--    payment_id TEXT,
--    brand TEXT,
--    funding TEXT,
--    wallet_type TEXT,
--    id TEXT,
--    created TEXT,
--    paid TEXT,
--    amount INTEGER,
--    amount_refunded INTEGER,
--    outcome_seller_message TEXT
--);

----- *** LIST TABLES *** -----
.tables

----- *** IMPORT CSV TO TABLE *** -----
.import --skip 1 /Users/vincentjoshuaskinner/Documents/Jobs/Assignments/Blidz/data_file_Q1/core_user_Product_Manager_Q1.csv customer
.import --skip 1 /Users/vincentjoshuaskinner/Documents/Jobs/Assignments/Blidz/data_file_Q1/stripe_data_charge_Product_Manager_Q1.csv charge

----- *** SET TABLE LAYOUT *** -----
.mode csv


--SELECT * FROM customer;
--SELECT * FROM charge;

-- DATA CLEANUP --
--CREATE TABLE customer_clean AS
--SELECT * FROM customer
--WHERE customer_id IS NOT NULL AND customer_id != ''
--GROUP BY customer_id;


-- * UNIQUE CHARGE --
--CREATE TABLE charge_clean AS
--SELECT * FROM charge
--WHERE payment_id IS NOT NULL AND payment_id != ''
--    AND brand IS NOT NULL AND brand != ''
--    AND funding IS NOT NULL AND funding != ''
--    AND wallet_type IS NOT NULL AND wallet_type != ''
--    AND created >= '2025-10-01 00:00:00'
--    AND created <  '2025-10-02 00:00:00'
--    AND amount > 0
--GROUP BY payment_id;

------ # Transactions -----
--SELECT
--    id,
--    COUNT(*) AS transaction_attempts
--FROM charge_clean
--GROUP BY id
--ORDER BY transaction_attempts DESC;
--
--SELECT COUNT(DISTINCT id) AS total_attempted_transactions
--FROM charge_clean;

--SELECT 
--    customer_id,
--    COUNT(*) AS total_transactions,
--    SUM(CASE WHEN paid = 'TRUE' THEN 1 ELSE 0 END) AS successful_transactions,
--    SUM(amount) AS total_attempted_value,
--    SUM(CASE WHEN paid = 'TRUE' THEN amount ELSE 0 END) AS total_successful_value
--FROM charge_clean
--GROUP BY customer_id;

-- JOINING TABLES --
SELECT customer_id, COUNT(*)
FROM charge_clean
GROUP BY customer_id
ORDER BY COUNT(*) DESC;

WITH filtered_customers AS (
    SELECT
        USER_ID,
        customer_id,
        signup_time,
        TRIM(LOWER(customer_id)) AS customer_id_norm
    FROM customer_clean
    WHERE signup_time IS NOT NULL
        AND signup_time != ''
        AND signup_time >= '2025-10-01 00:00:00'
        AND signup_time <  '2025-10-02 00:00:00'
),
charge_agg AS (
    SELECT
        customer_id,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN paid = 'TRUE' THEN 1 ELSE 0 END) AS successful_transactions,
        SUM(amount) AS total_attempted_value,
        SUM(CASE WHEN paid = 'TRUE' THEN amount ELSE 0 END) AS total_successful_value
    FROM charge_clean
    GROUP BY customer_id
)
SELECT
    fc.USER_ID,
    fc.customer_id,
    fc.signup_time,
    COALESCE(a.total_transactions, 0) AS total_transactions,
    COALESCE(a.successful_transactions, 0) AS successful_transactions,
    COALESCE(a.total_attempted_value, 0) AS total_attempted_value,
    COALESCE(a.total_successful_value, 0) AS total_successful_value
FROM filtered_customers fc
LEFT JOIN charge_agg a
    ON fc.customer_id = a.customer_id;

----- ** Additional queries to check totals ** -----
----- # Successful Transactions -----
--SELECT
--    id,
--    COUNT(*) AS successful_transactions
--FROM charge_clean
--WHERE paid = TRUE
--GROUP BY id
--ORDER BY successful_transactions DESC;

--SELECT
--    id,
--    SUM(CASE WHEN paid = 1 THEN 1 ELSE 0 END) AS successful_transactions
--FROM charge_clean
--GROUP BY id;

--SELECT
--    id,
--    COUNT(*) AS successful_transactions
--FROM charge_clean
--WHERE paid = 'TRUE'
--GROUP BY id;


--SELECT COUNT(*) AS total_successful_transactions
--FROM charge_clean
--WHERE paid = 'TRUE';
--
------- # Total attempted amounts -----
--SELECT SUM(amount) AS total_amount
--FROM charge_clean;
--
--
------- # Total successful charge amout -----
--SELECT
--    id,
--    SUM(amount) AS total_amount
--FROM charge_clean
--WHERE paid = 'TRUE';