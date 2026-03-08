-- =====================================================
-- CASE STUDY #2 – PAYMENTS / RISK / FRAUD ANALYTICS
-- Dataset: payments
--
-- Columns:
-- transaction_id
-- user_id
-- transaction_time
-- amount
-- merchant_id
-- country
-- ip_address
-- device_id
-- payment_method
-- status
-- =====================================================


-- =====================================================
-- 0) TABLE STRUCTURE CHECK
-- =====================================================

PRAGMA table_info(payments);



-- =====================================================
-- 1) DATASET OVERVIEW
-- =====================================================

SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved_tx,
    SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END) AS declined_tx,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT merchant_id) AS unique_merchants,
    SUM(amount) AS total_volume,
    AVG(amount) AS avg_tx_amount
FROM payments;



-- =====================================================
-- 2) PAYMENT BEHAVIOUR PER USER
-- =====================================================

SELECT
    user_id,
    COUNT(*) AS total_tx,
    SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved_tx,
    SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END) AS declined_tx,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM payments
GROUP BY user_id
ORDER BY total_tx DESC;



-- =====================================================
-- 3) HIGH DECLINE USERS
-- Condition:
-- - at least 10 transactions
-- - decline rate > 40%
-- =====================================================

SELECT
    user_id,
    COUNT(*) AS total_tx,
    SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END) AS declined_tx,
    CAST(SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS decline_rate
FROM payments
GROUP BY user_id
HAVING COUNT(*) >= 10
   AND CAST(SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END) AS REAL) / COUNT(*) > 0.40
ORDER BY decline_rate DESC, total_tx DESC;



-- =====================================================
-- 4) PAYMENT VELOCITY
-- Transactions in last 24h from user's latest transaction
-- =====================================================

WITH a AS (
    SELECT
        user_id,
        transaction_time,
        MAX(transaction_time) OVER (PARTITION BY user_id) AS last_tx
    FROM payments
),
b AS (
    SELECT
        user_id,
        (strftime('%s', last_tx) - strftime('%s', transaction_time)) / 3600.0 AS hours_diff
    FROM a
)
SELECT
    user_id,
    SUM(CASE WHEN hours_diff <= 24 THEN 1 ELSE 0 END) AS tx_24h
FROM b
GROUP BY user_id
ORDER BY tx_24h DESC;



-- =====================================================
-- 5) MULTI-DEVICE USERS
-- Users with >= 3 different devices in last 24h
-- =====================================================

WITH a AS (
    SELECT
        user_id,
        device_id,
        transaction_time,
        MAX(transaction_time) OVER (PARTITION BY user_id) AS last_txn
    FROM payments
),
b AS (
    SELECT
        user_id,
        device_id,
        last_txn,
        (strftime('%s', last_txn) - strftime('%s', transaction_time)) / 3600.0 AS hours_diff
    FROM a
)
SELECT
    user_id,
    COUNT(DISTINCT device_id) AS device_id_count
FROM b
WHERE hours_diff <= 24
GROUP BY user_id
HAVING COUNT(DISTINCT device_id) >= 3
ORDER BY device_id_count DESC;



-- =====================================================
-- 6) MULTI-COUNTRY ACTIVITY
-- Users with >= 2 countries in last 24h
-- =====================================================

WITH a AS (
    SELECT
        user_id,
        country,
        transaction_time,
        MAX(transaction_time) OVER (PARTITION BY user_id) AS last_txn
    FROM payments
),
b AS (
    SELECT
        user_id,
        transaction_time,
        country,
        last_txn,
        (strftime('%s', last_txn) - strftime('%s', transaction_time)) / 3600.0 AS hours_diff
    FROM a
)
SELECT
    user_id,
    COUNT(DISTINCT country) AS distinct_country_count
FROM b
WHERE hours_diff <= 24
GROUP BY user_id
HAVING COUNT(DISTINCT country) >= 2
ORDER BY distinct_country_count DESC;



-- =====================================================
-- 7) DECLINE STORM
-- Users with >= 5 declined transactions in last 24h
-- =====================================================

WITH a AS (
    SELECT
        user_id,
        status,
        transaction_time,
        MAX(transaction_time) OVER (PARTITION BY user_id) AS last_txn_time
    FROM payments
),
b AS (
    SELECT
        user_id,
        transaction_time,
        status,
        last_txn_time,
        (strftime('%s', last_txn_time) - strftime('%s', transaction_time)) / 3600.0 AS hours_diff
    FROM a
)
SELECT
    user_id,
    SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END) AS declined_count
FROM b
WHERE hours_diff <= 24
GROUP BY user_id
HAVING SUM(CASE WHEN status = 'declined' THEN 1 ELSE 0 END) >= 5
ORDER BY declined_count DESC;



-- =====================================================
-- 8) MERCHANT CONCENTRATION
-- Users with:
-- - at least 10 transactions
-- - top merchant ratio > 60%
-- =====================================================

WITH a AS (
    SELECT
        user_id,
        merchant_id,
        COUNT(*) OVER (PARTITION BY user_id, merchant_id) * 1.0
        / COUNT(*) OVER (PARTITION BY user_id) AS merchant_user_ratio,
        COUNT(*) OVER (PARTITION BY user_id) AS total_tx
    FROM payments
),
b AS (
    SELECT
        user_id,
        MAX(merchant_user_ratio) AS highest_merchant_ratio,
        MAX(total_tx) AS total_tx
    FROM a
    GROUP BY user_id
)
SELECT
    user_id,
    total_tx,
    highest_merchant_ratio
FROM b
WHERE highest_merchant_ratio > 0.60
  AND total_tx >= 10
ORDER BY highest_merchant_ratio DESC, total_tx DESC;



-- =====================================================
-- 9) RAPID MERCHANT SWITCHING (SELF JOIN)
-- Users with >= 4 different merchants within 1 hour
-- =====================================================

WITH a AS (
    SELECT
        user_id,
        transaction_time,
        merchant_id
    FROM payments
),
b AS (
    SELECT
        t1.user_id,
        t1.transaction_time,
        COUNT(DISTINCT t2.merchant_id) AS merchants_1h
    FROM a t1
    JOIN a t2
        ON t1.user_id = t2.user_id
       AND t2.transaction_time BETWEEN datetime(t1.transaction_time, '-1 hour')
                                   AND t1.transaction_time
    GROUP BY t1.user_id, t1.transaction_time
)
SELECT
    user_id,
    transaction_time,
    merchants_1h
FROM b
WHERE merchants_1h >= 4
ORDER BY merchants_1h DESC, transaction_time DESC;
