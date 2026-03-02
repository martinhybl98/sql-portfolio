/* ------------------------------------------------------------
   Goal 1 — Dataset overview
   What: total volume, number of users, purchases vs chargebacks
   Why: baseline understanding of dataset composition
------------------------------------------------------------ */
SELECT
  SUM(amount) AS total_amount,
  COUNT(DISTINCT user_id) AS unique_users,
  COUNT(*) FILTER (WHERE tx_type = 'purchase') AS purchase_count,
  COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS chargeback_count
FROM malta_fraud_case_transactions;


/* ------------------------------------------------------------
   Goal 2 — Chargeback ratio per user
   What: chargeback_count / purchase_count per user
   Why: ratio is often more informative than raw counts
------------------------------------------------------------ */
SELECT
  user_id,
  COUNT(*) FILTER (WHERE tx_type = 'purchase') AS purchase_count,
  COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS chargeback_count,
  CAST(COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS REAL)
    / NULLIF(COUNT(*) FILTER (WHERE tx_type = 'purchase'), 0) AS chargeback_ratio
FROM malta_fraud_case_transactions
GROUP BY user_id
HAVING CAST(COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS REAL)
    / NULLIF(COUNT(*) FILTER (WHERE tx_type = 'purchase'), 0) > 0;


/* ------------------------------------------------------------
   Goal 3 — High-risk users (example thresholds)
   Rule: purchase_count > 20 AND chargeback_ratio > 3%
   Why: remove low-activity noise and focus on meaningful behaviour
------------------------------------------------------------ */
SELECT
  user_id,
  COUNT(*) FILTER (WHERE tx_type = 'purchase') AS purchase_count,
  COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS chargeback_count,
  CAST(COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS REAL)
    / NULLIF(COUNT(*) FILTER (WHERE tx_type = 'purchase'), 0) * 100 AS chargeback_ratio_pct
FROM malta_fraud_case_transactions
GROUP BY user_id
HAVING COUNT(*) FILTER (WHERE tx_type = 'purchase') > 20
   AND CAST(COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS REAL)
     / NULLIF(COUNT(*) FILTER (WHERE tx_type = 'purchase'), 0) * 100 > 3;


/* ------------------------------------------------------------
   Goal 4 — Rank users by risk (count + ratio + volume)
   Rule: chargebacks >= 2 AND ratio >= 2%
   Why: triage list for manual review
------------------------------------------------------------ */
SELECT
  user_id,
  COUNT(*) FILTER (WHERE tx_type = 'purchase') AS purchase_count,
  COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS chargeback_count,
  SUM(amount) FILTER (WHERE tx_type='purchase') AS purchase_volume,
  CAST(COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS REAL)
    / NULLIF(COUNT(*) FILTER (WHERE tx_type = 'purchase'), 0) * 100 AS chargeback_ratio_pct
FROM malta_fraud_case_transactions
GROUP BY user_id
HAVING COUNT(*) FILTER (WHERE tx_type = 'chargeback') >= 2
   AND CAST(COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS REAL)
     / NULLIF(COUNT(*) FILTER (WHERE tx_type = 'purchase'), 0) * 100 >= 2
ORDER BY
  COUNT(*) FILTER (WHERE tx_type = 'chargeback') DESC,
  CAST(COUNT(*) FILTER (WHERE tx_type = 'chargeback') AS REAL)
    / NULLIF(COUNT(*) FILTER (WHERE tx_type = 'purchase'), 0) * 100 DESC,
  SUM(amount) FILTER (WHERE tx_type='purchase') DESC
LIMIT 10;

/* ------------------------------------------------------------
   Goal 5 — Multi-card in last 24 hours
   Rule: distinct cards >= 3 in last 24h (relative to last tx)
------------------------------------------------------------ */
WITH base AS (
  SELECT
    user_id,
    transaction_time,
    tx_type,
    card_id,
    MAX(transaction_time) OVER (PARTITION BY user_id) AS last_tx_time
  FROM malta_fraud_case_transactions
),
scored AS (
  SELECT
    user_id,
    tx_type,
    card_id,
    (strftime('%s', last_tx_time) - strftime('%s', transaction_time)) / 3600.0 AS hours_from_last
  FROM base
)
SELECT
  user_id,
  COUNT(DISTINCT card_id) FILTER (WHERE hours_from_last <= 24) AS cards_last_24h,
  COUNT(*) FILTER (WHERE tx_type = 'purchase' AND hours_from_last <= 24) AS purchases_last_24h
FROM scored
GROUP BY user_id
HAVING COUNT(DISTINCT card_id) FILTER (WHERE hours_from_last <= 24) >= 3
ORDER BY cards_last_24h DESC, purchases_last_24h DESC;


/* ------------------------------------------------------------
   Goal 6 — Multi-device in last 24 hours
   Rule: distinct devices >= 2 in last 24h
------------------------------------------------------------ */
WITH base AS (
  SELECT
    user_id,
    transaction_time,
    device_id,
    MAX(transaction_time) OVER (PARTITION BY user_id) AS last_tx_time
  FROM malta_fraud_case_transactions
),
scored AS (
  SELECT
    user_id,
    device_id,
    (strftime('%s', last_tx_time) - strftime('%s', transaction_time)) / 3600.0 AS hours_from_last
  FROM base
)
SELECT
  user_id,
  COUNT(DISTINCT device_id) FILTER (WHERE hours_from_last <= 24) AS devices_last_24h
FROM scored
GROUP BY user_id
HAVING COUNT(DISTINCT device_id) FILTER (WHERE hours_from_last <= 24) >= 2
ORDER BY devices_last_24h DESC;


/* ------------------------------------------------------------
   Goal 7 — Chargeback burst detection (<= 48 hours)
   What: list consecutive chargebacks where time gap <= 48h
   Output: (user_id, prev_cb_time, transaction_time, hours_diff)
------------------------------------------------------------ */
WITH cb AS (
  SELECT
    user_id,
    transaction_time,
    LAG(transaction_time) OVER (PARTITION BY user_id ORDER BY transaction_time) AS prev_cb_time
  FROM malta_fraud_case_transactions
  WHERE tx_type = 'chargeback'
),
diff AS (
  SELECT
    user_id,
    prev_cb_time,
    transaction_time,
    (strftime('%s', transaction_time) - strftime('%s', prev_cb_time)) / 3600.0 AS hours_diff
  FROM cb
  WHERE prev_cb_time IS NOT NULL
)
SELECT
  user_id,
  prev_cb_time,
  transaction_time,
  hours_diff
FROM diff
WHERE hours_diff <= 48
ORDER BY hours_diff, transaction_time DESC;


/* ------------------------------------------------------------
   Goal 8 — Card testing (small purchases burst)
   Rule: amount in (0,10] AND >= 5 purchases within 1 hour
   Method: self-join time window (SQLite-friendly)
------------------------------------------------------------ */
WITH small_tx AS (
  SELECT
    user_id,
    transaction_time,
    amount
  FROM malta_fraud_case_transactions
  WHERE tx_type = 'purchase'
    AND amount > 0
    AND amount <= 10
),
windowed AS (
  SELECT
    t1.user_id,
    t1.transaction_time,
    COUNT(*) AS purchases_in_1h
  FROM small_tx t1
  JOIN small_tx t2
    ON t1.user_id = t2.user_id
   AND t2.transaction_time BETWEEN datetime(t1.transaction_time, '-1 hour')
                               AND t1.transaction_time
  GROUP BY t1.user_id, t1.transaction_time
)
SELECT
  user_id,
  transaction_time,
  purchases_in_1h
FROM windowed
WHERE purchases_in_1h >= 5
ORDER BY purchases_in_1h DESC;


/* ------------------------------------------------------------
   Goal 9 — Multi-country anomaly (>= 2 countries within 24 hours)
   Method: self-join time window + COUNT(DISTINCT country)
------------------------------------------------------------ */
WITH base AS (
  SELECT
    user_id,
    transaction_time,
    country
  FROM malta_fraud_case_transactions
),
windowed AS (
  SELECT
    t1.user_id,
    t1.transaction_time,
    COUNT(DISTINCT t2.country) AS countries_in_24h
  FROM base t1
  JOIN base t2
    ON t1.user_id = t2.user_id
   AND t2.transaction_time BETWEEN datetime(t1.transaction_time, '-24 hours')
                               AND t1.transaction_time
  GROUP BY t1.user_id, t1.transaction_time
)
SELECT
  user_id,
  transaction_time,
  countries_in_24h
FROM windowed
WHERE countries_in_24h >= 2
ORDER BY countries_in_24h DESC
LIMIT 10;
