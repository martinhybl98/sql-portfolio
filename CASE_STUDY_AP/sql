Accounts Payable SQL Queries

Example SQL queries for common Accounts Payable reporting scenarios:

open balance per bill

unapplied payments, unpaid bills, vendor debt, vendor debt as of a specific date

AP aging analysis

Tables used:

BILLS, PAYMENTS_AP, PAYMENT_APPLICATIONS, VENDORS
    
-- =====================================================
-- Open balance per bill
-- Shows original bill amount, total applied payments, and remaining balance.
-- =====================================================
SELECT
    b.vendor_id,
    b.bill_id,
    b.amount AS bill_amount,
    COALESCE(SUM(pa.applied_amount), 0) AS applied_amount,
    b.amount - COALESCE(SUM(pa.applied_amount), 0) AS open_balance
FROM BILLS b
LEFT JOIN PAYMENT_APPLICATIONS pa
    ON pa.bill_id = b.bill_id
GROUP BY
    b.vendor_id,
    b.bill_id,
    b.amount
ORDER BY b.bill_id;


-- =====================================================
-- Unapplied payments
-- Shows payments and how much of each payment is still not assigned to a bill.
-- =====================================================

SELECT
    p.payment_id,
    p.amount AS payment_amount,
    COALESCE(SUM(pa.applied_amount), 0) AS applied_amount,
    p.amount - COALESCE(SUM(pa.applied_amount), 0) AS unapplied_amount
FROM PAYMENTS_AP p
LEFT JOIN PAYMENT_APPLICATIONS pa
    ON pa.payment_id = p.payment_id
GROUP BY
    p.payment_id,
    p.amount
ORDER BY p.payment_id;

-- =====================================================
-- Unpaid bills
-- Filters only bills where the remaining balance is greater than zero.
-- =====================================================

SELECT
    b.bill_id,
    b.amount AS bill_amount,
    COALESCE(SUM(pa.applied_amount), 0) AS applied_amount,
    b.amount - COALESCE(SUM(pa.applied_amount), 0) AS unpaid_balance
FROM BILLS b
LEFT JOIN PAYMENT_APPLICATIONS pa
    ON pa.bill_id = b.bill_id
GROUP BY
    b.bill_id,
    b.amount
HAVING b.amount - COALESCE(SUM(pa.applied_amount), 0) > 0
ORDER BY b.bill_id;



-- =====================================================
-- Remaining debt per vendor
-- Aggregates outstanding balances from all bills belonging to each vendor.
-- =====================================================

SELECT
    b.vendor_id,
    SUM(
        b.amount - COALESCE(pa_sum.paid_amount, 0)
    ) AS remaining_debt
FROM BILLS b
LEFT JOIN (
    SELECT
        bill_id,
        SUM(applied_amount) AS paid_amount
    FROM PAYMENT_APPLICATIONS
    GROUP BY bill_id
) pa_sum
    ON pa_sum.bill_id = b.bill_id
GROUP BY
    b.vendor_id
ORDER BY b.vendor_id;


-- =====================================================
-- Remaining vendor debt as of a specific date
-- Useful for financial reporting and month-end snapshots.
-- =====================================================

SELECT
    v.vendor_id,
    COALESCE(SUM(bill_debt.remaining_debt), 0) AS debt_as_of_2025_02_28
FROM VENDORS v
LEFT JOIN BILLS b
    ON b.vendor_id = v.vendor_id
LEFT JOIN (
    SELECT
        b.bill_id,
        b.amount - COALESCE(SUM(pa.applied_amount), 0) AS remaining_debt
    FROM BILLS b
    LEFT JOIN PAYMENT_APPLICATIONS pa
        ON pa.bill_id = b.bill_id
       AND pa.applied_date <= '2025-02-28'
    GROUP BY
        b.bill_id,
        b.amount
) bill_debt
    ON bill_debt.bill_id = b.bill_id
GROUP BY
    v.vendor_id
ORDER BY v.vendor_id;


-- =====================================================
-- AP aging analysis
-- Categorizes outstanding balances based on how overdue they are.
-- =====================================================

SELECT
    v.vendor_id,
    COALESCE(SUM(bill_debt.remaining_debt), 0) AS total_debt,
    SUM(
        CASE
            WHEN bill_debt.remaining_debt > 0
             AND CAST(julianday('2025-02-28') - julianday(bill_debt.due_date) AS INTEGER) BETWEEN 0 AND 30
            THEN bill_debt.remaining_debt
            ELSE 0
        END
    ) AS current_0_30,
    SUM(
        CASE
            WHEN bill_debt.remaining_debt > 0
             AND CAST(julianday('2025-02-28') - julianday(bill_debt.due_date) AS INTEGER) BETWEEN 31 AND 60
            THEN bill_debt.remaining_debt
            ELSE 0
        END
    ) AS overdue_31_60,
    SUM(
        CASE
            WHEN bill_debt.remaining_debt > 0
             AND CAST(julianday('2025-02-28') - julianday(bill_debt.due_date) AS INTEGER) >= 61
            THEN bill_debt.remaining_debt
            ELSE 0
        END
    ) AS overdue_61_plus
FROM VENDORS v
LEFT JOIN BILLS b
    ON b.vendor_id = v.vendor_id
LEFT JOIN (
    SELECT
        b.bill_id,
        b.due_date,
        b.amount - COALESCE(SUM(pa.applied_amount), 0) AS remaining_debt
    FROM BILLS b
    LEFT JOIN PAYMENT_APPLICATIONS pa
        ON pa.bill_id = b.bill_id
       AND pa.applied_date <= '2025-02-28'
    GROUP BY
        b.bill_id,
        b.amount,
        b.due_date
) bill_debt
    ON bill_debt.bill_id = b.bill_id
GROUP BY
    v.vendor_id
ORDER BY v.vendor_id;


