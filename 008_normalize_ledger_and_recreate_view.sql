-- Migration: 008_normalize_ledger_and_recreate_view.sql
-- 1. Normalize ref_type='purchases' -> 'purchase'
-- 2. Drop and recreate v_credits_ledger with correct schema

BEGIN;

-- Step 1: Normalize ref_type values
UPDATE timrx_billing.ledger_entries
SET ref_type = 'purchase'
WHERE ref_type = 'purchases';

-- Step 2: Drop existing view (required because column order/names changed)
DROP VIEW IF EXISTS timrx_billing.v_credits_ledger;

-- Step 3: Recreate view with correct column order
CREATE VIEW timrx_billing.v_credits_ledger AS
SELECT
  le.id                 AS ledger_id,
  le.identity_id,
  le.entry_type         AS source,
  le.amount_credits     AS credits_delta,
  le.ref_type,
  le.ref_id,
  le.meta,
  le.created_at,
  p.id                  AS purchase_id,
  p.provider,
  p.provider_payment_id,
  p.payment_id,
  p.plan_id,
  p.status              AS purchase_status,
  p.amount_gbp,
  p.currency,
  p.credits_granted     AS purchase_credits,
  p.paid_at
FROM timrx_billing.ledger_entries le
LEFT JOIN timrx_billing.purchases p
  ON le.ref_type = 'purchase'
 AND le.ref_id IS NOT NULL
 AND p.id::text = le.ref_id
ORDER BY le.created_at DESC;

COMMIT;
