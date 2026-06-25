-- ============================================================================
-- DEBUG: OpenAI Image Credit Flow Verification
-- CORRECTED: Uses timrx_billing schema (not timrx_app)
-- ============================================================================

-- 0. First, verify the schema and tables exist
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'timrx_billing';

-- Check which billing tables exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'timrx_billing'
ORDER BY table_name;

-- 1. Check if any OPENAI_IMAGE reservations exist
-- Expected: Should see 'held' status initially, then 'finalized' after job completes
SELECT
    id,
    identity_id,
    action_code,
    cost_credits,
    status,
    ref_job_id,
    created_at,
    captured_at,
    released_at
FROM timrx_billing.credit_reservations
WHERE action_code = 'OPENAI_IMAGE'
ORDER BY created_at DESC
LIMIT 20;

-- 2. Check ledger entries for OPENAI_IMAGE debits
-- Expected: Negative amount_credits entries when job finalizes
SELECT
    id,
    identity_id,
    entry_type,
    amount_credits,
    ref_type,
    ref_id,
    meta,
    created_at
FROM timrx_billing.ledger_entries
WHERE entry_type = 'RESERVATION_FINALIZE'
  AND meta::text LIKE '%OPENAI_IMAGE%'
ORDER BY created_at DESC
LIMIT 20;

-- 3. Check if OpenAI image jobs have reservation_ids
-- Expected: All jobs should have a non-null reservation_id
SELECT
    id,
    identity_id,
    provider,
    action_code,
    status,
    reservation_id,
    created_at
FROM timrx_billing.jobs
WHERE provider = 'openai'
   OR action_code = 'OPENAI_IMAGE'
   OR action_code = 'image-studio'
ORDER BY created_at DESC
LIMIT 20;

-- 4. Check action_costs table for OPENAI_IMAGE cost
-- Expected: Should show cost_credits = 10
SELECT * FROM timrx_billing.action_costs WHERE action_code = 'OPENAI_IMAGE';

-- 5. Check wallets table exists and has data
SELECT COUNT(*) as wallet_count FROM timrx_billing.wallets;

-- 6. Summary diagnostics
SELECT
    'RESERVATIONS' as check_type,
    COUNT(*) as total,
    SUM(CASE WHEN status = 'held' THEN 1 ELSE 0 END) as held,
    SUM(CASE WHEN status = 'finalized' THEN 1 ELSE 0 END) as finalized,
    SUM(CASE WHEN status = 'released' THEN 1 ELSE 0 END) as released
FROM timrx_billing.credit_reservations
WHERE action_code = 'OPENAI_IMAGE'
UNION ALL
SELECT
    'LEDGER_DEBITS' as check_type,
    COUNT(*) as total,
    SUM(CASE WHEN amount_credits < 0 THEN 1 ELSE 0 END) as debits,
    0 as _unused1,
    0 as _unused2
FROM timrx_billing.ledger_entries
WHERE entry_type = 'RESERVATION_FINALIZE'
  AND meta::text LIKE '%OPENAI_IMAGE%';

-- ============================================================================
-- IF TABLES DON'T EXIST:
-- Run this file on your database: schema.sql
--
-- psql $DATABASE_URL -f schema.sql
-- ============================================================================
