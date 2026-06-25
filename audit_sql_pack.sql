-- ╔══════════════════════════════════════════════════════════════╗
-- ║  TimrX Database Audit SQL Pack                              ║
-- ║  Safe to run in TablePlus — all read-only unless marked     ║
-- ║  Schemas: timrx_billing, timrx_app                          ║
-- ╚══════════════════════════════════════════════════════════════╝


-- ████████████████████████████████████████████████████████████████
-- █  SECTION A: READ-ONLY AUDIT QUERIES                         █
-- █  These do NOT modify anything. Safe to run at any time.     █
-- ████████████████████████████████████████████████████████████████


-- =========================================
-- A1) TABLE / COLUMN INVENTORY
-- What: Every column in both schemas with type, nullability, default
-- Why: Compare against repo schema.sql to detect drift
-- =========================================
SELECT
    table_schema,
    table_name,
    ordinal_position,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema IN ('timrx_billing', 'timrx_app')
ORDER BY table_schema, table_name, ordinal_position;


-- =========================================
-- A2) PRIMARY KEYS
-- What: PK constraint + column for every table
-- Why: Verify every table has a proper PK
-- =========================================
SELECT
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
    AND tc.table_schema IN ('timrx_billing', 'timrx_app')
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position;


-- =========================================
-- A3) FOREIGN KEYS
-- What: Every FK with source and target columns
-- Why: Verify referential integrity constraints exist
-- =========================================
SELECT
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_schema  AS ref_schema,
    ccu.table_name    AS ref_table,
    ccu.column_name   AS ref_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
    AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND tc.table_schema IN ('timrx_billing', 'timrx_app')
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;


-- =========================================
-- A4) UNIQUE CONSTRAINTS
-- What: Every unique constraint and its columns
-- Why: Verify idempotency guards and business rules
-- =========================================
SELECT
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'UNIQUE'
    AND tc.table_schema IN ('timrx_billing', 'timrx_app')
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position;


-- =========================================
-- A5) CHECK CONSTRAINTS
-- What: Every CHECK constraint with its expression
-- Why: Verify data validation rules (e.g., exactly-one-asset)
-- =========================================
SELECT
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    cc.check_clause
FROM information_schema.check_constraints cc
JOIN information_schema.table_constraints tc
    USING (constraint_name, constraint_schema)
WHERE tc.table_schema IN ('timrx_billing', 'timrx_app')
    AND tc.constraint_type = 'CHECK'
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;


-- =========================================
-- A6) INDEXES (including partial indexes)
-- What: Every index with full CREATE INDEX definition
-- Why: Verify idempotency indexes, performance indexes, dedup indexes
-- =========================================
SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname IN ('timrx_billing', 'timrx_app')
ORDER BY schemaname, tablename, indexname;


-- =========================================
-- A7) TRIGGERS
-- What: Every trigger with timing, event, and action
-- Why: Verify updated_at triggers, title regression guard, etc.
-- =========================================
SELECT
    event_object_schema,
    event_object_table,
    trigger_name,
    action_timing,
    event_manipulation,
    action_statement
FROM information_schema.triggers
WHERE event_object_schema IN ('timrx_billing', 'timrx_app')
ORDER BY event_object_schema, event_object_table, trigger_name;


-- =========================================
-- A8) VIEWS
-- What: Every view with its SQL definition
-- Why: Verify v_credits_ledger and v_wallet_ledger_comparison
-- =========================================
SELECT
    table_schema,
    table_name,
    view_definition
FROM information_schema.views
WHERE table_schema IN ('timrx_billing', 'timrx_app')
ORDER BY table_schema, table_name;


-- =========================================
-- A9) FUNCTIONS
-- What: Every user-defined function with full definition
-- Why: Detect untracked functions (e.g., set_updated_at, prevent_model_title_regression)
-- =========================================
SELECT
    n.nspname  AS schema,
    p.proname  AS function_name,
    pg_get_functiondef(p.oid) AS definition
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname IN ('timrx_billing', 'timrx_app')
ORDER BY n.nspname, p.proname;


-- =========================================
-- A10) SEQUENCES
-- What: All sequences with current values
-- Why: Verify invoice_number_seq, receipt_number_seq, etc.
-- =========================================
SELECT
    schemaname,
    sequencename,
    start_value,
    last_value,
    increment_by,
    max_value,
    cycle
FROM pg_sequences
WHERE schemaname IN ('timrx_billing', 'timrx_app')
ORDER BY schemaname, sequencename;


-- =========================================
-- A11) EXTENSIONS
-- What: Installed Postgres extensions
-- Why: Verify pgcrypto (for gen_random_uuid) and plpgsql
-- =========================================
SELECT extname, extversion
FROM pg_extension
ORDER BY extname;


-- =========================================
-- A12) ROW COUNTS (approximate, from stats)
-- What: Approximate row count per table (no full table scan)
-- Why: Quick health check — are tables populated as expected?
-- =========================================
SELECT
    schemaname,
    relname AS table_name,
    n_live_tup AS approx_rows
FROM pg_stat_user_tables
WHERE schemaname IN ('timrx_billing', 'timrx_app')
ORDER BY schemaname, relname;


-- =========================================
-- A13) ROLE INVENTORY
-- What: All database roles with privileges
-- Why: Know your actual role names BEFORE running any ALTER ROLE
-- !! READ THIS OUTPUT before running Section B role-level commands !!
-- =========================================
SELECT
    rolname,
    rolsuper,
    rolcreaterole,
    rolcreatedb,
    rolcanlogin
FROM pg_roles
ORDER BY rolname;


-- =========================================
-- A14) TABLE-LEVEL PRIVILEGES
-- What: Who can do what on each table
-- Why: Verify least-privilege — app role shouldn't have DROP/ALTER
-- =========================================
SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM information_schema.table_privileges
WHERE table_schema IN ('timrx_billing', 'timrx_app')
ORDER BY table_schema, table_name, grantee, privilege_type;


-- =========================================
-- A15) CURRENT CONNECTION SETTINGS
-- What: Timeouts and safety settings on this connection
-- Why: Verify statement_timeout, lock_timeout, idle_in_transaction
-- =========================================
SELECT
    name,
    setting,
    unit,
    source
FROM pg_settings
WHERE name IN (
    'statement_timeout',
    'lock_timeout',
    'idle_in_transaction_session_timeout',
    'search_path'
)
ORDER BY name;


-- ████████████████████████████████████████████████████████████████
-- █  SECTION A-INTEGRITY: DATA INTEGRITY CHECKS (READ-ONLY)    █
-- █  Detect orphans, duplicates, drift — no modifications.      █
-- ████████████████████████████████████████████████████████████████


-- =========================================
-- A16) WALLET BALANCE DRIFT
-- What: Compare cached wallet balance vs actual ledger sum
-- Why: If drift != 0, a user's displayed balance is wrong
-- CRITICAL: Any rows returned here need investigation
-- =========================================
SELECT
    w.identity_id,
    w.balance_credits  AS cached_balance,
    COALESCE(lg.total, 0) AS ledger_balance,
    w.balance_credits - COALESCE(lg.total, 0) AS drift
FROM timrx_billing.wallets w
LEFT JOIN (
    SELECT identity_id, SUM(amount_credits) AS total
    FROM timrx_billing.ledger_entries
    GROUP BY identity_id
) lg ON w.identity_id = lg.identity_id
WHERE w.balance_credits != COALESCE(lg.total, 0)
ORDER BY ABS(w.balance_credits - COALESCE(lg.total, 0)) DESC;


-- =========================================
-- A17) ORPHAN: Sessions without identities
-- =========================================
SELECT s.id AS session_id, s.identity_id
FROM timrx_billing.sessions s
LEFT JOIN timrx_billing.identities i ON s.identity_id = i.id
WHERE i.id IS NULL;


-- =========================================
-- A18) ORPHAN: Wallets without identities
-- =========================================
SELECT w.identity_id
FROM timrx_billing.wallets w
LEFT JOIN timrx_billing.identities i ON w.identity_id = i.id
WHERE i.id IS NULL;


-- =========================================
-- A19) ORPHAN: Ledger entries without identities
-- =========================================
SELECT le.id, le.identity_id, le.entry_type, le.amount_credits
FROM timrx_billing.ledger_entries le
LEFT JOIN timrx_billing.identities i ON le.identity_id = i.id
WHERE i.id IS NULL;


-- =========================================
-- A20) ORPHAN: Purchases without identities
-- =========================================
SELECT p.id, p.identity_id, p.status, p.credits_granted
FROM timrx_billing.purchases p
LEFT JOIN timrx_billing.identities i ON p.identity_id = i.id
WHERE i.id IS NULL;


-- =========================================
-- A21) ORPHAN: Jobs without identities
-- =========================================
SELECT j.id, j.identity_id, j.status
FROM timrx_billing.jobs j
LEFT JOIN timrx_billing.identities i ON j.identity_id = i.id
WHERE i.id IS NULL;


-- =========================================
-- A22) ORPHAN: Subscriptions without identities
-- =========================================
SELECT s.id, s.identity_id, s.status, s.plan_code
FROM timrx_billing.subscriptions s
LEFT JOIN timrx_billing.identities i ON s.identity_id = i.id
WHERE i.id IS NULL;


-- =========================================
-- A23) ORPHAN: Credit reservations pointing to missing jobs
-- =========================================
SELECT cr.id, cr.ref_job_id, cr.status
FROM timrx_billing.credit_reservations cr
LEFT JOIN timrx_billing.jobs j ON cr.ref_job_id = j.id
WHERE cr.ref_job_id IS NOT NULL AND j.id IS NULL;


-- =========================================
-- A24) ORPHAN: History items pointing to missing models
-- =========================================
SELECT hi.id, hi.model_id
FROM timrx_app.history_items hi
LEFT JOIN timrx_app.models m ON hi.model_id = m.id
WHERE hi.model_id IS NOT NULL AND m.id IS NULL;


-- =========================================
-- A25) ORPHAN: History items pointing to missing images
-- =========================================
SELECT hi.id, hi.image_id
FROM timrx_app.history_items hi
LEFT JOIN timrx_app.images im ON hi.image_id = im.id
WHERE hi.image_id IS NOT NULL AND im.id IS NULL;


-- =========================================
-- A26) ORPHAN: History items pointing to missing videos
-- =========================================
SELECT hi.id, hi.video_id
FROM timrx_app.history_items hi
LEFT JOIN timrx_app.videos v ON hi.video_id = v.id
WHERE hi.video_id IS NOT NULL AND v.id IS NULL;


-- =========================================
-- A27) ORPHAN: Models not linked to any history_item
-- =========================================
SELECT m.id, m.identity_id, m.created_at
FROM timrx_app.models m
LEFT JOIN timrx_app.history_items hi ON hi.model_id = m.id
WHERE hi.id IS NULL AND m.deleted_at IS NULL;


-- =========================================
-- A28) ORPHAN: Images not linked to any history_item
-- =========================================
SELECT im.id, im.identity_id, im.created_at
FROM timrx_app.images im
LEFT JOIN timrx_app.history_items hi ON hi.image_id = im.id
WHERE hi.id IS NULL AND im.deleted_at IS NULL;


-- =========================================
-- A29) ORPHAN: Videos not linked to any history_item
-- =========================================
SELECT v.id, v.identity_id, v.created_at
FROM timrx_app.videos v
LEFT JOIN timrx_app.history_items hi ON hi.video_id = v.id
WHERE hi.id IS NULL AND v.deleted_at IS NULL;


-- =========================================
-- A30) ORPHAN: Merged identities still owning resources
-- What: Identities that were merged but still have sessions/purchases/wallets
-- Why: Merge may not have fully migrated all data
-- =========================================
SELECT
    i.id,
    i.merged_into_id,
    (SELECT count(*) FROM timrx_billing.sessions s
     WHERE s.identity_id = i.id AND s.revoked_at IS NULL) AS active_sessions,
    (SELECT count(*) FROM timrx_billing.purchases p
     WHERE p.identity_id = i.id) AS purchases,
    (SELECT count(*) FROM timrx_billing.wallets w
     WHERE w.identity_id = i.id) AS wallets
FROM timrx_billing.identities i
WHERE i.merged_into_id IS NOT NULL;


-- =========================================
-- A31) ORPHAN: Subscription cycles without subscriptions
-- =========================================
SELECT sc.id, sc.subscription_id
FROM timrx_billing.subscription_cycles sc
LEFT JOIN timrx_billing.subscriptions s ON sc.subscription_id = s.id
WHERE s.id IS NULL;


-- =========================================
-- A32) ORPHAN: Invoice items without invoices
-- =========================================
SELECT ii.id, ii.invoice_id
FROM timrx_billing.invoice_items ii
LEFT JOIN timrx_billing.invoices inv ON ii.invoice_id = inv.id
WHERE inv.id IS NULL;


-- =========================================
-- A33) ORPHAN: Receipts without invoices
-- =========================================
SELECT r.id, r.invoice_id
FROM timrx_billing.receipts r
LEFT JOIN timrx_billing.invoices inv ON r.invoice_id = inv.id
WHERE inv.id IS NULL;


-- =========================================
-- A34) DUPLICATE: Purchases with same provider + payment_id
-- What: Should be prevented by unique index
-- Why: If rows returned, idempotency guard has a gap
-- =========================================
SELECT provider, provider_payment_id, count(*) AS cnt
FROM timrx_billing.purchases
WHERE provider_payment_id IS NOT NULL
GROUP BY provider, provider_payment_id
HAVING count(*) > 1;


-- =========================================
-- A35) DUPLICATE: Double-charged ledger entries
-- What: Same identity charged twice for same ref
-- Why: Should be prevented by uq_ledger_charge_idempotency
-- =========================================
SELECT identity_id, ref_type, ref_id, count(*) AS cnt
FROM timrx_billing.ledger_entries
WHERE entry_type = 'charge' AND ref_type IS NOT NULL AND ref_id IS NOT NULL
GROUP BY identity_id, ref_type, ref_id
HAVING count(*) > 1;


-- =========================================
-- A36) DUPLICATE: Multiple active subscriptions per identity
-- What: Each user should have at most 1 active subscription
-- =========================================
SELECT identity_id, count(*) AS cnt
FROM timrx_billing.subscriptions
WHERE status = 'active'
GROUP BY identity_id
HAVING count(*) > 1;


-- =========================================
-- A37) DUPLICATE: Multiple wallets per identity
-- What: Each identity should have exactly 1 wallet
-- =========================================
SELECT identity_id, count(*) AS cnt
FROM timrx_billing.wallets
GROUP BY identity_id
HAVING count(*) > 1;


-- =========================================
-- A38) DUPLICATE: Same email on multiple non-merged identities
-- What: After merges, only one identity per email should remain active
-- =========================================
SELECT lower(email) AS email, count(*) AS cnt
FROM timrx_billing.identities
WHERE email IS NOT NULL AND merged_into_id IS NULL
GROUP BY lower(email)
HAVING count(*) > 1;


-- =========================================
-- A39) DUPLICATE: History items pointing to same model
-- =========================================
SELECT model_id, count(*) AS cnt
FROM timrx_app.history_items
WHERE model_id IS NOT NULL
GROUP BY model_id
HAVING count(*) > 1;


-- =========================================
-- A40) DUPLICATE: History items pointing to same image
-- =========================================
SELECT image_id, count(*) AS cnt
FROM timrx_app.history_items
WHERE image_id IS NOT NULL
GROUP BY image_id
HAVING count(*) > 1;


-- =========================================
-- A41) DUPLICATE: History items pointing to same video
-- =========================================
SELECT video_id, count(*) AS cnt
FROM timrx_app.history_items
WHERE video_id IS NOT NULL
GROUP BY video_id
HAVING count(*) > 1;


-- =========================================
-- A42) DUPLICATE: Models with same provider + upstream_job_id
-- =========================================
SELECT provider, upstream_job_id, count(*) AS cnt
FROM timrx_app.models
WHERE upstream_job_id IS NOT NULL
GROUP BY provider, upstream_job_id
HAVING count(*) > 1;


-- =========================================
-- A43) MISSING INDEX CHECK: Sequential scan detection
-- What: Tables with high seq_scan and low idx_scan ratios
-- Why: Suggests missing indexes on frequently-queried columns
-- Note: Only meaningful after the DB has had real traffic
-- =========================================
SELECT
    schemaname,
    relname AS table_name,
    seq_scan,
    idx_scan,
    CASE
        WHEN (seq_scan + idx_scan) > 0
        THEN round(100.0 * idx_scan / (seq_scan + idx_scan), 1)
        ELSE 100
    END AS idx_usage_pct,
    n_live_tup AS approx_rows
FROM pg_stat_user_tables
WHERE schemaname IN ('timrx_billing', 'timrx_app')
    AND (seq_scan + idx_scan) > 0
ORDER BY idx_usage_pct ASC, seq_scan DESC;


-- =========================================
-- A44) MISSING INDEX CHECK: Key columns that should be indexed
-- What: Verify indexes exist on commonly-joined columns
-- Run each and check if indexname is returned
-- =========================================

-- subscriptions.identity_id
SELECT indexname FROM pg_indexes
WHERE schemaname = 'timrx_billing' AND tablename = 'subscriptions'
    AND indexdef LIKE '%identity_id%';

-- purchases.identity_id
SELECT indexname FROM pg_indexes
WHERE schemaname = 'timrx_billing' AND tablename = 'purchases'
    AND indexdef LIKE '%identity_id%';

-- ledger_entries.identity_id
SELECT indexname FROM pg_indexes
WHERE schemaname = 'timrx_billing' AND tablename = 'ledger_entries'
    AND indexdef LIKE '%identity_id%';

-- invoices.identity_id
SELECT indexname FROM pg_indexes
WHERE schemaname = 'timrx_billing' AND tablename = 'invoices'
    AND indexdef LIKE '%identity_id%';

-- email_outbox.status (for pending queue polling)
SELECT indexname FROM pg_indexes
WHERE schemaname = 'timrx_billing' AND tablename = 'email_outbox'
    AND indexdef LIKE '%status%';

-- jobs.identity_id
SELECT indexname FROM pg_indexes
WHERE schemaname = 'timrx_billing' AND tablename = 'jobs'
    AND indexdef LIKE '%identity_id%';


-- =========================================
-- A45) GHOST TABLE CHECK
-- What: Tables in live DB that may not be in the repo
-- Why: Detect experiments, manual creates, or untracked blog tables
-- =========================================
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('timrx_billing', 'timrx_app')
    AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;


-- ████████████████████████████████████████████████████████████████
-- █  SECTION B: OPTIONAL HARDENING DDL                          █
-- █  !! REVIEW EACH STATEMENT BEFORE RUNNING !!                 █
-- █  !! Replace role names with YOUR actual roles from A13 !!   █
-- ████████████████████████████████████████████████████████████████


-- =========================================
-- B1) ROLE-LEVEL TIMEOUTS
-- !! MANUAL REVIEW REQUIRED !!
-- !! Replace 'YOUR_APP_ROLE' with the actual role from A13 !!
-- !! For TimrX on RDS this is likely: postgres or timrx_admin !!
--
-- What these do:
--   statement_timeout: Kill queries running longer than 30s
--   idle_in_transaction: Kill idle-in-transaction after 60s
--   lock_timeout: Fail fast if lock not acquired in 10s
--
-- Caveat: Long-running admin scripts will need:
--   SET statement_timeout = '300000';  -- 5 min override
-- =========================================

-- ALTER ROLE YOUR_APP_ROLE SET statement_timeout = '30s';
-- ALTER ROLE YOUR_APP_ROLE SET idle_in_transaction_session_timeout = '60s';
-- ALTER ROLE YOUR_APP_ROLE SET lock_timeout = '10s';


-- =========================================
-- B2) READ-ONLY ROLE (for admin dashboards, TablePlus browsing)
-- What: Create a role that can only SELECT, never INSERT/UPDATE/DELETE
-- Why: Use this for monitoring, debugging, dashboards
-- Safe: Does not affect existing roles or permissions
-- =========================================

-- CREATE ROLE timrx_readonly;
-- GRANT USAGE ON SCHEMA timrx_billing, timrx_app TO timrx_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA timrx_billing TO timrx_readonly;
-- GRANT SELECT ON ALL TABLES IN SCHEMA timrx_app TO timrx_readonly;

-- To make future tables automatically readable:
-- ALTER DEFAULT PRIVILEGES IN SCHEMA timrx_billing GRANT SELECT ON TABLES TO timrx_readonly;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA timrx_app GRANT SELECT ON TABLES TO timrx_readonly;


-- ████████████████████████████████████████████████████████████████
-- █  SECTION C: MIGRATION TRACKING                              █
-- █  Creates a table to record which migrations have been       █
-- █  applied. Safe to run multiple times (IF NOT EXISTS).       █
-- ████████████████████████████████████████████████████████████████


-- =========================================
-- C1) CREATE MIGRATION TRACKER TABLE
-- What: A simple table recording each applied migration
-- Why: Know exactly which migrations are live vs pending
-- Safe: IF NOT EXISTS — will not drop existing data
-- =========================================
CREATE TABLE IF NOT EXISTS timrx_billing.schema_migrations (
    filename    TEXT PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum    TEXT
);


-- =========================================
-- C2) RECORD A MIGRATION AS APPLIED
-- What: Template for recording a migration after you run it
-- How: Copy this, replace filename and checksum, run after each migration
-- The ON CONFLICT prevents double-recording
-- =========================================

-- Template (copy and edit):
-- INSERT INTO timrx_billing.schema_migrations (filename, checksum)
-- VALUES ('NNN_migration_name.sql', 'sha256_or_note_here')
-- ON CONFLICT (filename) DO NOTHING;


-- =========================================
-- C3) REVIEW APPLIED MIGRATIONS
-- What: See all recorded migrations and when they were applied
-- =========================================
SELECT filename, applied_at, checksum
FROM timrx_billing.schema_migrations
ORDER BY applied_at, filename;


-- ████████████████████████████████████████████████████████████████
-- █  END OF AUDIT PACK                                          █
-- █                                                             █
-- █  Recommended run order:                                     █
-- █  1. Run all of Section A (read-only) — review results       █
-- █  2. Check A13 for your actual role names                    █
-- █  3. Uncomment and adapt Section B if needed                 █
-- █  4. Run Section C to set up migration tracking              █
-- █  5. Save this file — re-run after each deploy               █
-- ████████████████████████████████████████████████████████████████
