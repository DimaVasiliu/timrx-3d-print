-- ╔══════════════════════════════════════════════════════════════╗
-- ║  TimrX Privilege Hardening Plan                             ║
-- ║  Staged approach — inspection first, changes second         ║
-- ║  Run in TablePlus as postgres                               ║
-- ╚══════════════════════════════════════════════════════════════╝


-- ████████████████████████████████████████████████████████████████
-- █  PART A: AUDIT CHECKLIST (read-only)                        █
-- █  Run all of these first. Change nothing until you review.   █
-- ████████████████████████████████████████████████████████████████


-- =========================================
-- A1) WHICH ROLE DOES THE APP ACTUALLY USE?
-- What: Check the role embedded in DATABASE_URL
-- How: The app connects via DATABASE_URL env var on Render.
--       This query shows what role THIS session uses.
--       To check what the app uses, look at Render dashboard:
--       Dashboard → timrx-backend → Environment → DATABASE_URL
--       The role is between postgresql:// and : in the URL.
--
-- If you can't check Render right now, run this from the app:
--   SELECT current_user, session_user;
-- Or add a temporary /api/debug/role endpoint (remove after).
--
-- CURRENT (2026-03-21): App connects as timrx_admin (Phase 2 complete).
-- postgres is reserved for migrations, admin, and emergency.
-- =========================================
SELECT
    current_user     AS this_session_role,
    session_user     AS this_session_login,
    current_database() AS database_name;


-- =========================================
-- A2) ALL ROLES AND THEIR ATTRIBUTES
-- What: Complete role inventory
-- =========================================
SELECT
    r.rolname,
    r.rolsuper,
    r.rolcreaterole,
    r.rolcreatedb,
    r.rolcanlogin,
    r.rolconnlimit,
    ARRAY(
        SELECT m.rolname
        FROM pg_auth_members am
        JOIN pg_roles m ON am.roleid = m.oid
        WHERE am.member = r.oid
    ) AS member_of
FROM pg_roles r
WHERE r.rolname NOT LIKE 'pg_%'
    AND r.rolname NOT LIKE 'rds%'
ORDER BY r.rolname;


-- =========================================
-- A3) SCHEMA OWNERSHIP
-- What: Who owns each schema
-- =========================================
SELECT
    n.nspname  AS schema_name,
    r.rolname  AS owner
FROM pg_namespace n
JOIN pg_roles r ON n.nspowner = r.oid
WHERE n.nspname IN ('timrx_billing', 'timrx_app', 'public')
ORDER BY n.nspname;


-- =========================================
-- A4) TABLE OWNERSHIP
-- What: Who owns each table (should all be postgres currently)
-- =========================================
SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname IN ('timrx_billing', 'timrx_app')
ORDER BY schemaname, tablename;


-- =========================================
-- A5) TABLE-LEVEL GRANTS — SIDE-BY-SIDE COMPARISON
-- What: For each table, show what postgres / timrx_admin / timrx_readonly can do
-- Why: Spot unexpected privileges or missing grants
-- =========================================
SELECT
    t.schemaname || '.' || t.tablename AS full_table,
    -- postgres grants
    string_agg(DISTINCT CASE WHEN tp.grantee = 'postgres' THEN tp.privilege_type END, ', '
        ORDER BY CASE WHEN tp.grantee = 'postgres' THEN tp.privilege_type END
    ) AS postgres_privs,
    -- timrx_admin grants
    string_agg(DISTINCT CASE WHEN tp.grantee = 'timrx_admin' THEN tp.privilege_type END, ', '
        ORDER BY CASE WHEN tp.grantee = 'timrx_admin' THEN tp.privilege_type END
    ) AS timrx_admin_privs,
    -- timrx_readonly grants
    string_agg(DISTINCT CASE WHEN tp.grantee = 'timrx_readonly' THEN tp.privilege_type END, ', '
        ORDER BY CASE WHEN tp.grantee = 'timrx_readonly' THEN tp.privilege_type END
    ) AS timrx_readonly_privs
FROM pg_tables t
LEFT JOIN information_schema.table_privileges tp
    ON t.schemaname = tp.table_schema
    AND t.tablename = tp.table_name
    AND tp.grantee IN ('postgres', 'timrx_admin', 'timrx_readonly')
WHERE t.schemaname IN ('timrx_billing', 'timrx_app')
GROUP BY t.schemaname, t.tablename
ORDER BY t.schemaname, t.tablename;


-- =========================================
-- A6) SEQUENCE-LEVEL GRANTS
-- What: Who can use each sequence (invoice numbers, receipt numbers, etc.)
-- Why: If the app role can't use sequences, INSERT with DEFAULT will fail
-- =========================================
SELECT
    s.sequence_schema || '.' || s.sequence_name AS full_sequence,
    string_agg(DISTINCT usp.grantee || ':' || usp.privilege_type, ', ') AS grants
FROM information_schema.sequences s
LEFT JOIN information_schema.usage_privileges usp
    ON s.sequence_schema = usp.object_schema
    AND s.sequence_name = usp.object_name
    AND usp.object_type = 'SEQUENCE'
    AND usp.grantee IN ('postgres', 'timrx_admin', 'timrx_readonly')
WHERE s.sequence_schema IN ('timrx_billing', 'timrx_app')
GROUP BY s.sequence_schema, s.sequence_name
ORDER BY s.sequence_schema, s.sequence_name;


-- =========================================
-- A7) FUNCTION-LEVEL GRANTS
-- What: Who can EXECUTE each function
-- Why: Trigger functions need to be executable by the role that does UPDATEs
-- =========================================
SELECT
    n.nspname || '.' || p.proname AS full_function,
    pg_get_userbyid(p.proowner) AS owner,
    CASE WHEN has_function_privilege('timrx_admin', p.oid, 'EXECUTE')
         THEN 'YES' ELSE 'NO' END AS timrx_admin_can_execute,
    CASE WHEN has_function_privilege('timrx_readonly', p.oid, 'EXECUTE')
         THEN 'YES' ELSE 'NO' END AS timrx_readonly_can_execute
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname IN ('timrx_billing', 'timrx_app')
ORDER BY n.nspname, p.proname;


-- =========================================
-- A8) SCHEMA-LEVEL GRANTS
-- What: Who has USAGE and CREATE on each schema
-- Why: USAGE = can see objects. CREATE = can make new tables.
--       App role needs USAGE but should NOT need CREATE.
-- =========================================
SELECT
    n.nspname AS schema_name,
    CASE WHEN has_schema_privilege('postgres', n.nspname, 'USAGE')
         THEN 'YES' ELSE 'NO' END AS postgres_usage,
    CASE WHEN has_schema_privilege('postgres', n.nspname, 'CREATE')
         THEN 'YES' ELSE 'NO' END AS postgres_create,
    CASE WHEN has_schema_privilege('timrx_admin', n.nspname, 'USAGE')
         THEN 'YES' ELSE 'NO' END AS admin_usage,
    CASE WHEN has_schema_privilege('timrx_admin', n.nspname, 'CREATE')
         THEN 'YES' ELSE 'NO' END AS admin_create,
    CASE WHEN has_schema_privilege('timrx_readonly', n.nspname, 'USAGE')
         THEN 'YES' ELSE 'NO' END AS readonly_usage,
    CASE WHEN has_schema_privilege('timrx_readonly', n.nspname, 'CREATE')
         THEN 'YES' ELSE 'NO' END AS readonly_create
FROM pg_namespace n
WHERE n.nspname IN ('timrx_billing', 'timrx_app')
ORDER BY n.nspname;


-- =========================================
-- A9) ROLE-LEVEL TIMEOUT SETTINGS
-- What: Timeouts configured at the ROLE level (persist across connections)
-- Why: Verify what was set with ALTER ROLE ... SET
-- =========================================
SELECT
    r.rolname,
    unnest(r.rolconfig) AS setting
FROM pg_roles r
WHERE r.rolname IN ('postgres', 'timrx_admin', 'timrx_readonly')
    AND r.rolconfig IS NOT NULL
ORDER BY r.rolname;


-- =========================================
-- A10) DANGEROUS PRIVILEGE CHECK
-- What: Can timrx_admin or timrx_readonly do things they shouldn't?
-- Why: Least privilege means: no CREATE, no TRUNCATE, no DROP
-- =========================================

-- Can timrx_admin CREATE tables? (should be NO in Phase 2)
SELECT has_schema_privilege('timrx_admin', 'timrx_billing', 'CREATE') AS admin_can_create_in_billing;
SELECT has_schema_privilege('timrx_admin', 'timrx_app', 'CREATE') AS admin_can_create_in_app;

-- Can timrx_readonly INSERT/UPDATE/DELETE? (must be NO)
SELECT
    table_schema || '.' || table_name AS tbl,
    privilege_type
FROM information_schema.table_privileges
WHERE grantee = 'timrx_readonly'
    AND table_schema IN ('timrx_billing', 'timrx_app')
    AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')
ORDER BY tbl;


-- ████████████████████████████████████████████████████████████████
-- █  PART B: PHASE 1 — MINIMAL SAFE HARDENING                  █
-- █  Goal: Harden what we can without changing app connectivity █
-- █  App connects as timrx_admin (Phase 2 complete 2026-03-21)  █
-- █  Risk: LOW — does not affect how the app authenticates      █
-- ████████████████████████████████████████████████████████████████


-- =========================================
-- B1) SET ROLE-LEVEL TIMEOUTS ON postgres
-- What: Enforce timeouts at the role level (backup for db.py session-level)
-- Why: db.py already sets these per-session, but role-level is a safety net
--       for any direct connections, psql sessions, or code paths that skip db.py
-- Risk: LOW — the app already has session-level timeouts via db.py
-- Caveat: Long admin queries via TablePlus will also be subject to these.
--         Override per-session with: SET statement_timeout = '300000';
-- =========================================
ALTER ROLE postgres SET statement_timeout = '30s';
ALTER ROLE postgres SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE postgres SET lock_timeout = '10s';


-- =========================================
-- B2) ENSURE timrx_readonly IS TRULY READ-ONLY
-- What: Revoke any write privileges that may have been granted
-- Risk: NONE — this role should never write
-- =========================================
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA timrx_billing FROM timrx_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE
    ON ALL TABLES IN SCHEMA timrx_app FROM timrx_readonly;

-- Prevent future tables from being writable by readonly
ALTER DEFAULT PRIVILEGES IN SCHEMA timrx_billing
    REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLES FROM timrx_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA timrx_app
    REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLES FROM timrx_readonly;

-- Ensure readonly can still SELECT on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA timrx_billing
    GRANT SELECT ON TABLES TO timrx_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA timrx_app
    GRANT SELECT ON TABLES TO timrx_readonly;


-- =========================================
-- B3) SET TIMEOUTS ON timrx_readonly TOO
-- What: Prevent long-running monitoring queries from holding locks
-- Risk: NONE
-- =========================================
ALTER ROLE timrx_readonly SET statement_timeout = '60s';
ALTER ROLE timrx_readonly SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE timrx_readonly SET lock_timeout = '5s';


-- =========================================
-- B4) REVOKE CREATE ON SCHEMAS FROM timrx_admin
-- What: Prevent the admin role from creating new tables
-- Why: App code should never CREATE TABLE at runtime (schema changes are migrations)
-- Risk: NONE — app now connects as timrx_admin (Phase 2 complete 2026-03-21).
--       CREATE on schemas is correctly revoked. Startup DDL in
--       prompt_safety_service.py is a no-op (tables pre-exist in migrations).
-- =========================================
REVOKE CREATE ON SCHEMA timrx_billing FROM timrx_admin;
REVOKE CREATE ON SCHEMA timrx_app FROM timrx_admin;


-- =========================================
-- B5) VERIFY PHASE 1 RESULTS
-- Run these after applying B1-B4 to confirm everything took effect
-- =========================================

-- Role timeout settings
SELECT r.rolname, unnest(r.rolconfig) AS setting
FROM pg_roles r
WHERE r.rolname IN ('postgres', 'timrx_admin', 'timrx_readonly')
    AND r.rolconfig IS NOT NULL
ORDER BY r.rolname;

-- Readonly cannot write
SELECT table_schema || '.' || table_name AS tbl, privilege_type
FROM information_schema.table_privileges
WHERE grantee = 'timrx_readonly'
    AND table_schema IN ('timrx_billing', 'timrx_app')
    AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE');
-- Expected: 0 rows

-- Admin cannot CREATE
SELECT has_schema_privilege('timrx_admin', 'timrx_billing', 'CREATE') AS admin_billing_create;
SELECT has_schema_privilege('timrx_admin', 'timrx_app', 'CREATE') AS admin_app_create;
-- Expected: both FALSE


-- ████████████████████████████████████████████████████████████████
-- █  PART C: PHASE 2 — COMPLETE (2026-03-21)                   █
-- █  App now connects as timrx_admin.                           █
-- █  postgres is reserved for migrations, admin, and emergency. █
-- ████████████████████████████████████████████████████████████████

/*
═══════════════════════════════════════════════════════════════
PHASE 2 — COMPLETED 2026-03-21
═══════════════════════════════════════════════════════════════

What was done:
  - timrx_admin has SELECT/INSERT/UPDATE/DELETE on all app tables
  - timrx_admin has USAGE + SELECT on all sequences
  - timrx_admin has USAGE (not CREATE) on timrx_app and timrx_billing
  - Default privileges grant future tables/sequences to timrx_admin
  - safety_strikes and safety_rejections tables pre-created via migration
  - Idempotency indexes pre-created via migration
  - DATABASE_URL on Render switched to timrx_admin
  - postgres URL retained for emergency rollback

Role model:
  - timrx_admin : app runtime (DML only, no DDL)
  - postgres    : migrations, admin scripts, emergency
  - timrx_readonly : monitoring, analytics

ROLLBACK:
  If anything breaks, switch DATABASE_URL back to the postgres URL.
  No data is lost. No schema changes needed to revert.

═══════════════════════════════════════════════════════════════
*/


-- ████████████████████████████████████████████████████████████████
-- █  END OF PRIVILEGE HARDENING PLAN                            █
-- █                                                             █
-- █  Summary:                                                   █
-- █   Part A: Audit queries (read-only)                         █
-- █   Part B: Phase 1 — minimal hardening (applied)             █
-- █   Part C: Phase 2 — app on timrx_admin (complete 2026-03-21)█
-- ████████████████████████████████████████████████████████████████
