-- Migration 009: Add updated_at column to sessions table
-- This column tracks when a session was last used/updated (e.g., identity switch during restore)
--
-- Run this migration:
--   psql $DATABASE_URL -f migrations/009_sessions_updated_at.sql
--
-- Verify after running:
--   psql $DATABASE_URL -c "\d timrx_billing.sessions"
--   psql $DATABASE_URL -c "SELECT id, updated_at FROM timrx_billing.sessions LIMIT 1"

-- Add updated_at column with default NOW() for existing rows
ALTER TABLE timrx_billing.sessions
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Backfill existing rows: set updated_at = created_at for rows where it's still the default
-- (This is idempotent - safe to run multiple times)
UPDATE timrx_billing.sessions
SET updated_at = created_at
WHERE updated_at = created_at;  -- Only update rows that haven't been touched

-- Optional: Create index on updated_at for session cleanup queries
-- CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON timrx_billing.sessions(updated_at);

-- Verification query (run manually):
-- SELECT id, identity_id, created_at, updated_at, expires_at FROM timrx_billing.sessions LIMIT 5;
