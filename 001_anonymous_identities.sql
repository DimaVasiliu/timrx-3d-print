-- Migration: Allow anonymous identities (email can be NULL)
-- Run this after the initial schema.sql

BEGIN;

-- Make email nullable for anonymous-first flow
ALTER TABLE timrx_billing.identities
ALTER COLUMN email DROP NOT NULL;

-- Drop the old unique constraint and create a partial unique index
-- This allows multiple NULL emails but ensures non-NULL emails are unique
DROP INDEX IF EXISTS timrx_billing.identities_email_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_identities_email
ON timrx_billing.identities(email)
WHERE email IS NOT NULL;

-- Add index for faster session lookups by identity
CREATE INDEX IF NOT EXISTS idx_sessions_id_expires
ON timrx_billing.sessions(id, expires_at)
WHERE revoked_at IS NULL;

COMMIT;
