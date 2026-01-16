-- Migration: Enforce lowercase emails and case-insensitive uniqueness
-- Run this after 001_anonymous_identities.sql

BEGIN;

ALTER TABLE timrx_billing.identities
ALTER COLUMN email DROP NOT NULL;

ALTER TABLE timrx_billing.identities
DROP CONSTRAINT IF EXISTS identities_email_key;

ALTER TABLE timrx_billing.identities
DROP CONSTRAINT IF EXISTS chk_identities_email_lowercase;

DROP INDEX IF EXISTS timrx_billing.uq_identities_email;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_identities_email_lowercase'
      AND conrelid = 'timrx_billing.identities'::regclass
  ) THEN
    ALTER TABLE timrx_billing.identities
      ADD CONSTRAINT ck_identities_email_lowercase
      CHECK (email IS NULL OR email = lower(email));
  END IF;
END;
$$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_identities_email_lower
ON timrx_billing.identities (lower(email))
WHERE email IS NOT NULL;

COMMIT;
