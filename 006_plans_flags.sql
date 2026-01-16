-- Migration: Add plans includes_priority and meta fields

BEGIN;

ALTER TABLE timrx_billing.plans
ADD COLUMN IF NOT EXISTS includes_priority BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE timrx_billing.plans
ADD COLUMN IF NOT EXISTS meta JSONB;

COMMIT;
