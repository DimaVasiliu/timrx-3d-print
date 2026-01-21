-- Migration: Add wallets reserved_credits column

BEGIN;

ALTER TABLE timrx_billing.wallets
ADD COLUMN IF NOT EXISTS reserved_credits INT NOT NULL DEFAULT 0;

COMMIT;
