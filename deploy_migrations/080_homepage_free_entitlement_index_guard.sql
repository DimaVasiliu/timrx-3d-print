-- Guardrail for homepage free-generation entitlements.
--
-- The public homepage allows one free image, one free video, and one free 3D
-- generation per visitor. Older deployments briefly used global "once" indexes
-- that collapse those three entitlements into one. Keep this migration
-- idempotent so it can be run safely from TablePlus or deploy tooling.

BEGIN;

ALTER TABLE timrx_billing.free_generation_trials
    ADD COLUMN IF NOT EXISTS expires_at timestamptz;

DROP INDEX IF EXISTS timrx_billing.uq_free_trial_identity_once;
DROP INDEX IF EXISTS timrx_billing.uq_free_trial_session_once;
DROP INDEX IF EXISTS timrx_billing.uq_free_trial_fingerprint_once;

CREATE UNIQUE INDEX IF NOT EXISTS uq_free_trial_identity_type_once
ON timrx_billing.free_generation_trials(identity_id, generation_type)
WHERE identity_id IS NOT NULL
  AND status IN ('reserved', 'started', 'completed');

CREATE UNIQUE INDEX IF NOT EXISTS uq_free_trial_session_type_once
ON timrx_billing.free_generation_trials(anonymous_session_id, generation_type)
WHERE anonymous_session_id IS NOT NULL
  AND status IN ('reserved', 'started', 'completed');

CREATE UNIQUE INDEX IF NOT EXISTS uq_free_trial_fingerprint_type_once
ON timrx_billing.free_generation_trials(ip_hash, user_agent_hash, generation_type)
WHERE ip_hash IS NOT NULL
  AND user_agent_hash IS NOT NULL
  AND status IN ('reserved', 'started', 'completed');

CREATE INDEX IF NOT EXISTS idx_free_trial_attempt_limits
ON timrx_billing.free_generation_trials(ip_hash, generation_type, created_at);

CREATE INDEX IF NOT EXISTS idx_free_trial_expiry
ON timrx_billing.free_generation_trials(expires_at)
WHERE status = 'reserved' AND expires_at IS NOT NULL;

COMMIT;
