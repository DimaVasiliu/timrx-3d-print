-- Homepage one-free-generation gate.
-- Stores hashed visitor signals only. Raw IP addresses/user agents are not
-- persisted; hashes exist solely for abuse prevention and free-trial limits.
--
-- Production note: application request handlers must not run DDL. Apply this
-- migration with the database migration/admin role before enabling
-- HOMEPAGE_FREE_ENABLED in production.

CREATE TABLE IF NOT EXISTS timrx_billing.free_generation_trials (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    identity_id uuid REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,
    anonymous_session_id text,
    ip_hash text,
    user_agent_hash text,
    generation_type text NOT NULL,
    prompt_hash text NOT NULL,
    job_id uuid,
    reservation_id uuid REFERENCES timrx_billing.credit_reservations(id) ON DELETE SET NULL,
    status text NOT NULL DEFAULT 'reserved',
    blocked_reason text,
    trial_credit_amount integer NOT NULL DEFAULT 0,
    trial_credit_type text NOT NULL DEFAULT 'general',
    credit_granted_at timestamptz,
    credit_reversed_at timestamptz,
    meta jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE timrx_billing.free_generation_trials
ADD COLUMN IF NOT EXISTS reservation_id uuid REFERENCES timrx_billing.credit_reservations(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_free_trial_identity_once
ON timrx_billing.free_generation_trials(identity_id)
WHERE identity_id IS NOT NULL
  AND status IN ('reserved', 'started', 'completed');

CREATE UNIQUE INDEX IF NOT EXISTS uq_free_trial_session_once
ON timrx_billing.free_generation_trials(anonymous_session_id)
WHERE anonymous_session_id IS NOT NULL
  AND status IN ('reserved', 'started', 'completed');

CREATE UNIQUE INDEX IF NOT EXISTS uq_free_trial_fingerprint_once
ON timrx_billing.free_generation_trials(ip_hash, user_agent_hash)
WHERE ip_hash IS NOT NULL
  AND user_agent_hash IS NOT NULL
  AND status IN ('reserved', 'started', 'completed');

CREATE INDEX IF NOT EXISTS idx_free_trial_job
ON timrx_billing.free_generation_trials(job_id)
WHERE job_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_free_trial_reservation
ON timrx_billing.free_generation_trials(reservation_id)
WHERE reservation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_free_trial_created_status
ON timrx_billing.free_generation_trials(created_at, status);

CREATE INDEX IF NOT EXISTS idx_free_trial_ip_created
ON timrx_billing.free_generation_trials(ip_hash, created_at)
WHERE ip_hash IS NOT NULL;
