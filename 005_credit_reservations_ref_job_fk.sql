-- Migration: Add credit_reservations ref_job_id FK (not valid)

BEGIN;

ALTER TABLE timrx_billing.credit_reservations
DROP CONSTRAINT IF EXISTS fk_credit_reservations_ref_job;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_reservations_ref_job'
      AND conrelid = 'timrx_billing.credit_reservations'::regclass
  ) THEN
    ALTER TABLE timrx_billing.credit_reservations
      ADD CONSTRAINT fk_reservations_ref_job
      FOREIGN KEY (ref_job_id) REFERENCES timrx_billing.jobs(id)
      ON DELETE SET NULL
      NOT VALID;
  END IF;
END;
$$;

COMMIT;
