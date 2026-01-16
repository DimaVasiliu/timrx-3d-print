-- Migration: Enforce history_items model/image XOR

BEGIN;

ALTER TABLE timrx_app.history_items
DROP CONSTRAINT IF EXISTS chk_history_items_model_or_image;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_history_items_exactly_one_asset'
      AND conrelid = 'timrx_app.history_items'::regclass
  ) THEN
    ALTER TABLE timrx_app.history_items
      ADD CONSTRAINT ck_history_items_exactly_one_asset
      CHECK (((model_id IS NOT NULL)::int + (image_id IS NOT NULL)::int) = 1);
  END IF;
END;
$$;

COMMIT;
