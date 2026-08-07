BEGIN;

DO $$
DECLARE
  asset_table TEXT;
BEGIN
  FOREACH asset_table IN ARRAY ARRAY['models', 'images', 'videos']
  LOOP
    EXECUTE format(
      'ALTER TABLE timrx_app.%I
         ADD COLUMN IF NOT EXISTS share_to_inspire BOOLEAN DEFAULT FALSE,
         ADD COLUMN IF NOT EXISTS inspire_status TEXT NOT NULL DEFAULT ''auto'',
         ADD COLUMN IF NOT EXISTS quality_score SMALLINT NOT NULL DEFAULT 0,
         ADD COLUMN IF NOT EXISTS moderation_reason TEXT,
         ADD COLUMN IF NOT EXISTS curated_at TIMESTAMPTZ,
         ADD COLUMN IF NOT EXISTS curated_by TEXT',
      asset_table
    );

    EXECUTE format(
      'CREATE INDEX IF NOT EXISTS %I
         ON timrx_app.%I (inspire_status, quality_score DESC, created_at DESC)
         WHERE share_to_inspire = TRUE',
      'idx_' || asset_table || '_inspire_curated',
      asset_table
    );
  END LOOP;
END $$;

COMMIT;
