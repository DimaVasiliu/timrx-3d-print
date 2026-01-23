-- ============================================================
-- MIGRATION: Create community_posts table
-- Run in: TablePlus or psql against your production database
-- ============================================================

BEGIN;

CREATE TABLE IF NOT EXISTS timrx_app.community_posts (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id     UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,

  model_id        UUID REFERENCES timrx_app.models(id) ON DELETE SET NULL,
  image_id        UUID REFERENCES timrx_app.images(id) ON DELETE SET NULL,
  history_item_id UUID REFERENCES timrx_app.history_items(id) ON DELETE SET NULL,

  display_name    TEXT NOT NULL,
  prompt_public   TEXT,
  show_prompt     BOOLEAN NOT NULL DEFAULT FALSE,

  status          TEXT NOT NULL DEFAULT 'published', -- 'published'|'hidden'|'deleted'
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at      TIMESTAMPTZ,

  CONSTRAINT ck_community_one_ref
    CHECK (
      (CASE WHEN model_id IS NULL THEN 0 ELSE 1 END) +
      (CASE WHEN image_id IS NULL THEN 0 ELSE 1 END) +
      (CASE WHEN history_item_id IS NULL THEN 0 ELSE 1 END)
      = 1
    )
);

CREATE INDEX IF NOT EXISTS idx_community_posts_created_at
  ON timrx_app.community_posts (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_community_posts_identity
  ON timrx_app.community_posts (identity_id);

CREATE INDEX IF NOT EXISTS idx_community_posts_status
  ON timrx_app.community_posts (status);

-- Auto-touch updated_at
CREATE OR REPLACE FUNCTION timrx_app.touch_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_community_posts_updated_at ON timrx_app.community_posts;
CREATE TRIGGER trg_community_posts_updated_at
BEFORE UPDATE ON timrx_app.community_posts
FOR EACH ROW EXECUTE PROCEDURE timrx_app.touch_updated_at();

COMMIT;

-- ============================================================
-- VERIFICATION: Run these queries to confirm the migration
-- ============================================================
-- SELECT table_name, column_name, data_type
-- FROM information_schema.columns
-- WHERE table_schema = 'timrx_app' AND table_name = 'community_posts';
--
-- SELECT conname, pg_get_constraintdef(oid)
-- FROM pg_constraint
-- WHERE conrelid = 'timrx_app.community_posts'::regclass;
