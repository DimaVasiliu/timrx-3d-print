-- Migration 083: Library Phase 2 — favorites, tags, collections
--
-- Backing store for the three per-user organisation features on the unified
-- My Assets library:
--
--   * favorites   — a star on a card, one bit per (user, asset)
--   * tags        — free-text labels the user types, many per asset
--   * collections — named, ordered buckets ("Client X", "Print queue")
--
-- All three hang off timrx_app.history_items, which is the single row that
-- represents an asset regardless of whether the payload lives in models,
-- images or videos. Anchoring here (rather than on the three asset tables)
-- means the library can favourite/tag/collect any asset type with one code
-- path, and a user's organisation survives a model being re-textured.
--
-- Ownership is denormalised onto every table as identity_id. history_items
-- keeps identity_id ON DELETE SET NULL (an asset can outlive its account),
-- but organisation data is meaningless without its owner, so these cascade.
--
-- Idempotent: safe to run more than once.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Favorites
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS timrx_app.asset_favorites (
  identity_id  UUID        NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  history_id   UUID        NOT NULL REFERENCES timrx_app.history_items(id)  ON DELETE CASCADE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (identity_id, history_id)
);

-- "Show me my favourites, newest first" — the only read path that matters.
CREATE INDEX IF NOT EXISTS idx_asset_favorites_identity_created
  ON timrx_app.asset_favorites (identity_id, created_at DESC);

-- Reverse lookup: when rendering a page of assets we ask "which of these are
-- starred", so history_id needs its own entry point.
CREATE INDEX IF NOT EXISTS idx_asset_favorites_history
  ON timrx_app.asset_favorites (history_id);

-- ---------------------------------------------------------------------------
-- 2. Tags
-- ---------------------------------------------------------------------------
-- Tags are stored already-normalised (trimmed, lower-cased) in `tag`, with the
-- user's original casing kept in `label` for display. Normalising in the
-- column rather than in an expression index means the uniqueness guarantee is
-- real: "Fox", "fox " and "FOX" collapse to one tag per asset.
CREATE TABLE IF NOT EXISTS timrx_app.asset_tags (
  identity_id  UUID        NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  history_id   UUID        NOT NULL REFERENCES timrx_app.history_items(id)  ON DELETE CASCADE,
  tag          TEXT        NOT NULL,
  label        TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (identity_id, history_id, tag),
  CONSTRAINT ck_asset_tags_normalised CHECK (tag = lower(btrim(tag))),
  CONSTRAINT ck_asset_tags_length     CHECK (char_length(tag) BETWEEN 1 AND 48)
);

-- "Show everything tagged X" and the tag autocomplete both key on (owner, tag).
CREATE INDEX IF NOT EXISTS idx_asset_tags_identity_tag
  ON timrx_app.asset_tags (identity_id, tag);

CREATE INDEX IF NOT EXISTS idx_asset_tags_history
  ON timrx_app.asset_tags (history_id);

-- ---------------------------------------------------------------------------
-- 3. Collections
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS timrx_app.asset_collections (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id  UUID        NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  name         TEXT        NOT NULL,
  -- Optional accent so the library can colour-code collection chips.
  color        TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT ck_asset_collections_name_length CHECK (char_length(btrim(name)) BETWEEN 1 AND 64)
);

-- One "Print queue" per user, however they capitalise it on the day.
CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_collections_identity_name
  ON timrx_app.asset_collections (identity_id, lower(btrim(name)));

CREATE INDEX IF NOT EXISTS idx_asset_collections_identity_created
  ON timrx_app.asset_collections (identity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS timrx_app.asset_collection_items (
  collection_id UUID        NOT NULL REFERENCES timrx_app.asset_collections(id) ON DELETE CASCADE,
  history_id    UUID        NOT NULL REFERENCES timrx_app.history_items(id)     ON DELETE CASCADE,
  -- Manual ordering inside a collection; ties break on added_at.
  position      INTEGER     NOT NULL DEFAULT 0,
  added_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (collection_id, history_id)
);

CREATE INDEX IF NOT EXISTS idx_asset_collection_items_order
  ON timrx_app.asset_collection_items (collection_id, position, added_at DESC);

-- "Which collections is this asset in?" — shown on the card's menu.
CREATE INDEX IF NOT EXISTS idx_asset_collection_items_history
  ON timrx_app.asset_collection_items (history_id);

-- Keep updated_at honest so the library can sort collections by recent use.
CREATE OR REPLACE FUNCTION timrx_app.touch_asset_collection()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE timrx_app.asset_collections
     SET updated_at = NOW()
   WHERE id = COALESCE(NEW.collection_id, OLD.collection_id);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_asset_collection_items_touch ON timrx_app.asset_collection_items;
CREATE TRIGGER trg_asset_collection_items_touch
  AFTER INSERT OR DELETE ON timrx_app.asset_collection_items
  FOR EACH ROW EXECUTE FUNCTION timrx_app.touch_asset_collection();

COMMIT;

-- ---------------------------------------------------------------------------
-- Verification — run after applying; every row should report ok = true.
-- ---------------------------------------------------------------------------
-- SELECT 'asset_favorites'        AS object, to_regclass('timrx_app.asset_favorites')        IS NOT NULL AS ok
-- UNION ALL SELECT 'asset_tags',            to_regclass('timrx_app.asset_tags')             IS NOT NULL
-- UNION ALL SELECT 'asset_collections',     to_regclass('timrx_app.asset_collections')      IS NOT NULL
-- UNION ALL SELECT 'asset_collection_items',to_regclass('timrx_app.asset_collection_items') IS NOT NULL;
