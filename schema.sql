BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- SCHEMAS
-- ============================================================
CREATE SCHEMA IF NOT EXISTS timrx_billing;
CREATE SCHEMA IF NOT EXISTS timrx_app;

-- ============================================================
-- BILLING / SAFETY TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS timrx_billing.identities (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email            TEXT,
  email_verified   BOOLEAN NOT NULL DEFAULT FALSE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at     TIMESTAMPTZ,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT ck_identities_email_lowercase
    CHECK (email IS NULL OR email = lower(email))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_identities_email_lower
ON timrx_billing.identities (lower(email))
WHERE email IS NOT NULL;

ALTER TABLE timrx_billing.identities
  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Merge support: points source identity → canonical identity after merge.
-- NULL = this identity is canonical (not merged).
ALTER TABLE timrx_billing.identities
  ADD COLUMN IF NOT EXISTS merged_into_id UUID REFERENCES timrx_billing.identities(id);

-- Append-only audit log for identity merges.
-- Records every merge event for traceability. Never deleted or updated.
CREATE TABLE IF NOT EXISTS timrx_billing.identity_merges (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_identity_id   UUID NOT NULL REFERENCES timrx_billing.identities(id),
  target_identity_id   UUID NOT NULL REFERENCES timrx_billing.identities(id),
  merged_by            TEXT NOT NULL DEFAULT 'system',
  merge_reason         TEXT,
  merge_mode           TEXT NOT NULL DEFAULT 'manual',
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata             JSONB DEFAULT '{}'::jsonb,
  CONSTRAINT ck_merge_no_self CHECK (source_identity_id != target_identity_id)
);
CREATE INDEX IF NOT EXISTS idx_identity_merges_source
  ON timrx_billing.identity_merges(source_identity_id);
CREATE INDEX IF NOT EXISTS idx_identity_merges_target
  ON timrx_billing.identity_merges(target_identity_id);
CREATE INDEX IF NOT EXISTS idx_identity_merges_created
  ON timrx_billing.identity_merges(created_at DESC);

CREATE TABLE IF NOT EXISTS timrx_billing.magic_codes (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email        TEXT NOT NULL,
  code_hash    TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at   TIMESTAMPTZ NOT NULL,
  attempts     INT NOT NULL DEFAULT 0,
  consumed     BOOLEAN NOT NULL DEFAULT FALSE,
  consumed_at  TIMESTAMPTZ,
  ip_hash      TEXT
);
CREATE INDEX IF NOT EXISTS idx_magic_codes_email_created
ON timrx_billing.magic_codes(email, created_at DESC);

-- Migration for existing magic_codes table (run if table already exists without these columns)
ALTER TABLE timrx_billing.magic_codes ADD COLUMN IF NOT EXISTS consumed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE timrx_billing.magic_codes ADD COLUMN IF NOT EXISTS ip_hash TEXT;

CREATE TABLE IF NOT EXISTS timrx_billing.sessions (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id      UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at       TIMESTAMPTZ NOT NULL,
  revoked_at       TIMESTAMPTZ,
  ip_hash          TEXT,
  user_agent_hash  TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_identity_active
ON timrx_billing.sessions(identity_id, expires_at)
WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS timrx_billing.plans (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code             TEXT NOT NULL UNIQUE,
  name             TEXT NOT NULL,
  description      TEXT,
  price_gbp        NUMERIC(10,2) NOT NULL,
  currency         TEXT NOT NULL DEFAULT 'GBP',
  credit_grant     INT NOT NULL,
  includes_priority BOOLEAN NOT NULL DEFAULT FALSE,
  is_active        BOOLEAN NOT NULL DEFAULT TRUE,
  meta             JSONB,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS timrx_billing.action_costs (
  action_code      TEXT PRIMARY KEY,
  cost_credits     INT NOT NULL,
  provider         TEXT NOT NULL,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO timrx_billing.action_costs(action_code, cost_credits, provider)
VALUES
 ('MESHY_TEXT_TO_3D',  20, 'meshy'),
 ('MESHY_IMAGE_TO_3D', 30, 'meshy'),
 ('MESHY_REFINE',      10, 'meshy'),
 ('MESHY_RETEXTURE',   15, 'meshy'),
 ('OPENAI_IMAGE',      10, 'openai'),
 ('OPENAI_IMAGE_2K',   15, 'openai'),
 ('OPENAI_IMAGE_4K',   20, 'openai'),
 ('GEMINI_IMAGE',      10, 'google'),
 ('GEMINI_IMAGE_2K',   15, 'google'),
 ('GEMINI_IMAGE_4K',   20, 'google'),
 ('VIDEO_GENERATE',    75, 'vertex'),
 ('VIDEO_TEXT_GENERATE', 75, 'vertex'),
 ('VIDEO_IMAGE_ANIMATE', 110, 'vertex'),
 ('MESHY_MULTI_COLOR_PRINT', 10, 'meshy')
ON CONFLICT (action_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS timrx_billing.wallets (
  identity_id      UUID PRIMARY KEY REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  balance_credits  INT NOT NULL DEFAULT 0,
  reserved_credits INT NOT NULL DEFAULT 0,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS timrx_billing.ledger_entries (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id      UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  entry_type       TEXT NOT NULL,
  amount_credits   INT NOT NULL,
  ref_type         TEXT,
  ref_id           TEXT,
  meta             JSONB,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ledger_identity_created
ON timrx_billing.ledger_entries(identity_id, created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_refund_once
ON timrx_billing.ledger_entries (ref_type, ref_id, entry_type)
WHERE entry_type = 'refund';
CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_refund_per_purchase
ON timrx_billing.ledger_entries (identity_id, ref_id)
WHERE entry_type = 'refund' AND ref_type = 'purchase' AND ref_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_chargeback_per_purchase
ON timrx_billing.ledger_entries (identity_id, ref_id)
WHERE entry_type = 'chargeback' AND ref_type = 'purchase' AND ref_id IS NOT NULL;

-- Unique index for charge idempotency: (identity_id, ref_type, ref_id)
-- Ensures the same (identity, action, job_id/upstream_id) combo can only be charged once
CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_charge_idempotency
ON timrx_billing.ledger_entries(identity_id, ref_type, ref_id)
WHERE ref_type IS NOT NULL AND ref_id IS NOT NULL AND entry_type = 'charge';

CREATE TABLE IF NOT EXISTS timrx_billing.purchases (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id          UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  plan_id              UUID REFERENCES timrx_billing.plans(id) ON DELETE SET NULL,
  provider             TEXT NOT NULL DEFAULT 'stripe',
  provider_payment_id  TEXT NOT NULL,
  payment_id           TEXT,
  amount_gbp           NUMERIC(10,2) NOT NULL,
  currency             TEXT NOT NULL DEFAULT 'GBP',
  credits_granted      INT NOT NULL,
  status               TEXT NOT NULL DEFAULT 'pending',
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  paid_at              TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_purchases_provider_payment
ON timrx_billing.purchases(provider, provider_payment_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_purchases_provider_provider_payment_id
ON timrx_billing.purchases(provider, provider_payment_id);
CREATE UNIQUE INDEX IF NOT EXISTS purchases_provider_payment_id_ux
ON timrx_billing.purchases (provider, payment_id)
WHERE payment_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_purchases_provider_payment_id
ON timrx_billing.purchases (provider, provider_payment_id);

ALTER TABLE timrx_billing.purchases
  ADD COLUMN IF NOT EXISTS payment_id TEXT;
ALTER TABLE timrx_billing.purchases
  ADD COLUMN IF NOT EXISTS meta JSONB DEFAULT '{}'::jsonb;

-- ============================================================
-- EMAIL OUTBOX (Durable email queue for guaranteed delivery)
-- ============================================================
CREATE TABLE IF NOT EXISTS timrx_billing.email_outbox (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id      UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,
  purchase_id      UUID REFERENCES timrx_billing.purchases(id) ON DELETE SET NULL,

  to_email         TEXT NOT NULL,
  template         TEXT NOT NULL,  -- 'purchase_receipt', 'invoice_with_pdf', 'magic_code', etc.
  subject          TEXT,
  payload          JSONB NOT NULL DEFAULT '{}',

  status           TEXT NOT NULL DEFAULT 'pending',  -- pending, sent, failed
  attempts         INT NOT NULL DEFAULT 0,
  max_attempts     INT NOT NULL DEFAULT 5,
  last_error       TEXT,
  last_attempt_at  TIMESTAMPTZ,

  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  sent_at          TIMESTAMPTZ,
  failed_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_email_outbox_status_created
ON timrx_billing.email_outbox(status, created_at)
WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_email_outbox_identity
ON timrx_billing.email_outbox(identity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_email_outbox_purchase
ON timrx_billing.email_outbox(purchase_id);

-- Add email_status to purchases for tracking
ALTER TABLE timrx_billing.purchases
  ADD COLUMN IF NOT EXISTS email_status TEXT DEFAULT 'pending';

-- ============================================================

CREATE TABLE IF NOT EXISTS timrx_billing.credit_reservations (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id      UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  action_code      TEXT NOT NULL REFERENCES timrx_billing.action_costs(action_code),
  cost_credits     INT NOT NULL,
  status           TEXT NOT NULL DEFAULT 'held',
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at       TIMESTAMPTZ NOT NULL DEFAULT (now() + interval '20 minutes'),
  captured_at      TIMESTAMPTZ,
  released_at      TIMESTAMPTZ,
  ref_job_id       UUID,
  meta             JSONB
);
CREATE INDEX IF NOT EXISTS idx_reservations_identity_status
ON timrx_billing.credit_reservations(identity_id, status);
CREATE INDEX IF NOT EXISTS idx_reservations_expires
ON timrx_billing.credit_reservations(status, expires_at);

CREATE TABLE IF NOT EXISTS timrx_billing.jobs (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id      UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  provider         TEXT NOT NULL,
  action_code      TEXT NOT NULL REFERENCES timrx_billing.action_costs(action_code),
  status           TEXT NOT NULL DEFAULT 'queued',
  cost_credits     INT NOT NULL,
  reservation_id   UUID REFERENCES timrx_billing.credit_reservations(id) ON DELETE SET NULL,
  upstream_job_id  TEXT,
  prompt           TEXT,
  meta             JSONB,
  error_message    TEXT,
  priority         TEXT NOT NULL DEFAULT 'normal',
  idempotency_key  TEXT,
  job_type         TEXT,
  result_refs      JSONB DEFAULT '{}'::jsonb,
  stage            TEXT,
  progress         INT NOT NULL DEFAULT 0,
  started_at       TIMESTAMPTZ,
  finished_at      TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Durable worker fields (migration 037)
  claimed_by          TEXT,
  claimed_at          TIMESTAMPTZ,
  heartbeat_at        TIMESTAMPTZ,
  attempt_count       INTEGER NOT NULL DEFAULT 0,
  next_poll_at        TIMESTAMPTZ,
  last_provider_status TEXT,
  last_error_code     TEXT,
  last_error_message  TEXT,
  result_url          TEXT,
  thumbnail_url       TEXT,
  completed_at        TIMESTAMPTZ,
  -- Generation timing (migration 059)
  generation_duration_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_jobs_identity_created
ON timrx_billing.jobs(identity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_upstream
ON timrx_billing.jobs(provider, upstream_job_id);
CREATE INDEX IF NOT EXISTS idx_jobs_priority_status
ON timrx_billing.jobs(priority, status)
WHERE priority = 'queued_daily';

-- Idempotency: prevent duplicate jobs for the same user action
CREATE UNIQUE INDEX IF NOT EXISTS uq_jobs_identity_idempotency
ON timrx_billing.jobs(identity_id, idempotency_key)
WHERE idempotency_key IS NOT NULL;

-- Active job queries: find queued/pending/processing jobs by identity
CREATE INDEX IF NOT EXISTS idx_jobs_identity_active_status
ON timrx_billing.jobs(identity_id, status, created_at DESC)
WHERE status IN ('queued', 'pending', 'processing');

-- Stale job recovery: find jobs that may need status refresh
CREATE INDEX IF NOT EXISTS idx_jobs_stale_recovery
ON timrx_billing.jobs(status, updated_at)
WHERE status IN ('pending', 'processing') AND upstream_job_id IS NOT NULL;

-- Durable worker: claim query (heartbeat expiry checked at query time, not in index)
CREATE INDEX IF NOT EXISTS idx_jobs_worker_claim
ON timrx_billing.jobs(created_at)
WHERE status IN ('queued', 'dispatched', 'provider_pending', 'provider_processing', 'stalled');

-- Durable worker: stall detection
CREATE INDEX IF NOT EXISTS idx_jobs_heartbeat_stale
ON timrx_billing.jobs(heartbeat_at)
WHERE status IN ('provider_pending', 'provider_processing', 'dispatched')
  AND claimed_by IS NOT NULL;

-- Durable worker: poll scheduling
CREATE INDEX IF NOT EXISTS idx_jobs_next_poll
ON timrx_billing.jobs(next_poll_at)
WHERE next_poll_at IS NOT NULL
  AND status IN ('provider_pending', 'provider_processing');

ALTER TABLE timrx_billing.credit_reservations
  ADD CONSTRAINT fk_reservations_ref_job
  FOREIGN KEY (ref_job_id) REFERENCES timrx_billing.jobs(id)
  ON DELETE SET NULL
  NOT VALID;

CREATE OR REPLACE FUNCTION timrx_billing.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_touch ON timrx_billing.jobs;
CREATE TRIGGER trg_jobs_touch
BEFORE UPDATE ON timrx_billing.jobs
FOR EACH ROW EXECUTE FUNCTION timrx_billing.touch_updated_at();

CREATE TABLE IF NOT EXISTS timrx_billing.daily_limits (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id         UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  day_utc             DATE NOT NULL,
  meshy_jobs          INT NOT NULL DEFAULT 0,
  openai_images       INT NOT NULL DEFAULT 0,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(identity_id, day_utc)
);

INSERT INTO timrx_billing.plans
  (code, name, description, price_gbp, currency, credit_grant, is_active, includes_priority, meta)
VALUES
  ('starter_80',  'Starter', 'Entry pack',  7.99, 'GBP',  80,  TRUE, FALSE, '{}'::jsonb),
  ('creator_300', 'Creator', 'Most popular', 19.99, 'GBP', 300, TRUE, FALSE, '{}'::jsonb),
  ('studio_600',  'Studio',  'Best value', 34.99, 'GBP', 600, TRUE, FALSE, '{}'::jsonb)
ON CONFLICT (code) DO UPDATE
SET
  name = EXCLUDED.name,
  description = EXCLUDED.description,
  price_gbp = EXCLUDED.price_gbp,
  currency = EXCLUDED.currency,
  credit_grant = EXCLUDED.credit_grant,
  is_active = EXCLUDED.is_active,
  includes_priority = EXCLUDED.includes_priority,
  meta = EXCLUDED.meta;

-- ============================================================
-- APP TABLES (MODELS / IMAGES / HISTORY)
-- ============================================================

CREATE TABLE IF NOT EXISTS timrx_app.models (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id       UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,

  title             TEXT,
  prompt            TEXT,
  root_prompt       TEXT,

  provider          TEXT NOT NULL DEFAULT 'meshy',
  upstream_job_id   TEXT,
  status            TEXT NOT NULL DEFAULT 'processing',
  error_message     TEXT,

  s3_bucket         TEXT,
  glb_s3_key        TEXT,
  thumbnail_s3_key  TEXT,
  glb_url           TEXT,
  thumbnail_url     TEXT,
  content_hash      TEXT,
  stage             TEXT,
  upstream_id       TEXT,

  meta              JSONB,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ
);

ALTER TABLE timrx_app.models
ADD COLUMN IF NOT EXISTS content_hash TEXT;
ALTER TABLE timrx_app.models
ADD COLUMN IF NOT EXISTS stage TEXT;
ALTER TABLE timrx_app.models
ADD COLUMN IF NOT EXISTS upstream_id TEXT;

CREATE INDEX IF NOT EXISTS idx_models_identity_created
ON timrx_app.models(identity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_models_upstream
ON timrx_app.models(provider, upstream_job_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_models_provider_upstream_job
ON timrx_app.models(provider, upstream_job_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_models_provider_content_hash
ON timrx_app.models(provider, content_hash)
WHERE content_hash IS NOT NULL;

ALTER TABLE timrx_app.models
ADD CONSTRAINT IF NOT EXISTS models_provider_upstream_id_uniq UNIQUE (provider, upstream_id);

CREATE UNIQUE INDEX IF NOT EXISTS ux_models_provider_upstream
ON timrx_app.models(provider, upstream_id)
WHERE upstream_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_models_provider_upstream_job
ON timrx_app.models(provider, upstream_job_id)
WHERE upstream_job_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_models_provider_upstream_job_stage
ON timrx_app.models(provider, upstream_job_id, stage)
WHERE upstream_job_id IS NOT NULL AND stage IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_models_glb_s3_key
ON timrx_app.models(s3_bucket, glb_s3_key)
WHERE s3_bucket IS NOT NULL AND glb_s3_key IS NOT NULL;

-- Non-unique: derived models (rig, animate, remesh) share parent thumbnails
CREATE INDEX IF NOT EXISTS idx_models_thumb_s3_key
ON timrx_app.models(s3_bucket, thumbnail_s3_key)
WHERE s3_bucket IS NOT NULL AND thumbnail_s3_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS timrx_app.images (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id       UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,

  title             TEXT,
  prompt            TEXT,

  provider          TEXT NOT NULL DEFAULT 'openai',
  upstream_id       TEXT,
  status            TEXT NOT NULL DEFAULT 'processing',
  error_message     TEXT,

  s3_bucket         TEXT,
  image_s3_key      TEXT,
  source_s3_key     TEXT,
  thumbnail_s3_key  TEXT,

  image_url         TEXT,
  thumbnail_url     TEXT,
  content_hash      TEXT,

  width             INT,
  height            INT,
  meta              JSONB,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ
);

ALTER TABLE timrx_app.images
ADD COLUMN IF NOT EXISTS content_hash TEXT;

ALTER TABLE timrx_app.images
ADD CONSTRAINT IF NOT EXISTS images_provider_upstream_id_uniq UNIQUE (provider, upstream_id);

CREATE INDEX IF NOT EXISTS idx_images_identity_created
ON timrx_app.images(identity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_images_upstream
ON timrx_app.images(provider, upstream_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_provider_image_url_no_upstream
ON timrx_app.images(provider, image_url)
WHERE upstream_id IS NULL AND image_url IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_provider_upstream
ON timrx_app.images(provider, upstream_id)
WHERE upstream_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_provider_url
ON timrx_app.images(provider, image_url)
WHERE upstream_id IS NULL AND image_url IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_images_provider_content_hash
ON timrx_app.images(provider, content_hash)
WHERE content_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_images_provider_hash
ON timrx_app.images(provider, content_hash)
WHERE content_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_images_provider_image_url
ON timrx_app.images(provider, image_url)
WHERE upstream_id IS NULL AND image_url IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_images_provider_upstream
ON timrx_app.images(provider, upstream_id)
WHERE upstream_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_image_s3_key
ON timrx_app.images(s3_bucket, image_s3_key)
WHERE s3_bucket IS NOT NULL AND image_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_thumb_s3_key
ON timrx_app.images(s3_bucket, thumbnail_s3_key)
WHERE s3_bucket IS NOT NULL AND thumbnail_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_source_s3_key
ON timrx_app.images(s3_bucket, source_s3_key)
WHERE s3_bucket IS NOT NULL AND source_s3_key IS NOT NULL;

-- Videos table for Veo/video generation
CREATE TABLE IF NOT EXISTS timrx_app.videos (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id       UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,

  title             TEXT,
  prompt            TEXT,

  provider          TEXT NOT NULL DEFAULT 'google',
  upstream_id       TEXT,
  status            TEXT NOT NULL DEFAULT 'processing',
  error_message     TEXT,

  s3_bucket         TEXT,
  video_s3_key      TEXT,
  thumbnail_s3_key  TEXT,

  video_url         TEXT,
  thumbnail_url     TEXT,
  content_hash      TEXT,

  duration_seconds  INT,
  resolution        TEXT,
  aspect_ratio      TEXT,
  mime_type         TEXT DEFAULT 'video/mp4',
  meta              JSONB,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_videos_identity_created
ON timrx_app.videos(identity_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_videos_provider_upstream
ON timrx_app.videos(provider, upstream_id)
WHERE upstream_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_videos_video_s3_key
ON timrx_app.videos(s3_bucket, video_s3_key)
WHERE s3_bucket IS NOT NULL AND video_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_videos_provider_content_hash
ON timrx_app.videos(provider, content_hash)
WHERE content_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS timrx_app.history_items (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id       UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,

  item_type         TEXT NOT NULL,
  model_id          UUID REFERENCES timrx_app.models(id) ON DELETE SET NULL,
  image_id          UUID REFERENCES timrx_app.images(id) ON DELETE SET NULL,
  video_id          UUID REFERENCES timrx_app.videos(id) ON DELETE SET NULL,

  title             TEXT,
  stage             TEXT,
  status            TEXT NOT NULL DEFAULT 'processing',

  prompt            TEXT,
  root_prompt       TEXT,

  thumbnail_url     TEXT,
  glb_url           TEXT,
  image_url         TEXT,
  video_url         TEXT,

  prompt_fingerprint TEXT,
  lineage_origin_id UUID REFERENCES timrx_app.history_items(id) ON DELETE SET NULL,
  payload           JSONB,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ
);

-- Add video_id column for existing tables (idempotent)
ALTER TABLE timrx_app.history_items
ADD COLUMN IF NOT EXISTS video_id UUID REFERENCES timrx_app.videos(id) ON DELETE SET NULL;

ALTER TABLE timrx_app.history_items
ADD COLUMN IF NOT EXISTS video_url TEXT;

-- Drop old constraints and add strict version
-- Enforce exactly one of model_id, image_id, or video_id at all times
ALTER TABLE timrx_app.history_items
DROP CONSTRAINT IF EXISTS ck_history_items_exactly_one_asset;

ALTER TABLE timrx_app.history_items
DROP CONSTRAINT IF EXISTS ck_history_items_one_asset_when_finished;

ALTER TABLE timrx_app.history_items
ADD CONSTRAINT history_items_exactly_one_item
CHECK (
  ((model_id IS NOT NULL)::int +
   (image_id IS NOT NULL)::int +
   (video_id IS NOT NULL)::int) = 1
);

CREATE TABLE IF NOT EXISTS timrx_app.asset_saves (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  provider      TEXT NOT NULL,
  upstream_id   TEXT NOT NULL,
  stage         TEXT NOT NULL,
  saved_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  asset_type    TEXT,
  canonical_url TEXT
);

ALTER TABLE timrx_app.asset_saves
  ADD COLUMN IF NOT EXISTS asset_type TEXT;
ALTER TABLE timrx_app.asset_saves
  ADD COLUMN IF NOT EXISTS canonical_url TEXT;
ALTER TABLE timrx_app.asset_saves
  DROP CONSTRAINT IF EXISTS asset_saves_provider_upstream_id_stage_key;

ALTER TABLE timrx_app.asset_saves
  ADD CONSTRAINT IF NOT EXISTS asset_saves_provider_upstream_asset_type_uniq
  UNIQUE (provider, upstream_id, asset_type);

ALTER TABLE timrx_app.asset_saves
  ADD CONSTRAINT IF NOT EXISTS asset_saves_provider_upstream_asset_type_uniq_idx
  UNIQUE (provider, upstream_id, asset_type);

CREATE INDEX IF NOT EXISTS idx_asset_saves_upstream
ON timrx_app.asset_saves(provider, upstream_id, stage);

CREATE INDEX IF NOT EXISTS idx_asset_saves_url
ON timrx_app.asset_saves(provider, asset_type, canonical_url);

CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_saves_provider_upstream_stage
ON timrx_app.asset_saves(provider, upstream_id, stage);

CREATE UNIQUE INDEX IF NOT EXISTS ux_asset_saves_upstream
ON timrx_app.asset_saves(provider, asset_type, upstream_id)
WHERE upstream_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_asset_saves_url
ON timrx_app.asset_saves(provider, asset_type, canonical_url)
WHERE canonical_url IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_history_identity_created
ON timrx_app.history_items(identity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_history_items_image_id
ON timrx_app.history_items(image_id);

CREATE INDEX IF NOT EXISTS idx_history_items_model_id
ON timrx_app.history_items(model_id);

CREATE INDEX IF NOT EXISTS idx_history_items_video_id
ON timrx_app.history_items(video_id);

CREATE INDEX IF NOT EXISTS idx_history_type_status
ON timrx_app.history_items(item_type, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_history_items_lineage_origin
ON timrx_app.history_items(lineage_origin_id);

CREATE TABLE IF NOT EXISTS timrx_app.active_jobs (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id       UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE RESTRICT,

  provider          TEXT NOT NULL,
  action_code       TEXT NOT NULL,
  upstream_job_id   TEXT,
  status            TEXT NOT NULL DEFAULT 'queued',
  progress          INT NOT NULL DEFAULT 0,

  related_model_id  UUID REFERENCES timrx_app.models(id) ON DELETE SET NULL,
  related_image_id  UUID REFERENCES timrx_app.images(id) ON DELETE SET NULL,
  related_history_id UUID REFERENCES timrx_app.history_items(id) ON DELETE SET NULL,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_active_jobs_identity
ON timrx_app.active_jobs(identity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS timrx_app.activity_logs (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id  UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,
  level        TEXT NOT NULL DEFAULT 'info',
  event        TEXT NOT NULL,
  meta         JSONB,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION timrx_app.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_models_touch ON timrx_app.models;
CREATE TRIGGER trg_models_touch
BEFORE UPDATE ON timrx_app.models
FOR EACH ROW EXECUTE FUNCTION timrx_app.touch_updated_at();

DROP TRIGGER IF EXISTS trg_images_touch ON timrx_app.images;
CREATE TRIGGER trg_images_touch
BEFORE UPDATE ON timrx_app.images
FOR EACH ROW EXECUTE FUNCTION timrx_app.touch_updated_at();

DROP TRIGGER IF EXISTS trg_videos_touch ON timrx_app.videos;
CREATE TRIGGER trg_videos_touch
BEFORE UPDATE ON timrx_app.videos
FOR EACH ROW EXECUTE FUNCTION timrx_app.touch_updated_at();

DROP TRIGGER IF EXISTS trg_history_touch ON timrx_app.history_items;
CREATE TRIGGER trg_history_touch
BEFORE UPDATE ON timrx_app.history_items
FOR EACH ROW EXECUTE FUNCTION timrx_app.touch_updated_at();

DROP TRIGGER IF EXISTS trg_active_jobs_touch ON timrx_app.active_jobs;
CREATE TRIGGER trg_active_jobs_touch
BEFORE UPDATE ON timrx_app.active_jobs
FOR EACH ROW EXECUTE FUNCTION timrx_app.touch_updated_at();

DROP VIEW IF EXISTS timrx_billing.v_credits_ledger;
CREATE VIEW timrx_billing.v_credits_ledger AS
SELECT
  le.id                 AS ledger_id,
  le.identity_id,
  le.entry_type         AS source,
  le.amount_credits     AS credits_delta,
  le.ref_type,
  le.ref_id,
  le.meta,
  le.created_at,
  p.id                  AS purchase_id,
  p.provider,
  p.provider_payment_id,
  p.payment_id,
  p.plan_id,
  p.status              AS purchase_status,
  p.amount_gbp,
  p.currency,
  p.credits_granted     AS purchase_credits,
  p.paid_at
FROM timrx_billing.ledger_entries le
LEFT JOIN timrx_billing.purchases p
  ON le.ref_type = 'purchase'
 AND le.ref_id IS NOT NULL
 AND p.id::text = le.ref_id
ORDER BY le.created_at DESC;

UPDATE timrx_billing.ledger_entries
SET ref_type = 'purchase'
WHERE ref_type = 'purchases';

-- ============================================================
-- COMMUNITY POSTS TABLE (For Community Feed Feature)
-- ============================================================

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
DROP TRIGGER IF EXISTS trg_community_posts_updated_at ON timrx_app.community_posts;
CREATE TRIGGER trg_community_posts_updated_at
BEFORE UPDATE ON timrx_app.community_posts
FOR EACH ROW EXECUTE PROCEDURE timrx_app.touch_updated_at();

COMMIT;
