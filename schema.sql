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
  CONSTRAINT ck_identities_email_lowercase
    CHECK (email IS NULL OR email = lower(email))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_identities_email_lower
ON timrx_billing.identities (lower(email))
WHERE email IS NOT NULL;

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
 ('MESHY_TEXT_TO_3D', 20, 'meshy'),
 ('MESHY_REFINE',     10, 'meshy'),
 ('MESHY_RETEXTURE',  10, 'meshy'),
 ('MESHY_IMAGE_TO_3D',30, 'meshy'),
 ('OPENAI_IMAGE',     12, 'openai')
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

CREATE TABLE IF NOT EXISTS timrx_billing.purchases (
  id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id          UUID NOT NULL REFERENCES timrx_billing.identities(id) ON DELETE CASCADE,
  plan_id              UUID REFERENCES timrx_billing.plans(id) ON DELETE SET NULL,
  provider             TEXT NOT NULL DEFAULT 'stripe',
  provider_payment_id  TEXT NOT NULL,
  amount_gbp           NUMERIC(10,2) NOT NULL,
  currency             TEXT NOT NULL DEFAULT 'GBP',
  credits_granted      INT NOT NULL,
  status               TEXT NOT NULL DEFAULT 'pending',
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  paid_at              TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_purchases_provider_payment
ON timrx_billing.purchases(provider, provider_payment_id);

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
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_identity_created
ON timrx_billing.jobs(identity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_upstream
ON timrx_billing.jobs(provider, upstream_job_id);

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

INSERT INTO timrx_billing.plans(code, name, description, price_gbp, credit_grant, is_active)
VALUES
 ('starter_80',  'Starter',  'Try the tools. Great for a few generations.',  7.99,  80, TRUE),
 ('creator_300',  'Creator',  'Regular use. Better value bundle.',           19.99,  300, TRUE),
 ('studio_600',  'Studio',   'Heavy use. Best value.',                     34.99, 600, TRUE)
ON CONFLICT (code) DO NOTHING;

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

  meta              JSONB,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ
);

ALTER TABLE timrx_app.models
ADD COLUMN IF NOT EXISTS content_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_models_identity_created
ON timrx_app.models(identity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_models_upstream
ON timrx_app.models(provider, upstream_job_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_models_provider_upstream_job
ON timrx_app.models(provider, upstream_job_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_models_provider_content_hash
ON timrx_app.models(provider, content_hash)
WHERE content_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_models_glb_s3_key
ON timrx_app.models(s3_bucket, glb_s3_key)
WHERE s3_bucket IS NOT NULL AND glb_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_models_thumb_s3_key
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

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_provider_upstream_id
ON timrx_app.images(provider, upstream_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_provider_image_url_no_upstream
ON timrx_app.images(provider, image_url)
WHERE upstream_id IS NULL AND image_url IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_provider_content_hash
ON timrx_app.images(provider, content_hash)
WHERE content_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_image_s3_key
ON timrx_app.images(s3_bucket, image_s3_key)
WHERE s3_bucket IS NOT NULL AND image_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_thumb_s3_key
ON timrx_app.images(s3_bucket, thumbnail_s3_key)
WHERE s3_bucket IS NOT NULL AND thumbnail_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_source_s3_key
ON timrx_app.images(s3_bucket, source_s3_key)
WHERE s3_bucket IS NOT NULL AND source_s3_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS timrx_app.history_items (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id       UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,

  item_type         TEXT NOT NULL,
  model_id          UUID REFERENCES timrx_app.models(id) ON DELETE SET NULL,
  image_id          UUID REFERENCES timrx_app.images(id) ON DELETE SET NULL,

  title             TEXT,
  stage             TEXT,
  status            TEXT NOT NULL DEFAULT 'processing',

  prompt            TEXT,
  root_prompt       TEXT,

  thumbnail_url     TEXT,
  glb_url           TEXT,
  image_url         TEXT,

  prompt_fingerprint TEXT,
  payload           JSONB,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at        TIMESTAMPTZ,
  CONSTRAINT ck_history_items_exactly_one_asset
    CHECK (((model_id IS NOT NULL)::int + (image_id IS NOT NULL)::int) = 1)
);

CREATE TABLE IF NOT EXISTS timrx_app.asset_saves (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  provider      TEXT NOT NULL,
  asset_type    TEXT NOT NULL DEFAULT 'model',
  upstream_id   TEXT,
  canonical_url TEXT,
  stage         TEXT NOT NULL,
  saved_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE timrx_app.asset_saves
  ADD COLUMN IF NOT EXISTS asset_type TEXT NOT NULL DEFAULT 'model';
ALTER TABLE timrx_app.asset_saves
  ADD COLUMN IF NOT EXISTS canonical_url TEXT;
ALTER TABLE timrx_app.asset_saves
  ALTER COLUMN upstream_id DROP NOT NULL;
ALTER TABLE timrx_app.asset_saves
  DROP CONSTRAINT IF EXISTS asset_saves_provider_upstream_id_stage_key;

CREATE UNIQUE INDEX IF NOT EXISTS ux_asset_saves_provider_asset_upstream
ON timrx_app.asset_saves(provider, asset_type, upstream_id)
WHERE upstream_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_asset_saves_provider_asset_canonical
ON timrx_app.asset_saves(provider, asset_type, canonical_url)
WHERE canonical_url IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_history_identity_created
ON timrx_app.history_items(identity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_history_type_status
ON timrx_app.history_items(item_type, status, created_at DESC);

CREATE TABLE IF NOT EXISTS timrx_app.active_jobs (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  identity_id       UUID REFERENCES timrx_billing.identities(id) ON DELETE SET NULL,

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

DROP TRIGGER IF EXISTS trg_history_touch ON timrx_app.history_items;
CREATE TRIGGER trg_history_touch
BEFORE UPDATE ON timrx_app.history_items
FOR EACH ROW EXECUTE FUNCTION timrx_app.touch_updated_at();

DROP TRIGGER IF EXISTS trg_active_jobs_touch ON timrx_app.active_jobs;
CREATE TRIGGER trg_active_jobs_touch
BEFORE UPDATE ON timrx_app.active_jobs
FOR EACH ROW EXECUTE FUNCTION timrx_app.touch_updated_at();

COMMIT;
