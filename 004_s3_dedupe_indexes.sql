-- Migration: Add S3 dedupe unique indexes

BEGIN;

DROP INDEX IF EXISTS timrx_app.uq_models_s3_glb_key;
DROP INDEX IF EXISTS timrx_app.uq_models_s3_thumbnail_key;
DROP INDEX IF EXISTS timrx_app.uq_images_s3_image_key;
DROP INDEX IF EXISTS timrx_app.uq_images_s3_thumbnail_key;
DROP INDEX IF EXISTS timrx_app.uq_images_s3_source_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_models_glb_s3_key
ON timrx_app.models(s3_bucket, glb_s3_key)
WHERE s3_bucket IS NOT NULL AND glb_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_models_thumb_s3_key
ON timrx_app.models(s3_bucket, thumbnail_s3_key)
WHERE s3_bucket IS NOT NULL AND thumbnail_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_image_s3_key
ON timrx_app.images(s3_bucket, image_s3_key)
WHERE s3_bucket IS NOT NULL AND image_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_thumb_s3_key
ON timrx_app.images(s3_bucket, thumbnail_s3_key)
WHERE s3_bucket IS NOT NULL AND thumbnail_s3_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_images_source_s3_key
ON timrx_app.images(s3_bucket, source_s3_key)
WHERE s3_bucket IS NOT NULL AND source_s3_key IS NOT NULL;

COMMIT;
