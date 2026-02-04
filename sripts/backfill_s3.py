#!/usr/bin/env python3
"""
Backfill history_items assets (models, images, videos) from ephemeral
provider URLs to permanent S3 storage, and populate the normalized
timrx_app.models / images / videos tables.

Usage:
    python scripts/backfill_s3.py                   # dry-run (list candidates)
    python scripts/backfill_s3.py --apply           # persist uploads + DB updates
    python scripts/backfill_s3.py --apply --limit 5 # first 5 rows per type
    python scripts/backfill_s3.py --videos-only     # only videos
"""
import argparse
import json
import os
import sys
from typing import Any

# Ensure the backend package is importable when running from scripts/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception as exc:
    print(f"[backfill_s3] ERROR: psycopg not available: {exc}")
    sys.exit(1)

try:
    from backend.config import AWS_BUCKET_MODELS
    from backend.utils import (
        get_content_type_from_url,
        sanitize_filename,
        unpack_upload_result,
    )
    from backend.services.s3_service import (
        get_s3_key_from_url,
        is_s3_url,
        safe_upload_to_s3,
    )
except Exception as exc:
    print(f"[backfill_s3] ERROR: cannot import backend helpers: {exc}")
    sys.exit(1)


APP_SCHEMA = os.getenv("APP_SCHEMA", "timrx_app")


MODEL_QUERY = f"""
SELECT id, identity_id, item_type, stage, title, prompt, root_prompt, glb_url, thumbnail_url, payload
FROM {APP_SCHEMA}.history_items
WHERE item_type = 'model'
  AND glb_url IS NOT NULL
  AND glb_url NOT LIKE '%amazonaws.com%'
ORDER BY created_at DESC
""".strip()

IMAGE_QUERY = f"""
SELECT id, identity_id, item_type, stage, title, prompt, root_prompt, image_url, thumbnail_url, payload
FROM {APP_SCHEMA}.history_items
WHERE item_type = 'image'
  AND image_url IS NOT NULL
  AND image_url NOT LIKE '%amazonaws.com%'
ORDER BY created_at DESC
""".strip()

VIDEO_QUERY = f"""
SELECT id, identity_id, item_type, stage, title, prompt, root_prompt, video_url, thumbnail_url, payload
FROM {APP_SCHEMA}.history_items
WHERE item_type = 'video'
  AND video_url IS NOT NULL
  AND video_url NOT LIKE '%amazonaws.com%'
ORDER BY created_at DESC
""".strip()


def load_payload(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            data = json.loads(value)
            if isinstance(data, dict):
                return data
        except Exception:
            return {}
    return {}


def slugify(title: str | None, prompt: str | None, fallback: str, job_id: str) -> str:
    base = title or prompt or f"{fallback}-{job_id[:8]}"
    slug = sanitize_filename(base) or f"{fallback}-{job_id[:8]}"
    return slug[:60]


def infer_provider_model(row: dict, payload: dict) -> str:
    if payload.get("provider"):
        return str(payload["provider"])
    url = row.get("glb_url") or ""
    if "meshy" in url:
        return "meshy"
    return "meshy"


def infer_provider_image(row: dict, payload: dict) -> str:
    if payload.get("provider"):
        return str(payload["provider"])
    url = row.get("image_url") or ""
    if "meshy" in url:
        return "meshy"
    if "openai" in url or "blob.core" in url:
        return "openai"
    job_type = (payload.get("job_type") or "").lower()
    if "openai" in job_type or "image" in job_type:
        return "openai"
    return "openai"


def infer_provider_video(row: dict, payload: dict) -> str:
    if payload.get("provider"):
        return str(payload["provider"])
    url = row.get("video_url") or ""
    if "runway" in url or "runwayml" in url:
        return "runway"
    if "cloudfront" in url:
        # Runway uses CloudFront for ephemeral output URLs
        return "runway"
    return "google"


def backfill_models(cur, rows: list[dict], apply: bool) -> None:
    total = 0
    for idx, row in enumerate(rows, start=1):
        if not row.get("glb_url") or is_s3_url(row.get("glb_url")):
            continue
        payload = load_payload(row.get("payload"))
        provider = infer_provider_model(row, payload)
        if provider != "meshy":
            continue

        job_id = str(row["id"])
        user_id = row.get("identity_id")
        slug = slugify(row.get("title") or payload.get("title"), row.get("prompt") or payload.get("prompt"), "model", job_id)
        glb_url = row.get("glb_url")
        thumb_url = row.get("thumbnail_url")
        content_type = get_content_type_from_url(glb_url)
        if content_type == "application/octet-stream":
            content_type = "model/gltf-binary"

        total += 1
        print(f"[backfill_s3] model {total}: job={job_id} url={glb_url[:80]}...")
        if not apply:
            continue

        savepoint = f"bf_model_{idx}"
        cur.execute(f"SAVEPOINT {savepoint}")
        try:
            upload_result = safe_upload_to_s3(
                glb_url,
                content_type,
                "models",
                slug,
                user_id=user_id,
                key_base=f"models/{job_id}/{slug}",
                return_hash=True,
                provider=provider,
            )
            glb_s3_url, content_hash, glb_s3_key_from_upload, _reused = unpack_upload_result(upload_result)
            if not is_s3_url(glb_s3_url):
                raise RuntimeError("model upload did not return S3 url")

            if thumb_url and not is_s3_url(thumb_url):
                thumb_url = safe_upload_to_s3(
                    thumb_url,
                    "image/png",
                    "thumbnails",
                    slug,
                    user_id=user_id,
                    key_base=f"thumbnails/{job_id}/{slug}",
                    provider=provider,
                )

            glb_s3_key = glb_s3_key_from_upload or get_s3_key_from_url(glb_s3_url)
            thumb_s3_key = get_s3_key_from_url(thumb_url)
            upstream_id = payload.get("upstream_id") or payload.get("original_job_id") or payload.get("job_id") or job_id
            s3_bucket = AWS_BUCKET_MODELS if AWS_BUCKET_MODELS else None

            model_meta = dict(payload) if isinstance(payload, dict) else {}
            model_meta.setdefault("backfill", True)
            model_meta["s3_bucket"] = s3_bucket
            model_meta["glb_url"] = glb_s3_url
            model_meta["thumbnail_url"] = thumb_url
            model_meta["glb_s3_key"] = glb_s3_key
            model_meta["thumbnail_s3_key"] = thumb_s3_key
            model_meta["slug"] = slug

            cur.execute(f"""
                INSERT INTO {APP_SCHEMA}.models (
                    id, identity_id,
                    title, prompt, root_prompt,
                    provider, upstream_id,
                    status,
                    s3_bucket,
                    glb_url, thumbnail_url,
                    glb_s3_key, thumbnail_s3_key,
                    content_hash,
                    stage,
                    meta
                ) VALUES (
                    gen_random_uuid(), %s,
                    %s, %s, %s,
                    %s, %s,
                    %s,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (provider, upstream_id) DO UPDATE
                SET identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.models.identity_id),
                    title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.models.title),
                    prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.models.prompt),
                    root_prompt = COALESCE(EXCLUDED.root_prompt, {APP_SCHEMA}.models.root_prompt),
                    status = 'ready',
                    s3_bucket = COALESCE(EXCLUDED.s3_bucket, {APP_SCHEMA}.models.s3_bucket),
                    glb_url = COALESCE(EXCLUDED.glb_url, {APP_SCHEMA}.models.glb_url),
                    thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.models.thumbnail_url),
                    glb_s3_key = COALESCE(EXCLUDED.glb_s3_key, {APP_SCHEMA}.models.glb_s3_key),
                    thumbnail_s3_key = COALESCE(EXCLUDED.thumbnail_s3_key, {APP_SCHEMA}.models.thumbnail_s3_key),
                    content_hash = COALESCE(EXCLUDED.content_hash, {APP_SCHEMA}.models.content_hash),
                    stage = COALESCE(EXCLUDED.stage, {APP_SCHEMA}.models.stage),
                    meta = COALESCE(EXCLUDED.meta, {APP_SCHEMA}.models.meta),
                    updated_at = NOW()
                RETURNING id
            """, (
                user_id,
                row.get("title"),
                row.get("prompt"),
                row.get("root_prompt"),
                provider,
                upstream_id,
                "ready",
                s3_bucket,
                glb_s3_url,
                thumb_url,
                glb_s3_key,
                thumb_s3_key,
                content_hash,
                row.get("stage"),
                json.dumps(model_meta),
            ))
            model_id = cur.fetchone()["id"]

            cur.execute(f"""
                UPDATE {APP_SCHEMA}.history_items
                SET glb_url = %s,
                    thumbnail_url = %s,
                    model_id = %s,
                    payload = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (glb_s3_url, thumb_url, model_id, json.dumps(model_meta), job_id))
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        except Exception as exc:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            print(f"[backfill_s3] model backfill failed for {job_id}: {exc}")


def backfill_images(cur, rows: list[dict], apply: bool) -> None:
    total = 0
    for idx, row in enumerate(rows, start=1):
        if not row.get("image_url") or is_s3_url(row.get("image_url")):
            continue
        payload = load_payload(row.get("payload"))
        provider = infer_provider_image(row, payload)

        job_id = str(row["id"])
        user_id = row.get("identity_id")
        slug = slugify(row.get("title") or payload.get("title"), row.get("prompt") or payload.get("prompt"), "image", job_id)
        image_url = row.get("image_url")
        thumb_url = row.get("thumbnail_url")
        content_type = get_content_type_from_url(image_url)
        if content_type == "application/octet-stream":
            content_type = "image/png"

        total += 1
        print(f"[backfill_s3] image {total}: job={job_id} url={image_url[:80]}...")
        if not apply:
            continue

        savepoint = f"bf_image_{idx}"
        cur.execute(f"SAVEPOINT {savepoint}")
        try:
            upload_result = safe_upload_to_s3(
                image_url,
                content_type,
                "images",
                slug,
                user_id=user_id,
                key_base=f"images/{job_id}/{slug}",
                return_hash=True,
                provider=provider,
            )
            image_s3_url, content_hash, image_s3_key_from_upload, _reused = unpack_upload_result(upload_result)
            if not is_s3_url(image_s3_url):
                raise RuntimeError("image upload did not return S3 url")

            if thumb_url and not is_s3_url(thumb_url):
                thumb_url = safe_upload_to_s3(
                    thumb_url,
                    "image/png",
                    "thumbnails",
                    slug,
                    user_id=user_id,
                    key_base=f"thumbnails/{job_id}/{slug}",
                    provider=provider,
                )

            image_s3_key = image_s3_key_from_upload or get_s3_key_from_url(image_s3_url)
            thumb_s3_key = get_s3_key_from_url(thumb_url)
            upstream_id = payload.get("upstream_id") or payload.get("original_id") or payload.get("image_id") or job_id
            s3_bucket = AWS_BUCKET_MODELS if AWS_BUCKET_MODELS else None

            image_meta = dict(payload) if isinstance(payload, dict) else {}
            image_meta.setdefault("backfill", True)
            image_meta["s3_bucket"] = s3_bucket
            image_meta["image_url"] = image_s3_url
            image_meta["thumbnail_url"] = thumb_url
            image_meta["image_s3_key"] = image_s3_key
            image_meta["thumbnail_s3_key"] = thumb_s3_key
            image_meta["slug"] = slug

            cur.execute(f"""
                INSERT INTO {APP_SCHEMA}.images (
                    id, identity_id,
                    title, prompt,
                    provider, upstream_id,
                    status,
                    s3_bucket,
                    image_url, thumbnail_url,
                    image_s3_key, thumbnail_s3_key,
                    content_hash,
                    meta
                ) VALUES (
                    gen_random_uuid(), %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s
                )
                ON CONFLICT (provider, upstream_id) DO UPDATE
                SET identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.images.identity_id),
                    title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.images.title),
                    prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.images.prompt),
                    status = 'ready',
                    s3_bucket = COALESCE(EXCLUDED.s3_bucket, {APP_SCHEMA}.images.s3_bucket),
                    image_url = COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.images.image_url),
                    thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.images.thumbnail_url),
                    image_s3_key = COALESCE(EXCLUDED.image_s3_key, {APP_SCHEMA}.images.image_s3_key),
                    thumbnail_s3_key = COALESCE(EXCLUDED.thumbnail_s3_key, {APP_SCHEMA}.images.thumbnail_s3_key),
                    content_hash = COALESCE(EXCLUDED.content_hash, {APP_SCHEMA}.images.content_hash),
                    meta = COALESCE(EXCLUDED.meta, {APP_SCHEMA}.images.meta),
                    updated_at = NOW()
                RETURNING id
            """, (
                user_id,
                row.get("title"),
                row.get("prompt"),
                provider,
                upstream_id,
                "ready",
                s3_bucket,
                image_s3_url,
                thumb_url,
                image_s3_key,
                thumb_s3_key,
                content_hash,
                json.dumps(image_meta),
            ))
            image_id = cur.fetchone()["id"]

            cur.execute(f"""
                UPDATE {APP_SCHEMA}.history_items
                SET image_url = %s,
                    thumbnail_url = %s,
                    image_id = %s,
                    payload = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (image_s3_url, thumb_url, image_id, json.dumps(image_meta), job_id))
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        except Exception as exc:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            print(f"[backfill_s3] image backfill failed for {job_id}: {exc}")


def backfill_videos(cur, rows: list[dict], apply: bool) -> None:
    total = 0
    for idx, row in enumerate(rows, start=1):
        if not row.get("video_url") or is_s3_url(row.get("video_url")):
            continue
        payload = load_payload(row.get("payload"))
        provider = infer_provider_video(row, payload)

        job_id = str(row["id"])
        user_id = row.get("identity_id")
        slug = slugify(
            row.get("title") or payload.get("title"),
            row.get("prompt") or payload.get("prompt"),
            "video",
            job_id,
        )
        video_url = row.get("video_url")
        thumb_url = row.get("thumbnail_url")
        content_type = get_content_type_from_url(video_url)
        if content_type == "application/octet-stream":
            content_type = "video/mp4"

        total += 1
        print(f"[backfill_s3] video {total}: job={job_id} provider={provider} url={video_url[:80]}...")
        if not apply:
            continue

        savepoint = f"bf_video_{idx}"
        cur.execute(f"SAVEPOINT {savepoint}")
        try:
            upload_result = safe_upload_to_s3(
                video_url,
                content_type,
                "videos",
                slug,
                user_id=user_id,
                key_base=f"videos/{provider}/{user_id or 'public'}/{job_id}",
                return_hash=True,
                provider=provider,
            )
            video_s3_url, content_hash, video_s3_key_from_upload, _reused = unpack_upload_result(upload_result)
            if not is_s3_url(video_s3_url):
                raise RuntimeError("video upload did not return S3 url")

            if thumb_url and not is_s3_url(thumb_url):
                thumb_url = safe_upload_to_s3(
                    thumb_url,
                    "image/jpeg",
                    "thumbnails",
                    slug,
                    user_id=user_id,
                    key_base=f"thumbnails/{user_id or 'public'}/{job_id}",
                    provider=provider,
                )

            video_s3_key = video_s3_key_from_upload or get_s3_key_from_url(video_s3_url)
            thumb_s3_key = get_s3_key_from_url(thumb_url)
            upstream_id = (
                payload.get("upstream_id")
                or payload.get("operation_name")
                or payload.get("task_id")
                or job_id
            )
            s3_bucket = AWS_BUCKET_MODELS if AWS_BUCKET_MODELS else None

            video_meta = dict(payload) if isinstance(payload, dict) else {}
            video_meta.setdefault("backfill", True)
            video_meta["s3_bucket"] = s3_bucket
            video_meta["video_url"] = video_s3_url
            video_meta["thumbnail_url"] = thumb_url
            video_meta["video_s3_key"] = video_s3_key
            video_meta["thumbnail_s3_key"] = thumb_s3_key
            video_meta["slug"] = slug

            cur.execute(f"""
                INSERT INTO {APP_SCHEMA}.videos (
                    id, identity_id,
                    title, prompt, root_prompt,
                    provider, upstream_id,
                    status,
                    s3_bucket,
                    video_url, thumbnail_url,
                    video_s3_key, thumbnail_s3_key,
                    content_hash,
                    duration_seconds, resolution, aspect_ratio,
                    mime_type,
                    meta
                ) VALUES (
                    gen_random_uuid(), %s,
                    %s, %s, %s,
                    %s, %s,
                    %s,
                    %s,
                    %s, %s,
                    %s, %s,
                    %s,
                    %s, %s, %s,
                    %s,
                    %s
                )
                ON CONFLICT (provider, upstream_id)
                WHERE upstream_id IS NOT NULL
                DO UPDATE
                SET identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.videos.identity_id),
                    title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.videos.title),
                    prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.videos.prompt),
                    root_prompt = COALESCE(EXCLUDED.root_prompt, {APP_SCHEMA}.videos.root_prompt),
                    status = 'ready',
                    s3_bucket = COALESCE(EXCLUDED.s3_bucket, {APP_SCHEMA}.videos.s3_bucket),
                    video_url = COALESCE(EXCLUDED.video_url, {APP_SCHEMA}.videos.video_url),
                    thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.videos.thumbnail_url),
                    video_s3_key = COALESCE(EXCLUDED.video_s3_key, {APP_SCHEMA}.videos.video_s3_key),
                    thumbnail_s3_key = COALESCE(EXCLUDED.thumbnail_s3_key, {APP_SCHEMA}.videos.thumbnail_s3_key),
                    content_hash = COALESCE(EXCLUDED.content_hash, {APP_SCHEMA}.videos.content_hash),
                    meta = COALESCE(EXCLUDED.meta, {APP_SCHEMA}.videos.meta),
                    updated_at = NOW()
                RETURNING id
            """, (
                user_id,
                row.get("title"),
                row.get("prompt"),
                row.get("root_prompt"),
                provider,
                upstream_id,
                "ready",
                s3_bucket,
                video_s3_url,
                thumb_url,
                video_s3_key,
                thumb_s3_key,
                content_hash,
                payload.get("duration_seconds"),
                payload.get("resolution"),
                payload.get("aspect_ratio"),
                content_type,
                json.dumps(video_meta),
            ))
            video_id = cur.fetchone()["id"]

            cur.execute(f"""
                UPDATE {APP_SCHEMA}.history_items
                SET video_url = %s,
                    thumbnail_url = %s,
                    video_id = %s,
                    payload = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (video_s3_url, thumb_url, video_id, json.dumps(video_meta), job_id))
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
        except Exception as exc:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
            cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            print(f"[backfill_s3] video backfill failed for {job_id}: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill history_items to S3 + normalized tables.")
    parser.add_argument("--apply", action="store_true", help="Perform uploads and DB updates.")
    parser.add_argument("--limit", type=int, default=0, help="Limit rows per pass (0 = no limit).")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--models-only", action="store_true", help="Only backfill models.")
    group.add_argument("--images-only", action="store_true", help="Only backfill images.")
    group.add_argument("--videos-only", action="store_true", help="Only backfill videos.")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        print("[backfill_s3] ERROR: DATABASE_URL not set")
        return 1
    if args.apply and not AWS_BUCKET_MODELS:
        print("[backfill_s3] ERROR: AWS_BUCKET_MODELS not configured")
        return 1

    try:
        from urllib.parse import urlparse
        parsed = urlparse(db_url)
        safe_netloc = parsed.hostname or parsed.netloc
        safe_db = (parsed.path or "").lstrip("/")
    except Exception:
        safe_netloc = "unknown"
        safe_db = "unknown"
    print(f"[backfill_s3] Target DB: host={safe_netloc} db={safe_db} schema={APP_SCHEMA}")
    print(f"[backfill_s3] S3 bucket={AWS_BUCKET_MODELS or 'unset'} apply={args.apply} limit={args.limit}")

    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO timrx_app, timrx_billing, public;")

    limit_clause = f" LIMIT {args.limit}" if args.limit else ""
    run_all = not (args.models_only or args.images_only or args.videos_only)

    with conn, conn.cursor(row_factory=dict_row) as cur:
        if run_all or args.models_only:
            cur.execute(MODEL_QUERY + limit_clause)
            model_rows = cur.fetchall()
            print(f"[backfill_s3] model candidates: {len(model_rows)}")
            backfill_models(cur, model_rows, args.apply)

        if run_all or args.images_only:
            cur.execute(IMAGE_QUERY + limit_clause)
            image_rows = cur.fetchall()
            print(f"[backfill_s3] image candidates: {len(image_rows)}")
            backfill_images(cur, image_rows, args.apply)

        if run_all or args.videos_only:
            cur.execute(VIDEO_QUERY + limit_clause)
            video_rows = cur.fetchall()
            print(f"[backfill_s3] video candidates: {len(video_rows)}")
            backfill_videos(cur, video_rows, args.apply)

    conn.close()
    if not args.apply:
        print("[backfill_s3] Dry-run complete. Re-run with --apply to persist changes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
