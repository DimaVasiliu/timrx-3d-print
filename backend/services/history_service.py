"""
History Service - History Item Management
------------------------------------------
Migration Status: COMPLETE

This module owns:
- local history store (dev-only)
- history item validation and lookup
- normalized DB persistence for images/models/history
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any, Tuple
from urllib.parse import urlparse

import boto3

from backend.config import APP_SCHEMA, AWS_ACCESS_KEY_ID, AWS_BUCKET_MODELS, AWS_REGION, AWS_SECRET_ACCESS_KEY
from backend.db import USE_DB, get_conn, dict_row, Tables
from backend.services.meshy_service import _filter_model_urls
from backend.services.s3_service import (
    ensure_s3_url_for_data_uri,
    get_s3_key_from_url,
    is_s3_url,
    safe_upload_to_s3,
    collect_s3_keys,
)
from backend.utils import (
    build_canonical_url,
    derive_display_title,
    log_db_continue,
    normalize_epoch_ms,
    sanitize_filename,
    unpack_upload_result,
)


# Dedicated S3 client for cleanup operations
_s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# ─────────────────────────────────────────────────────────────
# Local dev history store (only when DB is disabled)
# ─────────────────────────────────────────────────────────────
APP_DIR = Path(__file__).resolve().parents[2]
HISTORY_STORE_PATH = APP_DIR / "history_store.json"
LOCAL_DEV_MODE = not USE_DB


def load_history_store() -> list:
    """
    DEV ONLY: Load history from local JSON file.
    In production, this returns empty (DB is the source of truth).
    """
    if not LOCAL_DEV_MODE:
        return []
    if not HISTORY_STORE_PATH.exists():
        return []
    try:
        data = json.loads(HISTORY_STORE_PATH.read_text(encoding="utf-8") or "[]")
        return data if isinstance(data, list) else []
    except Exception:
        return []


def get_canonical_model_row(
    identity_id: str | None,
    *,
    model_id: str | None = None,
    upstream_job_id: str | None = None,
    alt_upstream_job_id: str | None = None,
):
    """Return canonical model row for this identity (if any)."""
    if not USE_DB or not identity_id:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if model_id:
                    cur.execute(
                        f"""
                        SELECT id, title, glb_url, thumbnail_url, model_urls, textured_model_urls
                        FROM {Tables.MODELS}
                        WHERE id = %s AND identity_id = %s
                        LIMIT 1
                        """,
                        (model_id, identity_id),
                    )
                    row = cur.fetchone()
                    if row:
                        return row
                for candidate in (upstream_job_id, alt_upstream_job_id):
                    if not candidate:
                        continue
                    cur.execute(
                        f"""
                        SELECT id, title, glb_url, thumbnail_url, model_urls, textured_model_urls
                        FROM {Tables.MODELS}
                        WHERE identity_id = %s AND upstream_job_id = %s
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (identity_id, str(candidate)),
                    )
                    row = cur.fetchone()
                    if row:
                        return row
    except Exception as e:
        log_db_continue("get_canonical_model_row", e)
    return None


def get_canonical_image_row(
    identity_id: str | None,
    *,
    image_id: str | None = None,
    upstream_id: str | None = None,
    alt_upstream_id: str | None = None,
):
    """Return canonical image row for this identity (if any)."""
    if not USE_DB or not identity_id:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if image_id:
                    cur.execute(
                        f"""
                        SELECT id, title, image_url, thumbnail_url
                        FROM {Tables.IMAGES}
                        WHERE id = %s AND identity_id = %s
                        LIMIT 1
                        """,
                        (image_id, identity_id),
                    )
                    row = cur.fetchone()
                    if row:
                        return row
                for candidate in (upstream_id, alt_upstream_id):
                    if not candidate:
                        continue
                    cur.execute(
                        f"""
                        SELECT id, title, image_url, thumbnail_url
                        FROM {Tables.IMAGES}
                        WHERE identity_id = %s AND upstream_id = %s
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        (identity_id, str(candidate)),
                    )
                    row = cur.fetchone()
                    if row:
                        return row
    except Exception as e:
        log_db_continue("get_canonical_image_row", e)
    return None


def save_history_store(arr: list) -> None:
    """
    DEV ONLY: Save history to local JSON file.
    In production, this is a no-op.
    """
    if not LOCAL_DEV_MODE:
        return
    try:
        HISTORY_STORE_PATH.write_text(json.dumps(arr, ensure_ascii=False, indent=2))
    except Exception:
        pass


def _local_history_id(item: dict, fallback_id: str | None = None) -> str | None:
    """Pick a stable identifier for local history persistence."""
    if not isinstance(item, dict):
        return fallback_id
    return item.get("id") or item.get("job_id") or fallback_id


def upsert_history_local(item: dict, *, merge: bool = False) -> bool:
    """
    DEV ONLY: Persist a history item to the local JSON store.
    In production, this is a no-op.
    """
    if not LOCAL_DEV_MODE:
        return False
    try:
        item_id = _local_history_id(item)
        if not item_id:
            return False
        arr = load_history_store()
        if not isinstance(arr, list):
            arr = []
        updated = False
        for idx, existing in enumerate(arr):
            if not isinstance(existing, dict):
                continue
            if _local_history_id(existing) == item_id:
                arr[idx] = {**existing, **item} if merge else item
                updated = True
                break
        if not updated:
            arr.insert(0, item)
        save_history_store(arr)
        return True
    except Exception as e:
        print(f"[DEV] Failed to upsert local history: {e}")
        return False


def delete_history_local(item_id: str) -> bool:
    """
    DEV ONLY: Delete a history item from local JSON store.
    In production, this is a no-op.
    """
    if not LOCAL_DEV_MODE:
        return False
    try:
        arr = load_history_store()
        if not isinstance(arr, list):
            arr = []
        filtered = [
            x for x in arr
            if not (isinstance(x, dict) and _local_history_id(x) == item_id)
        ]
        save_history_store(filtered)
        return True
    except Exception as e:
        print(f"[DEV] Failed to delete local history item {item_id}: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# Helper mappings
# ─────────────────────────────────────────────────────────────

def _map_action_code(job_type: str) -> str:
    job = (job_type or "").lower()
    mapping = {
        "text-to-3d": "MESHY_TEXT_TO_3D",
        "text_to_3d": "MESHY_TEXT_TO_3D",
        "image-to-3d": "MESHY_IMAGE_TO_3D",
        "image_to_3d": "MESHY_IMAGE_TO_3D",
        "texture": "MESHY_RETEXTURE",
        "retexture": "MESHY_RETEXTURE",
        "remesh": "MESHY_REFINE",
        "rig": "MESHY_RIG",
        "rigging": "MESHY_RIG",
        "image": "OPENAI_IMAGE",
        "openai_image": "OPENAI_IMAGE",
    }
    if job in mapping:
        return mapping[job]
    if "image" in job:
        return "OPENAI_IMAGE"
    return "MESHY_TEXT_TO_3D"


def _map_provider(job_type: str) -> str:
    job = (job_type or "").lower()
    return "openai" if "image" in job else "meshy"


# ─────────────────────────────────────────────────────────────
# History validation helpers
# ─────────────────────────────────────────────────────────────

def _validate_history_item_asset_ids(model_id, image_id, context: str = "") -> bool:
    """Enforce XOR constraint on history_items model_id/image_id."""
    has_model = model_id is not None
    has_image = image_id is not None
    if has_model == has_image:
        print(f"[WARN] history_items XOR violation ({context}): model_id={model_id}, image_id={image_id}")
        return False
    return True


def _parse_s3_bucket_and_key(url: str) -> Tuple[str | None, str | None]:
    if not is_s3_url(url):
        return None, None
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        path = parsed.path.lstrip("/")

        if host.endswith(".amazonaws.com") and ".s3." in host:
            bucket = host.split(".s3.")[0]
            key = path
            return bucket, key if key else None

        if host.startswith("s3.") and host.endswith(".amazonaws.com"):
            parts = path.split("/", 1)
            if len(parts) >= 1:
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else None
                return bucket, key

        return None, None
    except Exception:
        return None, None


def _lookup_asset_id_for_history(
    cur,
    item_type: str,
    job_id: str,
    glb_url: str | None = None,
    image_url: str | None = None,
    user_id: str | None = None,
    provider: str | None = None,
):
    """
    Try to find an existing model_id or image_id for a history item.
    Returns (model_id, image_id, reason) tuple.
    """
    model_id = None
    image_id = None

    if item_type == "image":
        if job_id:
            prov = provider or "openai"
            cur.execute(
                f"SELECT id FROM {Tables.IMAGES} WHERE provider = %s AND upstream_id = %s",
                (prov, str(job_id)),
            )
            rows = cur.fetchall()
            if len(rows) == 1:
                image_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
            elif len(rows) > 1:
                ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                print(
                    f"[WARN] _lookup_asset_id_for_history: multiple images for provider={prov}, upstream_id={job_id}: {ids}"
                )
                return None, None, "ambiguous_asset_match"

        if not image_id and image_url:
            bucket, key = _parse_s3_bucket_and_key(image_url)
            if bucket and key:
                cur.execute(
                    f"SELECT id FROM {Tables.IMAGES} WHERE s3_bucket = %s AND image_s3_key = %s",
                    (bucket, key),
                )
                rows = cur.fetchall()
                if len(rows) == 1:
                    image_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
                elif len(rows) > 1:
                    ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                    print(
                        f"[WARN] _lookup_asset_id_for_history: multiple images for s3_bucket={bucket}, key={key}: {ids}"
                    )
                    return None, None, "ambiguous_asset_match"

        if not image_id and image_url:
            bucket, key = _parse_s3_bucket_and_key(image_url)
            if bucket and key:
                cur.execute(
                    f"SELECT id FROM {Tables.IMAGES} WHERE s3_bucket = %s AND thumbnail_s3_key = %s",
                    (bucket, key),
                )
                rows = cur.fetchall()
                if len(rows) == 1:
                    image_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
                elif len(rows) > 1:
                    ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                    print(
                        f"[WARN] _lookup_asset_id_for_history: multiple images for s3_bucket={bucket}, thumb_key={key}: {ids}"
                    )
                    return None, None, "ambiguous_asset_match"
    else:
        if job_id:
            prov = provider or "meshy"
            cur.execute(
                f"SELECT id FROM {Tables.MODELS} WHERE provider = %s AND upstream_job_id = %s",
                (prov, str(job_id)),
            )
            rows = cur.fetchall()
            if len(rows) == 1:
                model_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
            elif len(rows) > 1:
                ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                print(
                    f"[WARN] _lookup_asset_id_for_history: multiple models for provider={prov}, upstream_job_id={job_id}: {ids}"
                )
                return None, None, "ambiguous_asset_match"

        if not model_id and glb_url:
            bucket, key = _parse_s3_bucket_and_key(glb_url)
            if bucket and key:
                cur.execute(
                    f"SELECT id FROM {Tables.MODELS} WHERE s3_bucket = %s AND glb_s3_key = %s",
                    (bucket, key),
                )
                rows = cur.fetchall()
                if len(rows) == 1:
                    model_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
                elif len(rows) > 1:
                    ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                    print(
                        f"[WARN] _lookup_asset_id_for_history: multiple models for s3_bucket={bucket}, key={key}: {ids}"
                    )
                    return None, None, "ambiguous_asset_match"

    if model_id or image_id:
        return model_id, image_id, None
    return None, None, "missing_asset_reference"


# ─────────────────────────────────────────────────────────────
# Normalized DB persistence
# ─────────────────────────────────────────────────────────────

def save_image_to_normalized_db(
    image_id: str,
    image_url: str,
    prompt: str,
    ai_model: str,
    size: str,
    image_urls: list | None = None,
    user_id: str | None = None,
):
    if not USE_DB:
        print("[DB] USE_DB is False, skipping save_image_to_normalized_db")
        return False

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                existing_history_id = None
                if user_id:
                    cur.execute(
                        f"""
                        SELECT id, image_id FROM {Tables.HISTORY_ITEMS}
                        WHERE (payload->>'original_id' = %s OR id::text = %s) AND identity_id = %s
                        LIMIT 1
                        """,
                        (image_id, image_id, user_id),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT id, image_id FROM {Tables.HISTORY_ITEMS}
                        WHERE (payload->>'original_id' = %s OR id::text = %s) AND identity_id IS NULL
                        LIMIT 1
                        """,
                        (image_id, image_id),
                    )
                existing = cur.fetchone()
                if existing:
                    existing_history_id = str(existing[0])

                title = derive_display_title(prompt, None)
                s3_slug = sanitize_filename(title or prompt or "image") or "image"
                s3_slug = s3_slug[:60]
                s3_user_id = str(user_id) if user_id else "public"
                job_key = sanitize_filename(str(image_id)) or "image"
                image_key_base = f"images/{s3_user_id}/{job_key}/{s3_slug}"

                image_content_hash = None
                image_s3_key_from_upload = None
                image_reused = None
                original_image_url = image_url
                uploaded_url_cache: dict[str, str] = {}
                if image_url:
                    upload_result = safe_upload_to_s3(
                        image_url,
                        "image/png",
                        "images",
                        image_id,
                        user_id=user_id,
                        key_base=image_key_base,
                        return_hash=True,
                        provider="openai",
                    )
                    image_url, image_content_hash, image_s3_key_from_upload, image_reused = unpack_upload_result(upload_result)
                    if AWS_BUCKET_MODELS and image_url and not is_s3_url(image_url):
                        print(f"[WARN] canonical url is not S3: image_url={image_url[:80]}")
                        image_url = None
                    if original_image_url:
                        uploaded_url_cache[original_image_url] = image_url
                    if image_url:
                        uploaded_url_cache[image_url] = image_url
                if image_urls:
                    normalized_urls = []
                    for i, url in enumerate(image_urls):
                        if not url:
                            normalized_urls.append(url)
                            continue
                        if url in uploaded_url_cache:
                            normalized_urls.append(uploaded_url_cache[url])
                            continue
                        s3_url = safe_upload_to_s3(
                            url,
                            "image/png",
                            "images",
                            f"{image_id}_{i}",
                            user_id=user_id,
                            key_base=f"{image_key_base}_{i}",
                            provider="openai",
                        )
                        uploaded_url_cache[url] = s3_url
                        normalized_urls.append(s3_url)
                    image_urls = normalized_urls
                image_s3_key = image_s3_key_from_upload or get_s3_key_from_url(image_url)
                thumbnail_s3_key = get_s3_key_from_url(image_url)

                width, height = 1024, 1024
                if size and "x" in size:
                    parts = size.split("x")
                    try:
                        width, height = int(parts[0]), int(parts[1])
                    except ValueError:
                        pass

                history_uuid = existing_history_id or str(uuid.uuid4())
                image_uuid = str(uuid.uuid4())
                upstream_id = image_id if image_id else None

                payload = {
                    "original_id": image_id,
                    "ai_model": ai_model,
                    "size": size,
                    "image_urls": image_urls or [image_url],
                    "s3_bucket": AWS_BUCKET_MODELS if AWS_BUCKET_MODELS else None,
                    "image_url": image_url,
                    "thumbnail_url": image_url,
                }

                image_meta = json.dumps(
                    {
                        "prompt": prompt,
                        "ai_model": ai_model,
                        "size": size,
                        "format": "png",
                        "image_urls": image_urls or [image_url],
                    }
                )

                existing_by_hash_id = None
                if image_content_hash:
                    cur.execute(
                        f"""
                        SELECT id FROM {Tables.IMAGES}
                        WHERE provider = %s AND content_hash = %s
                        LIMIT 1
                        """,
                        ("openai", image_content_hash),
                    )
                    row = cur.fetchone()
                    if row:
                        existing_by_hash_id = row[0]

                s3_bucket = AWS_BUCKET_MODELS if AWS_BUCKET_MODELS else None
                if existing_by_hash_id:
                    cur.execute(
                        f"""
                        UPDATE {Tables.IMAGES}
                        SET identity_id = COALESCE(%s, identity_id),
                            title = CASE
                                WHEN %s IS NOT NULL
                                 AND %s <> ''
                                 AND %s NOT IN ('3D Model', 'Untitled')
                                THEN %s
                                ELSE title
                            END,
                            prompt = COALESCE(%s, prompt),
                            upstream_id = COALESCE(upstream_id, %s),
                            status = %s,
                            s3_bucket = COALESCE(%s, s3_bucket),
                            image_url = COALESCE(%s, image_url),
                            thumbnail_url = COALESCE(%s, thumbnail_url),
                            image_s3_key = COALESCE(%s, image_s3_key),
                            thumbnail_s3_key = COALESCE(%s, thumbnail_s3_key),
                            width = COALESCE(%s, width),
                            height = COALESCE(%s, height),
                            content_hash = COALESCE(%s, content_hash),
                            meta = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING id
                        """,
                        (
                            user_id,
                            title,
                            title,
                            title,
                            title,
                            prompt,
                            upstream_id,
                            "ready",
                            s3_bucket,
                            image_url,
                            image_url,
                            image_s3_key,
                            thumbnail_s3_key,
                            width,
                            height,
                            image_content_hash,
                            image_meta,
                            existing_by_hash_id,
                        ),
                    )
                elif upstream_id:
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.IMAGES} (
                            id, identity_id,
                            title, prompt,
                            provider, upstream_id, status,
                            s3_bucket,
                            image_url, thumbnail_url,
                            image_s3_key, thumbnail_s3_key,
                            width, height,
                            content_hash,
                            meta
                        ) VALUES (
                            %s, %s,
                            %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s,
                            %s, %s,
                            %s, %s,
                            %s,
                            %s
                        )
                    ON CONFLICT (provider, upstream_id) DO UPDATE
                    SET identity_id = COALESCE(EXCLUDED.identity_id, {Tables.IMAGES}.identity_id),
                        title = CASE
                            WHEN EXCLUDED.title IS NOT NULL
                             AND EXCLUDED.title <> ''
                             AND EXCLUDED.title NOT IN ('3D Model', 'Untitled')
                            THEN EXCLUDED.title
                            ELSE {Tables.IMAGES}.title
                        END,
                        prompt = COALESCE(EXCLUDED.prompt, {Tables.IMAGES}.prompt),
                        status = EXCLUDED.status,
                        s3_bucket = COALESCE(EXCLUDED.s3_bucket, {Tables.IMAGES}.s3_bucket),
                        image_url = COALESCE(EXCLUDED.image_url, {Tables.IMAGES}.image_url),
                        thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {Tables.IMAGES}.thumbnail_url),
                            image_s3_key = COALESCE(EXCLUDED.image_s3_key, {Tables.IMAGES}.image_s3_key),
                            thumbnail_s3_key = COALESCE(EXCLUDED.thumbnail_s3_key, {Tables.IMAGES}.thumbnail_s3_key),
                            width = COALESCE(EXCLUDED.width, {Tables.IMAGES}.width),
                            height = COALESCE(EXCLUDED.height, {Tables.IMAGES}.height),
                            content_hash = COALESCE(EXCLUDED.content_hash, {Tables.IMAGES}.content_hash),
                            meta = EXCLUDED.meta,
                            updated_at = NOW()
                        RETURNING id
                        """,
                        (
                            image_uuid,
                            user_id,
                            title,
                            prompt,
                            "openai",
                            upstream_id,
                            "ready",
                            s3_bucket,
                            image_url,
                            image_url,
                            image_s3_key,
                            thumbnail_s3_key,
                            width,
                            height,
                            image_content_hash,
                            image_meta,
                        ),
                    )
                elif image_url:
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.IMAGES} (
                            id, identity_id,
                            title, prompt,
                            provider, upstream_id, status,
                            image_url, thumbnail_url,
                            width, height,
                            content_hash,
                            meta
                        ) VALUES (
                            %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s,
                            %s,
                            %s
                        )
                    ON CONFLICT (provider, image_url) WHERE upstream_id IS NULL AND image_url IS NOT NULL DO UPDATE
                    SET identity_id = COALESCE(EXCLUDED.identity_id, {Tables.IMAGES}.identity_id),
                        title = CASE
                            WHEN EXCLUDED.title IS NOT NULL
                             AND EXCLUDED.title <> ''
                             AND EXCLUDED.title NOT IN ('3D Model', 'Untitled')
                            THEN EXCLUDED.title
                            ELSE {Tables.IMAGES}.title
                        END,
                        prompt = COALESCE(EXCLUDED.prompt, {Tables.IMAGES}.prompt),
                        status = EXCLUDED.status,
                        image_url = COALESCE(EXCLUDED.image_url, {Tables.IMAGES}.image_url),
                        thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {Tables.IMAGES}.thumbnail_url),
                            width = COALESCE(EXCLUDED.width, {Tables.IMAGES}.width),
                            height = COALESCE(EXCLUDED.height, {Tables.IMAGES}.height),
                            content_hash = COALESCE(EXCLUDED.content_hash, {Tables.IMAGES}.content_hash),
                            meta = EXCLUDED.meta,
                            updated_at = NOW()
                        RETURNING id
                        """,
                        (
                            image_uuid,
                            user_id,
                            title,
                            prompt,
                            "openai",
                            None,
                            "ready",
                            image_url,
                            image_url,
                            width,
                            height,
                            image_content_hash,
                            image_meta,
                        ),
                    )
                else:
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.IMAGES} (
                            id, identity_id,
                            title, prompt,
                            provider, upstream_id, status,
                            image_url, thumbnail_url,
                            width, height,
                            content_hash,
                            meta
                        ) VALUES (
                            %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s,
                            %s,
                            %s
                        )
                        RETURNING id
                        """,
                        (
                            image_uuid,
                            user_id,
                            title,
                            prompt,
                            "openai",
                            None,
                            "ready",
                            None,
                            None,
                            width,
                            height,
                            image_content_hash,
                            image_meta,
                        ),
                    )

                image_row = cur.fetchone()
                if not image_row:
                    raise RuntimeError("[DB] Failed to upsert images row (no id returned)")
                returned_image_id = image_row[0]
                print(f"[DB] image persisted: image_id={returned_image_id} image_url={image_url} thumb={image_url}")

                if existing_history_id:
                    cur.execute(
                        f"""
                        UPDATE {Tables.HISTORY_ITEMS}
                        SET item_type = %s,
                            status = COALESCE(%s, status),
                            stage = COALESCE(%s, stage),
                            title = CASE
                                WHEN %s IS NOT NULL
                                 AND %s <> ''
                                 AND %s NOT IN ('3D Model', 'Untitled')
                                THEN %s
                                ELSE title
                            END,
                            prompt = COALESCE(%s, prompt),
                            root_prompt = COALESCE(%s, root_prompt),
                            identity_id = COALESCE(%s, identity_id),
                            thumbnail_url = COALESCE(%s, thumbnail_url),
                            image_url = COALESCE(%s, image_url),
                            image_id = %s,
                            payload = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            "image",
                            "finished",
                            "image",
                            title,
                            title,
                            title,
                            title,
                            prompt,
                            None,
                            user_id,
                            image_url,
                            image_url,
                            returned_image_id,
                            json.dumps(payload),
                            history_uuid,
                        ),
                    )
                else:
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.HISTORY_ITEMS} (
                            id, identity_id, item_type, status, stage,
                            title, prompt, root_prompt,
                            thumbnail_url, image_url,
                            image_id,
                            payload
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s,
                            %s
                        )
                        ON CONFLICT (id) DO UPDATE
                        SET item_type = EXCLUDED.item_type,
                            status = COALESCE(EXCLUDED.status, {Tables.HISTORY_ITEMS}.status),
                            stage = COALESCE(EXCLUDED.stage, {Tables.HISTORY_ITEMS}.stage),
                            title = CASE
                                WHEN EXCLUDED.title IS NOT NULL
                                 AND EXCLUDED.title <> ''
                                 AND EXCLUDED.title NOT IN ('3D Model', 'Untitled')
                                THEN EXCLUDED.title
                                ELSE {Tables.HISTORY_ITEMS}.title
                            END,
                            prompt = COALESCE(EXCLUDED.prompt, {Tables.HISTORY_ITEMS}.prompt),
                            root_prompt = COALESCE(EXCLUDED.root_prompt, {Tables.HISTORY_ITEMS}.root_prompt),
                            identity_id = COALESCE(EXCLUDED.identity_id, {Tables.HISTORY_ITEMS}.identity_id),
                            thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {Tables.HISTORY_ITEMS}.thumbnail_url),
                            image_url = COALESCE(EXCLUDED.image_url, {Tables.HISTORY_ITEMS}.image_url),
                            image_id = COALESCE(EXCLUDED.image_id, {Tables.HISTORY_ITEMS}.image_id),
                            payload = EXCLUDED.payload,
                            updated_at = NOW()
                        """,
                        (
                            history_uuid,
                            user_id,
                            "image",
                            "finished",
                            "image",
                            title,
                            prompt,
                            None,
                            image_url,
                            image_url,
                            returned_image_id,
                            json.dumps(payload),
                        ),
                    )
            conn.commit()
        print(f"[DB] Saved image {image_id} -> {history_uuid} to normalized tables (user_id={user_id})")
        return returned_image_id
    except Exception as e:
        print(f"[DB] Failed to save image {image_id}: {e}")
        return None


def save_finished_job_to_normalized_db(job_id: str, status_data: dict, job_meta: dict, job_type: str = "model", user_id: str | None = None):
    """
    Save finished job data to normalized tables (history_items, models, images).
    """
    model_log_info = None
    image_log_info = None

    if not user_id:
        user_id = job_meta.get("identity_id") or job_meta.get("user_id")

    if not USE_DB:
        print("[DB] USE_DB is False, skipping save_finished_job_to_normalized_db")
        return False

    try:
        with get_conn() as conn:
            cur = conn.cursor(row_factory=dict_row)
            db_errors: list[dict[str, str]] = []
            glb_url = status_data.get("glb_url") or status_data.get("textured_glb_url")
            thumbnail_url = status_data.get("thumbnail_url")
            image_url = status_data.get("image_url")
            model_urls = _filter_model_urls(status_data.get("model_urls") or {})
            textured_model_urls = _filter_model_urls(status_data.get("textured_model_urls") or {})
            textured_glb_url = status_data.get("textured_glb_url")
            rigged_glb_url = status_data.get("rigged_character_glb_url")
            rigged_fbx_url = status_data.get("rigged_character_fbx_url")
            raw_texture_urls = status_data.get("texture_urls")

            texture_items: list[tuple[str, str]] = []
            if isinstance(raw_texture_urls, str):
                texture_items = [("texture", raw_texture_urls)]
            elif isinstance(raw_texture_urls, list):
                for idx, item in enumerate(raw_texture_urls):
                    label = f"texture_{idx}"
                    url = item
                    if isinstance(item, dict):
                        url = item.get("url") or item.get("href")
                    texture_items.append((label, url))
            elif isinstance(raw_texture_urls, dict):
                texture_items = list(raw_texture_urls.items())
            elif raw_texture_urls:
                texture_items = [("texture", raw_texture_urls)]

            normalized_textures = []
            for map_type, url in texture_items:
                if isinstance(url, dict):
                    url = url.get("url") or url.get("href")
                if isinstance(url, str) and url:
                    normalized_textures.append((str(map_type or "texture"), url))

            final_stage = status_data.get("stage") or job_meta.get("stage") or "preview"
            final_prompt = job_meta.get("prompt")
            root_prompt = job_meta.get("root_prompt") or final_prompt
            provider = _map_provider(job_type)
            s3_bucket = AWS_BUCKET_MODELS or None
            image_job_types = ("image", "image-studio", "openai-image", "image-gen", "openai_image")
            is_image_output = (
                (job_type or "").lower() in image_job_types
                or bool(status_data.get("image_url"))
                or (job_meta.get("stage") == "image")
                or (job_meta.get("item_type") == "image")
            )
            asset_type = "image" if is_image_output else "model"

            if user_id:
                cur.execute(
                    f"""
                    SELECT status, glb_url, image_url, item_type
                    FROM {Tables.HISTORY_ITEMS}
                    WHERE (id::text = %s OR payload->>'original_job_id' = %s)
                      AND (identity_id = %s OR identity_id IS NULL)
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (str(job_id), str(job_id), user_id),
                )
            else:
                cur.execute(
                    f"""
                    SELECT status, glb_url, image_url, item_type
                    FROM {Tables.HISTORY_ITEMS}
                    WHERE (id::text = %s OR payload->>'original_job_id' = %s)
                      AND identity_id IS NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (str(job_id), str(job_id)),
                )
            existing_history = cur.fetchone()
            if existing_history:
                asset_type = existing_history.get("item_type") or ("image" if job_type in ("image", "openai_image") else "model")
                asset_url = existing_history.get("image_url") if asset_type == "image" else existing_history.get("glb_url")
                if existing_history.get("status") == "finished" and is_s3_url(asset_url):
                    print(f"[DB] save_finished_job skipped: already finished with S3 (job_id={job_id}, stage={final_stage})")
                    cur.close()
                    return {"success": True, "db_ok": True, "skipped": True}

            try:
                cur.execute(
                    f"""
                    SELECT stage, canonical_url
                    FROM {APP_SCHEMA}.asset_saves
                    WHERE provider = %s AND asset_type = %s AND upstream_id = %s
                    LIMIT 1
                    """,
                    (provider, asset_type, str(job_id)),
                )
                existing_save = cur.fetchone()
                if existing_save and existing_save.get("stage") == final_stage and existing_save.get("canonical_url"):
                    print(
                        f"[DB] save_finished_job skipped: already saved (provider={provider}, job_id={job_id}, stage={final_stage})"
                    )
                    cur.close()
                    return {"success": True, "db_ok": len(db_errors) == 0, "db_errors": db_errors or None, "skipped": True}
            except Exception as e:
                log_db_continue("asset_saves_precheck", e)
                db_errors.append({"op": "asset_saves_precheck", "error": str(e)})
                existing_save = None

            s3_name = job_meta.get("prompt") or job_meta.get("title") or "model"
            s3_name_safe = sanitize_filename(s3_name) or "model"
            s3_name_safe = s3_name_safe[:80]
            job_key = str(job_id)
            s3_key_name = s3_name_safe

            print(f"[DB] save_finished_job: job_id={job_id}, job_type={job_type}")

            model_content_hash = None
            model_s3_key_from_upload = None
            model_reused = None
            thumbnail_content_hash = None
            thumbnail_s3_key_from_upload = None
            thumbnail_reused = None
            glb_candidate = textured_glb_url or glb_url or textured_model_urls.get("glb") or model_urls.get("glb")
            obj_candidate = textured_model_urls.get("obj") or model_urls.get("obj") or status_data.get("obj_url")
            primary_model_source = glb_candidate or obj_candidate or rigged_glb_url
            primary_content_type = "model/gltf-binary"
            if primary_model_source:
                from backend.utils import get_content_type_from_url

                primary_content_type = get_content_type_from_url(primary_model_source)
                if primary_content_type == "application/octet-stream":
                    primary_content_type = "model/gltf-binary"

            if primary_model_source:
                upload_result = safe_upload_to_s3(
                    primary_model_source,
                    primary_content_type,
                    "models",
                    s3_name_safe,
                    user_id=user_id,
                    return_hash=True,
                    provider=provider,
                )
                primary_glb_url, model_content_hash, model_s3_key_from_upload, model_reused = unpack_upload_result(upload_result)
                glb_url = primary_glb_url
                if primary_content_type.startswith("model/gltf"):
                    if textured_glb_url:
                        textured_glb_url = primary_glb_url
                    if rigged_glb_url:
                        rigged_glb_url = primary_glb_url
                else:
                    textured_glb_url = None
                    rigged_glb_url = None
            if thumbnail_url:
                upload_result = safe_upload_to_s3(
                    thumbnail_url,
                    "image/png",
                    "thumbnails",
                    s3_name_safe,
                    user_id=user_id,
                    infer_content_type=False,
                    provider=provider,
                    return_hash=True,
                )
                thumbnail_url, thumbnail_content_hash, thumbnail_s3_key_from_upload, thumbnail_reused = unpack_upload_result(
                    upload_result
                )
            rigged_fbx_url = None

            s3_glb_url = primary_glb_url if primary_model_source else None
            s3_thumbnail_url = thumbnail_url
            if AWS_BUCKET_MODELS and s3_glb_url and not is_s3_url(s3_glb_url):
                print(f"[WARN] canonical url is not S3: glb_url={s3_glb_url[:80]}")
                s3_glb_url = None
            if AWS_BUCKET_MODELS and s3_thumbnail_url and not is_s3_url(s3_thumbnail_url):
                print(f"[WARN] canonical url is not S3: thumbnail_url={s3_thumbnail_url[:80]}")
                s3_thumbnail_url = None
            final_glb_url = s3_glb_url
            final_thumbnail_url = s3_thumbnail_url

            texture_s3_urls: dict[str, str] = {}
            texture_urls: list[str] = []
            for idx, (map_type, url) in enumerate(normalized_textures):
                safe_map_type = sanitize_filename(map_type) or f"texture_{idx}"
                texture_key_base = f"textures/{job_key}/{s3_key_name}/{safe_map_type}"
                uploaded_url = safe_upload_to_s3(
                    url,
                    "image/png",
                    "textures",
                    f"{s3_name}_{safe_map_type}",
                    user_id=user_id,
                    key_base=texture_key_base,
                    infer_content_type=False,
                    provider=provider,
                )
                texture_s3_urls[safe_map_type] = uploaded_url
                if uploaded_url:
                    texture_urls.append(uploaded_url)

            model_urls_uploaded = {}
            textured_model_urls_uploaded = {}
            if final_glb_url:
                primary_ext = "obj" if primary_content_type == "model/obj" else "glb"
                model_urls_uploaded[primary_ext] = final_glb_url
                if textured_glb_url and primary_ext == "glb":
                    textured_model_urls_uploaded["glb"] = final_glb_url

            model_urls = model_urls_uploaded
            textured_model_urls = textured_model_urls_uploaded

            canonical_raw = final_glb_url if asset_type == "model" else (image_url or final_thumbnail_url)
            canonical_url = build_canonical_url(canonical_raw)

            try:
                cur.execute("SAVEPOINT asset_saves_upsert")
                cur.execute(
                    f"""
                    INSERT INTO {APP_SCHEMA}.asset_saves (
                        provider,
                        upstream_id,
                        asset_type,
                        stage,
                        canonical_url,
                        saved_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (provider, upstream_id, asset_type)
                    DO UPDATE
                    SET canonical_url = EXCLUDED.canonical_url,
                        stage = EXCLUDED.stage,
                        saved_at = NOW()
                    """,
                    (provider, str(job_id), asset_type, final_stage, canonical_url),
                )
                cur.execute("RELEASE SAVEPOINT asset_saves_upsert")
            except Exception as e:
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT asset_saves_upsert")
                    cur.execute("RELEASE SAVEPOINT asset_saves_upsert")
                except Exception:
                    pass
                log_db_continue("asset_saves_upsert", e)
                db_errors.append({"op": "asset_saves_upsert", "error": str(e)})

            item_type = "image" if is_image_output else "model"

            try:
                uuid.UUID(str(job_id))
                history_uuid = str(job_id)
                print(f"[DB] Using job_id as history UUID: {history_uuid}")
            except (ValueError, TypeError):
                history_uuid = str(uuid.uuid4())
                print(f"[DB] Generated new history UUID: {history_uuid} (job_id {job_id} not a valid UUID)")

            payload = {
                "original_job_id": job_id,
                "job_type": job_type,
                "root_prompt": job_meta.get("root_prompt") or job_meta.get("prompt"),
                "art_style": job_meta.get("art_style"),
                "ai_model": job_meta.get("model") or job_meta.get("ai_model"),
                "license": job_meta.get("license", "private"),
                "symmetry_mode": job_meta.get("symmetry_mode"),
                "is_a_t_pose": job_meta.get("is_a_t_pose", False),
                "batch_count": job_meta.get("batch_count", 1),
                "batch_slot": job_meta.get("batch_slot"),
                "batch_group_id": job_meta.get("batch_group_id"),
                "preview_task_id": job_meta.get("preview_task_id") or status_data.get("preview_task_id"),
                "source_task_id": job_meta.get("source_task_id"),
                "cover_image_url": status_data.get("cover_image_url"),
                "texture_prompt": job_meta.get("texture_prompt"),
                "s3_bucket": s3_bucket,
                "enable_pbr": job_meta.get("enable_pbr", False),
                "enable_original_uv": job_meta.get("enable_original_uv", False),
                "topology": job_meta.get("topology"),
                "target_polycount": job_meta.get("target_polycount"),
                "should_remesh": job_meta.get("should_remesh", False),
                "textured_glb_url": textured_glb_url,
                "rigged_character_glb_url": rigged_glb_url,
                "rigged_character_fbx_url": rigged_fbx_url,
                "texture_urls": texture_urls,
                "texture_s3_urls": texture_s3_urls,
                "model_urls": model_urls,
                "textured_model_urls": textured_model_urls,
            }
            if final_glb_url:
                payload["glb_url"] = final_glb_url
            if image_url:
                payload["image_url"] = image_url
            if final_thumbnail_url:
                payload["thumbnail_url"] = final_thumbnail_url

            final_title = derive_display_title(job_meta.get("prompt"), job_meta.get("title"))
            glb_s3_key = model_s3_key_from_upload or get_s3_key_from_url(final_glb_url)
            thumbnail_s3_key = thumbnail_s3_key_from_upload or get_s3_key_from_url(final_thumbnail_url)
            image_s3_key = None
            thumb_s3_key = None
            image_reused = None

            db_save_ok = True
            history_item_id = None
            model_id = None
            image_id = None
            try:
                cur.execute("SAVEPOINT normalized_save")
                if final_glb_url or model_urls or textured_model_urls or rigged_glb_url:
                    model_meta = {
                        "textured_glb_url": textured_glb_url,
                        "rigged_character_glb_url": rigged_glb_url,
                        "rigged_fbx_url": rigged_fbx_url,
                        "texture_urls": texture_urls,
                        "texture_s3_urls": texture_s3_urls,
                        "model_urls": model_urls,
                        "textured_model_urls": textured_model_urls,
                        "stage": final_stage,
                        "s3_bucket": s3_bucket,
                    }

                    existing_by_hash_id = None
                    if model_content_hash:
                        cur.execute(
                            f"""
                            SELECT id FROM {Tables.MODELS}
                            WHERE provider = %s AND content_hash = %s
                            LIMIT 1
                            """,
                            (provider, model_content_hash),
                        )
                        row = cur.fetchone()
                        if row:
                            existing_by_hash_id = row["id"]

                    model_row = None
                    cur.execute("SAVEPOINT model_upsert")
                    try:
                        if existing_by_hash_id:
                            cur.execute(
                                f"""
                                UPDATE {Tables.MODELS}
                                SET identity_id = COALESCE(%s, identity_id),
                                    title = CASE
                                        WHEN %s IS NOT NULL
                                         AND %s <> ''
                                         AND %s NOT IN ('3D Model', 'Untitled')
                                        THEN %s
                                        ELSE title
                                    END,
                                    prompt = COALESCE(%s, prompt),
                                    root_prompt = COALESCE(%s, root_prompt),
                                    upstream_job_id = COALESCE(upstream_job_id, %s),
                                    status = 'ready',
                                    s3_bucket = COALESCE(%s, s3_bucket),
                                    glb_url = %s,
                                    thumbnail_url = %s,
                                    glb_s3_key = COALESCE(%s, glb_s3_key),
                                    thumbnail_s3_key = COALESCE(%s, thumbnail_s3_key),
                                    content_hash = COALESCE(%s, content_hash),
                                    stage = COALESCE(%s, stage),
                                    meta = COALESCE(%s, meta),
                                    updated_at = NOW()
                                WHERE id = %s
                                RETURNING id
                                """,
                                (
                                    user_id,
                                    final_title,
                                    final_title,
                                    final_title,
                                    final_title,
                                    final_prompt,
                                    root_prompt,
                                    job_id,
                                    s3_bucket,
                                    final_glb_url,
                                    final_thumbnail_url,
                                    glb_s3_key,
                                    thumbnail_s3_key,
                                    model_content_hash,
                                    final_stage,
                                    json.dumps(model_meta),
                                    existing_by_hash_id,
                                ),
                            )
                            model_row = cur.fetchone()
                        else:
                            cur.execute(
                                f"""
                                INSERT INTO {Tables.MODELS} (
                                    id, identity_id,
                                    title, prompt, root_prompt,
                                    provider, upstream_job_id,
                                    status,
                                    s3_bucket,
                                    glb_url, thumbnail_url,
                                    glb_s3_key, thumbnail_s3_key,
                                    content_hash,
                                    stage,
                                    meta
                                ) VALUES (
                                    %s, %s,
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
                                ON CONFLICT (provider, upstream_job_id) DO UPDATE
                                SET identity_id = COALESCE(EXCLUDED.identity_id, {Tables.MODELS}.identity_id),
                                    title = CASE
                                        WHEN EXCLUDED.title IS NOT NULL
                                         AND EXCLUDED.title <> ''
                                         AND EXCLUDED.title NOT IN ('3D Model', 'Untitled')
                                        THEN EXCLUDED.title
                                        ELSE {Tables.MODELS}.title
                                    END,
                                    prompt = COALESCE(EXCLUDED.prompt, {Tables.MODELS}.prompt),
                                    root_prompt = COALESCE(EXCLUDED.root_prompt, {Tables.MODELS}.root_prompt),
                                    status = 'ready',
                                    s3_bucket = COALESCE(EXCLUDED.s3_bucket, {Tables.MODELS}.s3_bucket),
                                    glb_url = EXCLUDED.glb_url,
                                    thumbnail_url = EXCLUDED.thumbnail_url,
                                    glb_s3_key = COALESCE(EXCLUDED.glb_s3_key, {Tables.MODELS}.glb_s3_key),
                                    thumbnail_s3_key = COALESCE(EXCLUDED.thumbnail_s3_key, {Tables.MODELS}.thumbnail_s3_key),
                                    content_hash = COALESCE(EXCLUDED.content_hash, {Tables.MODELS}.content_hash),
                                    stage = COALESCE(EXCLUDED.stage, {Tables.MODELS}.stage),
                                    meta = COALESCE(EXCLUDED.meta, {Tables.MODELS}.meta),
                                    updated_at = NOW()
                                RETURNING id
                                """,
                                (
                                    str(uuid.uuid4()),
                                    user_id,
                                    final_title,
                                    final_prompt,
                                    root_prompt,
                                    provider,
                                    job_id,
                                    "ready",
                                    s3_bucket,
                                    final_glb_url,
                                    final_thumbnail_url,
                                    glb_s3_key,
                                    thumbnail_s3_key,
                                    model_content_hash,
                                    final_stage,
                                    json.dumps(model_meta),
                                ),
                            )
                            model_row = cur.fetchone()
                    except Exception as e:
                        # Fallback for deployments missing the unique constraint used by ON CONFLICT
                        if "no unique or exclusion constraint" not in str(e).lower():
                            raise
                        cur.execute("ROLLBACK TO SAVEPOINT model_upsert")
                        print("[DB] models upsert fallback: missing unique constraint, using manual upsert")
                        cur.execute(
                            f"""
                            SELECT id FROM {Tables.MODELS}
                            WHERE provider = %s AND upstream_job_id = %s
                            LIMIT 1
                            """,
                            (provider, job_id),
                        )
                        existing_row = cur.fetchone()
                        if existing_row:
                            cur.execute(
                                f"""
                                UPDATE {Tables.MODELS}
                                SET identity_id = COALESCE(%s, identity_id),
                                    title = CASE
                                        WHEN %s IS NOT NULL
                                         AND %s <> ''
                                         AND %s NOT IN ('3D Model', 'Untitled')
                                        THEN %s
                                        ELSE title
                                    END,
                                    prompt = COALESCE(%s, prompt),
                                    root_prompt = COALESCE(%s, root_prompt),
                                    upstream_job_id = COALESCE(upstream_job_id, %s),
                                    status = 'ready',
                                    s3_bucket = COALESCE(%s, s3_bucket),
                                    glb_url = %s,
                                    thumbnail_url = %s,
                                    glb_s3_key = COALESCE(%s, glb_s3_key),
                                    thumbnail_s3_key = COALESCE(%s, thumbnail_s3_key),
                                    content_hash = COALESCE(%s, content_hash),
                                    stage = COALESCE(%s, stage),
                                    meta = COALESCE(%s, meta),
                                    updated_at = NOW()
                                WHERE id = %s
                                RETURNING id
                                """,
                                (
                                    user_id,
                                    final_title,
                                    final_title,
                                    final_title,
                                    final_title,
                                    final_prompt,
                                    root_prompt,
                                    job_id,
                                    s3_bucket,
                                    final_glb_url,
                                    final_thumbnail_url,
                                    glb_s3_key,
                                    thumbnail_s3_key,
                                    model_content_hash,
                                    final_stage,
                                    json.dumps(model_meta),
                                    existing_row["id"],
                                ),
                            )
                        else:
                            cur.execute(
                                f"""
                                INSERT INTO {Tables.MODELS} (
                                    id, identity_id,
                                    title, prompt, root_prompt,
                                    provider, upstream_job_id,
                                    status,
                                    s3_bucket,
                                    glb_url, thumbnail_url,
                                    glb_s3_key, thumbnail_s3_key,
                                    content_hash,
                                    stage,
                                    meta
                                ) VALUES (
                                    %s, %s,
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
                                RETURNING id
                                """,
                                (
                                    str(uuid.uuid4()),
                                    user_id,
                                    final_title,
                                    final_prompt,
                                    root_prompt,
                                    provider,
                                    job_id,
                                    "ready",
                                    s3_bucket,
                                    final_glb_url,
                                    final_thumbnail_url,
                                    glb_s3_key,
                                    thumbnail_s3_key,
                                    model_content_hash,
                                    final_stage,
                                    json.dumps(model_meta),
                                ),
                            )
                        model_row = cur.fetchone()
                    finally:
                        try:
                            cur.execute("RELEASE SAVEPOINT model_upsert")
                        except Exception:
                            pass
                    if not model_row:
                        raise RuntimeError("[DB] Failed to upsert model row (no id returned)")
                    model_id = model_row["id"]
                    if glb_s3_key:
                        model_log_info = {
                            "key": glb_s3_key,
                            "hash": model_content_hash,
                            "reused": bool(model_reused),
                        }

                if item_type == "image" and image_url:
                    image_meta = {
                        "stage": final_stage,
                        "job_type": job_type,
                        "s3_bucket": s3_bucket,
                    }
                    image_upstream_id = job_meta.get("image_id") or status_data.get("image_id") or str(job_id)
                    image_slug = sanitize_filename(final_title or final_prompt or "image") or "image"
                    image_slug = image_slug[:60]
                    image_user = str(user_id) if user_id else "public"
                    image_job_key = sanitize_filename(str(job_id)) or "job"
                    image_content_hash = None
                    image_s3_key_from_upload = None
                    image_reused = None
                    original_image_url = image_url
                    if image_url and not is_s3_url(image_url):
                        upload_result = safe_upload_to_s3(
                            image_url,
                            "image/png",
                            "images",
                            image_slug,
                            user_id=user_id,
                            key_base=f"images/{image_user}/{image_job_key}/{image_slug}",
                            return_hash=True,
                            provider=provider,
                        )
                        image_url, image_content_hash, image_s3_key_from_upload, image_reused = unpack_upload_result(
                            upload_result
                        )
                    if thumbnail_url and (thumbnail_url == original_image_url or thumbnail_url == image_url):
                        thumbnail_url = image_url
                    elif thumbnail_url and not is_s3_url(thumbnail_url):
                        thumbnail_url = safe_upload_to_s3(
                            thumbnail_url,
                            "image/png",
                            "thumbnails",
                            image_slug,
                            user_id=user_id,
                            key_base=f"thumbnails/{image_user}/{image_job_key}/{image_slug}",
                            provider=provider,
                        )
                    if AWS_BUCKET_MODELS and image_url and not is_s3_url(image_url):
                        print(f"[WARN] canonical url is not S3: image_url={image_url[:80]}")
                        image_url = None
                    if AWS_BUCKET_MODELS and thumbnail_url and not is_s3_url(thumbnail_url):
                        print(f"[WARN] canonical url is not S3: thumbnail_url={thumbnail_url[:80]}")
                        thumbnail_url = None
                    image_s3_key = image_s3_key_from_upload or get_s3_key_from_url(image_url)
                    thumb_s3_key = get_s3_key_from_url(thumbnail_url)
                    image_row = None
                    cur.execute("SAVEPOINT image_upsert")
                    try:
                        cur.execute(
                            f"""
                            INSERT INTO {Tables.IMAGES} (
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
                                %s, %s,
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
                            SET identity_id = COALESCE(EXCLUDED.identity_id, {Tables.IMAGES}.identity_id),
                                title = CASE
                                    WHEN EXCLUDED.title IS NOT NULL
                                     AND EXCLUDED.title <> ''
                                     AND EXCLUDED.title NOT IN ('3D Model', 'Untitled')
                                    THEN EXCLUDED.title
                                    ELSE {Tables.IMAGES}.title
                                END,
                                prompt = COALESCE(EXCLUDED.prompt, {Tables.IMAGES}.prompt),
                                status = 'ready',
                                s3_bucket = COALESCE(EXCLUDED.s3_bucket, {Tables.IMAGES}.s3_bucket),
                                image_url = COALESCE(EXCLUDED.image_url, {Tables.IMAGES}.image_url),
                                thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {Tables.IMAGES}.thumbnail_url),
                                image_s3_key = COALESCE(EXCLUDED.image_s3_key, {Tables.IMAGES}.image_s3_key),
                                thumbnail_s3_key = COALESCE(EXCLUDED.thumbnail_s3_key, {Tables.IMAGES}.thumbnail_s3_key),
                                content_hash = COALESCE(EXCLUDED.content_hash, {Tables.IMAGES}.content_hash),
                                meta = COALESCE(EXCLUDED.meta, {Tables.IMAGES}.meta),
                                updated_at = NOW()
                            RETURNING id
                            """,
                            (
                                str(uuid.uuid4()),
                                user_id,
                                final_title,
                                final_prompt,
                                provider,
                                image_upstream_id,
                                "ready",
                                AWS_BUCKET_MODELS,
                                image_url,
                                thumbnail_url,
                                image_s3_key,
                                thumb_s3_key,
                                image_content_hash,
                                json.dumps(image_meta),
                            ),
                        )
                        image_row = cur.fetchone()
                    except Exception as e:
                        if "no unique or exclusion constraint" not in str(e).lower():
                            raise
                        cur.execute("ROLLBACK TO SAVEPOINT image_upsert")
                        print("[DB] images upsert fallback: missing unique constraint, using manual upsert")
                        cur.execute(
                            f"""
                            SELECT id FROM {Tables.IMAGES}
                            WHERE provider = %s AND upstream_id = %s
                            LIMIT 1
                            """,
                            (provider, image_upstream_id),
                        )
                        existing_row = cur.fetchone()
                        if existing_row:
                            cur.execute(
                                f"""
                                UPDATE {Tables.IMAGES}
                                SET identity_id = COALESCE(%s, identity_id),
                                    title = CASE
                                        WHEN %s IS NOT NULL
                                         AND %s <> ''
                                         AND %s NOT IN ('3D Model', 'Untitled')
                                        THEN %s
                                        ELSE title
                                    END,
                                    prompt = COALESCE(%s, prompt),
                                    status = 'ready',
                                    s3_bucket = COALESCE(%s, s3_bucket),
                                    image_url = COALESCE(%s, image_url),
                                    thumbnail_url = COALESCE(%s, thumbnail_url),
                                    image_s3_key = COALESCE(%s, image_s3_key),
                                    thumbnail_s3_key = COALESCE(%s, thumbnail_s3_key),
                                    content_hash = COALESCE(%s, content_hash),
                                    meta = COALESCE(%s, meta),
                                    updated_at = NOW()
                                WHERE id = %s
                                RETURNING id
                                """,
                                (
                                    user_id,
                                    final_title,
                                    final_title,
                                    final_title,
                                    final_title,
                                    final_prompt,
                                    AWS_BUCKET_MODELS,
                                    image_url,
                                    thumbnail_url,
                                    image_s3_key,
                                    thumb_s3_key,
                                    image_content_hash,
                                    json.dumps(image_meta),
                                    existing_row["id"],
                                ),
                            )
                        else:
                            cur.execute(
                                f"""
                                INSERT INTO {Tables.IMAGES} (
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
                                    %s, %s,
                                    %s, %s,
                                    %s, %s,
                                    %s,
                                    %s,
                                    %s, %s,
                                    %s, %s,
                                    %s,
                                    %s
                                )
                                RETURNING id
                                """,
                                (
                                    str(uuid.uuid4()),
                                    user_id,
                                    final_title,
                                    final_prompt,
                                    provider,
                                    image_upstream_id,
                                    "ready",
                                    AWS_BUCKET_MODELS,
                                    image_url,
                                    thumbnail_url,
                                    image_s3_key,
                                    thumb_s3_key,
                                    image_content_hash,
                                    json.dumps(image_meta),
                                ),
                            )
                        image_row = cur.fetchone()
                    finally:
                        try:
                            cur.execute("RELEASE SAVEPOINT image_upsert")
                        except Exception:
                            pass
                    if not image_row:
                        raise RuntimeError("[DB] Failed to upsert image row (no id returned)")
                    image_id = image_row["id"]
                    if image_s3_key:
                        image_log_info = {
                            "key": image_s3_key,
                            "hash": image_content_hash,
                            "reused": bool(image_reused),
                        }
                cur.execute(
                    f"""
                    INSERT INTO {Tables.HISTORY_ITEMS} (
                        id, identity_id, item_type, status, stage,
                        title, prompt, root_prompt,
                        thumbnail_url, glb_url, image_url,
                        model_id, image_id,
                        payload
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s
                    )
                    ON CONFLICT (id) DO UPDATE
                    SET status = 'finished',
                        stage = EXCLUDED.stage,
                        title = CASE
                            WHEN EXCLUDED.title IS NOT NULL
                             AND EXCLUDED.title <> ''
                             AND EXCLUDED.title NOT IN ('3D Model', 'Untitled')
                            THEN EXCLUDED.title
                            ELSE {Tables.HISTORY_ITEMS}.title
                        END,
                        prompt = COALESCE(EXCLUDED.prompt, {Tables.HISTORY_ITEMS}.prompt),
                        root_prompt = COALESCE(EXCLUDED.root_prompt, {Tables.HISTORY_ITEMS}.root_prompt),
                        identity_id = COALESCE(EXCLUDED.identity_id, {Tables.HISTORY_ITEMS}.identity_id),
                        thumbnail_url = EXCLUDED.thumbnail_url,
                        glb_url = EXCLUDED.glb_url,
                        image_url = EXCLUDED.image_url,
                        model_id = COALESCE(EXCLUDED.model_id, {Tables.HISTORY_ITEMS}.model_id),
                        image_id = COALESCE(EXCLUDED.image_id, {Tables.HISTORY_ITEMS}.image_id),
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (
                        history_uuid,
                        user_id,
                        item_type,
                        "finished",
                        final_stage,
                        final_title,
                        final_prompt,
                        root_prompt,
                        final_thumbnail_url,
                        final_glb_url,
                        image_url if item_type == "image" else None,
                        model_id,
                        image_id,
                        json.dumps(payload),
                    ),
                )
                history_row = cur.fetchone()
                if not history_row:
                    raise RuntimeError("[DB] Failed to upsert history_items row (no id returned)")
                history_item_id = history_row["id"]
                cur.execute("RELEASE SAVEPOINT normalized_save")
            except Exception as e:
                db_save_ok = False
                db_errors.append({"op": "normalized_save", "error": str(e)})
                log_db_continue("normalized_save", e)
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT normalized_save")
                    cur.execute("RELEASE SAVEPOINT normalized_save")
                except Exception as rollback_err:
                    log_db_continue("normalized_save_rollback", rollback_err)
                cleanup_keys = []
                if model_reused is False and glb_s3_key:
                    cleanup_keys.append(glb_s3_key)
                if thumbnail_reused is False and thumbnail_s3_key:
                    cleanup_keys.append(thumbnail_s3_key)
                if image_reused is False and image_s3_key:
                    cleanup_keys.append(image_s3_key)
                if cleanup_keys and AWS_BUCKET_MODELS:
                    try:
                        _s3.delete_objects(
                            Bucket=AWS_BUCKET_MODELS,
                            Delete={"Objects": [{"Key": key} for key in cleanup_keys]},
                        )
                        print(f"[S3] cleanup: deleted {len(cleanup_keys)} objects after DB failure")
                    except Exception as cleanup_err:
                        print(f"[S3] cleanup failed: {cleanup_err}; keys={cleanup_keys}")
                else:
                    print(f"[S3] cleanup skipped; keys={cleanup_keys}")

            cur.close()
            conn.commit()
        if db_save_ok and history_item_id:
            if model_id:
                print(f"[DB] model persisted: model_id={model_id} glb_url={final_glb_url} history_item_id={history_item_id}")
            if image_id:
                print(f"[DB] image persisted: image_id={image_id} image_url={image_url} history_item_id={history_item_id}")
        if db_save_ok and model_log_info is not None:
            print(f"[S3] model stored: key={model_log_info['key']} hash={model_log_info['hash']} reused={model_log_info['reused']}")
        if db_save_ok and image_log_info is not None:
            print(f"[S3] image stored: key={image_log_info['key']} hash={image_log_info['hash']} reused={image_log_info['reused']}")
        if db_save_ok:
            print(f"[DB] Saved finished job {job_id} -> {history_uuid} to normalized tables")
        else:
            print(f"[DB] save_finished_job completed with db_ok=False job_id={job_id}")

        return {
            "success": True,
            "db_ok": db_save_ok and len(db_errors) == 0,
            "db_errors": db_errors or None,
            "glb_url": final_glb_url,
            "thumbnail_url": final_thumbnail_url,
            "textured_glb_url": textured_glb_url,
            "rigged_character_glb_url": rigged_glb_url,
            "rigged_character_fbx_url": rigged_fbx_url,
            "texture_urls": texture_urls,
            "texture_s3_urls": texture_s3_urls,
            "model_urls": model_urls,
            "textured_model_urls": textured_model_urls,
        }
    except Exception as e:
        print(f"[DB] Failed to save finished job {job_id}: {e}")
        final_glb_url = locals().get("final_glb_url")
        final_thumbnail_url = locals().get("final_thumbnail_url")
        image_url = locals().get("image_url")
        if final_glb_url and is_s3_url(final_glb_url):
            print(f"[DB] ERROR: S3 upload succeeded but DB save failed job_id={job_id} glb_url={final_glb_url}")
        if final_thumbnail_url and is_s3_url(final_thumbnail_url):
            print(f"[DB] ERROR: S3 upload succeeded but DB save failed job_id={job_id} thumbnail_url={final_thumbnail_url}")
        if image_url and is_s3_url(image_url):
            print(f"[DB] ERROR: S3 upload succeeded but DB save failed job_id={job_id} image_url={image_url}")
        return None


# Utility for history deletion cleanup (used by routes)

def delete_s3_objects(keys: list[str]) -> int:
    if not keys or not AWS_BUCKET_MODELS:
        return 0
    deleted = 0
    for i in range(0, len(keys), 1000):
        chunk = [{"Key": key} for key in keys[i : i + 1000] if key]
        if not chunk:
            continue
        resp = _s3.delete_objects(Bucket=AWS_BUCKET_MODELS, Delete={"Objects": chunk, "Quiet": True})
        deleted += len(resp.get("Deleted", []) or [])
        errs = resp.get("Errors") or []
        if errs:
            raise RuntimeError(f"S3 delete errors: {errs}")
    return deleted


__all__ = [
    "load_history_store",
    "save_history_store",
    "upsert_history_local",
    "delete_history_local",
    "_local_history_id",
    "_validate_history_item_asset_ids",
    "_lookup_asset_id_for_history",
    "save_image_to_normalized_db",
    "save_finished_job_to_normalized_db",
    "collect_s3_keys",
    "delete_s3_objects",
]
