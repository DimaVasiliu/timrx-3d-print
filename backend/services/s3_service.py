"""
S3 service migrated from app.py.

This module provides a stable home for all S3 upload/download helpers without
changing the monolith yet. Routes can migrate gradually.
"""

from __future__ import annotations

import base64
import os
import uuid
from typing import Any
from urllib.parse import urlparse

import boto3
import requests
from botocore.exceptions import ClientError

from backend.config import config
from backend.utils import (
    compute_sha256,
    get_content_type_for_extension,
    get_content_type_from_url,
    get_extension_for_content_type,
    sanitize_filename,
    unpack_upload_result,
    wrap_upload_result,
)


# Create a dedicated S3 client for the modular service layer.
_s3 = boto3.client(
    "s3",
    region_name=config.AWS_REGION,
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
)


def build_hash_s3_key(prefix: str, provider: str | None, content_hash: str, content_type: str) -> str:
    """
    Build S3 key with content hash for deduplication.

    Key format: {prefix}/{provider}/{content_hash}{ext}
    Example: images/google/abc123def456.png

    IMPORTANT: Always pass the correct provider to organize assets properly:
    - "openai" for OpenAI/GPT images
    - "google" for Gemini Imagen/AI Studio images and videos
    - "vertex" for Vertex AI Veo videos
    - "runway" for Runway video generation
    - "meshy" for Meshy 3D models
    - "user" for user-uploaded source images
    """
    # Normalize provider - default to "unknown" if not provided
    # All video/image/3D providers must be listed here
    KNOWN_PROVIDERS = {"openai", "google", "vertex", "runway", "meshy", "user", "unknown"}
    raw_provider = (provider or "").lower().strip()
    safe_provider = sanitize_filename(raw_provider) if raw_provider else "unknown"

    # Warn if provider is not in the known list
    if safe_provider and safe_provider not in KNOWN_PROVIDERS:
        print(f"[S3] Warning: unrecognized provider '{provider}', using as-is: {safe_provider}")

    safe_provider = safe_provider or "unknown"

    if prefix == "models":
        ext = ".glb"
    else:
        ext = get_extension_for_content_type(content_type)
        if not ext and prefix in ("images", "thumbnails", "textures", "source_images"):
            ext = ".png"
    return f"{prefix}/{safe_provider}/{content_hash}{ext}"


def ensure_s3_key_ext(key: str, content_type: str) -> str:
    if not key:
        return key
    ext = os.path.splitext(key)[1]
    if ext:
        return key
    suffix = get_extension_for_content_type(content_type)
    return f"{key}{suffix}" if suffix else key


def build_s3_url(key: str) -> str:
    return f"https://{config.AWS_BUCKET_MODELS}.s3.{config.AWS_REGION}.amazonaws.com/{key}"


def s3_key_exists(key: str) -> bool:
    try:
        _s3.head_object(Bucket=config.AWS_BUCKET_MODELS, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def is_s3_url(url: str) -> bool:
    if not isinstance(url, str):
        return False
    return "s3." in url and "amazonaws.com" in url


def parse_s3_key(url: str) -> str | None:
    if not is_s3_url(url):
        return None
    try:
        parsed = urlparse(url)
        return parsed.path.lstrip("/") or None
    except Exception:
        return None


def get_s3_key_from_url(url: str) -> str | None:
    return parse_s3_key(url)


def presign_s3_key(key: str, expires_in: int = 3600) -> str | None:
    if not key or not config.AWS_BUCKET_MODELS:
        return None
    try:
        return _s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": config.AWS_BUCKET_MODELS, "Key": key},
            ExpiresIn=expires_in,
        )
    except Exception as e:
        print(f"[S3] Failed to presign key {key}: {e}")
        return None


def presign_s3_url(url: str, expires_in: int = 3600) -> str | None:
    key = parse_s3_key(url)
    if not key:
        return None
    return presign_s3_key(key, expires_in=expires_in)


def collect_s3_keys(history_row: dict) -> list[str]:
    """
    Collect S3 keys from a history row for deletion.

    Extracts keys from:
    - Direct URL fields: thumbnail_url, glb_url, image_url, video_url
    - Payload nested fields: model_urls, texture_urls, image_urls
    """
    keys: set[str] = set()
    if not isinstance(history_row, dict):
        return []

    # Include video_url for video assets
    for field in ("thumbnail_url", "glb_url", "image_url", "video_url"):
        key = parse_s3_key(history_row.get(field))
        if key:
            keys.add(key)

    payload = history_row.get("payload") or {}
    if isinstance(payload, str):
        try:
            import json

            payload = json.loads(payload)
        except Exception:
            payload = {}
    if isinstance(payload, dict):
        for key_name in ("model_urls", "texture_urls", "image_urls"):
            value = payload.get(key_name)
            if isinstance(value, dict):
                for entry in value.values():
                    s3_key = parse_s3_key(entry)
                    if s3_key:
                        keys.add(s3_key)
            elif isinstance(value, (list, tuple, set)):
                for entry in value:
                    s3_key = parse_s3_key(entry)
                    if s3_key:
                        keys.add(s3_key)
            else:
                s3_key = parse_s3_key(value)
                if s3_key:
                    keys.add(s3_key)

    return sorted(keys)


def upload_bytes_to_s3(
    data_bytes: bytes,
    content_type: str = "application/octet-stream",
    prefix: str = "models",
    name: str | None = None,
    user_id: str | None = None,
    key: str | None = None,
    return_hash: bool = False,
):
    if not config.AWS_BUCKET_MODELS:
        raise RuntimeError("AWS_BUCKET_MODELS not configured")

    if not key:
        if not user_id:
            raise ValueError("user_id required for S3 upload")
        ext = get_extension_for_content_type(content_type)
        unique_id = uuid.uuid4().hex[:12]
        if name:
            safe_name = sanitize_filename(name)
            key = (
                f"{prefix}/{user_id}/{safe_name}_{unique_id}{ext}"
                if safe_name
                else f"{prefix}/{user_id}/{unique_id}{ext}"
            )
        else:
            key = f"{prefix}/{user_id}/{unique_id}{ext}"
    else:
        key = ensure_s3_key_ext(key.lstrip("/"), content_type)

    content_hash = compute_sha256(data_bytes) if return_hash else None
    if s3_key_exists(key):
        s3_url = build_s3_url(key)
        # print(f"[S3] SKIP: Key exists -> {s3_url}")
        return wrap_upload_result(s3_url, content_hash, return_hash, s3_key=key, reused=True)

    # print(
    #     f"[S3] Uploading {len(data_bytes)} bytes to bucket={config.AWS_BUCKET_MODELS}, "
    #     f"key={key}, content_type={content_type}"
    # )
    _s3.put_object(
        Bucket=config.AWS_BUCKET_MODELS,
        Key=key,
        Body=data_bytes,
        ContentType=content_type,
    )
    s3_url = build_s3_url(key)
    # print(f"[S3] SUCCESS: Uploaded {len(data_bytes)} bytes -> {s3_url}")
    return wrap_upload_result(s3_url, content_hash, return_hash, s3_key=key, reused=False)


def upload_url_to_s3(
    url: str,
    content_type: str | None = None,
    prefix: str = "models",
    name: str | None = None,
    user_id: str | None = None,
    key: str | None = None,
    return_hash: bool = False,
):
    if key:
        key = ensure_s3_key_ext(key.lstrip("/"), content_type or "application/octet-stream")
        if s3_key_exists(key):
            s3_url = build_s3_url(key)
            # print(f"[S3] SKIP: Key exists -> {s3_url}")
            return wrap_upload_result(s3_url, None, return_hash, s3_key=key, reused=True)

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    ct = content_type or resp.headers.get("Content-Type", "application/octet-stream")
    return upload_bytes_to_s3(resp.content, ct, prefix, name, user_id, key=key, return_hash=return_hash)


def safe_upload_to_s3(
    url: str | dict,
    content_type: str,
    prefix: str,
    name: str | None = None,
    user_id: str | None = None,
    key: str | None = None,
    key_base: str | None = None,
    infer_content_type: bool = True,
    return_hash: bool = False,
    upstream_id: str | None = None,
    stage: str | None = None,
    provider: str | None = None,
):
    original_url = url
    if isinstance(url, dict):
        url = url.get("url") or url.get("href")
    if not isinstance(url, str):
        return wrap_upload_result(original_url, None, return_hash)

    if infer_content_type:
        inferred_type = "application/octet-stream"
        if url and not url.startswith("data:"):
            inferred_type = get_content_type_from_url(url)
            if inferred_type != "application/octet-stream":
                content_type = inferred_type

    if not url:
        return wrap_upload_result(url, None, return_hash)

    if not config.AWS_BUCKET_MODELS:
        msg = "[S3] SKIP: AWS_BUCKET_MODELS not configured, returning original URL"
        if config.REQUIRE_AWS_UPLOADS:
            raise RuntimeError(msg)
        print(msg)
        return wrap_upload_result(url, None, return_hash)

    if config.REQUIRE_AWS_UPLOADS and (not config.AWS_ACCESS_KEY_ID or not config.AWS_SECRET_ACCESS_KEY):
        raise RuntimeError("[S3] AWS credentials not configured")

    if is_s3_url(url):
        s3_key = get_s3_key_from_url(url)
        return wrap_upload_result(url, None, return_hash, s3_key=s3_key, reused=True)

    resolved_type = content_type or "application/octet-stream"
    try:
        if url.startswith("data:"):
            header, b64data = url.split(",", 1)
            if ":" in header and ";" in header:
                resolved_type = header.split(":")[1].split(";")[0] or resolved_type
            data_bytes = base64.b64decode(b64data)
        else:
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            if infer_content_type:
                header_type = resp.headers.get("Content-Type")
                if header_type:
                    resolved_type = header_type
            data_bytes = resp.content
    except Exception as e:
        print(f"[S3] ERROR: Failed to fetch bytes for {prefix}: {e}")
        raise

    content_hash = compute_sha256(data_bytes)
    s3_key = build_hash_s3_key(prefix, provider, content_hash, resolved_type)
    if s3_key_exists(s3_key):
        s3_url = build_s3_url(s3_key)
        # print(f"[S3] SKIP: Key exists -> {s3_url}")
        return wrap_upload_result(s3_url, content_hash if return_hash else None, return_hash, s3_key=s3_key, reused=True)

    _s3.put_object(
        Bucket=config.AWS_BUCKET_MODELS,
        Key=s3_key,
        Body=data_bytes,
        ContentType=resolved_type,
    )
    s3_url = build_s3_url(s3_key)
    # print(f"[S3] SUCCESS: Uploaded {len(data_bytes)} bytes -> {s3_url}")
    return wrap_upload_result(s3_url, content_hash if return_hash else None, return_hash, s3_key=s3_key, reused=False)


def ensure_s3_url_for_data_uri(
    url: str,
    prefix: str,
    key_base: str,
    user_id: str | None = None,
    name: str | None = None,
    provider: str | None = None,
) -> str | None:
    if not isinstance(url, str) or not url.startswith("data:"):
        return url
    try:
        s3_url = safe_upload_to_s3(
            url,
            "image/png",
            prefix,
            name or prefix,
            user_id=user_id,
            key_base=key_base,
            provider=provider,
        )
    except Exception as e:
        print(f"[S3] ERROR: Failed to upload data URI for {prefix}: {e}")
        return None
    if isinstance(s3_url, str) and s3_url.startswith("data:"):
        return None
    return s3_url


def upload_base64_to_s3(
    data_url: str,
    prefix: str = "images",
    name: str | None = None,
    user_id: str | None = None,
    key: str | None = None,
    key_base: str | None = None,
    return_hash: bool = False,
):
    header, b64data = data_url.split(",", 1)
    mime = "image/png"
    if ":" in header and ";" in header:
        mime = header.split(":")[1].split(";")[0]
    image_bytes = base64.b64decode(b64data)
    if key_base and not key:
        key = ensure_s3_key_ext(key_base, mime)
    return upload_bytes_to_s3(image_bytes, mime, prefix, name, user_id, key=key, return_hash=return_hash)

# --- Phase 6 convenience wrappers ---


def compute_content_hash(content: bytes) -> str:
    """Stable content hash helper (SHA256 hex)."""
    return compute_sha256(content)


def upload_model_to_s3(url, job_id, filename_hint=None, content_type=None) -> dict:
    """Upload a model URL/data URI to S3 with deterministic hashing."""
    result = safe_upload_to_s3(
        url,
        content_type or "model/gltf-binary",
        "models",
        filename_hint or f"model_{job_id}",
        user_id=job_id,
        provider="meshy",
        return_hash=True,
    )
    u, h, k, reused = unpack_upload_result(result)
    return {"url": u, "hash": h, "key": k, "reused": bool(reused)}


def upload_image_to_s3(url, job_id, filename_hint=None, provider: str = "openai") -> dict:
    """
    Upload an image URL/data URI to S3 with deterministic hashing.

    Args:
        url: Image URL or data URI
        job_id: Job identifier
        filename_hint: Optional filename hint
        provider: Provider name for S3 key path ("openai", "google", etc.)
                  IMPORTANT: Pass the correct provider to avoid mixing images
                  from different providers in the same S3 folder.

    Returns:
        Dict with url, hash, key, reused
    """
    # Safeguard: normalize provider to known values
    safe_provider = provider.lower() if provider else "unknown"
    if safe_provider not in ("openai", "google", "meshy", "user"):
        print(f"[S3] Warning: unknown provider '{provider}', using 'unknown' folder")
        safe_provider = "unknown"

    result = safe_upload_to_s3(
        url,
        "image/png",
        "images",
        filename_hint or f"image_{job_id}",
        user_id=job_id,
        provider=safe_provider,
        return_hash=True,
    )
    u, h, k, reused = unpack_upload_result(result)
    return {"url": u, "hash": h, "key": k, "reused": bool(reused)}


def save_finished_job_to_normalized_db(job_id, status_out, meta, job_type, user_id) -> dict:
    """Persist finished job data to normalized tables (history/items/models/images)."""
    from backend.services.history_service import save_finished_job_to_normalized_db as _save

    return _save(job_id, status_out, meta, job_type=job_type, user_id=user_id)


# ─────────────────────────────────────────────────────────────
# S3 Deletion Helpers
# ─────────────────────────────────────────────────────────────


def delete_s3_objects_safe(keys: list[str], source: str = "unknown") -> dict:
    """
    Delete S3 objects idempotently - safe to retry, logs errors but doesn't raise.

    Args:
        keys: List of S3 keys to delete
        source: Description of deletion source for logging (e.g., "history_item_delete")

    Returns:
        Dict with deletion stats:
        {
            "deleted": int,        # Number of successfully deleted objects
            "already_missing": int, # Objects that didn't exist (idempotent)
            "errors": list[dict],  # Any errors that occurred
            "keys_attempted": int  # Total keys attempted
        }
    """
    result = {
        "deleted": 0,
        "already_missing": 0,
        "errors": [],
        "keys_attempted": len(keys) if keys else 0,
    }

    if not keys:
        return result

    if not config.AWS_BUCKET_MODELS:
        # print(f"[S3] SKIP delete ({source}): AWS_BUCKET_MODELS not configured")
        return result

    # Filter out empty/None keys
    valid_keys = [k for k in keys if k]
    if not valid_keys:
        return result

    result["keys_attempted"] = len(valid_keys)

    # Process in chunks of 1000 (S3 limit)
    for i in range(0, len(valid_keys), 1000):
        chunk = [{"Key": key} for key in valid_keys[i : i + 1000]]
        if not chunk:
            continue

        try:
            resp = _s3.delete_objects(
                Bucket=config.AWS_BUCKET_MODELS,
                Delete={"Objects": chunk, "Quiet": False},  # Get detailed response
            )

            # Count successful deletions
            deleted_list = resp.get("Deleted", []) or []
            result["deleted"] += len(deleted_list)

            # Check for errors (but don't raise)
            errors = resp.get("Errors") or []
            for err in errors:
                err_code = err.get("Code", "")
                err_key = err.get("Key", "")

                # NoSuchKey means already deleted - that's fine (idempotent)
                if err_code == "NoSuchKey":
                    result["already_missing"] += 1
                    # print(f"[S3] OK ({source}): Key already deleted: {err_key}")
                else:
                    result["errors"].append({
                        "key": err_key,
                        "code": err_code,
                        "message": err.get("Message", ""),
                    })
                    print(f"[S3] ERROR ({source}): Failed to delete {err_key}: {err_code} - {err.get('Message', '')}")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = e.response.get("Error", {}).get("Message", str(e))
            result["errors"].append({
                "key": "batch",
                "code": error_code,
                "message": error_msg,
            })
            print(f"[S3] ERROR ({source}): Batch delete failed: {error_code} - {error_msg}")

        except Exception as e:
            result["errors"].append({
                "key": "batch",
                "code": "UnknownError",
                "message": str(e),
            })
            print(f"[S3] ERROR ({source}): Unexpected error during batch delete: {e}")

    # Summary log
    # if result["deleted"] > 0 or result["already_missing"] > 0:
    #     print(
    #         f"[S3] Cleanup ({source}): deleted={result['deleted']}, "
    #         f"already_missing={result['already_missing']}, errors={len(result['errors'])}"
    #     )

    return result


def collect_s3_keys_from_model(model_id: str) -> list[str]:
    """
    Collect all S3 keys associated with a model from the models table.

    Returns list of S3 keys (glb_s3_key, thumbnail_s3_key).
    """
    from backend.db import USE_DB, get_conn, Tables, dict_row

    keys = []
    if not USE_DB or not model_id:
        return keys

    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT glb_s3_key, thumbnail_s3_key, glb_url, thumbnail_url
                    FROM {Tables.MODELS}
                    WHERE id = %s
                    """,
                    (model_id,),
                )
                row = cur.fetchone()
                if row:
                    # Direct S3 keys
                    if row.get("glb_s3_key"):
                        keys.append(row["glb_s3_key"])
                    if row.get("thumbnail_s3_key"):
                        keys.append(row["thumbnail_s3_key"])
                    # Parse from URLs as fallback
                    if row.get("glb_url"):
                        k = parse_s3_key(row["glb_url"])
                        if k and k not in keys:
                            keys.append(k)
                    if row.get("thumbnail_url"):
                        k = parse_s3_key(row["thumbnail_url"])
                        if k and k not in keys:
                            keys.append(k)
    except Exception as e:
        print(f"[S3] ERROR: Failed to collect S3 keys for model {model_id}: {e}")

    return keys


def collect_s3_keys_from_image(image_id: str) -> list[str]:
    """
    Collect all S3 keys associated with an image from the images table.

    Returns list of S3 keys (image_s3_key, thumbnail_s3_key, source_s3_key).
    """
    from backend.db import USE_DB, get_conn, Tables, dict_row

    keys = []
    if not USE_DB or not image_id:
        return keys

    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT image_s3_key, thumbnail_s3_key, source_s3_key,
                           image_url, thumbnail_url
                    FROM {Tables.IMAGES}
                    WHERE id = %s
                    """,
                    (image_id,),
                )
                row = cur.fetchone()
                if row:
                    # Direct S3 keys
                    if row.get("image_s3_key"):
                        keys.append(row["image_s3_key"])
                    if row.get("thumbnail_s3_key"):
                        keys.append(row["thumbnail_s3_key"])
                    if row.get("source_s3_key"):
                        keys.append(row["source_s3_key"])
                    # Parse from URLs as fallback
                    if row.get("image_url"):
                        k = parse_s3_key(row["image_url"])
                        if k and k not in keys:
                            keys.append(k)
                    if row.get("thumbnail_url"):
                        k = parse_s3_key(row["thumbnail_url"])
                        if k and k not in keys:
                            keys.append(k)
    except Exception as e:
        print(f"[S3] ERROR: Failed to collect S3 keys for image {image_id}: {e}")

    return keys


def collect_s3_keys_from_video(video_id: str) -> list[str]:
    """
    Collect all S3 keys associated with a video from the videos table.

    Returns list of S3 keys (video_s3_key, thumbnail_s3_key).
    """
    from backend.db import USE_DB, get_conn, Tables, dict_row

    keys = []
    if not USE_DB or not video_id:
        return keys

    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT video_s3_key, thumbnail_s3_key, video_url, thumbnail_url
                    FROM {Tables.VIDEOS}
                    WHERE id = %s
                    """,
                    (video_id,),
                )
                row = cur.fetchone()
                if row:
                    # Direct S3 keys
                    if row.get("video_s3_key"):
                        keys.append(row["video_s3_key"])
                    if row.get("thumbnail_s3_key"):
                        keys.append(row["thumbnail_s3_key"])
                    # Parse from URLs as fallback
                    if row.get("video_url"):
                        k = parse_s3_key(row["video_url"])
                        if k and k not in keys:
                            keys.append(k)
                    if row.get("thumbnail_url"):
                        k = parse_s3_key(row["thumbnail_url"])
                        if k and k not in keys:
                            keys.append(k)
    except Exception as e:
        print(f"[S3] ERROR: Failed to collect S3 keys for video {video_id}: {e}")

    return keys


def collect_all_s3_keys_for_history_item(
    history_row: dict,
    model_id: str | None = None,
    image_id: str | None = None,
    video_id: str | None = None,
) -> list[str]:
    """
    Comprehensively collect ALL S3 keys for a history item and its related assets.

    This combines:
    1. Keys from the history row itself (URLs in payload)
    2. Keys from the related model record
    3. Keys from the related image record
    4. Keys from the related video record

    Args:
        history_row: The history_items row dict
        model_id: Optional model UUID (if not in history_row)
        image_id: Optional image UUID (if not in history_row)
        video_id: Optional video UUID (if not in history_row)

    Returns:
        Deduplicated list of all S3 keys to delete
    """
    keys: set[str] = set()

    # 1. Collect from history row (URLs)
    history_keys = collect_s3_keys(history_row)
    keys.update(history_keys)

    # 2. Collect from model table
    mid = model_id or (history_row.get("model_id") if history_row else None)
    if mid:
        model_keys = collect_s3_keys_from_model(str(mid))
        keys.update(model_keys)

    # 3. Collect from image table
    iid = image_id or (history_row.get("image_id") if history_row else None)
    if iid:
        image_keys = collect_s3_keys_from_image(str(iid))
        keys.update(image_keys)

    # 4. Collect from video table
    vid = video_id or (history_row.get("video_id") if history_row else None)
    if vid:
        video_keys = collect_s3_keys_from_video(str(vid))
        keys.update(video_keys)

    return sorted(keys)
