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
    safe_provider = sanitize_filename(provider or "unknown") or "unknown"
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


def collect_s3_keys(history_row: dict) -> list[str]:
    keys: set[str] = set()
    if not isinstance(history_row, dict):
        return []

    for field in ("thumbnail_url", "glb_url", "image_url"):
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
        print(f"[S3] SKIP: Key exists -> {s3_url}")
        return wrap_upload_result(s3_url, content_hash, return_hash, s3_key=key, reused=True)

    print(
        f"[S3] Uploading {len(data_bytes)} bytes to bucket={config.AWS_BUCKET_MODELS}, "
        f"key={key}, content_type={content_type}"
    )
    _s3.put_object(
        Bucket=config.AWS_BUCKET_MODELS,
        Key=key,
        Body=data_bytes,
        ContentType=content_type,
        ACL="public-read",
    )
    s3_url = build_s3_url(key)
    print(f"[S3] SUCCESS: Uploaded {len(data_bytes)} bytes -> {s3_url}")
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
            print(f"[S3] SKIP: Key exists -> {s3_url}")
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
        print(f"[S3] SKIP: Key exists -> {s3_url}")
        return wrap_upload_result(s3_url, content_hash if return_hash else None, return_hash, s3_key=s3_key, reused=True)

    _s3.put_object(
        Bucket=config.AWS_BUCKET_MODELS,
        Key=s3_key,
        Body=data_bytes,
        ContentType=resolved_type,
        ACL="public-read",
    )
    s3_url = build_s3_url(s3_key)
    print(f"[S3] SUCCESS: Uploaded {len(data_bytes)} bytes -> {s3_url}")
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


def upload_image_to_s3(url, job_id, filename_hint=None) -> dict:
    """Upload an image URL/data URI to S3 with deterministic hashing."""
    result = safe_upload_to_s3(
        url,
        "image/png",
        "images",
        filename_hint or f"image_{job_id}",
        user_id=job_id,
        provider="openai",
        return_hash=True,
    )
    u, h, k, reused = unpack_upload_result(result)
    return {"url": u, "hash": h, "key": k, "reused": bool(reused)}


def save_finished_job_to_normalized_db(job_id, status_out, meta, job_type, user_id) -> dict:
    """Persist finished job data to normalized tables (history/items/models/images)."""
    from backend.services.history_service import save_finished_job_to_normalized_db as _save

    return _save(job_id, status_out, meta, job_type=job_type, user_id=user_id)
