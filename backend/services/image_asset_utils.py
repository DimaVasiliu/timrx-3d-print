"""
Helpers for image provider request assets and parameter coercion.

Advanced image providers in TimrX accept image references from the frontend as
plain HTTPS URLs or data URLs. This module normalizes those assets so provider
services can either upload them to S3-backed URLs or send them as multipart
files upstream without duplicating parsing logic.
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from typing import Any, Iterable

import requests

from backend.services.s3_service import safe_upload_to_s3


DEFAULT_IMAGE_CONTENT_TYPE = "image/png"


def coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def coerce_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except Exception:
        return default


def coerce_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except Exception:
        return default


def normalize_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(item).strip() for item in parsed if str(item).strip()]
            except Exception:
                pass
        return [part.strip() for part in text.split(",") if part.strip()]
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, dict)):
        out: list[str] = []
        for item in value:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                out.append(text)
        return out
    return []


def normalize_asset_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, dict):
        maybe_url = value.get("data_url") or value.get("url") or value.get("href")
        return [str(maybe_url).strip()] if maybe_url else []
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray)):
        out: list[str] = []
        for item in value:
            if item is None:
                continue
            if isinstance(item, dict):
                maybe_url = item.get("data_url") or item.get("url") or item.get("href")
                if maybe_url:
                    out.append(str(maybe_url).strip())
                continue
            text = str(item).strip()
            if text:
                out.append(text)
        return out
    return []


def asset_content_type(asset: str, default: str = DEFAULT_IMAGE_CONTENT_TYPE) -> str:
    if asset.startswith("data:"):
        try:
            return asset.split(":", 1)[1].split(";", 1)[0] or default
        except Exception:
            return default
    return default


def decode_asset_bytes(asset: str, default_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE) -> tuple[bytes, str]:
    if not isinstance(asset, str) or not asset.strip():
        raise ValueError("asset is required")
    asset = asset.strip()
    if asset.startswith("data:"):
        header, payload = asset.split(",", 1)
        content_type = asset_content_type(asset, default_content_type)
        return base64.b64decode(payload), content_type
    response = requests.get(asset, timeout=120)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type") or default_content_type
    return response.content, content_type


def build_multipart_file(
    asset: str,
    *,
    default_name: str,
    default_content_type: str = DEFAULT_IMAGE_CONTENT_TYPE,
) -> tuple[str, bytes, str]:
    content, content_type = decode_asset_bytes(asset, default_content_type=default_content_type)
    ext = _extension_for_content_type(content_type)
    filename = default_name if default_name.endswith(ext) else f"{default_name}{ext}"
    return (filename, content, content_type)


def ensure_asset_url(
    asset: str | None,
    *,
    provider: str,
    identity_id: str | None,
    prefix: str = "source_images",
    name: str | None = None,
) -> str | None:
    if not asset:
        return None
    text = str(asset).strip()
    if not text:
        return None
    if text.startswith("http://") or text.startswith("https://"):
        return text
    upload_name = name or f"{provider}-{uuid.uuid4().hex[:8]}"
    uploaded = safe_upload_to_s3(
        text,
        DEFAULT_IMAGE_CONTENT_TYPE,
        prefix,
        upload_name,
        user_id=identity_id,
        provider=provider,
    )
    if isinstance(uploaded, tuple):
        return str(uploaded[0] or "")
    return str(uploaded or "")


def color_members_from_hex_list(values: Any) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    for raw in normalize_string_list(values):
        item = raw.strip()
        if not item:
            continue
        weight = None
        if ":" in item:
            color_text, weight_text = item.split(":", 1)
            item = color_text.strip()
            try:
                weight = float(weight_text.strip())
            except Exception:
                weight = None
        hex_value = item.lower().lstrip("#")
        if len(hex_value) != 6 or any(c not in "0123456789abcdef" for c in hex_value):
            continue
        member: dict[str, Any] = {"hex": f"#{hex_value}"}
        if weight is not None:
            member["weight"] = weight
        members.append(member)
    return members


def _extension_for_content_type(content_type: str) -> str:
    normalized = (content_type or "").split(";", 1)[0].strip().lower()
    mapping = {
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/webp": ".webp",
        "image/svg+xml": ".svg",
    }
    return mapping.get(normalized, os.path.splitext(normalized)[1] or ".png")
