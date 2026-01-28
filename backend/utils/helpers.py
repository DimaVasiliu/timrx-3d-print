"""
General helper utilities migrated from app.py.

These functions are intentionally dependency-light so they can be reused
across services and routes without pulling in Flask app globals.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any
from urllib.parse import quote, unquote, urlparse, urlunparse


def now_s() -> int:
    """Current epoch seconds as int."""
    return int(time.time())


def clamp_int(value: Any, minimum: int, maximum: int, default: int) -> int:
    """Clamp a value to an integer within [minimum, maximum]."""
    try:
        return max(minimum, min(maximum, int(value)))
    except (TypeError, ValueError):
        return default


def normalize_epoch_ms(value: Any) -> int:
    """
    Accept seconds, ms, ISO strings, or numeric-like strings and
    return an epoch value in milliseconds.
    """
    try:
        if value is None:
            return int(time.time() * 1000)
        if isinstance(value, str):
            raw = value.strip()
            if raw.isdigit():
                value = float(raw)
            else:
                try:
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    return int(dt.timestamp() * 1000)
                except Exception:
                    pass
        if isinstance(value, (int, float)):
            if value > 1e15:  # looks like ns
                return int(value / 1000)
            if value < 1e12:  # looks like seconds
                return int(value * 1000)
            return int(value)
    except Exception:
        pass
    return int(time.time() * 1000)


def normalize_license(value: Any) -> str:
    """Normalize license strings into the app's expected values."""
    raw = str(value or "").strip().lower()
    return "cc-by-4" if raw.startswith("cc") else "private"


def derive_display_title(prompt: str | None, explicit_title: str | None) -> str:
    """
    Derive a human-friendly display title for models/images/history items.

    Rules:
    - If explicit_title exists and non-empty -> use it
    - Else if prompt exists and non-empty -> use prompt.strip() truncated to 100 chars
    - Else -> "Untitled"
    """
    if isinstance(explicit_title, str) and explicit_title.strip():
        return explicit_title.strip()
    if isinstance(prompt, str) and prompt.strip():
        return prompt.strip()[:100]
    return "Untitled"


def sanitize_filename(name: str, max_length: int = 50) -> str:
    """
    Sanitize a string to be safe for use in keys/filenames.
    - Converts to lowercase
    - Replaces spaces and special chars with underscores
    - Limits length
    """
    if not name:
        return ""
    safe = name.lower().strip()
    safe = re.sub(r"[^a-z0-9_\-]", "_", safe)
    safe = re.sub(r"_+", "_", safe)
    safe = safe.strip("_")
    if len(safe) > max_length:
        safe = safe[:max_length].rstrip("_")
    return safe


def compute_sha256(data_bytes: bytes) -> str:
    """Compute a SHA256 hex digest for raw bytes."""
    return hashlib.sha256(data_bytes).hexdigest()


def get_extension_for_content_type(content_type: str) -> str:
    """Get file extension based on content type."""
    ext_map = {
        "model/gltf-binary": ".glb",
        "model/gltf+json": ".gltf",
        "application/octet-stream": ".glb",
        "image/png": ".png",
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/webp": ".webp",
        "application/x-fbx": ".fbx",
        "model/vnd.usdz+zip": ".usdz",
        "model/obj": ".obj",
        "model/stl": ".stl",
    }
    return ext_map.get(content_type, "")


def get_content_type_for_extension(ext: str) -> str:
    """Get MIME type based on file extension."""
    ext_map = {
        ".glb": "model/gltf-binary",
        ".gltf": "model/gltf+json",
        ".fbx": "application/x-fbx",
        ".obj": "model/obj",
        ".stl": "model/stl",
        ".usdz": "model/vnd.usdz+zip",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }
    return ext_map.get((ext or "").lower(), "application/octet-stream")


def get_content_type_from_url(url: str) -> str:
    """Infer MIME type from URL extension when possible."""
    try:
        path = urlparse(url).path or ""
        ext = os.path.splitext(path)[1]
        return get_content_type_for_extension(ext)
    except Exception:
        return "application/octet-stream"


def wrap_upload_result(value: str, content_hash: str, return_hash: bool, s3_key: str | None = None, reused: bool = False):
    """Wrap upload results in a structured dict when requested."""
    if not return_hash:
        return value
    return {"url": value, "hash": content_hash, "key": s3_key, "reused": reused}


def unpack_upload_result(result):
    """Unpack structured upload results into a 4-tuple."""
    if isinstance(result, dict):
        return result.get("url"), result.get("hash"), result.get("key"), result.get("reused")
    if isinstance(result, tuple):
        if len(result) == 2:
            return result[0], result[1], None, None
        if len(result) == 3:
            return result[0], result[1], result[2], None
        if len(result) >= 4:
            return result[0], result[1], result[2], result[3]
    return result, None, None, None


def build_canonical_url(url: str) -> str | None:
    """Build a canonical URL (strip query/fragment, normalize path)."""
    if not isinstance(url, str) or not url:
        return None
    if url.startswith("data:"):
        return None
    try:
        parsed = urlparse(url.strip())
        if not parsed.scheme or not parsed.netloc:
            base = url.split("?", 1)[0].split("#", 1)[0].strip()
            return base or None
        host = (parsed.hostname or parsed.netloc).lower()
        netloc = host
        if parsed.port:
            netloc = f"{host}:{parsed.port}"
        path = unquote(parsed.path or "")
        path = re.sub(r"\s+", " ", path).strip()
        path = quote(path, safe="/-_.~")
        return urlunparse((parsed.scheme, netloc, path, "", "", ""))
    except Exception:
        return url.split("?", 1)[0].split("#", 1)[0].strip() or None

_logger = logging.getLogger("timrx.helpers")


def _mask_value(val: Any, max_len: int = 400) -> str:
    try:
        s = json.dumps(val, ensure_ascii=False)
    except Exception:
        s = str(val)
    if len(s) > max_len:
        return s[:max_len] + "…"
    return s


def _scrub_secrets(data: Any) -> Any:
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            key = str(k).lower()
            if any(t in key for t in ("key", "token", "secret", "auth")):
                cleaned[k] = "***"
            else:
                cleaned[k] = _scrub_secrets(v)
        return cleaned
    if isinstance(data, list):
        return [_scrub_secrets(x) for x in data]
    return data


def log_event(event_name: str, data: dict) -> None:
    """Lightweight debug logging that avoids leaking secrets."""
    try:
        safe_payload = _scrub_secrets(data)
        _logger.info("[debug] %s :: %s", event_name, _mask_value(safe_payload))
    except Exception as e:
        _logger.warning("[debug] %s :: failed to log (%s)", event_name, e)


def log_status_summary(prefix: str, job_id: str, status: dict) -> None:
    """Compact status logging for polling endpoints."""
    try:
        st = status or {}
        has_model = bool(
            st.get("glb_url")
            or st.get("textured_glb_url")
            or st.get("rigged_character_glb_url")
            or (isinstance(st.get("model_urls"), dict) and any(st.get("model_urls").values()))
            or (isinstance(st.get("textured_model_urls"), dict) and any(st.get("textured_model_urls").values()))
        )
        _logger.info(
            "[status] %s job=%s status=%s pct=%s has_model=%s glb=%s",
            prefix,
            job_id,
            st.get("status") or st.get("task_status"),
            st.get("pct") or st.get("progress") or st.get("progress_percentage"),
            has_model,
            (st.get("glb_url") or st.get("textured_glb_url") or st.get("rigged_character_glb_url") or "")[:128],
        )
    except Exception as e:
        _logger.warning("[status] %s job=%s log-failed: %s", prefix, job_id, e)


def log_db_continue(op: str, err: Exception) -> None:
    """Log DB errors that should not break the request flow."""
    try:
        _logger.warning("[DB] CONTINUE: %s failed: %s: %s", op, type(err).__name__, err)
    except Exception:
        print(f"[DB] CONTINUE: {op} failed: {type(err).__name__}: {err}")
