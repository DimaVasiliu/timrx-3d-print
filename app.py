import os, json, time, base64, uuid, hashlib, re
import boto3
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any
from urllib.parse import urlparse, urlunparse, quote, unquote
from datetime import datetime
from botocore.exceptions import ClientError

load_dotenv()

# Add backend directories to Python path for module imports
# NOTE: Use append() instead of insert(0) to avoid shadowing root modules
# This prevents circular import issues where modules could be imported from wrong location
import sys
_app_dir = Path(__file__).resolve().parent
_backend_dir = _app_dir / "backend"
_routes_dir = _backend_dir / "routes"
_services_dir = _backend_dir / "services"
for _dir in [_backend_dir, _routes_dir, _services_dir]:
    if str(_dir) not in sys.path:
        sys.path.append(str(_dir))

import requests
try:
    import psycopg
    from psycopg.rows import dict_row
    print("[DB] psycopg3 imported successfully")
    PSYCOPG_VERSION = 3
except Exception as e:
    print(f"[DB] Failed to import psycopg3: {e}")
    psycopg = None
    PSYCOPG_VERSION = 0
from flask import Flask, request, jsonify, Response, abort, g, send_from_directory, redirect
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

# Import config for FRONTEND_BASE_URL
try:
    import config as cfg
except ImportError:
    cfg = None
    print("[APP] Warning: Could not import config module")

# ─────────────────────────────────────────────────────────────
# Credits System - Flat Module Imports
# ─────────────────────────────────────────────────────────────
# Import blueprints individually so one failure doesn't break all
_loaded_blueprints = []

# Required: me blueprint
try:
    from me import bp as me_bp
    _loaded_blueprints.append(("me", me_bp, "/api/me"))
except ImportError as e:
    print(f"[WARN] me blueprint not loaded: {e}")
    me_bp = None

# Required: admin blueprint
try:
    from admin import bp as admin_bp
    _loaded_blueprints.append(("admin", admin_bp, "/api/admin"))
except ImportError as e:
    print(f"[WARN] admin blueprint not loaded: {e}")
    admin_bp = None

# Optional: billing blueprint
try:
    from billing import bp as billing_bp
    _loaded_blueprints.append(("billing", billing_bp, "/api/billing"))
except ImportError as e:
    print(f"[WARN] billing blueprint not loaded: {e}")
    billing_bp = None

# Optional: auth blueprint
try:
    from auth import bp as auth_bp
    _loaded_blueprints.append(("auth", auth_bp, "/api/auth"))
except ImportError as e:
    print(f"[WARN] auth blueprint not loaded: {e}")
    auth_bp = None

# Optional: jobs blueprint
try:
    from jobs import bp as jobs_bp
    _loaded_blueprints.append(("jobs", jobs_bp, "/api/jobs"))
except ImportError as e:
    print(f"[WARN] jobs blueprint not loaded: {e}")
    jobs_bp = None

# Optional: credits blueprint (wallet balance + charge)
try:
    from credits import bp as credits_bp
    _loaded_blueprints.append(("credits", credits_bp, "/api/credits"))
except ImportError as e:
    print(f"[WARN] credits blueprint not loaded: {e}")
    credits_bp = None

# Import DatabaseError for error handler
try:
    from db import DatabaseError
except ImportError:
    DatabaseError = None

# Import credit services for enforcement
try:
    from reservation_service import ReservationService, ReservationStatus
    from wallet_service import WalletService
    from pricing_service import PricingService
    CREDITS_AVAILABLE = True
    print("[CREDITS] Credit enforcement services loaded")
except ImportError as e:
    print(f"[CREDITS] Credit services not available: {e}")
    CREDITS_AVAILABLE = False
    ReservationService = None
    WalletService = None
    PricingService = None

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
DEFAULT_MODEL_TITLE = "3D Model"
APP_DIR = Path(__file__).resolve().parent
CACHE_DIR = APP_DIR / "cache_images"
CACHE_DIR.mkdir(exist_ok=True)
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_DB = bool(DATABASE_URL and psycopg)
print(f"[DB] DATABASE_URL configured: {bool(DATABASE_URL)}, psycopg available: {bool(psycopg)}, USE_DB: {USE_DB}")
APP_SCHEMA = "timrx_app"

# ─────────────────────────────────────────────────────────────
# Environment Detection
# ─────────────────────────────────────────────────────────────
FLASK_ENV = os.getenv("FLASK_ENV", "production").lower()
IS_DEV = FLASK_ENV in ("development", "dev", "local")
print(f"[ENV] FLASK_ENV={FLASK_ENV}, IS_DEV={IS_DEV}")

# ─────────────────────────────────────────────────────────────
# LOCAL DEV ONLY: JSON file storage (disabled in production)
# These files are ONLY used when DATABASE_URL is not configured.
# In production, all data goes to PostgreSQL.
# ─────────────────────────────────────────────────────────────
LOCAL_DEV_MODE = not USE_DB  # True only when no database is configured
STORE_PATH = APP_DIR / "job_store.json"  # DEV ONLY: job metadata
HISTORY_STORE_PATH = APP_DIR / "history_store.json"  # DEV ONLY: history items
if LOCAL_DEV_MODE:
    print("[DEV] LOCAL_DEV_MODE enabled - using JSON file storage (job_store.json, history_store.json)")
else:
    print("[PROD] Database mode - local JSON files disabled")

# ─────────────────────────────────────────────────────────────
# AWS S3 Config
# ─────────────────────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "eu-west-2")
AWS_BUCKET_MODELS = os.getenv("AWS_BUCKET_MODELS", "")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# If true, require AWS creds/bucket to be present (useful on Render to forbid Meshy fallback)
REQUIRE_AWS_UPLOADS = os.getenv("REQUIRE_AWS_UPLOADS", "false").strip().lower() == "true"

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

# Hosts allowed for proxying GLB/asset fetches
DEFAULT_PROXY_HOSTS = {
    "assets.meshy.ai",
}
if AWS_BUCKET_MODELS:
    DEFAULT_PROXY_HOSTS.add(f"{AWS_BUCKET_MODELS}.s3.{AWS_REGION}.amazonaws.com")
PROXY_ALLOWED_HOSTS = set(
    h.strip().lower()
    for h in os.getenv("PROXY_ALLOWED_HOSTS", "").split(",")
    if h.strip()
) or DEFAULT_PROXY_HOSTS

def sanitize_filename(name: str, max_length: int = 50) -> str:
    """
    Sanitize a string to be safe for use in S3 keys/filenames.
    - Converts to lowercase
    - Replaces spaces and special chars with underscores
    - Limits length
    """
    if not name:
        return ""
    # Convert to lowercase and replace spaces with underscores
    safe = name.lower().strip()
    # Replace any non-alphanumeric chars (except underscores and hyphens) with underscores
    safe = re.sub(r'[^a-z0-9_\-]', '_', safe)
    # Collapse multiple underscores
    safe = re.sub(r'_+', '_', safe)
    # Remove leading/trailing underscores
    safe = safe.strip('_')
    # Limit length
    if len(safe) > max_length:
        safe = safe[:max_length].rstrip('_')
    return safe

def get_extension_for_content_type(content_type: str) -> str:
    """Get file extension based on content type."""
    ext_map = {
        "model/gltf-binary": ".glb",
        "model/gltf+json": ".gltf",
        "application/octet-stream": ".glb",  # Default for models
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

def compute_sha256(data_bytes: bytes) -> str:
    return hashlib.sha256(data_bytes).hexdigest()

def _wrap_upload_result(value: str, content_hash: str, return_hash: bool, s3_key: str = None, reused: bool = False):
    if not return_hash:
        return value
    return {"url": value, "hash": content_hash, "key": s3_key, "reused": reused}

def _unpack_upload_result(result):
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

def build_hash_s3_key(prefix: str, provider: str, content_hash: str, content_type: str) -> str:
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
    return f"https://{AWS_BUCKET_MODELS}.s3.{AWS_REGION}.amazonaws.com/{key}"

def s3_key_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=AWS_BUCKET_MODELS, Key=key)
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

def get_s3_key_from_url(url: str) -> str:
    return parse_s3_key(url)

def parse_s3_key(url: str) -> str:
    if not is_s3_url(url):
        return None
    try:
        parsed = urlparse(url)
        return parsed.path.lstrip("/") or None
    except Exception:
        return None

def build_canonical_url(url: str) -> str:
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

def collect_s3_keys(history_row: dict) -> list:
    keys = set()
    if not isinstance(history_row, dict):
        return []

    for field in ("thumbnail_url", "glb_url", "image_url"):
        key = parse_s3_key(history_row.get(field))
        if key:
            keys.add(key)

    payload = history_row.get("payload") or {}
    if isinstance(payload, str):
        try:
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

def log_db_continue(op: str, err: Exception) -> None:
    print(f"[DB] CONTINUE: {op} failed: {type(err).__name__}: {err}")

# Module-level helper to filter model URLs (used in multiple places)
def _filter_model_urls(urls: Any) -> dict:
    """Filter model URLs dict to only include glb and obj formats."""
    if not isinstance(urls, dict):
        return {}
    filtered = {}
    for key in ("glb", "obj"):
        val = urls.get(key)
        if val:
            filtered[key] = val
    return filtered

# Module-level status mapping for Meshy API responses
MESHY_STATUS_MAP = {
    "PENDING": "pending",
    "IN_PROGRESS": "running",
    "SUCCEEDED": "done",
    "FAILED": "failed",
    "COMPLETED": "done",
    "FINISHED": "done",
    "SUCCESS": "done",
    "CANCELED": "failed",
    "CANCELLED": "failed",
    "TIMEOUT": "failed",
}

def ensure_s3_url_for_data_uri(url: str, prefix: str, key_base: str, user_id: str = None, name: str = None, provider: str = None) -> str:
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

def upload_bytes_to_s3(data_bytes: bytes, content_type: str = "application/octet-stream", prefix: str = "models", name: str = None, user_id: str = None, key: str = None, return_hash: bool = False) -> str:
    """
    Upload raw bytes to S3 and return the public URL.
    Key structure: {prefix}/{user_id}/{name}_{uuid}{ext} or {prefix}/{user_id}/{uuid}{ext}

    Args:
        data_bytes: Raw bytes to upload
        content_type: MIME type of the content
        prefix: folder prefix, e.g. 'models', 'images', 'thumbnails'
        name: optional human-readable name to include in the key
        user_id: REQUIRED - user UUID for namespacing (raises error if missing)
    """
    if not AWS_BUCKET_MODELS:
        print("[S3] ERROR: AWS_BUCKET_MODELS not configured!")
        raise RuntimeError("AWS_BUCKET_MODELS not configured")

    if not key:
        if not user_id:
            print("[S3] ERROR: user_id required for S3 upload (per-user namespacing)")
            raise ValueError("user_id required for S3 upload")

        # Get file extension based on content type
        ext = get_extension_for_content_type(content_type)

        # Build key with user namespace: {prefix}/{user_id}/{name}_{uuid}{ext}
        unique_id = uuid.uuid4().hex[:12]  # Shorter unique ID
        if name:
            safe_name = sanitize_filename(name)
            key = f"{prefix}/{user_id}/{safe_name}_{unique_id}{ext}" if safe_name else f"{prefix}/{user_id}/{unique_id}{ext}"
        else:
            key = f"{prefix}/{user_id}/{unique_id}{ext}"
    else:
        key = ensure_s3_key_ext(key.lstrip("/"), content_type)

    content_hash = compute_sha256(data_bytes) if return_hash else None
    if s3_key_exists(key):
        s3_url = build_s3_url(key)
        print(f"[S3] SKIP: Key exists -> {s3_url}")
        return _wrap_upload_result(s3_url, content_hash, return_hash, s3_key=key, reused=True)

    print(f"[S3] Uploading {len(data_bytes)} bytes to bucket={AWS_BUCKET_MODELS}, key={key}, content_type={content_type}")
    try:
        s3.put_object(
            Bucket=AWS_BUCKET_MODELS,
            Key=key,
            Body=data_bytes,
            ContentType=content_type,
            ACL='public-read',  # Make the object publicly readable
        )
        s3_url = build_s3_url(key)
        print(f"[S3] SUCCESS: Uploaded {len(data_bytes)} bytes -> {s3_url}")
        return _wrap_upload_result(s3_url, content_hash, return_hash, s3_key=key, reused=False)
    except Exception as e:
        print(f"[S3] ERROR: put_object failed for {key}: {e}")
        print(f"[S3] HINT: If you see AccessControlListNotSupported, disable 'Block public access' in S3 bucket settings")
        import traceback
        traceback.print_exc()
        raise

def upload_url_to_s3(url: str, content_type: str = None, prefix: str = "models", name: str = None, user_id: str = None, key: str = None, return_hash: bool = False) -> str:
    """
    Download file from URL and upload to S3.
    Returns the S3 public URL.

    Args:
        url: URL to download from
        content_type: MIME type (auto-detected if None)
        prefix: folder prefix
        name: optional human-readable name to include in the key
        user_id: REQUIRED - user UUID for namespacing
    """
    if key:
        key = ensure_s3_key_ext(key.lstrip("/"), content_type or "application/octet-stream")
        if s3_key_exists(key):
            s3_url = build_s3_url(key)
            print(f"[S3] SKIP: Key exists -> {s3_url}")
            return _wrap_upload_result(s3_url, None, return_hash, s3_key=key, reused=True)

    print(f"[S3] Downloading from URL: {url[:100]}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    ct = content_type or resp.headers.get("Content-Type", "application/octet-stream")
    print(f"[S3] Downloaded {len(resp.content)} bytes, content-type={ct}")
    return upload_bytes_to_s3(resp.content, ct, prefix, name, user_id, key=key, return_hash=return_hash)

def safe_upload_to_s3(url: str, content_type: str, prefix: str, name: str = None, user_id: str = None, key: str = None, key_base: str = None, infer_content_type: bool = True, return_hash: bool = False, upstream_id: str = None, stage: str = None, provider: str = None) -> str:
    """
    Safely upload URL to S3 using a deterministic hash-based key.
    Always downloads/decodes to bytes to compute the content hash.

    Args:
        url: URL to upload (or base64 data URL)
        content_type: MIME type
        prefix: folder prefix
        name: optional human-readable name (unused for deterministic keys)
        provider: asset provider namespace for hash-based keys
    """
    original_url = url
    if isinstance(url, dict):
        url = url.get("url") or url.get("href")
    if not isinstance(url, str):
        print(f"[S3] SKIP: URL not a string for {prefix} (type={type(original_url).__name__})")
        return _wrap_upload_result(original_url, None, return_hash)
    url_preview = (url[:60] if isinstance(url, str) else str(url)[:60]) if url is not None else "None"
    if infer_content_type:
        inferred_type = "application/octet-stream"
        if url and not url.startswith("data:"):
            inferred_type = get_content_type_from_url(url)
            if inferred_type != "application/octet-stream":
                content_type = inferred_type
    print(f"[S3] safe_upload_to_s3 called: prefix={prefix}, provider={provider}, url={url_preview}...")
    if not url:
        print(f"[S3] SKIP: No URL provided for {prefix}")
        return _wrap_upload_result(url, None, return_hash)
    if not AWS_BUCKET_MODELS:
        msg = "[S3] SKIP: AWS_BUCKET_MODELS not configured, returning original URL"
        if REQUIRE_AWS_UPLOADS:
            raise RuntimeError(msg)
        print(msg)
        return _wrap_upload_result(url, None, return_hash)
    if REQUIRE_AWS_UPLOADS and (not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY):
        raise RuntimeError("[S3] AWS credentials not configured")
    if is_s3_url(url):
        s3_key = get_s3_key_from_url(url)
        return _wrap_upload_result(url, None, return_hash, s3_key=s3_key, reused=True)

    resolved_type = content_type or "application/octet-stream"
    data_bytes = None
    try:
        if url.startswith("data:"):
            header, b64data = url.split(",", 1)
            if ":" in header and ";" in header:
                resolved_type = header.split(":")[1].split(";")[0] or resolved_type
            data_bytes = base64.b64decode(b64data)
        else:
            print(f"[S3] Downloading from URL: {url[:100]}...")
            resp = requests.get(url, timeout=120)
            resp.raise_for_status()
            if infer_content_type:
                header_type = resp.headers.get("Content-Type")
                if header_type:
                    resolved_type = header_type
            data_bytes = resp.content
            print(f"[S3] Downloaded {len(data_bytes)} bytes, content-type={resolved_type}")
    except Exception as e:
        print(f"[S3] ERROR: Failed to fetch bytes for {prefix}: {e}")
        raise

    content_hash = compute_sha256(data_bytes)
    s3_key = build_hash_s3_key(prefix, provider, content_hash, resolved_type)
    if s3_key_exists(s3_key):
        s3_url = build_s3_url(s3_key)
        print(f"[S3] SKIP: Key exists -> {s3_url}")
        return _wrap_upload_result(s3_url, content_hash if return_hash else None, return_hash, s3_key=s3_key, reused=True)

    print(f"[S3] Uploading {len(data_bytes)} bytes to bucket={AWS_BUCKET_MODELS}, key={s3_key}, content_type={resolved_type}")
    try:
        s3.put_object(
            Bucket=AWS_BUCKET_MODELS,
            Key=s3_key,
            Body=data_bytes,
            ContentType=resolved_type,
            ACL='public-read',
        )
        s3_url = build_s3_url(s3_key)
        print(f"[S3] SUCCESS: Uploaded {len(data_bytes)} bytes -> {s3_url}")
        return _wrap_upload_result(s3_url, content_hash if return_hash else None, return_hash, s3_key=s3_key, reused=False)
    except Exception as e:
        print(f"[S3] ERROR: Failed to upload {prefix}: {e}")
        import traceback
        traceback.print_exc()
        raise

def upload_base64_to_s3(data_url: str, prefix: str = "images", name: str = None, user_id: str = None, key: str = None, key_base: str = None, return_hash: bool = False) -> str:
    """
    Upload a base64 data URL to S3 and return the public URL.

    Args:
        data_url: base64 data URL (data:image/png;base64,...)
        prefix: folder prefix
        name: optional human-readable name to include in the key
        user_id: REQUIRED - user UUID for namespacing
    """
    try:
        # Parse data URL: data:image/png;base64,iVBORw0K...
        header, b64data = data_url.split(",", 1)
        # Extract mime type
        mime = "image/png"
        if ":" in header and ";" in header:
            mime = header.split(":")[1].split(";")[0]

        # Decode base64
        image_bytes = base64.b64decode(b64data)

        # Upload to S3
        if key_base and not key:
            key = ensure_s3_key_ext(key_base, mime)
        return upload_bytes_to_s3(image_bytes, mime, prefix, name, user_id, key=key, return_hash=return_hash)
    except Exception as e:
        print(f"[S3] Failed to parse/upload base64: {e}")
        raise

MESHY_API_KEY  = os.getenv("MESHY_API_KEY", "").strip()
MESHY_API_BASE = os.getenv("MESHY_API_BASE", "https://api.meshy.ai").rstrip("/")

# Log presence of maintenance scripts shipped with the build (no imports, no execution).
try:
    scripts_dir = os.path.join(os.path.dirname(__file__), "scripts")
    backfill_path = os.path.join(scripts_dir, "backfill_s3.py")
    dedupe_path = os.path.join(scripts_dir, "dedupe_db.py")
    print(f"[Scripts] backfill_s3.py present={os.path.exists(backfill_path)} path={backfill_path}")
    print(f"[Scripts] dedupe_db.py present={os.path.exists(dedupe_path)} path={dedupe_path}")
except Exception as e:
    print(f"[Scripts] presence check failed: {e}")

# OpenAI images (DALL·E / GPT-Image)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
ALLOWED_IMAGE_HOSTS = {
    "oaidalleapiprodscus.blob.core.windows.net",
    "oaidalleapiprodscus.bblob.core.windows.net",  # sometimes typoed; harmless
    "oaidalleapiprodscus.blob.core.windows.net:443",
}

# ─────────────────────────────────────────────────────────────
# CORS Configuration
# ─────────────────────────────────────────────────────────────
# DEV: Allow localhost origins for local development
# PROD: Restrict to specific domains via ALLOWED_ORIGINS env var
#
# Example env var (comma-separated URLs only, no variable name prefix):
#   ALLOWED_ORIGINS=https://timrx.live,https://www.timrx.live,http://localhost:5503
#
DEV_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:5173",
    "http://localhost:5500",
    "http://localhost:5501",
    "http://localhost:5502",
    "http://localhost:5503",
    "http://localhost:8080",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5500",
    "http://127.0.0.1:5503",
]

# Production origins for TimrX frontend
PROD_ORIGINS = [
    "https://timrx.live",
    "https://www.timrx.live",
    "https://3d.timrx.live",
]


def _parse_cors_origins() -> list:
    """
    Parse ALLOWED_ORIGINS env var into a clean list of URLs.
    Handles common misconfiguration issues.
    """
    # Step 1: Get raw env var exactly as-is
    raw = os.getenv("ALLOWED_ORIGINS", "")
    print(f"[CORS] ────────────────────────────────────────────────────────")
    print(f"[CORS] Raw os.getenv('ALLOWED_ORIGINS'): {raw!r}")
    print(f"[CORS] Length: {len(raw)} chars")

    # Step 2: Strip whitespace
    raw = raw.strip()

    if not raw:
        if IS_DEV:
            print("[CORS] No ALLOWED_ORIGINS set, using dev + prod defaults")
            return DEV_ORIGINS + PROD_ORIGINS
        else:
            print("[CORS] No ALLOWED_ORIGINS set, using production defaults")
            return PROD_ORIGINS

    if raw == "*":
        # Wildcard mode: can't use literal * with credentials, use explicit list
        print("[CORS] Wildcard '*' requested - using prod + dev origins for credentials support")
        return PROD_ORIGINS + DEV_ORIGINS

    # Step 3: SANITIZE - Strip accidental "ALLOWED_ORIGINS=" prefix from the ENTIRE string
    # This happens when env var is misconfigured as: ALLOWED_ORIGINS=ALLOWED_ORIGINS=https://...
    # Check case-insensitive but preserve the actual URL casing
    if raw.upper().startswith("ALLOWED_ORIGINS="):
        old_raw = raw
        raw = raw[len("ALLOWED_ORIGINS="):]
        print(f"[CORS] WARNING: Stripped 'ALLOWED_ORIGINS=' prefix from value")
        print(f"[CORS] Before: {old_raw!r}")
        print(f"[CORS] After:  {raw!r}")

    # Step 4: Parse comma-separated list
    origins = [o.strip() for o in raw.split(",") if o.strip()]
    print(f"[CORS] After split: {origins}")

    # Step 5: Validate each origin is a valid URL
    valid_origins = []
    for origin in origins:
        if origin.startswith("http://") or origin.startswith("https://"):
            valid_origins.append(origin)
        else:
            print(f"[CORS] WARNING: Skipping invalid origin (not http/https): {origin!r}")

    if not valid_origins:
        print("[CORS] WARNING: No valid origins parsed, falling back to production defaults")
        return PROD_ORIGINS

    return valid_origins


# Parse and validate CORS origins at startup
ALLOWED_ORIGINS = _parse_cors_origins()

# Log the final parsed list clearly
print(f"[CORS] ════════════════════════════════════════════════════════")
print(f"[CORS] Final allowed origins ({len(ALLOWED_ORIGINS)} total):")
for i, origin in enumerate(ALLOWED_ORIGINS, 1):
    print(f"[CORS]   {i}. {origin}")
print(f"[CORS] ════════════════════════════════════════════════════════")

app = Flask(__name__)

# Configure CORS with explicit origin list (required for credentials)
# NO wildcards - explicit origins only for cookie/credential support
CORS(
    app,
    resources={r"/api/*": {"origins": ALLOWED_ORIGINS}},
    supports_credentials=True,  # Required for httpOnly cookies
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
    expose_headers=["Content-Type"],
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
)


@app.after_request
def _ensure_cors_headers(response):
    """
    Ensure CORS headers are present on all responses, including errors.
    Flask-CORS handles most cases, but this ensures preflight responses work.

    Headers set:
    - Access-Control-Allow-Origin: <origin> (NOT wildcard when using credentials)
    - Access-Control-Allow-Credentials: true
    - Vary: Origin (required for caching with dynamic origin)
    - Access-Control-Allow-Headers: Content-Type, Authorization, X-Requested-With
    - Access-Control-Allow-Methods: GET, POST, PUT, PATCH, DELETE, OPTIONS
    """
    origin = request.headers.get("Origin")

    # Always set Vary: Origin for proper caching when CORS varies by origin
    response.headers["Vary"] = "Origin"

    if origin and origin in ALLOWED_ORIGINS:
        # Explicitly set headers (Flask-CORS may miss some edge cases)
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Credentials"] = "true"

        # For preflight (OPTIONS) requests, include method and header info
        if request.method == "OPTIONS":
            response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization, X-Requested-With"

    return response

@app.before_request
def _set_anonymous_user():
    # Auth removed; keep request user_id consistently set to None.
    g.user_id = None

# ─────────────────────────────────────────────────────────────
# Register Blueprints (credits system)
# ─────────────────────────────────────────────────────────────
for name, bp, prefix in _loaded_blueprints:
    app.register_blueprint(bp, url_prefix=prefix)
# Log loaded blueprints for Render startup verification
print(f"[BOOT] Loaded blueprints: {[name for name, _, _ in _loaded_blueprints]}")

# ─────────────────────────────────────────────────────────────
# GET /api/wallet - Simple wallet balance endpoint
# ─────────────────────────────────────────────────────────────
@app.route("/api/wallet", methods=["GET"])
def api_wallet():
    """
    Get current wallet credits for the active identity/session.

    Response (200):
    {
        "identity_id": "uuid",
        "credits_balance": 150
    }

    Response (401 - no session):
    {
        "error": {"code": "UNAUTHORIZED", "message": "No valid session"}
    }
    """
    from identity_service import IdentityService

    # Get session from cookie
    session_id = IdentityService.get_session_id_from_request(request)
    if not session_id:
        return jsonify({
            "error": {
                "code": "UNAUTHORIZED",
                "message": "No valid session",
            }
        }), 401

    # Validate session
    identity = IdentityService.validate_session(session_id)
    if not identity:
        return jsonify({
            "error": {
                "code": "UNAUTHORIZED",
                "message": "Invalid or expired session",
            }
        }), 401

    identity_id = str(identity["id"])

    # Get balance
    try:
        balance = WalletService.get_balance(identity_id) if WalletService else 0
        return jsonify({
            "identity_id": identity_id,
            "credits_balance": balance,
        })
    except Exception as e:
        print(f"[WALLET] Error fetching balance: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to fetch wallet balance",
            }
        }), 500

# ─────────────────────────────────────────────────────────────
# Seed Default Plans (ensures plans exist in DB)
# ─────────────────────────────────────────────────────────────
if CREDITS_AVAILABLE and PricingService:
    try:
        PricingService.seed_plans()
    except Exception as e:
        print(f"[APP] Warning: Failed to seed plans: {e}")

# ─────────────────────────────────────────────────────────────
# Frontend Serving (same-origin for cookies)
# ─────────────────────────────────────────────────────────────
# Find frontend directory - check multiple locations
FRONTEND_DIR = None
_possible_frontend_paths = [
    APP_DIR / "frontend",                    # Deployed: frontend/ in same dir as app.py
    APP_DIR / ".." / ".." / "Frontend",      # Local dev: TimrX/Backend/meshy -> TimrX/Frontend
    APP_DIR.parent.parent / "Frontend",      # Alternative path
]
for _fp in _possible_frontend_paths:
    if _fp.exists() and _fp.is_dir():
        FRONTEND_DIR = _fp.resolve()
        break

# Get FRONTEND_BASE_URL for redirects when FRONTEND_DIR is not available
_FRONTEND_BASE_URL = None
if cfg and hasattr(cfg.config, 'FRONTEND_BASE_URL'):
    _FRONTEND_BASE_URL = cfg.config.FRONTEND_BASE_URL.rstrip("/") if cfg.config.FRONTEND_BASE_URL else None

if FRONTEND_DIR:
    print(f"[FRONTEND] Serving from: {FRONTEND_DIR}")
elif _FRONTEND_BASE_URL:
    print(f"[FRONTEND] No local frontend dir. Will redirect HTML routes to: {_FRONTEND_BASE_URL}")
else:
    print("[FRONTEND] WARNING: No FRONTEND_DIR or FRONTEND_BASE_URL. HTML routes will 404.")


def _redirect_to_frontend(path: str):
    """
    Redirect to frontend URL, preserving query string.
    Used when FRONTEND_DIR is not available but FRONTEND_BASE_URL is set.
    """
    if not _FRONTEND_BASE_URL:
        return jsonify({"error": "Frontend not configured. Set FRONTEND_BASE_URL env var."}), 404
    # Preserve query string (e.g., ?checkout=success)
    query = request.query_string.decode('utf-8')
    target_url = f"{_FRONTEND_BASE_URL}/{path.lstrip('/')}"
    if query:
        target_url = f"{target_url}?{query}"
    return redirect(target_url, code=302)


@app.route("/")
def serve_hub():
    """Serve hub.html at root."""
    if not FRONTEND_DIR:
        return _redirect_to_frontend("hub.html")
    return send_from_directory(FRONTEND_DIR, "hub.html")

@app.route("/3dprint")
@app.route("/3dprint.html")
def serve_3dprint():
    """Serve 3dprint.html."""
    if not FRONTEND_DIR:
        return _redirect_to_frontend("3dprint.html")
    return send_from_directory(FRONTEND_DIR, "3dprint.html")

@app.route("/hub.html")
def serve_hub_html():
    """Serve hub.html explicitly."""
    if not FRONTEND_DIR:
        return _redirect_to_frontend("hub.html")
    return send_from_directory(FRONTEND_DIR, "hub.html")

@app.route("/index.html")
def serve_index_html():
    """Serve index.html."""
    if not FRONTEND_DIR:
        return _redirect_to_frontend("index.html")
    return send_from_directory(FRONTEND_DIR, "index.html")

# Serve static assets (CSS, JS, etc.)
@app.route("/<path:filename>")
def serve_static_file(filename):
    """Serve static files from frontend directory."""
    if not FRONTEND_DIR:
        # For static assets, redirect if FRONTEND_BASE_URL is set
        if _FRONTEND_BASE_URL and filename.endswith(('.css', '.js', '.html', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.woff', '.woff2', '.ttf', '.eot')):
            return _redirect_to_frontend(filename)
        abort(404)
    # Security: only allow certain extensions
    allowed_extensions = {'.css', '.js', '.html', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.woff', '.woff2', '.ttf', '.eot'}
    ext = os.path.splitext(filename)[1].lower()
    if ext not in allowed_extensions:
        abort(404)
    try:
        return send_from_directory(FRONTEND_DIR, filename)
    except Exception:
        abort(404)

# ─────────────────────────────────────────────────────────────
# JSON Error Handlers
# Returns consistent error format: {error: {code, message, details?}}
# ─────────────────────────────────────────────────────────────
def make_error_response(code: str, message: str, status: int, details: dict = None):
    """Create a consistent error response."""
    error_body = {
        "error": {
            "code": code,
            "message": message,
        }
    }
    if details:
        error_body["error"]["details"] = details
    return jsonify(error_body), status


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    """Handle all HTTP exceptions with JSON response."""
    code = e.name.upper().replace(" ", "_")
    return make_error_response(code, e.description or str(e), e.code)


@app.errorhandler(400)
def handle_bad_request(e):
    """Handle 400 Bad Request."""
    message = e.description if hasattr(e, 'description') else "Bad request"
    return make_error_response("BAD_REQUEST", message, 400)


@app.errorhandler(401)
def handle_unauthorized(e):
    """Handle 401 Unauthorized."""
    return make_error_response("UNAUTHORIZED", "Authentication required", 401)


@app.errorhandler(403)
def handle_forbidden(e):
    """Handle 403 Forbidden."""
    return make_error_response("FORBIDDEN", "Access denied", 403)


@app.errorhandler(404)
def handle_not_found(e):
    """Handle 404 Not Found."""
    return make_error_response("NOT_FOUND", "Resource not found", 404)


@app.errorhandler(405)
def handle_method_not_allowed(e):
    """Handle 405 Method Not Allowed."""
    return make_error_response("METHOD_NOT_ALLOWED", "Method not allowed", 405)


@app.errorhandler(422)
def handle_unprocessable_entity(e):
    """Handle 422 Unprocessable Entity."""
    message = e.description if hasattr(e, 'description') else "Unprocessable entity"
    return make_error_response("UNPROCESSABLE_ENTITY", message, 422)


@app.errorhandler(429)
def handle_rate_limit(e):
    """Handle 429 Too Many Requests."""
    return make_error_response("RATE_LIMITED", "Too many requests, please slow down", 429)


@app.errorhandler(500)
def handle_internal_error(e):
    """Handle 500 Internal Server Error."""
    # Log the actual error for debugging
    print(f"[ERROR] Internal server error: {e}")
    return make_error_response("INTERNAL_ERROR", "An internal error occurred", 500)


# Handle DatabaseError from db module
if DatabaseError is not None:
    @app.errorhandler(DatabaseError)
    def handle_database_error(e):
        """Handle database errors."""
        print(f"[ERROR] Database error: {e}")
        return make_error_response(
            "DATABASE_ERROR",
            "A database error occurred",
            500,
            details={"type": type(e).__name__} if IS_DEV else None
        )


# ─────────────────────────────────────────────────────────────
# DEV ONLY: Local JSON file storage for job metadata
# In production (USE_DB=True), this is used as in-memory cache only
# ─────────────────────────────────────────────────────────────
_job_store_cache: Dict[str, Any] = {}  # In-memory cache for job metadata

def load_store() -> Dict[str, Any]:
    """
    Load job metadata store.
    - In production: returns in-memory cache only (no file I/O)
    - In dev mode: loads from job_store.json
    """
    global _job_store_cache
    if not LOCAL_DEV_MODE:
        return _job_store_cache
    # DEV ONLY: Load from file
    if not STORE_PATH.exists():
        return _job_store_cache
    try:
        _job_store_cache = json.loads(STORE_PATH.read_text(encoding="utf-8") or "{}")
        return _job_store_cache
    except Exception:
        return _job_store_cache

def save_store(data: Dict[str, Any]) -> None:
    """
    Save job metadata store.
    - In production: updates in-memory cache only (no file I/O)
    - In dev mode: writes to job_store.json
    """
    global _job_store_cache
    _job_store_cache = data
    if not LOCAL_DEV_MODE:
        return  # PROD: no file writes
    # DEV ONLY: Write to file
    try:
        STORE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"[DEV] Failed to save job_store.json: {e}")

# DEV ONLY: Initialize store file
if LOCAL_DEV_MODE and not STORE_PATH.exists():
    save_store({})

def load_history_store() -> list:
    """
    DEV ONLY: Load history from local JSON file.
    In production, this returns empty (DB is the source of truth).
    """
    if not LOCAL_DEV_MODE:
        return []  # PROD: DB is source of truth
    if not HISTORY_STORE_PATH.exists():
        return []
    try:
        data = json.loads(HISTORY_STORE_PATH.read_text(encoding="utf-8") or "[]")
        return data if isinstance(data, list) else []
    except Exception:
        return []

def save_history_store(arr: list) -> None:
    """
    DEV ONLY: Save history to local JSON file.
    In production, this is a no-op (DB is the source of truth).
    """
    if not LOCAL_DEV_MODE:
        return  # PROD: no local history file
    try:
        HISTORY_STORE_PATH.write_text(json.dumps(arr, ensure_ascii=False, indent=2))
    except Exception:
        pass

def _local_history_id(item: dict, fallback_id: str = None) -> str | None:
    """
    Pick a stable identifier for local history persistence.
    DEV ONLY helper.
    """
    if not isinstance(item, dict):
        return fallback_id
    return item.get("id") or item.get("job_id") or fallback_id

def upsert_history_local(item: dict, *, merge: bool = False) -> bool:
    """
    DEV ONLY: Persist a history item to the local JSON store.
    In production, this is a no-op.
    """
    if not LOCAL_DEV_MODE:
        return False  # PROD: no local history
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
        return False  # PROD: no local history
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
# Database helpers (Postgres)
# ─────────────────────────────────────────────────────────────
def get_db_conn():
    if not USE_DB:
        return None
    try:
        conn = psycopg.connect(DATABASE_URL, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute("SET search_path TO timrx_app, timrx_billing, public;")
        return conn
    except Exception as e:
        print(f"[DB] Connection error: {e}")
        return None

def ensure_history_table():
    """
    Verify database connection works. Tables are expected to already exist
    (created via TablePlus or migrations).
    """
    if not USE_DB:
        print("[DB] USE_DB is False, skipping table verification")
        return
    conn = get_db_conn()
    if not conn:
        print("[DB] Could not connect to database for table verification")
        return
    try:
        image_log_info = None
        with conn, conn.cursor() as cur:
            # Just verify tables exist, don't try to create them
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = 'history_items'
                );
            """, (APP_SCHEMA,))
            exists = cur.fetchone()[0]
            if exists:
                print("[DB] timrx_app.history_items exists - database connection verified!")
            else:
                print("[DB] WARNING: timrx_app.history_items does NOT exist!")
        conn.close()
    except Exception as e:
        print(f"[DB] Error verifying tables: {e}")
        try:
            conn.close()
        except Exception:
            pass

ensure_history_table()

def ensure_active_jobs_table():
    """Verify timrx_app.active_jobs exists (created via migrations)."""
    if not USE_DB:
        return
    conn = get_db_conn()
    if not conn:
        return
    try:
        model_log_info = None
        image_log_info = None
        with conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = 'active_jobs'
                );
            """, (APP_SCHEMA,))
            exists = cur.fetchone()[0]
            if exists:
                print("[DB] timrx_app.active_jobs exists - database connection verified!")
            else:
                print("[DB] WARNING: timrx_app.active_jobs does NOT exist!")
        conn.close()
    except Exception as e:
        print(f"[DB] Failed to verify active_jobs table: {e}")
        try:
            conn.close()
        except Exception:
            pass

ensure_active_jobs_table()

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


def _validate_history_item_asset_ids(model_id, image_id, context: str = ""):
    """
    Validate that exactly one of model_id or image_id is set (XOR constraint).
    Returns True if valid, False otherwise. Logs a warning if invalid.
    """
    has_model = model_id is not None
    has_image = image_id is not None
    if has_model == has_image:  # Both set or both NULL = invalid
        print(f"[WARN] history_items XOR violation ({context}): model_id={model_id}, image_id={image_id}")
        return False
    return True


def _parse_s3_bucket_and_key(url: str):
    """
    Extract S3 bucket and key from an S3 URL.
    Returns (bucket, key) tuple, or (None, None) if not an S3 URL.

    Supports formats:
    - https://bucket.s3.region.amazonaws.com/key
    - https://s3.region.amazonaws.com/bucket/key
    """
    if not is_s3_url(url):
        return None, None
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        path = parsed.path.lstrip("/")

        # Format: bucket.s3.region.amazonaws.com/key
        if host.endswith(".amazonaws.com") and ".s3." in host:
            bucket = host.split(".s3.")[0]
            key = path
            return bucket, key if key else None

        # Format: s3.region.amazonaws.com/bucket/key
        if host.startswith("s3.") and host.endswith(".amazonaws.com"):
            parts = path.split("/", 1)
            if len(parts) >= 1:
                bucket = parts[0]
                key = parts[1] if len(parts) > 1 else None
                return bucket, key

        return None, None
    except Exception:
        return None, None


def _lookup_asset_id_for_history(cur, item_type: str, job_id: str, glb_url: str = None, image_url: str = None, user_id: str = None, provider: str = None):
    """
    Try to find an existing model_id or image_id for a history item.
    Returns (model_id, image_id, reason) tuple:
    - If found: (model_id or None, image_id or None, None)
    - If ambiguous match: (None, None, "ambiguous_asset_match")
    - If not found: (None, None, "missing_asset_reference")

    Strict priority order (never uses weak matching):
    1. provider + upstream_id (strongest key)
    2. s3_bucket + s3_key (exact match on unique index)
    3. Never falls back to raw URL matching without S3 key extraction

    If multiple matches found, returns (None, None, "ambiguous_asset_match") and logs warning.
    """
    model_id = None
    image_id = None

    if item_type == "image":
        # Priority 1: Look up by provider + upstream_id (strongest key)
        if job_id:
            prov = provider or "openai"
            cur.execute(f"""
                SELECT id FROM {APP_SCHEMA}.images
                WHERE provider = %s AND upstream_id = %s
            """, (prov, str(job_id)))
            rows = cur.fetchall()
            if len(rows) == 1:
                image_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
            elif len(rows) > 1:
                ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                print(f"[WARN] _lookup_asset_id_for_history: multiple images for provider={prov}, upstream_id={job_id}: {ids}")
                return None, None, "ambiguous_asset_match"

        # Priority 2: Look up by s3_bucket + image_s3_key (unique index)
        if not image_id and image_url:
            bucket, key = _parse_s3_bucket_and_key(image_url)
            if bucket and key:
                cur.execute(f"""
                    SELECT id FROM {APP_SCHEMA}.images
                    WHERE s3_bucket = %s AND image_s3_key = %s
                """, (bucket, key))
                rows = cur.fetchall()
                if len(rows) == 1:
                    image_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
                elif len(rows) > 1:
                    ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                    print(f"[WARN] _lookup_asset_id_for_history: multiple images for s3_bucket={bucket}, key={key}: {ids}")
                    return None, None, "ambiguous_asset_match"

        # Priority 3: Look up by s3_bucket + thumbnail_s3_key (unique index)
        if not image_id and image_url:
            bucket, key = _parse_s3_bucket_and_key(image_url)
            if bucket and key:
                cur.execute(f"""
                    SELECT id FROM {APP_SCHEMA}.images
                    WHERE s3_bucket = %s AND thumbnail_s3_key = %s
                """, (bucket, key))
                rows = cur.fetchall()
                if len(rows) == 1:
                    image_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
                elif len(rows) > 1:
                    ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                    print(f"[WARN] _lookup_asset_id_for_history: multiple images for s3_bucket={bucket}, thumb_key={key}: {ids}")
                    return None, None, "ambiguous_asset_match"
    else:
        # Priority 1: Look up by provider + upstream_job_id (strongest key)
        if job_id:
            prov = provider or "meshy"
            cur.execute(f"""
                SELECT id FROM {APP_SCHEMA}.models
                WHERE provider = %s AND upstream_job_id = %s
            """, (prov, str(job_id)))
            rows = cur.fetchall()
            if len(rows) == 1:
                model_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
            elif len(rows) > 1:
                ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                print(f"[WARN] _lookup_asset_id_for_history: multiple models for provider={prov}, upstream_job_id={job_id}: {ids}")
                return None, None, "ambiguous_asset_match"

        # Priority 2: Look up by s3_bucket + glb_s3_key (unique index)
        if not model_id and glb_url:
            bucket, key = _parse_s3_bucket_and_key(glb_url)
            if bucket and key:
                cur.execute(f"""
                    SELECT id FROM {APP_SCHEMA}.models
                    WHERE s3_bucket = %s AND glb_s3_key = %s
                """, (bucket, key))
                rows = cur.fetchall()
                if len(rows) == 1:
                    model_id = rows[0][0] if isinstance(rows[0], tuple) else rows[0].get("id")
                elif len(rows) > 1:
                    ids = [r[0] if isinstance(r, tuple) else r.get("id") for r in rows]
                    print(f"[WARN] _lookup_asset_id_for_history: multiple models for s3_bucket={bucket}, key={key}: {ids}")
                    return None, None, "ambiguous_asset_match"

    # Return result with reason if not found
    if model_id or image_id:
        return model_id, image_id, None
    return None, None, "missing_asset_reference"


def save_active_job_to_db(job_id: str, job_type: str, stage: str = None, metadata: dict = None, user_id: str = None):
    """Save active job to database for recovery"""
    if not USE_DB:
        return False
    conn = get_db_conn()
    if not conn:
        return False
    try:
        job_meta = metadata or {}
        payload = dict(job_meta)
        payload.setdefault("job_type", job_type)
        payload.setdefault("stage", stage)
        payload.setdefault("original_job_id", job_id)

        try:
            uuid.UUID(str(job_id))
            history_id = str(job_id)
        except (ValueError, TypeError):
            history_id = str(uuid.uuid4())

        item_type = "image" if (job_type or "").lower() in ("image", "openai_image") else "model"
        title = job_meta.get("title") or (job_meta.get("prompt", "")[:50] if job_meta.get("prompt") else None)
        prompt = job_meta.get("prompt")
        root_prompt = job_meta.get("root_prompt")
        thumbnail_url = job_meta.get("thumbnail_url")
        glb_url = job_meta.get("glb_url")
        image_url = job_meta.get("image_url")

        s3_user_id = user_id or "public"
        provider = _map_provider(job_type)
        thumb_key_base = f"thumbnails/{s3_user_id}/{job_id}"
        image_key_base = f"images/{s3_user_id}/{job_id}"
        if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
            thumbnail_url = ensure_s3_url_for_data_uri(thumbnail_url, "thumbnails", thumb_key_base, user_id=user_id, name="thumbnail", provider=provider)
        if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
            image_url = ensure_s3_url_for_data_uri(image_url, "images", image_key_base, user_id=user_id, name="image", provider=provider)
        payload["thumbnail_url"] = thumbnail_url
        payload["image_url"] = image_url

        with conn, conn.cursor(row_factory=dict_row) as cur:
            # NOTE: We no longer create history_items placeholders here.
            # The history_item will be created by the job completion webhook
            # after the model/image asset exists (required by XOR constraint).
            # Use active_jobs table for tracking in-progress jobs.

            action_code = _map_action_code(job_type)
            provider = _map_provider(job_type)
            cur.execute(f"""
                SELECT id FROM {APP_SCHEMA}.active_jobs
                WHERE upstream_job_id = %s
                LIMIT 1
            """, (job_id,))
            existing = cur.fetchone()
            progress = int(job_meta.get("pct") or 0)
            if existing:
                cur.execute(f"""
                    UPDATE {APP_SCHEMA}.active_jobs
                    SET identity_id = COALESCE(%s, identity_id),
                        provider = %s,
                        action_code = %s,
                        status = 'running',
                        progress = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (user_id, provider, action_code, progress, existing["id"]))
            else:
                cur.execute(f"""
                    INSERT INTO {APP_SCHEMA}.active_jobs (
                        id, identity_id, provider, action_code, upstream_job_id,
                        status, progress
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        'running', %s
                    )
                """, (str(uuid.uuid4()), user_id, provider, action_code, job_id, progress))
        conn.close()
        return True
    except Exception as e:
        print(f"[DB] Failed to save active job {job_id}: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return False

def get_active_jobs_from_db(user_id: str = None):
    """Retrieve active jobs from database, filtered by user_id if provided"""
    if not USE_DB:
        return []
    conn = get_db_conn()
    if not conn:
        return []
    try:
        with conn, conn.cursor(row_factory=dict_row) as cur:
            if user_id:
                cur.execute(f"""
                    SELECT aj.upstream_job_id AS job_id,
                           aj.status,
                           aj.progress,
                           aj.action_code,
                           aj.provider,
                           aj.created_at,
                           hi.stage,
                           hi.payload
                    FROM {APP_SCHEMA}.active_jobs aj
                    LEFT JOIN {APP_SCHEMA}.history_items hi
                        ON aj.related_history_id = hi.id
                    WHERE aj.status IN ('queued', 'running')
                      AND aj.identity_id = %s
                    ORDER BY aj.created_at DESC
                """, (user_id,))
            else:
                # Anonymous users only see jobs without user_id
                cur.execute(f"""
                    SELECT aj.upstream_job_id AS job_id,
                           aj.status,
                           aj.progress,
                           aj.action_code,
                           aj.provider,
                           aj.created_at,
                           hi.stage,
                           hi.payload
                    FROM {APP_SCHEMA}.active_jobs aj
                    LEFT JOIN {APP_SCHEMA}.history_items hi
                        ON aj.related_history_id = hi.id
                    WHERE aj.status IN ('queued', 'running')
                      AND aj.identity_id IS NULL
                    ORDER BY aj.created_at DESC
                """)
            rows = cur.fetchall()
        conn.close()
        results = []
        for row in rows:
            payload = row["payload"] if row["payload"] else {}
            if isinstance(payload, str):
                payload = json.loads(payload)
            results.append({
                "job_id": row["job_id"],
                "job_type": payload.get("job_type"),
                "stage": row["stage"] or payload.get("stage"),
                "metadata": payload,
                "status": row["status"],
                "progress": row["progress"],
                "created_at": row["created_at"],
            })
        return results
    except Exception as e:
        print(f"[DB] Failed to get active jobs: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return []

def mark_job_completed_in_db(job_id: str, user_id: str = None):
    """Mark job as completed in database (only if user owns it or job has no user)"""
    if not USE_DB:
        return
    conn = get_db_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            if user_id:
                cur.execute(f"""
                    UPDATE {APP_SCHEMA}.active_jobs
                    SET status = 'succeeded', updated_at = NOW()
                    WHERE upstream_job_id = %s
                      AND (identity_id = %s OR identity_id IS NULL)
                """, (job_id, user_id))
            else:
                # Anonymous can only complete jobs without user_id
                cur.execute(f"""
                    UPDATE {APP_SCHEMA}.active_jobs
                    SET status = 'succeeded', updated_at = NOW()
                    WHERE upstream_job_id = %s AND identity_id IS NULL
                """, (job_id,))
            # Clean up old completed jobs (keep last 100 per user)
            if user_id:
                cur.execute(f"""
                    DELETE FROM {APP_SCHEMA}.active_jobs
                    WHERE id IN (
                        SELECT id FROM {APP_SCHEMA}.active_jobs
                        WHERE status = 'succeeded' AND identity_id = %s
                        ORDER BY updated_at DESC
                        OFFSET 100
                    )
                """, (user_id,))
            else:
                cur.execute(f"""
                    DELETE FROM {APP_SCHEMA}.active_jobs
                    WHERE id IN (
                        SELECT id FROM {APP_SCHEMA}.active_jobs
                        WHERE status = 'succeeded' AND identity_id IS NULL
                        ORDER BY updated_at DESC
                        OFFSET 100
                    )
                """)
        conn.close()
    except Exception as e:
        print(f"[DB] Failed to mark job completed {job_id}: {e}")
        try:
            conn.close()
        except Exception:
            pass

def save_image_to_normalized_db(image_id: str, image_url: str, prompt: str, ai_model: str, size: str, image_urls: list = None, user_id: str = None):
    """
    Save generated image to normalized tables (history_items, images).
    Called immediately after OpenAI image generation completes.
    Uses the user's schema with UUID primary keys and correct column names.
    """
    if not USE_DB:
        print("[DB] USE_DB is False, skipping save_image_to_normalized_db")
        return False
    conn = get_db_conn()
    if not conn:
        print("[DB] Could not get DB connection for save_image_to_normalized_db")
        return False

    try:
        with conn, conn.cursor() as cur:
            # Check if this image already exists for this user (idempotent updates)
            existing_history_id = None
            if user_id:
                cur.execute(f"""
                    SELECT id, image_id FROM {APP_SCHEMA}.history_items
                    WHERE (payload->>'original_id' = %s OR id::text = %s) AND identity_id = %s
                    LIMIT 1
                """, (image_id, image_id, user_id))
            else:
                cur.execute(f"""
                    SELECT id, image_id FROM {APP_SCHEMA}.history_items
                    WHERE (payload->>'original_id' = %s OR id::text = %s) AND identity_id IS NULL
                    LIMIT 1
                """, (image_id, image_id))
            existing = cur.fetchone()
            if existing:
                existing_history_id = str(existing[0])

            title = (prompt[:50] if prompt else "Generated Image")
            s3_slug = sanitize_filename(title or prompt or "image") or "image"
            s3_slug = s3_slug[:60]
            s3_user_id = str(user_id) if user_id else "public"
            job_key = sanitize_filename(str(image_id)) or "image"
            image_key_base = f"images/{s3_user_id}/{job_key}/{s3_slug}"
            # Upload images to S3 for permanent storage (OpenAI URLs expire, base64 needs storage)
            image_content_hash = None
            image_s3_key_from_upload = None
            image_reused = None
            original_image_url = image_url
            uploaded_url_cache = {}
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
                image_url, image_content_hash, image_s3_key_from_upload, image_reused = _unpack_upload_result(upload_result)
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
            if image_s3_key:
                image_log_info = {
                    "key": image_s3_key,
                    "hash": image_content_hash,
                    "reused": bool(image_reused),
                }

            # Parse size for width/height
            width, height = 1024, 1024
            if size and 'x' in size:
                parts = size.split('x')
                try:
                    width, height = int(parts[0]), int(parts[1])
                except ValueError:
                    pass

            # Generate UUIDs for idempotent writes
            history_uuid = existing_history_id or str(uuid.uuid4())
            image_uuid = str(uuid.uuid4())
            upstream_id = image_id if image_id else None

            # Store extra metadata in payload JSONB
            payload = {
                "original_id": image_id,
                "ai_model": ai_model,
                "size": size,
                "image_urls": image_urls or [image_url],
                "s3_bucket": AWS_BUCKET_MODELS if AWS_BUCKET_MODELS else None,
                "image_url": image_url,
                "thumbnail_url": image_url,
            }

            image_meta = json.dumps({
                "prompt": prompt,
                "ai_model": ai_model,
                "size": size,
                "format": "png",
                "image_urls": image_urls or [image_url],
            })

            existing_by_hash_id = None
            if image_content_hash:
                cur.execute(f"""
                    SELECT id FROM {APP_SCHEMA}.images
                    WHERE provider = %s AND content_hash = %s
                    LIMIT 1
                """, ("openai", image_content_hash))
                row = cur.fetchone()
                if row:
                    existing_by_hash_id = row[0]

            s3_bucket = AWS_BUCKET_MODELS if AWS_BUCKET_MODELS else None
            if existing_by_hash_id:
                cur.execute(f"""
                    UPDATE {APP_SCHEMA}.images
                    SET identity_id = COALESCE(%s, identity_id),
                        title = COALESCE(%s, title),
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
                """, (
                    user_id,
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
                ))
            elif upstream_id:
                cur.execute(f"""
                    INSERT INTO {APP_SCHEMA}.images (
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
                    SET identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.images.identity_id),
                        title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.images.title),
                        prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.images.prompt),
                        status = EXCLUDED.status,
                        s3_bucket = COALESCE(EXCLUDED.s3_bucket, {APP_SCHEMA}.images.s3_bucket),
                        image_url = COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.images.image_url),
                        thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.images.thumbnail_url),
                        image_s3_key = COALESCE(EXCLUDED.image_s3_key, {APP_SCHEMA}.images.image_s3_key),
                        thumbnail_s3_key = COALESCE(EXCLUDED.thumbnail_s3_key, {APP_SCHEMA}.images.thumbnail_s3_key),
                        width = COALESCE(EXCLUDED.width, {APP_SCHEMA}.images.width),
                        height = COALESCE(EXCLUDED.height, {APP_SCHEMA}.images.height),
                        content_hash = COALESCE(EXCLUDED.content_hash, {APP_SCHEMA}.images.content_hash),
                        meta = EXCLUDED.meta,
                        updated_at = NOW()
                    RETURNING id
                """, (
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
                ))
            elif image_url:
                cur.execute(f"""
                    INSERT INTO {APP_SCHEMA}.images (
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
                    SET identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.images.identity_id),
                        title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.images.title),
                        prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.images.prompt),
                        status = EXCLUDED.status,
                        image_url = COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.images.image_url),
                        thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.images.thumbnail_url),
                        width = COALESCE(EXCLUDED.width, {APP_SCHEMA}.images.width),
                        height = COALESCE(EXCLUDED.height, {APP_SCHEMA}.images.height),
                        content_hash = COALESCE(EXCLUDED.content_hash, {APP_SCHEMA}.images.content_hash),
                        meta = EXCLUDED.meta,
                        updated_at = NOW()
                    RETURNING id
                """, (
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
                ))
            else:
                cur.execute(f"""
                    INSERT INTO {APP_SCHEMA}.images (
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
                """, (
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
                ))

            image_row = cur.fetchone()
            if not image_row:
                raise RuntimeError("[DB] Failed to upsert images row (no id returned)")
            returned_image_id = image_row[0]
            print(f"[DB] image persisted: image_id={returned_image_id} image_url={image_url} thumb={image_url}")

            if existing_history_id:
                cur.execute(f"""
                    UPDATE {APP_SCHEMA}.history_items
                    SET item_type = %s,
                        status = COALESCE(%s, status),
                        stage = COALESCE(%s, stage),
                        title = COALESCE(%s, title),
                        prompt = COALESCE(%s, prompt),
                        root_prompt = COALESCE(%s, root_prompt),
                        identity_id = COALESCE(%s, identity_id),
                        thumbnail_url = COALESCE(%s, thumbnail_url),
                        image_url = COALESCE(%s, image_url),
                        image_id = %s,
                        payload = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (
                    "image",
                    "finished",
                    "image",
                    title,
                    prompt,
                    None,
                    user_id,
                    image_url,
                    image_url,
                    returned_image_id,
                    json.dumps(payload),
                    history_uuid,
                ))
                print(f"[history] updated item: history_id={history_uuid}, model_id=None, image_id={returned_image_id}, job_id={image_id}")
            else:
                cur.execute(f"""
                    INSERT INTO {APP_SCHEMA}.history_items (
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
                        status = COALESCE(EXCLUDED.status, {APP_SCHEMA}.history_items.status),
                        stage = COALESCE(EXCLUDED.stage, {APP_SCHEMA}.history_items.stage),
                        title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.history_items.title),
                        prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.history_items.prompt),
                        root_prompt = COALESCE(EXCLUDED.root_prompt, {APP_SCHEMA}.history_items.root_prompt),
                        identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.history_items.identity_id),
                        thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.history_items.thumbnail_url),
                        image_url = COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.history_items.image_url),
                        image_id = COALESCE(EXCLUDED.image_id, {APP_SCHEMA}.history_items.image_id),
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                """, (
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
                ))
                print(f"[history] inserted item: history_id={history_uuid}, model_id=None, image_id={returned_image_id}, job_id={image_id}")

        conn.close()
        if image_log_info:
            print(f"[S3] image stored: key={image_log_info['key']} hash={image_log_info['hash']} reused={image_log_info['reused']}")
        return returned_image_id
    except Exception as e:
        print(f"[DB] Failed to save image {image_id}: {e}")
        if image_url and is_s3_url(image_url):
            print(f"[DB] ERROR: S3 upload succeeded but DB save failed image_id={image_id} image_url={image_url}")
        import traceback
        traceback.print_exc()
        try:
            conn.close()
        except Exception:
            pass
        return None

def save_finished_job_to_normalized_db(job_id: str, status_data: dict, job_meta: dict, job_type: str = 'model', user_id: str = None):
    """
    Save finished job data to normalized tables (history_items, models, images).
    Called when a job status becomes 'done'.
    Uses the user's schema with UUID primary keys and correct column names.
    user_id is extracted from job_meta if not provided.
    """
    model_log_info = None
    image_log_info = None
    # Get user_id from job_meta if not provided
    if not user_id:
        user_id = job_meta.get("user_id")
    if not USE_DB:
        print("[DB] USE_DB is False, skipping save_finished_job_to_normalized_db")
        return False
    conn = get_db_conn()
    if not conn:
        print("[DB] Could not get DB connection for save_finished_job_to_normalized_db")
        return False

    try:
        with conn, conn.cursor(row_factory=dict_row) as cur:
            db_errors = []
            # Merge status_data and job_meta
            glb_url = status_data.get("glb_url") or status_data.get("textured_glb_url")
            thumbnail_url = status_data.get("thumbnail_url")
            image_url = status_data.get("image_url")
            model_urls = _filter_model_urls(status_data.get("model_urls") or {})
            textured_model_urls = _filter_model_urls(status_data.get("textured_model_urls") or {})
            textured_glb_url = status_data.get("textured_glb_url")
            rigged_glb_url = status_data.get("rigged_character_glb_url")
            rigged_fbx_url = status_data.get("rigged_character_fbx_url")
            raw_texture_urls = status_data.get("texture_urls")
            texture_items = []
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

            # Idempotency guard: already finished with S3 URL for this asset type
            if user_id:
                cur.execute(f"""
                    SELECT status, glb_url, image_url, item_type
                    FROM {APP_SCHEMA}.history_items
                    WHERE (id::text = %s OR payload->>'original_job_id' = %s)
                      AND (identity_id = %s OR identity_id IS NULL)
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (str(job_id), str(job_id), user_id))
            else:
                cur.execute(f"""
                    SELECT status, glb_url, image_url, item_type
                    FROM {APP_SCHEMA}.history_items
                    WHERE (id::text = %s OR payload->>'original_job_id' = %s)
                      AND identity_id IS NULL
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (str(job_id), str(job_id)))
            existing_history = cur.fetchone()
            if existing_history:
                asset_type = existing_history.get("item_type") or ("image" if job_type in ("image", "openai_image") else "model")
                asset_url = existing_history.get("image_url") if asset_type == "image" else existing_history.get("glb_url")
                if existing_history.get("status") == "finished" and is_s3_url(asset_url):
                    print(f"[DB] save_finished_job skipped: already finished with S3 (job_id={job_id}, stage={final_stage})")
                    conn.close()
                    return {"success": True, "db_ok": True, "skipped": True}

            try:
                cur.execute(f"""
                    SELECT stage, canonical_url
                    FROM {APP_SCHEMA}.asset_saves
                    WHERE provider = %s AND asset_type = %s AND upstream_id = %s
                    LIMIT 1
                """, (provider, asset_type, str(job_id)))
                existing_save = cur.fetchone()
                if existing_save and existing_save.get("stage") == final_stage and existing_save.get("canonical_url"):
                    print(f"[DB] save_finished_job skipped: already saved (provider={provider}, job_id={job_id}, stage={final_stage})")
                    conn.close()
                    return {"success": True, "db_ok": len(db_errors) == 0, "db_errors": db_errors or None, "skipped": True}
            except Exception as e:
                log_db_continue("asset_saves_precheck", e)
                db_errors.append({"op": "asset_saves_precheck", "error": str(e)})
                existing_save = None

            # Get the name for S3 files from prompt or title
            s3_name = job_meta.get("prompt") or job_meta.get("title") or "model"
            s3_name_safe = sanitize_filename(s3_name) or "model"
            s3_name_safe = s3_name_safe[:80]
            job_key = str(job_id)
            s3_key_name = s3_name_safe

            print(f"[DB] save_finished_job: job_id={job_id}, job_type={job_type}")
            print(f"[DB] Input URLs from Meshy:")
            print(f"[DB]   glb_url: {glb_url[:80] if glb_url else 'None'}...")
            print(f"[DB]   thumbnail_url: {thumbnail_url[:80] if thumbnail_url else 'None'}...")
            print(f"[DB]   textured_glb_url: {textured_glb_url[:80] if textured_glb_url else 'None'}...")
            print(f"[DB] job_meta: title={job_meta.get('title')}, prompt={job_meta.get('prompt', '')[:50]}...")
            print(f"[DB] S3 model key will use: models/{provider}/<content_hash>.glb")

            # Upload ALL URLs to S3 for permanent storage (Meshy URLs expire)
            model_content_hash = None
            model_s3_key_from_upload = None
            model_reused = None
            thumbnail_content_hash = None
            thumbnail_s3_key_from_upload = None
            thumbnail_reused = None
            glb_candidate = textured_glb_url or glb_url or textured_model_urls.get("glb") or model_urls.get("glb")
            obj_candidate = textured_model_urls.get("obj") or model_urls.get("obj") or status_data.get("obj_url")
            primary_model_source = glb_candidate or obj_candidate or rigged_glb_url
            primary_content_type = get_content_type_from_url(primary_model_source) if primary_model_source else "model/gltf-binary"
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
                primary_glb_url, model_content_hash, model_s3_key_from_upload, model_reused = _unpack_upload_result(upload_result)
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
                thumbnail_url, thumbnail_content_hash, thumbnail_s3_key_from_upload, thumbnail_reused = _unpack_upload_result(upload_result)
            rigged_fbx_url = None

            # Prefer textured output as the canonical model when available
            if not primary_model_source:
                primary_glb_url = None
            s3_glb_url = primary_glb_url
            s3_thumbnail_url = thumbnail_url
            if AWS_BUCKET_MODELS and s3_glb_url and not is_s3_url(s3_glb_url):
                print(f"[WARN] canonical url is not S3: glb_url={s3_glb_url[:80]}")
                s3_glb_url = None
            if AWS_BUCKET_MODELS and s3_thumbnail_url and not is_s3_url(s3_thumbnail_url):
                print(f"[WARN] canonical url is not S3: thumbnail_url={s3_thumbnail_url[:80]}")
                s3_thumbnail_url = None
            final_glb_url = s3_glb_url
            final_thumbnail_url = s3_thumbnail_url

            # Upload texture images to S3
            texture_s3_urls = {}
            texture_urls = []
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
                cur.execute(f"""
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
                """, (provider, str(job_id), asset_type, final_stage, canonical_url))
                cur.execute("RELEASE SAVEPOINT asset_saves_upsert")
            except Exception as e:
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT asset_saves_upsert")
                    cur.execute("RELEASE SAVEPOINT asset_saves_upsert")
                except Exception:
                    pass
                log_db_continue("asset_saves_upsert", e)
                db_errors.append({"op": "asset_saves_upsert", "error": str(e)})

            # Determine item type
            item_type = "image" if is_image_output else "model"

            # Use the original job_id as the history item ID if it's a valid UUID
            # This ensures frontend and backend use the same ID
            try:
                uuid.UUID(str(job_id))
                history_uuid = str(job_id)
                print(f"[DB] Using job_id as history UUID: {history_uuid}")
            except (ValueError, TypeError):
                history_uuid = str(uuid.uuid4())
                print(f"[DB] Generated new history UUID: {history_uuid} (job_id {job_id} not a valid UUID)")

            # Store extra metadata in payload JSONB (all the fields that don't fit the schema)
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
                # S3 URLs for all model variants
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

            # Log final URLs after S3 upload
            final_title = job_meta.get("title") or (job_meta.get("prompt", "")[:50] if job_meta.get("prompt") else DEFAULT_MODEL_TITLE)
            glb_s3_key = model_s3_key_from_upload or get_s3_key_from_url(final_glb_url)
            thumbnail_s3_key = thumbnail_s3_key_from_upload or get_s3_key_from_url(final_thumbnail_url)
            image_s3_key = None
            thumb_s3_key = None
            image_reused = None
            print(f"[DB] Final URLs after S3 upload:")
            print(f"[DB]   glb_url: {final_glb_url[:80] if final_glb_url else 'None'}...")
            print(f"[DB]   thumbnail_url: {final_thumbnail_url[:80] if final_thumbnail_url else 'None'}...")
            print(f"[DB]   title: {final_title}")

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
                        cur.execute(f"""
                            SELECT id FROM {APP_SCHEMA}.models
                            WHERE provider = %s AND content_hash = %s
                            LIMIT 1
                        """, (provider, model_content_hash))
                        row = cur.fetchone()
                        if row:
                            existing_by_hash_id = row["id"]

                    if existing_by_hash_id:
                        cur.execute(f"""
                            UPDATE {APP_SCHEMA}.models
                            SET identity_id = COALESCE(%s, identity_id),
                                title = COALESCE(%s, title),
                                prompt = COALESCE(%s, prompt),
                                root_prompt = COALESCE(%s, root_prompt),
                                upstream_id = COALESCE(upstream_id, %s),
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
                        """, (
                            user_id,
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
                        ))
                    else:
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
                            ON CONFLICT (provider, upstream_id) DO UPDATE
                            SET identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.models.identity_id),
                                title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.models.title),
                                prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.models.prompt),
                                root_prompt = COALESCE(EXCLUDED.root_prompt, {APP_SCHEMA}.models.root_prompt),
                                status = 'ready',
                                s3_bucket = COALESCE(EXCLUDED.s3_bucket, {APP_SCHEMA}.models.s3_bucket),
                                glb_url = EXCLUDED.glb_url,
                                thumbnail_url = EXCLUDED.thumbnail_url,
                                glb_s3_key = COALESCE(EXCLUDED.glb_s3_key, {APP_SCHEMA}.models.glb_s3_key),
                                thumbnail_s3_key = COALESCE(EXCLUDED.thumbnail_s3_key, {APP_SCHEMA}.models.thumbnail_s3_key),
                                content_hash = COALESCE(EXCLUDED.content_hash, {APP_SCHEMA}.models.content_hash),
                                stage = COALESCE(EXCLUDED.stage, {APP_SCHEMA}.models.stage),
                                meta = COALESCE(EXCLUDED.meta, {APP_SCHEMA}.models.meta),
                                updated_at = NOW()
                            RETURNING id
                        """, (
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
                        ))
                    model_row = cur.fetchone()
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
                        image_url, image_content_hash, image_s3_key_from_upload, image_reused = _unpack_upload_result(upload_result)
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
                    ))
                    image_row = cur.fetchone()
                    if not image_row:
                        raise RuntimeError("[DB] Failed to upsert image row (no id returned)")
                    image_id = image_row["id"]
                    if image_s3_key:
                        image_log_info = {
                            "key": image_s3_key,
                            "hash": image_content_hash,
                            "reused": bool(image_reused),
                        }

                # XOR validation: exactly one of model_id or image_id must be set
                has_model = model_id is not None
                has_image = image_id is not None
                if has_model == has_image:  # Both set or both NULL = invalid
                    if not has_model and not has_image:
                        print(f"[WARN] history_items XOR violation: both model_id and image_id are NULL (job_id={job_id})")
                        # Skip history_items insert - no asset was created
                        raise RuntimeError(f"Cannot create history_items: no model or image asset created for job {job_id}")
                    else:
                        print(f"[WARN] history_items XOR violation: both model_id={model_id} and image_id={image_id} are set (job_id={job_id})")
                        # Prefer the asset matching item_type
                        if item_type == "image":
                            model_id = None
                        else:
                            image_id = None
                        print(f"[WARN] Resolved: keeping {item_type}_id, cleared the other")

                cur.execute(f"""
                    INSERT INTO {APP_SCHEMA}.history_items (
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
                        title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.history_items.title),
                        prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.history_items.prompt),
                        root_prompt = COALESCE(EXCLUDED.root_prompt, {APP_SCHEMA}.history_items.root_prompt),
                        identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.history_items.identity_id),
                        thumbnail_url = EXCLUDED.thumbnail_url,
                        glb_url = EXCLUDED.glb_url,
                        image_url = EXCLUDED.image_url,
                        -- XOR enforcement: use EXCLUDED values directly, clearing the other if one is set
                        model_id = CASE
                            WHEN EXCLUDED.model_id IS NOT NULL THEN EXCLUDED.model_id
                            WHEN EXCLUDED.image_id IS NOT NULL THEN NULL
                            ELSE {APP_SCHEMA}.history_items.model_id
                        END,
                        image_id = CASE
                            WHEN EXCLUDED.image_id IS NOT NULL THEN EXCLUDED.image_id
                            WHEN EXCLUDED.model_id IS NOT NULL THEN NULL
                            ELSE {APP_SCHEMA}.history_items.image_id
                        END,
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                    RETURNING id
                """, (
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
                ))
                history_row = cur.fetchone()
                if not history_row:
                    raise RuntimeError("[DB] Failed to upsert history_items row (no id returned)")
                history_item_id = history_row["id"]
                print(f"[history] inserted item: history_id={history_item_id}, model_id={model_id}, image_id={image_id}, job_id={job_id}")
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
                if cleanup_keys:
                    try:
                        s3.delete_objects(
                            Bucket=AWS_BUCKET_MODELS,
                            Delete={"Objects": [{"Key": key} for key in cleanup_keys]},
                        )
                        print(f"[S3] cleanup: deleted {len(cleanup_keys)} objects after DB failure")
                    except Exception as cleanup_err:
                        print(f"[S3] cleanup failed: {cleanup_err}; keys={cleanup_keys}")
                else:
                    print(f"[S3] cleanup skipped; keys={cleanup_keys}")

        conn.close()
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
        # Return the S3 URLs so the calling endpoint can use them in the API response
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
        import traceback
        traceback.print_exc()
        try:
            conn.close()
        except Exception:
            pass
        return None

def delete_active_job_from_db(job_id: str, user_id: str = None):
    """Remove job from active jobs table (only if user owns it or job has no user)"""
    if not USE_DB:
        return
    conn = get_db_conn()
    if not conn:
        return
    try:
        with conn, conn.cursor() as cur:
            if user_id:
                cur.execute(
                    f"DELETE FROM {APP_SCHEMA}.active_jobs WHERE upstream_job_id = %s AND (identity_id = %s OR identity_id IS NULL)",
                    (job_id, user_id),
                )
            else:
                cur.execute(
                    f"DELETE FROM {APP_SCHEMA}.active_jobs WHERE upstream_job_id = %s AND identity_id IS NULL",
                    (job_id,),
                )
        conn.close()
    except Exception as e:
        print(f"[DB] Failed to delete active job {job_id}: {e}")
        try:
            conn.close()
        except Exception:
            pass

def verify_job_ownership(job_id: str, user_id: str = None) -> bool:
    """
    Verify that a user owns a job (or job is unowned for anonymous users).
    Checks both local store and database.
    Returns True if user can access this job, False otherwise.
    """
    # First check local store (in-memory jobs)
    store = load_store()
    if job_id in store:
        job_user_id = store[job_id].get("user_id")
        if user_id:
            # Logged in user: can access their own jobs or unowned jobs
            return job_user_id == user_id or job_user_id is None
        else:
            # Anonymous: can only access unowned jobs
            return job_user_id is None

    # Check database
    if not USE_DB:
        return True  # Dev/no-DB mode stays permissive for local testing

    conn = get_db_conn()
    if not conn:
        # In production with DB configured, fail closed if we cannot verify ownership
        print(f"[DB] verify_job_ownership: DB unavailable for {job_id}, denying access")
        return False

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT identity_id FROM {APP_SCHEMA}.active_jobs WHERE upstream_job_id = %s
                UNION
                SELECT identity_id FROM {APP_SCHEMA}.history_items WHERE id::text = %s
                   OR payload->>'original_job_id' = %s
                   OR payload->>'job_id' = %s
                LIMIT 1
            """, (job_id, job_id, job_id, job_id))
            row = cur.fetchone()
        conn.close()

        if not row:
            # Job not in DB - could be very new (not yet persisted) or doesn't exist
            # For security, only allow if job is in local store (checked above)
            return False

        job_user_id = str(row[0]) if row[0] else None
        if user_id:
            return job_user_id == user_id or job_user_id is None
        else:
            return job_user_id is None
    except Exception as e:
        print(f"[DB] verify_job_ownership failed for {job_id}: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return True  # On error, allow (best effort)

def resolve_meshy_job_id(job_id: str) -> str:
    """
    Resolve a job ID to the original Meshy job ID.
    If the ID is our database UUID, look up the original_job_id from payload.
    If it's already a Meshy job ID (in local store), return as-is.
    """
    if not job_id:
        return job_id

    # First check if it's in the local store (these are Meshy job IDs)
    store = load_store()
    if job_id in store:
        print(f"[Resolve] {job_id} found in local store, using as-is")
        return job_id

    # Check database for original_job_id
    if USE_DB:
        conn = get_db_conn()
        if conn:
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Look up by our database ID to find original Meshy job ID
                    cur.execute(f"""
                        SELECT payload->>'original_job_id' as original_job_id,
                               payload->>'job_id' as job_id_field
                        FROM {APP_SCHEMA}.history_items
                        WHERE id::text = %s
                        LIMIT 1
                    """, (job_id,))
                    row = cur.fetchone()
                conn.close()

                if row:
                    original_id = row.get("original_job_id") or row.get("job_id_field")
                    if original_id:
                        print(f"[Resolve] {job_id} -> original Meshy ID: {original_id}")
                        return original_id
            except Exception as e:
                print(f"[Resolve] Error looking up original job ID: {e}")
                try:
                    conn.close()
                except Exception:
                    pass

    # Return as-is if no mapping found
    print(f"[Resolve] {job_id} - no mapping found, using as-is")
    return job_id

def get_job_metadata(job_id: str, local_store: dict | None = None) -> dict:
    """
    Look up job metadata from local store first, then fall back to database.
    Returns dict with prompt, title, art_style, root_prompt, etc.
    """
    print(f"[Metadata] Looking up metadata for job_id={job_id}")
    if not job_id:
        print("[Metadata] No job_id provided, returning empty dict")
        return {}

    # First check local store
    if local_store is None:
        local_store = load_store()

    meta = local_store.get(job_id, {})
    if meta and (meta.get("prompt") or meta.get("title")):
        print(f"[Metadata] Found in local store: prompt={meta.get('prompt', '')[:40]}..., title={meta.get('title')}")
        return meta

    print(f"[Metadata] Not in local store, checking database...")

    # Fall back to database lookup
    if not USE_DB:
        print("[Metadata] USE_DB is False, returning empty meta")
        return meta

    conn = get_db_conn()
    if not conn:
        print("[Metadata] Could not get DB connection")
        return meta

    try:
        with conn.cursor(row_factory=dict_row) as cur:
            # First check active_jobs table for identity_id / related history
            cur.execute(f"""
                SELECT identity_id, related_history_id
                FROM {APP_SCHEMA}.active_jobs
                WHERE upstream_job_id = %s
                LIMIT 1
            """, (job_id,))
            active_job = cur.fetchone()
            active_user_id = None
            if active_job:
                active_user_id = str(active_job["identity_id"]) if active_job["identity_id"] else None

            row = None
            if active_job and active_job.get("related_history_id"):
                cur.execute(f"""
                    SELECT id, title, prompt, stage, payload, identity_id
                    FROM {APP_SCHEMA}.history_items
                    WHERE id = %s
                    LIMIT 1
                """, (active_job["related_history_id"],))
                row = cur.fetchone()

            if not row:
                # Look up by original_job_id in payload, or by ID directly
                cur.execute(f"""
                    SELECT id, title, prompt, stage, payload, identity_id
                    FROM {APP_SCHEMA}.history_items
                    WHERE payload->>'original_job_id' = %s
                       OR payload->>'preview_task_id' = %s
                       OR id::text = %s
                    LIMIT 1
                """, (job_id, job_id, job_id))
                row = cur.fetchone()
        conn.close()

        if row:
            payload = row["payload"] if row["payload"] else {}
            result = {
                "prompt": row["prompt"] or payload.get("prompt"),
                "title": row["title"] or payload.get("title"),
                "root_prompt": payload.get("root_prompt") or row["prompt"] or payload.get("prompt"),
                "art_style": payload.get("art_style"),
                "stage": row["stage"] or payload.get("stage"),
                "user_id": str(row["identity_id"]) if row["identity_id"] else active_user_id,
            }
            print(f"[Metadata] Found in DB: prompt={result.get('prompt', '')[:40] if result.get('prompt') else 'None'}..., title={result.get('title')}, user_id={result.get('user_id')}")
            return result
        elif active_job:
            print(f"[Metadata] Found in active_jobs: user_id={active_user_id}")
            return {"user_id": active_user_id}
        else:
            print(f"[Metadata] Not found in database for job_id={job_id}")
    except Exception as e:
        print(f"[Metadata] ERROR: Failed to get job metadata for {job_id}: {e}")
        try:
            conn.close()
        except Exception:
            pass

    return meta

# ─────────────────────────────────────────────────────────────
# Meshy helpers
# ─────────────────────────────────────────────────────────────
def _auth_headers():
    if not MESHY_API_KEY:
        raise RuntimeError("MESHY_API_KEY not set")
    return {"Authorization": f"Bearer {MESHY_API_KEY}", "Content-Type": "application/json"}

def mesh_post(path: str, payload: dict) -> dict:
    url = f"{MESHY_API_BASE}{path}"
    r = requests.post(url, headers=_auth_headers(), json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"POST {path} -> {r.status_code}: {r.text[:500]}")
    return r.json()

def mesh_get(path: str) -> dict:
    url = f"{MESHY_API_BASE}{path}"
    r = requests.get(url, headers=_auth_headers(), timeout=60)
    if not r.ok:
        raise RuntimeError(f"GET {path} -> {r.status_code}: {r.text[:500]}")
    return r.json()

# OpenAI image generation (DALL·E / GPT-Image)
def openai_image_generate(prompt: str, size: str = "1024x1024", model: str = "gpt-image-1", n: int = 1, response_format: str = "url") -> dict:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    url = "https://api.openai.com/v1/images/generations"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "prompt": prompt,
        "size": size,
        "n": max(1, min(4, int(n or 1))),
    }
    # gpt-image-1 doesn't support response_format parameter
    if model != "gpt-image-1":
        payload["response_format"] = response_format
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"OpenAI image -> {r.status_code}: {r.text[:500]}")
    try:
        return r.json()
    except Exception:
        raise RuntimeError(f"OpenAI image returned non-JSON: {r.text[:200]}")

# ─────────────────────────────────────────────────────────────
# Utils
# ─────────────────────────────────────────────────────────────
def now_s() -> int:
    return int(time.time())

def clamp_int(value, minimum: int, maximum: int, default: int) -> int:
    try:
        return max(minimum, min(maximum, int(value)))
    except (TypeError, ValueError):
        return default

def normalize_epoch_ms(value: Any) -> int:
    """
    Accepts seconds, ms, ISO strings, or numeric-like strings and
    returns an epoch value in milliseconds.
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
    raw = (str(value or "").strip().lower())
    return "cc-by-4" if raw.startswith("cc") else "private"

def _mask_value(val: Any, max_len: int = 400):
    try:
        s = json.dumps(val, ensure_ascii=False)
    except Exception:
        s = str(val)
    if len(s) > max_len:
        return s[:max_len] + "…"
    return s

def scrub_secrets(data: Any):
    """
    Recursively mask any fields that look like keys/tokens/secrets to avoid leaking.
    """
    if isinstance(data, dict):
        cleaned = {}
        for k, v in data.items():
            if any(t in k.lower() for t in ["key", "token", "secret", "auth"]):
                cleaned[k] = "***"
            else:
                cleaned[k] = scrub_secrets(v)
        return cleaned
    if isinstance(data, list):
        return [scrub_secrets(x) for x in data]
    return data

def log_event(label: str, payload: Any):
    try:
        safe_payload = scrub_secrets(payload)
        app.logger.info("[debug] %s :: %s", label, _mask_value(safe_payload))
    except Exception as e:
        app.logger.warning("[debug] %s :: failed to log (%s)", label, e)

def _task_containers(ms: dict) -> list[dict]:
    """
    Meshy responses may wrap the actual payload in `data`, `result`, or `task_result`.
    Return a list of dicts in priority order so lookups can scan through them.
    """
    containers = []
    if isinstance(ms, dict):
        containers.append(ms)
        for key in ("data", "result", "task_result"):
            val = ms.get(key)
            if isinstance(val, dict):
                containers.append(val)
            # Sometimes outputs are wrapped inside a list/dict under the same key
            if isinstance(val, list):
                containers.extend([x for x in val if isinstance(x, dict)])
        # Common extra nesting keys from Meshy payloads
        for key in ("output", "outputs"):
            val = ms.get(key)
            if isinstance(val, dict):
                containers.append(val)
            if isinstance(val, list):
                containers.extend([x for x in val if isinstance(x, dict)])
    return containers or [{}]

def _pick_first(containers: list[dict], keys: list[str], default=None):
    for c in containers:
        if not isinstance(c, dict):
            continue
        for k in keys:
            val = c.get(k)
            if val not in (None, "", []):
                return val
    return default

def extract_model_urls(ms: dict):
    """
    Meshy responses sometimes return URLs in slightly different buckets or nesting.
    Surface the ones the frontend expects to see.
    """
    containers = _task_containers(ms)
    model_urls: dict = {}
    textured_model_urls: dict = {}
    textured_glb_url = None
    rigged_glb = None
    rigged_fbx = None
    glb_candidates: list[str] = []

    def pick_url(container: dict) -> str | None:
        if not isinstance(container, dict):
            return None
        return container.get("glb") or container.get("obj")

    for c in containers:
        if not isinstance(c, dict):
            continue
        if not model_urls and isinstance(c.get("model_urls"), dict):
            model_urls = _filter_model_urls(c.get("model_urls") or {})
        if not textured_model_urls and isinstance(c.get("textured_model_urls"), dict):
            textured_model_urls = _filter_model_urls(c.get("textured_model_urls") or {})
        # Some responses put outputs in a nested "output" dict
        if not model_urls and isinstance(c.get("output_model_urls"), dict):
            model_urls = _filter_model_urls(c.get("output_model_urls") or {})
        if not textured_model_urls and isinstance(c.get("output_textured_model_urls"), dict):
            textured_model_urls = _filter_model_urls(c.get("output_textured_model_urls") or {})
        if not textured_glb_url and c.get("textured_glb_url"):
            textured_glb_url = c.get("textured_glb_url")
        if not rigged_glb and c.get("rigged_character_glb_url"):
            rigged_glb = c.get("rigged_character_glb_url")
        if not rigged_fbx and c.get("rigged_character_fbx_url"):
            rigged_fbx = c.get("rigged_character_fbx_url")

        glb_candidates.extend([
            url for url in [
                c.get("textured_glb_url"),
                c.get("textured_model_url"),
                pick_url(c.get("textured_model_urls") or {}),
                c.get("glb_url"),
                c.get("model_url"),
                c.get("output_model_url"),
                c.get("mesh_url"),
                c.get("mesh_download_url"),
                c.get("gltf_url"),
                c.get("gltf_download_url"),
                pick_url(c.get("model_urls") or {}),
                pick_url(c.get("output_model_urls") or {}),
                c.get("rigged_character_glb_url"),
            ] if url
        ])

    # Sometimes Meshy returns a bare URL as `result`; catch that.
    if not glb_candidates and isinstance(ms, dict) and isinstance(ms.get("result"), str) and ms["result"].startswith("http"):
        glb_candidates.append(ms["result"])

    # Prioritize textured models, then regular models
    glb_url = (
        textured_glb_url
        or textured_model_urls.get("glb")
        or model_urls.get("glb")
        or next((u for u in glb_candidates if u), None)
        or textured_model_urls.get("obj")
        or model_urls.get("obj")
        or rigged_glb
    )

    return glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, rigged_fbx

def log_status_summary(route: str, job_id: str, payload: dict):
    """
    Lightweight status logging for debugging stuck jobs without being spammy.
    """
    try:
        glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, _ = extract_model_urls(payload or {})
        has_model = bool(glb_url or textured_glb_url or rigged_glb)
        # Also consider populated dicts as "has something"
        has_model = has_model or bool(
            (model_urls and isinstance(model_urls, dict) and any(model_urls.values())) or
            (textured_model_urls and isinstance(textured_model_urls, dict) and any(textured_model_urls.values()))
        )
        app.logger.info(
            "[status] %s job=%s status=%s pct=%s has_model=%s glb=%s",
            route,
            job_id,
            payload.get("status") or payload.get("task_status"),
            payload.get("pct") or payload.get("progress") or payload.get("progress_percentage"),
            has_model,
            (glb_url or textured_glb_url or rigged_glb or "")[:128],
        )
    except Exception as e:
        app.logger.warning("[status] %s job=%s log-failed: %s", route, job_id, e)

def normalize_status(ms: dict) -> dict:
    """
    Map Meshy task to the shape your frontend expects.
    """
    containers = _task_containers(ms)
    st_raw = (_pick_first(containers, ["status", "task_status"]) or "").upper()
    status = MESHY_STATUS_MAP.get(st_raw, st_raw.lower() or "pending")
    try:
        pct = int(
            _pick_first(containers, ["progress", "progress_percentage", "progress_percent", "percent"]) or 0
        )
    except Exception:
        pct = 0
    mode = (_pick_first(containers, ["mode", "stage"]) or "").strip().lower()
    stage = "refine" if mode == "refine" else (mode or "preview")

    glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, rigged_fbx = extract_model_urls(ms)

    return {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": stage,
        "thumbnail_url": _pick_first(containers, ["thumbnail_url", "cover_image_url", "image"]),
        "glb_url": glb_url,
        "model_urls": model_urls,
        "textured_model_urls": textured_model_urls,
        "textured_glb_url": textured_glb_url,
        "rigged_character_glb_url": rigged_glb,
        "rigged_character_fbx_url": rigged_fbx,
        "created_at": normalize_epoch_ms(_pick_first(containers, ["created_at", "created_at_ts", "created_time"])),
        "preview_task_id": _pick_first(containers, ["preview_task_id", "preview_task"]),
    }

def normalize_meshy_task(ms: dict, *, stage: str) -> dict:
    containers = _task_containers(ms)
    st_raw = (_pick_first(containers, ["status", "task_status"]) or "").upper()
    status = MESHY_STATUS_MAP.get(st_raw, st_raw.lower() or "pending")
    try:
        pct = int(
            _pick_first(containers, ["progress", "progress_percentage", "progress_percent", "percent"]) or 0
        )
    except Exception:
        pct = 0

    glb_url, model_urls, textured_model_urls, textured_glb_url, rigged_glb, rigged_fbx = extract_model_urls(ms)

    return {
        "id": _pick_first(containers, ["id", "task_id"]),
        "status": status,
        "pct": pct,
        "stage": (_pick_first(containers, ["stage"]) or "").strip().lower() or stage,
        "thumbnail_url": _pick_first(containers, ["thumbnail_url", "cover_image_url", "image"]),
        "glb_url": glb_url,
        "model_urls": model_urls,
        "textured_model_urls": textured_model_urls,
        "textured_glb_url": textured_glb_url,
        "texture_urls": _pick_first(containers, ["texture_urls", "textures"]),
        "basic_animations": _pick_first(containers, ["basic_animations", "animations"]),
        "rigged_character_glb_url": rigged_glb,
        "rigged_character_fbx_url": rigged_fbx,
        "created_at": normalize_epoch_ms(_pick_first(containers, ["created_at", "created_at_ts", "created_time"])),
    }

def build_source_payload(body: dict):
    input_task_id = (body.get("input_task_id") or "").strip()
    model_url = (body.get("model_url") or "").strip()
    if input_task_id and model_url:
        return None, "Provide only one of input_task_id or model_url"
    if not input_task_id and not model_url:
        return None, "input_task_id or model_url required"

    if input_task_id:
        # Resolve to original Meshy job ID if this is our database UUID
        resolved_id = resolve_meshy_job_id(input_task_id)
        print(f"[build_source_payload] Resolved input_task_id: {input_task_id} -> {resolved_id}")
        return {"input_task_id": resolved_id}, None
    else:
        return {"model_url": model_url}, None

# ─────────────────────────────────────────────────────────────
# Credit Enforcement Helpers
# ─────────────────────────────────────────────────────────────
def check_and_reserve_credits(identity_id: str, action_key: str, job_id: str):
    """
    Check if user has enough credits and reserve them for the job.

    Args:
        identity_id: User's identity ID (from g.identity_id or g.user_id)
        action_key: Action key for pricing (e.g., 'text_to_3d_generate')
        job_id: Job ID to associate with the reservation

    Returns:
        (reservation_id, None) on success
        (None, error_response) on failure (can return directly from endpoint)
    """
    if not CREDITS_AVAILABLE:
        # Credits system not loaded - allow request (graceful degradation)
        print(f"[CREDITS] System not available, allowing {action_key} without credit check")
        return None, None

    if not identity_id:
        # No identity - allow anonymous (legacy behavior)
        print(f"[CREDITS] No identity for {action_key}, allowing without credit check")
        return None, None

    try:
        # Get cost for this action
        cost = PricingService.get_action_cost(action_key)
        if cost == 0:
            print(f"[CREDITS] No cost defined for {action_key}, allowing")
            return None, None

        # Check available balance
        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)
        available = max(0, balance - reserved)

        print(f"[CREDITS] {action_key}: cost={cost}, balance={balance}, reserved={reserved}, available={available}")

        if available < cost:
            # Insufficient credits
            return None, jsonify({
                "ok": False,
                "error": {
                    "code": "INSUFFICIENT_CREDITS",
                    "message": f"Insufficient credits. Need {cost}, have {available}.",
                    "required": cost,
                    "available": available,
                    "balance": balance,
                    "reserved": reserved,
                }
            }), 402

        # Reserve credits
        result = ReservationService.reserve_credits(
            identity_id=identity_id,
            action_key=action_key,
            job_id=job_id,
            meta={"action_key": action_key, "source": "legacy_endpoint"},
        )

        reservation_id = result["reservation"]["id"]
        print(f"[CREDITS] Reserved {cost} credits for {action_key}, reservation_id={reservation_id}")
        return reservation_id, None

    except ValueError as e:
        # Insufficient credits error from ReservationService
        error_msg = str(e)
        print(f"[CREDITS] Reservation failed: {error_msg}")

        if "INSUFFICIENT_CREDITS" in error_msg:
            # Parse the error to get details
            parts = error_msg.split(":")
            required = available = 0
            for part in parts:
                if "required=" in part:
                    try:
                        required = int(part.split("=")[1].strip())
                    except:
                        pass
                elif "available=" in part:
                    try:
                        available = int(part.split("=")[1].strip())
                    except:
                        pass

            return None, jsonify({
                "ok": False,
                "error": {
                    "code": "INSUFFICIENT_CREDITS",
                    "message": error_msg,
                    "required": required,
                    "available": available,
                }
            }), 402

        return None, jsonify({
            "ok": False,
            "error": {
                "code": "CREDIT_ERROR",
                "message": str(e),
            }
        }), 400

    except Exception as e:
        print(f"[CREDITS] Unexpected error: {e}")
        # Don't block on credit system errors - allow request
        return None, None


def finalize_reservation(reservation_id: str):
    """Finalize (capture) a credit reservation after successful job start."""
    if not CREDITS_AVAILABLE or not reservation_id:
        return

    try:
        ReservationService.finalize_reservation(reservation_id)
        print(f"[CREDITS] Finalized reservation {reservation_id}")
    except Exception as e:
        print(f"[CREDITS] Failed to finalize reservation {reservation_id}: {e}")


def release_reservation(reservation_id: str, reason: str = "job_failed"):
    """Release a credit reservation after job failure."""
    if not CREDITS_AVAILABLE or not reservation_id:
        return

    try:
        ReservationService.release_reservation(reservation_id, reason)
        print(f"[CREDITS] Released reservation {reservation_id} ({reason})")
    except Exception as e:
        print(f"[CREDITS] Failed to release reservation {reservation_id}: {e}")


# Action key mapping for legacy endpoints
LEGACY_ACTION_KEYS = {
    "text-to-3d-preview": "text_to_3d_generate",
    "text-to-3d-refine": "refine",
    "image-to-3d": "image_to_3d_generate",
    "remesh": "remesh",
    "texture": "texture",
    "rig": "rig",
    "image-studio": "image_studio_generate",
}


def check_credits_available(identity_id: str, action_key: str):
    """
    Check if user has enough credits for an action BEFORE calling upstream API.
    Does NOT reserve or charge - just a balance check.

    Args:
        identity_id: User's identity ID
        action_key: Action key from LEGACY_ACTION_KEYS (e.g., 'text_to_3d_generate')

    Returns:
        (True, cost, None) if credits available
        (False, cost, error_response) if insufficient - return error_response from route
    """
    if not CREDITS_AVAILABLE:
        print(f"[CREDITS] System not available, allowing {action_key} without credit check")
        return True, 0, None

    if not identity_id:
        print(f"[CREDITS] No identity for {action_key}, allowing without credit check")
        return True, 0, None

    try:
        cost = PricingService.get_action_cost(action_key)
        if cost == 0:
            print(f"[CREDITS] No cost defined for {action_key}, allowing")
            return True, 0, None

        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)
        available = max(0, balance - reserved)

        print(f"[CREDITS] Check {action_key}: cost={cost}, balance={balance}, reserved={reserved}, available={available}")

        if available < cost:
            return False, cost, (jsonify({
                "ok": False,
                "error": "insufficient_credits",
                "code": "INSUFFICIENT_CREDITS",
                "message": f"Insufficient credits. Need {cost}, have {available}.",
                "required": cost,
                "available": available,
                "balance": balance,
            }), 402)

        return True, cost, None

    except Exception as e:
        print(f"[CREDITS] Check error for {action_key}: {e}")
        # Don't block on credit system errors
        return True, 0, None


def charge_credits_for_action(identity_id: str, action_key: str, upstream_job_id: str, metadata: dict = None):
    """
    Charge credits AFTER successful upstream API call.
    Uses upstream_job_id as idempotency key to prevent double-charging.

    Args:
        identity_id: User's identity ID
        action_key: Action key from LEGACY_ACTION_KEYS
        upstream_job_id: The job ID returned by Meshy/OpenAI (idempotency key)
        metadata: Optional metadata to store with the charge

    Returns:
        (True, new_balance, ledger_entry_id) on success
        (False, 0, error_message) on failure (should not happen if check passed first)
    """
    if not CREDITS_AVAILABLE:
        return True, 0, None

    if not identity_id:
        return True, 0, None

    try:
        cost = PricingService.get_action_cost(action_key)
        if cost == 0:
            return True, WalletService.get_balance(identity_id), None

        # Import the charge function from credits module
        from credits import _charge_credits_idempotent

        result = _charge_credits_idempotent(
            identity_id=identity_id,
            action=action_key,
            ref_type=action_key,
            ref_id=upstream_job_id,
            cost_credits=cost,
            meta={
                "upstream_job_id": upstream_job_id,
                "source": "legacy_endpoint",
                **(metadata or {}),
            },
        )

        if result["idempotent"]:
            print(f"[CREDITS] Idempotent charge for {action_key}, job={upstream_job_id}")
        else:
            print(f"[CREDITS] Charged {cost} credits for {action_key}, job={upstream_job_id}, new_balance={result['new_balance']}")

        return True, result["new_balance"], None

    except ValueError as e:
        error_msg = str(e)
        print(f"[CREDITS] Charge failed for {action_key}: {error_msg}")
        # This shouldn't happen if check_credits_available passed, but handle gracefully
        return False, 0, error_msg

    except Exception as e:
        print(f"[CREDITS] Unexpected charge error for {action_key}: {e}")
        # Don't fail the request on credit errors - job already started
        return True, 0, None


def get_current_balance(identity_id: str):
    """
    Get current available balance for an identity.
    Returns dict with balance/reserved/available, or None if credits not available.
    """
    if not CREDITS_AVAILABLE or not identity_id:
        return None
    try:
        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)
        available = max(0, balance - reserved)
        return {"balance": balance, "reserved": reserved, "available": available}
    except Exception as e:
        print(f"[CREDITS] Failed to get balance: {e}")
        return None


# ─────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"ok": True})

@app.route("/api/db-check", methods=["GET"])
def db_check():
    if not USE_DB:
        return jsonify({"ok": False, "error": "db_disabled"}), 503
    conn = get_db_conn()
    if not conn:
        return jsonify({"ok": False, "error": "db_unavailable"}), 503
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            _ = cur.fetchone()
        conn.close()
        return jsonify({"ok": True, "db": "connected"})
    except Exception as e:
        print(f"[DB] db_check failed: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return jsonify({"ok": False, "error": "db_query_failed"}), 503

# ---- Start preview (Text → 3D) ----
@app.route("/api/text-to-3d/start", methods=["POST", "OPTIONS"])
def api_text_to_3d_start():
    if request.method == "OPTIONS":
        return ("", 204)

    user_id = g.user_id  # May be None for anonymous users
    identity_id = getattr(g, 'identity_id', None) or user_id  # Use billing identity if available

    body = request.get_json(silent=True) or {}
    log_event("text-to-3d/start:incoming", body)
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "prompt required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Generate a temporary job ID for credit reservation (before Meshy call)
    temp_job_id = f"text3d-{uuid.uuid4().hex[:12]}"

    # Check and reserve credits BEFORE calling Meshy
    action_key = LEGACY_ACTION_KEYS["text-to-3d-preview"]
    reservation_id, credit_error = check_and_reserve_credits(identity_id, action_key, temp_job_id)
    if credit_error:
        return credit_error

    payload = {
        "mode": "preview",
        "prompt": prompt,
        # Meshy-6 default
        "ai_model": body.get("model") or "latest",
    }
    # pass-through options your UI may send
    art_style = body.get("art_style")
    if art_style:
        payload["art_style"] = art_style

    symmetry_mode = (body.get("symmetry_mode") or "").strip().lower()
    if symmetry_mode in {"off", "auto", "on"}:
        payload["symmetry_mode"] = symmetry_mode

    if "is_a_t_pose" in body:
        payload["is_a_t_pose"] = bool(body.get("is_a_t_pose"))

    license_choice = normalize_license(body.get("license"))
    batch_count = clamp_int(body.get("batch_count"), 1, 8, 1)
    batch_slot = clamp_int(body.get("batch_slot"), 1, batch_count, 1)
    batch_group_id = (body.get("batch_group_id") or "").strip() or None

    try:
        resp = mesh_post("/openapi/v2/text-to-3d", payload)
        log_event("text-to-3d/start:meshy-resp", resp)
        job_id = resp.get("result")
        if not job_id:
            # Release reservation on failure
            release_reservation(reservation_id, "meshy_no_job_id")
            return jsonify({"error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        # Release reservation on failure
        release_reservation(reservation_id, "meshy_api_error")
        return jsonify({"error": str(e)}), 502

    # Finalize reservation - job started successfully
    finalize_reservation(reservation_id)

    store = load_store()
    store[job_id] = {
        "stage": "preview",
        "prompt": prompt,
        "root_prompt": prompt,  # This is the start of a chain
        "art_style": art_style or "realistic",
        "model": payload["ai_model"],
        "created_at": now_s() * 1000,
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "is_a_t_pose": bool(body.get("is_a_t_pose")),
        "batch_count": batch_count,
        "batch_slot": batch_slot,
        "batch_group_id": batch_group_id,
        "user_id": user_id,  # Track user who started the job
        "reservation_id": reservation_id,  # Track credit reservation
    }
    save_store(store)

    # Save to DB for recovery
    save_active_job_to_db(job_id, "text-to-3d", "preview", store[job_id], user_id)

    # Get updated balance after credit deduction
    balance_info = get_current_balance(identity_id)
    new_balance = balance_info["available"] if balance_info else None

    return jsonify({"job_id": job_id, "new_balance": new_balance})

# ---- Refine from preview ----
@app.route("/api/text-to-3d/refine", methods=["POST", "OPTIONS"])
def api_text_to_3d_refine():
    if request.method == "OPTIONS":
        return ("", 204)

    user_id = g.user_id  # May be None for anonymous users
    identity_id = getattr(g, 'identity_id', None) or user_id

    body = request.get_json(silent=True) or {}
    log_event("text-to-3d/refine:incoming", body)
    preview_task_id_input = (body.get("preview_task_id") or "").strip()
    if not preview_task_id_input:
        return jsonify({"error": "preview_task_id required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Generate a temporary job ID for credit reservation
    temp_job_id = f"refine-{uuid.uuid4().hex[:12]}"

    # Check and reserve credits BEFORE calling Meshy
    action_key = LEGACY_ACTION_KEYS["text-to-3d-refine"]
    reservation_id, credit_error = check_and_reserve_credits(identity_id, action_key, temp_job_id)
    if credit_error:
        return credit_error

    # Resolve to original Meshy job ID if this is our database UUID
    preview_task_id = resolve_meshy_job_id(preview_task_id_input)
    print(f"[Refine] Resolved preview_task_id: {preview_task_id_input} -> {preview_task_id}")

    payload = {
        "mode": "refine",
        "preview_task_id": preview_task_id,
        "enable_pbr": bool(body.get("enable_pbr", True)),
    }
    texture_prompt = body.get("texture_prompt")
    if texture_prompt:
        payload["texture_prompt"] = texture_prompt

    try:
        resp = mesh_post("/openapi/v2/text-to-3d", payload)
        log_event("text-to-3d/refine:meshy-resp", resp)
        job_id = resp.get("result")
        if not job_id:
            release_reservation(reservation_id, "meshy_no_job_id")
            return jsonify({"error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        release_reservation(reservation_id, "meshy_api_error")
        return jsonify({"error": str(e)}), 502

    # Finalize reservation - job started successfully
    finalize_reservation(reservation_id)

    store = load_store()
    # Copy metadata from preview job (try both input ID and resolved ID)
    preview_meta = get_job_metadata(preview_task_id_input, store)
    if not preview_meta.get("prompt"):
        preview_meta = get_job_metadata(preview_task_id, store)
    original_prompt = preview_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = preview_meta.get("root_prompt") or original_prompt
    store[job_id] = {
        "stage": "refine",
        "preview_task_id": preview_task_id,  # Store the resolved Meshy ID
        "created_at": now_s() * 1000,
        # Copy from preview job
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": f"(refine) {original_prompt[:40]}" if original_prompt else body.get("title", DEFAULT_MODEL_TITLE),
        "art_style": preview_meta.get("art_style"),
        "texture_prompt": texture_prompt,
        "user_id": user_id,
        "reservation_id": reservation_id,
    }
    save_store(store)

    # Save to DB for recovery
    save_active_job_to_db(job_id, "text-to-3d", "refine", store[job_id], user_id)

    # Get updated balance after credit deduction
    balance_info = get_current_balance(identity_id)
    new_balance = balance_info["available"] if balance_info else None

    return jsonify({"job_id": job_id, "new_balance": new_balance})

# ---- (Soft) Remesh start (re-run preview with flags) ----
@app.route("/api/text-to-3d/remesh-start", methods=["POST", "OPTIONS"])
def api_text_to_3d_remesh_start():
    if request.method == "OPTIONS":
        return ("", 204)

    user_id = g.user_id
    identity_id = getattr(g, 'identity_id', None) or user_id

    body = request.get_json(silent=True) or {}
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "prompt required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Check credits BEFORE calling Meshy
    action_key = LEGACY_ACTION_KEYS["remesh"]
    has_credits, cost, credit_error = check_credits_available(identity_id, action_key)
    if not has_credits:
        return credit_error

    payload = {
        "mode": "preview",
        "prompt": prompt,
        "ai_model": body.get("model") or "latest",
        # mesh-friendly defaults for a cleaner topology
        "topology": "triangle",
        "should_remesh": True,
        "target_polycount": body.get("target_polycount", 45000),
        "art_style": body.get("art_style", "realistic"),
    }

    symmetry_mode = (body.get("symmetry_mode") or "").strip().lower()
    if symmetry_mode in {"off", "auto", "on"}:
        payload["symmetry_mode"] = symmetry_mode

    if "is_a_t_pose" in body:
        payload["is_a_t_pose"] = bool(body.get("is_a_t_pose"))

    license_choice = normalize_license(body.get("license"))
    batch_count = clamp_int(body.get("batch_count"), 1, 8, 1)
    batch_slot = clamp_int(body.get("batch_slot"), 1, batch_count, 1)

    try:
        resp = mesh_post("/openapi/v2/text-to-3d", payload)
        job_id = resp.get("result")
        if not job_id:
            return jsonify({"error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    # Charge credits AFTER successful Meshy call (idempotent via job_id)
    _, new_balance, _ = charge_credits_for_action(identity_id, action_key, job_id, {"prompt": prompt[:100]})

    store = load_store()
    store[job_id] = {
        "stage": "preview",
        "prompt": prompt,
        "art_style": payload["art_style"],
        "model": payload["ai_model"],
        "created_at": now_s() * 1000,
        "remesh_like": True,
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "is_a_t_pose": bool(body.get("is_a_t_pose")),
        "batch_count": batch_count,
        "batch_slot": batch_slot,
        "user_id": user_id,
    }
    save_store(store)
    return jsonify({"job_id": job_id, "new_balance": new_balance or None})

# ---- Status ----
@app.route("/api/text-to-3d/status/<job_id>", methods=["GET", "OPTIONS"])
def api_text_to_3d_status(job_id):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("text-to-3d/status:incoming", {"job_id": job_id})
    if not job_id:
        return jsonify({"error": "job_id required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Verify job ownership
    user_id = g.user_id
    if not verify_job_ownership(job_id, user_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v2/text-to-3d/{job_id}")
        log_event("text-to-3d/status:meshy-resp", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404

    out = normalize_status(ms)
    log_status_summary("text-to-3d", job_id, out)

    # persist last-known bits
    store = load_store()
    # Get metadata from local store OR database (in case server restarted)
    meta = store.get(job_id) or get_job_metadata(job_id, store) or {}
    # surface stored batch metadata back to caller if Meshy doesn't return it
    for key in ("batch_count", "batch_slot", "batch_group_id", "license", "symmetry_mode", "is_a_t_pose"):
        if key in meta and key not in out:
            out[key] = meta.get(key)
    meta.update({
        "last_status": out["status"],
        "last_pct": out["pct"],
        "stage": out["stage"],
    })
    if out.get("glb_url"):
        meta["glb_url"] = out["glb_url"]
    if out.get("thumbnail_url"):
        meta["thumbnail_url"] = out["thumbnail_url"]
    store[job_id] = meta
    save_store(store)

    # Save to normalized DB tables when job finishes
    if out["status"] == "done" and (out.get("glb_url") or out.get("thumbnail_url")):
        user_id = meta.get("user_id") or getattr(g, 'user_id', None)
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type='text-to-3d', user_id=user_id)

        # Update response with S3 URLs so frontend gets permanent URLs
        if s3_result and s3_result.get("success"):
            if s3_result.get("glb_url"):
                out["glb_url"] = s3_result["glb_url"]
            if s3_result.get("thumbnail_url"):
                out["thumbnail_url"] = s3_result["thumbnail_url"]
            if s3_result.get("textured_glb_url"):
                out["textured_glb_url"] = s3_result["textured_glb_url"]
            if s3_result.get("model_urls"):
                out["model_urls"] = s3_result["model_urls"]
            if s3_result.get("texture_urls"):
                out["texture_urls"] = s3_result["texture_urls"]
            if s3_result.get("db_ok") is False:
                out["db_ok"] = False
                out["db_errors"] = s3_result.get("db_errors")

    return jsonify(out)

# ---- List active/known jobs (for resume logic) ----
@app.route("/api/text-to-3d/list", methods=["GET", "OPTIONS"])
def api_text_to_3d_list():
    if request.method == "OPTIONS":
        return ("", 204)
    store = load_store()
    items = [{"job_id": jid, **meta} for jid, meta in store.items()]
    # Keep small: only return ids (your frontend only checks presence)
    return jsonify([x["job_id"] for x in items if "job_id" in x] or list(store.keys()))

# ---- Save active job to database ----
@app.route("/api/jobs/save", methods=["POST", "OPTIONS"])
def api_save_active_job():
    if request.method == "OPTIONS":
        return ("", 204)
    try:
        user_id = g.user_id
        data = request.get_json() or {}
        job_id = data.get("job_id")
        job_type = data.get("job_type", "unknown")
        stage = data.get("stage")
        metadata = data.get("metadata", {})

        if not job_id:
            return jsonify({"error": "job_id required"}), 400

        success = save_active_job_to_db(job_id, job_type, stage, metadata, user_id)
        return jsonify({"success": success, "job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---- Get all active jobs from database ----
@app.route("/api/jobs/active", methods=["GET", "OPTIONS"])
def api_get_active_jobs():
    if request.method == "OPTIONS":
        return ("", 204)
    try:
        user_id = g.user_id
        jobs = get_active_jobs_from_db(user_id)
        return jsonify(jobs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---- Mark job as completed ----
@app.route("/api/jobs/<job_id>/complete", methods=["POST", "OPTIONS"])
def api_complete_job(job_id):
    if request.method == "OPTIONS":
        return ("", 204)
    try:
        user_id = g.user_id
        mark_job_completed_in_db(job_id, user_id)
        return jsonify({"success": True, "job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---- Delete active job ----
@app.route("/api/jobs/<job_id>", methods=["DELETE", "OPTIONS"])
def api_delete_active_job(job_id):
    if request.method == "OPTIONS":
        return ("", 204)
    try:
        user_id = g.user_id
        delete_active_job_from_db(job_id, user_id)
        return jsonify({"success": True, "job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ---- Proxy GLB (to avoid CORS on raw Meshy URLs) ----
@app.route("/api/proxy-glb", methods=["GET", "OPTIONS"])
def api_proxy_glb():
    if request.method == "OPTIONS":
        return ("", 204)
    u = request.args.get("u", "").strip()
    if not u:
        return jsonify({"error": "u query param required"}), 400
    # Basic allowlist: only proxy http(s)
    p = urlparse(u)
    if p.scheme not in ("http", "https"):
        abort(400)
    host = (p.hostname or "").lower()
    if host not in PROXY_ALLOWED_HOSTS:
        return jsonify({"error": "Host not allowed"}), 400

    try:
        r = requests.get(u, stream=True, timeout=60)
    except Exception:
        abort(502)

    def gen():
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                yield chunk

    headers = {
        "Content-Type": r.headers.get("Content-Type", "application/octet-stream"),
        # Allow frontend to fetch without exposing server as an open proxy
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "public, max-age=3600",
    }
    return Response(gen(), status=r.status_code, headers=headers)

# ---- Meshy Remesh ----
@app.route("/api/mesh/remesh", methods=["POST", "OPTIONS"])
def api_mesh_remesh():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    user_id = g.user_id
    identity_id = getattr(g, 'identity_id', None) or user_id

    body = request.get_json(silent=True) or {}
    log_event("mesh/remesh:incoming", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"error": err}), 400

    # Check credits BEFORE calling Meshy
    action_key = LEGACY_ACTION_KEYS["remesh"]
    has_credits, cost, credit_error = check_credits_available(identity_id, action_key)
    if not has_credits:
        return credit_error

    payload = {
        **source,
        "target_formats": body.get("target_formats") or ["glb"],
    }
    topology = (body.get("topology") or "").strip().lower()
    if topology in {"triangle", "quad"}:
        payload["topology"] = topology
    try:
        tp = int(body.get("target_polycount"))
        if tp > 0:
            payload["target_polycount"] = tp
    except Exception:
        pass

    try:
        rh = float(body.get("resize_height"))
        if rh > 0:
            payload["resize_height"] = rh
    except Exception:
        pass

    origin_at = (body.get("origin_at") or "").strip().lower()
    if origin_at in {"bottom", "center"}:
        payload["origin_at"] = origin_at

    if body.get("convert_format_only") is not None:
        payload["convert_format_only"] = bool(body.get("convert_format_only"))

    try:
        resp = mesh_post("/openapi/v1/remesh", payload)
        log_event("mesh/remesh:meshy-resp", resp)
        job_id = resp.get("result") or resp.get("id")
        if not job_id:
            return jsonify({"error": "No job id in response", "raw": resp}), 502

        # Charge credits AFTER successful Meshy call (idempotent via job_id)
        _, new_balance, _ = charge_credits_for_action(identity_id, action_key, job_id)

        # Save metadata to store (checks local store AND database for source)
        store = load_store()
        source_task_id = body.get("source_task_id") or body.get("model_task_id")
        source_meta = get_job_metadata(source_task_id, store) if source_task_id else {}
        original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
        root_prompt = source_meta.get("root_prompt") or original_prompt
        store[job_id] = {
            "stage": "remesh",
            "source_task_id": source_task_id,
            "created_at": now_s() * 1000,
            "prompt": original_prompt,
            "root_prompt": root_prompt,
            "title": f"(remesh) {original_prompt[:40]}" if original_prompt else body.get("title", DEFAULT_MODEL_TITLE),
            "topology": topology,
            "target_polycount": payload.get("target_polycount"),
            "user_id": user_id,
        }
        save_store(store)

        # Save to DB for recovery
        save_active_job_to_db(job_id, "remesh", "remesh", store[job_id], user_id)

        return jsonify({"job_id": job_id, "new_balance": new_balance or None})
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/mesh/remesh/<job_id>", methods=["GET", "OPTIONS"])
def api_mesh_remesh_status(job_id):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("mesh/remesh/status:incoming", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Verify job ownership
    user_id = g.user_id
    if not verify_job_ownership(job_id, user_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v1/remesh/{job_id}")
        log_event("mesh/remesh/status:meshy-resp", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    out = normalize_meshy_task(ms, stage="remesh")
    log_status_summary("mesh/remesh", job_id, out)

    # Save to normalized DB tables when job finishes
    if out["status"] == "done" and (out.get("glb_url") or out.get("thumbnail_url")):
        store = load_store()
        # Get metadata from local store OR database (in case server restarted)
        meta = get_job_metadata(job_id, store)

        # Always try to get prompt/title from source task if not set
        source_id = meta.get("source_task_id") or out.get("source_task_id")
        if source_id and (not meta.get("prompt") or not meta.get("title")):
            source_meta = get_job_metadata(source_id, store)
            if not meta.get("prompt"):
                meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
            if not meta.get("root_prompt"):
                meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        # Generate title from prompt if missing
        if not meta.get("title"):
            prompt_for_title = meta.get("prompt") or meta.get("root_prompt") or ""
            meta["title"] = f"(remesh) {prompt_for_title[:40]}" if prompt_for_title else f"(remesh) {DEFAULT_MODEL_TITLE}"

        user_id = meta.get("user_id") or getattr(g, 'user_id', None)
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type='remesh', user_id=user_id)

        # Update response with S3 URLs so frontend gets permanent URLs
        if s3_result and s3_result.get("success"):
            if s3_result.get("glb_url"):
                out["glb_url"] = s3_result["glb_url"]
            if s3_result.get("thumbnail_url"):
                out["thumbnail_url"] = s3_result["thumbnail_url"]
            if s3_result.get("model_urls"):
                out["model_urls"] = s3_result["model_urls"]
            if s3_result.get("db_ok") is False:
                out["db_ok"] = False
                out["db_errors"] = s3_result.get("db_errors")

    return jsonify(out)

# ---- Meshy Retexture ----
@app.route("/api/mesh/retexture", methods=["POST", "OPTIONS"])
def api_mesh_retexture():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    user_id = g.user_id
    identity_id = getattr(g, 'identity_id', None) or user_id

    body = request.get_json(silent=True) or {}
    log_event("mesh/retexture:incoming", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"error": err}), 400

    prompt = (body.get("text_style_prompt") or "").strip()
    style_img = (body.get("image_style_url") or "").strip()
    if not prompt and not style_img:
        return jsonify({"error": "text_style_prompt or image_style_url required"}), 400

    # Check credits BEFORE calling Meshy
    action_key = LEGACY_ACTION_KEYS["texture"]
    has_credits, cost, credit_error = check_credits_available(identity_id, action_key)
    if not has_credits:
        return credit_error

    payload = {
        **source,
        "enable_original_uv": bool(body.get("enable_original_uv", True)),
        "enable_pbr": bool(body.get("enable_pbr", False)),
    }
    if prompt:
        payload["text_style_prompt"] = prompt
    if style_img:
        payload["image_style_url"] = style_img
    ai_model = (body.get("ai_model") or "").strip()
    if ai_model:
        payload["ai_model"] = ai_model

    try:
        resp = mesh_post("/openapi/v1/retexture", payload)
        log_event("mesh/retexture:meshy-resp", resp)
        job_id = resp.get("result") or resp.get("id")
        if not job_id:
            return jsonify({"error": "No job id in response", "raw": resp}), 502

        # Charge credits AFTER successful Meshy call (idempotent via job_id)
        _, new_balance, _ = charge_credits_for_action(identity_id, action_key, job_id, {"texture_prompt": prompt[:100] if prompt else None})

        # Save metadata to store (checks local store AND database for source)
        store = load_store()
        source_task_id = body.get("source_task_id") or body.get("model_task_id")
        source_meta = get_job_metadata(source_task_id, store) if source_task_id else {}
        original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
        root_prompt = source_meta.get("root_prompt") or original_prompt
        store[job_id] = {
            "stage": "texture",
            "source_task_id": source_task_id,
            "created_at": now_s() * 1000,
            "prompt": original_prompt,
            "root_prompt": root_prompt,
            "title": f"(texture) {original_prompt[:40]}" if original_prompt else body.get("title", DEFAULT_MODEL_TITLE),
            "texture_prompt": prompt,
            "enable_pbr": bool(body.get("enable_pbr", False)),
            "user_id": user_id,
        }
        save_store(store)

        # Save to DB for recovery
        save_active_job_to_db(job_id, "retexture", "texture", store[job_id], user_id)

        return jsonify({"job_id": job_id, "new_balance": new_balance or None})
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/mesh/retexture/<job_id>", methods=["GET", "OPTIONS"])
def api_mesh_retexture_status(job_id):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("mesh/retexture/status:incoming", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Verify job ownership
    user_id = g.user_id
    if not verify_job_ownership(job_id, user_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v1/retexture/{job_id}")
        log_event("mesh/retexture/status:meshy-resp", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    out = normalize_meshy_task(ms, stage="texture")
    log_status_summary("mesh/retexture", job_id, out)

    # Save to normalized DB tables when job finishes
    if out["status"] == "done" and (out.get("glb_url") or out.get("textured_glb_url") or out.get("thumbnail_url")):
        store = load_store()
        # Get metadata from local store OR database (in case server restarted)
        meta = get_job_metadata(job_id, store)
        print(f"[Texture] Job {job_id} done, meta={meta}")

        # Always try to get prompt/title from source task if not set
        source_id = meta.get("source_task_id") or out.get("source_task_id")
        if source_id and (not meta.get("prompt") or not meta.get("title")):
            source_meta = get_job_metadata(source_id, store)
            print(f"[Texture] Source {source_id} meta={source_meta}")
            if not meta.get("prompt"):
                meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
            if not meta.get("root_prompt"):
                meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        # Generate title from prompt if missing
        if not meta.get("title"):
            prompt_for_title = meta.get("prompt") or meta.get("root_prompt") or ""
            meta["title"] = f"(texture) {prompt_for_title[:40]}" if prompt_for_title else f"(texture) {DEFAULT_MODEL_TITLE}"

        user_id = meta.get("user_id") or getattr(g, 'user_id', None)
        print(f"[Texture] Saving to DB: title={meta.get('title')}, user_id={user_id}")
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type='texture', user_id=user_id)

        # Update response with S3 URLs so frontend gets permanent URLs
        if s3_result and s3_result.get("success"):
            if s3_result.get("glb_url"):
                out["glb_url"] = s3_result["glb_url"]
            if s3_result.get("thumbnail_url"):
                out["thumbnail_url"] = s3_result["thumbnail_url"]
            if s3_result.get("textured_glb_url"):
                out["textured_glb_url"] = s3_result["textured_glb_url"]
            if s3_result.get("texture_urls"):
                out["texture_urls"] = s3_result["texture_urls"]
            if s3_result.get("model_urls"):
                out["model_urls"] = s3_result["model_urls"]
            if s3_result.get("db_ok") is False:
                out["db_ok"] = False
                out["db_errors"] = s3_result.get("db_errors")

    return jsonify(out)

# ---- Meshy Rigging ----
@app.route("/api/mesh/rigging", methods=["POST", "OPTIONS"])
def api_mesh_rigging():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    user_id = g.user_id
    identity_id = getattr(g, 'identity_id', None) or user_id

    body = request.get_json(silent=True) or {}
    log_event("mesh/rigging:incoming", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"error": err}), 400

    # Check credits BEFORE calling Meshy
    action_key = LEGACY_ACTION_KEYS["rig"]
    has_credits, cost, credit_error = check_credits_available(identity_id, action_key)
    if not has_credits:
        return credit_error

    payload = {**source}
    try:
        h = float(body.get("height_meters"))
        if h > 0:
            payload["height_meters"] = h
    except Exception:
        pass
    tex_img = (body.get("texture_image_url") or "").strip()
    if tex_img:
        payload["texture_image_url"] = tex_img

    try:
        resp = mesh_post("/openapi/v1/rigging", payload)
        log_event("mesh/rigging:meshy-resp", resp)
        job_id = resp.get("result") or resp.get("id")
        if not job_id:
            return jsonify({"error": "No job id in response", "raw": resp}), 502

        # Charge credits AFTER successful Meshy call (idempotent via job_id)
        _, new_balance, _ = charge_credits_for_action(identity_id, action_key, job_id)

        # Save metadata to store (checks local store AND database for source)
        store = load_store()
        source_task_id = body.get("source_task_id") or body.get("model_task_id")
        source_meta = get_job_metadata(source_task_id, store) if source_task_id else {}
        original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
        root_prompt = source_meta.get("root_prompt") or original_prompt
        store[job_id] = {
            "stage": "rigging",
            "source_task_id": source_task_id,
            "created_at": now_s() * 1000,
            "prompt": original_prompt,
            "root_prompt": root_prompt,
            "title": f"(rigged) {original_prompt[:40]}" if original_prompt else body.get("title", DEFAULT_MODEL_TITLE),
            "user_id": user_id,
        }
        save_store(store)

        # Save to DB for recovery
        save_active_job_to_db(job_id, "rigging", "rigging", store[job_id], user_id)

        return jsonify({"job_id": job_id, "new_balance": new_balance or None})
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/mesh/rigging/<job_id>", methods=["GET", "OPTIONS"])
def api_mesh_rigging_status(job_id):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("mesh/rigging/status:incoming", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Verify job ownership
    user_id = g.user_id
    if not verify_job_ownership(job_id, user_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v1/rigging/{job_id}")
        log_event("mesh/rigging/status:meshy-resp", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    out = normalize_meshy_task(ms, stage="rig")
    log_status_summary("mesh/rigging", job_id, out)

    # Save to normalized DB tables when job finishes
    if out["status"] == "done" and (out.get("rigged_character_glb_url") or out.get("thumbnail_url")):
        store = load_store()
        # Get metadata from local store OR database (in case server restarted)
        meta = get_job_metadata(job_id, store)

        # Always try to get prompt/title from source task if not set
        source_id = meta.get("source_task_id") or out.get("source_task_id")
        if source_id and (not meta.get("prompt") or not meta.get("title")):
            source_meta = get_job_metadata(source_id, store)
            if not meta.get("prompt"):
                meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
            if not meta.get("root_prompt"):
                meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        # Generate title from prompt if missing
        if not meta.get("title"):
            prompt_for_title = meta.get("prompt") or meta.get("root_prompt") or ""
            meta["title"] = f"(rigged) {prompt_for_title[:40]}" if prompt_for_title else f"(rigged) {DEFAULT_MODEL_TITLE}"

        user_id = meta.get("user_id") or getattr(g, 'user_id', None)
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type='rig', user_id=user_id)

        # Update response with S3 URLs so frontend gets permanent URLs
        if s3_result and s3_result.get("success"):
            if s3_result.get("glb_url"):
                out["glb_url"] = s3_result["glb_url"]
            if s3_result.get("thumbnail_url"):
                out["thumbnail_url"] = s3_result["thumbnail_url"]
            if s3_result.get("rigged_character_glb_url"):
                out["rigged_character_glb_url"] = s3_result["rigged_character_glb_url"]
            if s3_result.get("rigged_character_fbx_url"):
                out["rigged_character_fbx_url"] = s3_result["rigged_character_fbx_url"]
            if s3_result.get("model_urls"):
                out["model_urls"] = s3_result["model_urls"]
            if s3_result.get("db_ok") is False:
                out["db_ok"] = False
                out["db_errors"] = s3_result.get("db_errors")

    return jsonify(out)

# ---- Meshy Image to 3D ----
@app.route("/api/image-to-3d/start", methods=["POST", "OPTIONS"])
def api_image_to_3d_start():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    user_id = g.user_id
    identity_id = getattr(g, 'identity_id', None) or user_id

    body = request.get_json(silent=True) or {}
    log_event("image-to-3d/start:incoming", body)
    image_url = (body.get("image_url") or "").strip()
    if not image_url:
        return jsonify({"error": "image_url required"}), 400

    # Check credits BEFORE calling Meshy
    action_key = LEGACY_ACTION_KEYS["image-to-3d"]
    has_credits, cost, credit_error = check_credits_available(identity_id, action_key)
    if not has_credits:
        return credit_error

    prompt = (body.get("prompt") or "").strip()
    payload = {
        "image_url": image_url,  # Send original URL to Meshy (they need to fetch it)
        "prompt": prompt,
        "ai_model": body.get("model") or "latest",
        # Explicitly request textured output when supported
        "enable_pbr": True,
    }
    try:
        resp = mesh_post("/openapi/v1/image-to-3d", payload)
        log_event("image-to-3d/start:meshy-resp", resp)
        job_id = resp.get("result") or resp.get("id")
        if not job_id:
            return jsonify({"error": "No job id in response", "raw": resp}), 502

        # Charge credits AFTER successful Meshy call (idempotent via job_id)
        _, new_balance, _ = charge_credits_for_action(identity_id, action_key, job_id, {"prompt": prompt[:100] if prompt else None})

        s3_name = prompt if prompt else "image_to_3d_source"
        s3_user_id = user_id or "public"

        # Upload source image to S3 for permanent storage with deterministic key
        # safe_upload_to_s3 handles unauthenticated uploads (user_id=None -> public namespace)
        s3_image_url = image_url
        if AWS_BUCKET_MODELS:
            try:
                s3_image_url = safe_upload_to_s3(
                    image_url,
                    "image/png",
                    "source_images",
                    s3_name,
                    user_id=user_id,
                    key_base=f"source_images/{s3_user_id}/{job_id}",
                    provider="user",
                )
                print(f"[image-to-3d] Uploaded source image to S3: {s3_image_url}")
            except Exception as e:
                print(f"[image-to-3d] Failed to upload source image to S3: {e}, using original URL")

        # Save metadata to store so we can retrieve it when job finishes
        store = load_store()
        store[job_id] = {
            "stage": "image3d",
            "created_at": now_s() * 1000,
            "prompt": prompt,
            "root_prompt": prompt,  # This is the start of a chain
            "title": f"(image2-3d) {prompt[:40]}" if prompt else f"(image2-3d) {DEFAULT_MODEL_TITLE}",
            "image_url": s3_image_url,  # Store S3 URL for our records
            "original_image_url": image_url,  # Keep original for reference
            "ai_model": payload.get("ai_model"),
            "user_id": user_id,
        }
        save_store(store)

        # Save to DB for recovery
        save_active_job_to_db(job_id, "image-to-3d", "image3d", store[job_id], user_id)

        return jsonify({"job_id": job_id, "new_balance": new_balance or None})
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/api/image-to-3d/status/<job_id>", methods=["GET", "OPTIONS"])
def api_image_to_3d_status(job_id):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("image-to-3d/status:incoming", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Verify job ownership
    user_id = g.user_id
    if not verify_job_ownership(job_id, user_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v1/image-to-3d/{job_id}")
        log_event("image-to-3d/status:meshy-resp", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    out = normalize_meshy_task(ms, stage="image3d")
    log_status_summary("image-to-3d", job_id, out)

    # Save to normalized DB tables when job finishes
    if out["status"] == "done" and (out.get("glb_url") or out.get("thumbnail_url")):
        store = load_store()
        # Get metadata from local store OR database (in case server restarted)
        meta = get_job_metadata(job_id, store)

        # Get prompt from metadata or Meshy response
        if not meta.get("prompt"):
            meta["prompt"] = out.get("prompt") or ""
        if not meta.get("root_prompt"):
            meta["root_prompt"] = meta.get("prompt")

        # Generate title from prompt if missing
        if not meta.get("title"):
            prompt_for_title = meta.get("prompt") or ""
            meta["title"] = f"(image-to-3d) {prompt_for_title[:40]}" if prompt_for_title else f"(image-to-3d) {DEFAULT_MODEL_TITLE}"

        user_id = meta.get("user_id") or getattr(g, 'user_id', None)
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type='image-to-3d', user_id=user_id)

        # Update response with S3 URLs so frontend gets permanent URLs
        if s3_result and s3_result.get("success"):
            if s3_result.get("glb_url"):
                out["glb_url"] = s3_result["glb_url"]
            if s3_result.get("thumbnail_url"):
                out["thumbnail_url"] = s3_result["thumbnail_url"]
            if s3_result.get("textured_glb_url"):
                out["textured_glb_url"] = s3_result["textured_glb_url"]
            if s3_result.get("model_urls"):
                out["model_urls"] = s3_result["model_urls"]
            if s3_result.get("texture_urls"):
                out["texture_urls"] = s3_result["texture_urls"]

    return jsonify(out)

# ---- Nano Banana Image Generation (disabled) ----
@app.route("/api/nano/image", methods=["POST", "OPTIONS"])
def api_nano_image():
    return jsonify({"error": "NanoBanana disabled"}), 410

@app.route("/api/nano/image/<job_id>", methods=["GET", "OPTIONS"])
def api_nano_image_status(job_id):
    return jsonify({"error": "NanoBanana disabled"}), 410

# ---- OpenAI (DALL·E / GPT-Image) Image Generation ----
@app.route("/api/image/openai", methods=["POST", "OPTIONS"])
def api_openai_image():
    if request.method == "OPTIONS":
        return ("", 204)

    user_id = g.user_id
    identity_id = getattr(g, 'identity_id', None) or user_id

    if not OPENAI_API_KEY:
        return jsonify({"error": "OPENAI_API_KEY not configured"}), 503

    body = request.get_json(silent=True) or {}
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "prompt required"}), 400

    # normalize size
    size_raw = (body.get("size") or body.get("resolution") or "1024x1024").lower()
    size_map = {
        "256x256": "256x256",
        "512x512": "512x512",
        "1024x1024": "1024x1024",
        "1024x1792": "1024x1792",
        "1792x1024": "1792x1024",
    }
    size = "1024x1024"
    for key in size_map:
        if key in size_raw:
            size = size_map[key]
            break

    model = (body.get("model") or os.getenv("OPENAI_IMAGE_MODEL") or "gpt-image-1").strip()
    n = int(body.get("n") or 1)
    response_format = (body.get("response_format") or "url").strip()

    # Check credits BEFORE calling OpenAI
    # Note: For n > 1, we check for n * cost (charge per image)
    action_key = LEGACY_ACTION_KEYS["image-studio"]
    if CREDITS_AVAILABLE and identity_id:
        try:
            base_cost = PricingService.get_action_cost(action_key)
            total_cost = base_cost * n
            balance = WalletService.get_balance(identity_id)
            reserved = WalletService.get_reserved_credits(identity_id)
            available = max(0, balance - reserved)

            print(f"[CREDITS] OpenAI image: n={n}, base_cost={base_cost}, total_cost={total_cost}, available={available}")

            if total_cost > 0 and available < total_cost:
                return jsonify({
                    "ok": False,
                    "error": "insufficient_credits",
                    "code": "INSUFFICIENT_CREDITS",
                    "message": f"Insufficient credits. Need {total_cost}, have {available}.",
                    "required": total_cost,
                    "available": available,
                    "balance": balance,
                }), 402
        except Exception as e:
            print(f"[CREDITS] Check error for OpenAI image: {e}")

    try:
        resp = openai_image_generate(prompt=prompt, size=size, model=model, n=n, response_format=response_format)
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    data_list = resp.get("data") or []
    urls = []
    b64_first = None
    for item in data_list:
        if not isinstance(item, dict):
            continue
        if item.get("url"):
            urls.append(item["url"])
        elif item.get("b64_json"):
            if not b64_first:
                b64_first = item["b64_json"]
            urls.append(f"data:image/png;base64,{item['b64_json']}")

    # Save to normalized DB tables
    image_id = None
    if urls:
        client_id = (body.get("client_id") or "").strip()
        image_id = client_id or f"img_{int(time.time() * 1000)}"
        save_image_to_normalized_db(
            image_id=image_id,
            image_url=urls[0],
            prompt=prompt,
            ai_model=model,
            size=size,
            image_urls=urls,
            user_id=user_id
        )

        # Charge credits AFTER successful OpenAI call (idempotent via image_id)
        # For n > 1, we charge n times the base cost
        new_balance = None
        if CREDITS_AVAILABLE and identity_id:
            try:
                base_cost = PricingService.get_action_cost(action_key)
                total_cost = base_cost * n
                if total_cost > 0:
                    from credits import _charge_credits_idempotent
                    result = _charge_credits_idempotent(
                        identity_id=identity_id,
                        action=action_key,
                        ref_type=action_key,
                        ref_id=image_id,
                        cost_credits=total_cost,
                        meta={"prompt": prompt[:100], "n": n, "model": model, "size": size},
                    )
                    new_balance = result.get("new_balance")
                    print(f"[CREDITS] Charged {total_cost} credits for OpenAI image, image_id={image_id}, n={n}, new_balance={new_balance}")
            except Exception as e:
                print(f"[CREDITS] Failed to charge for OpenAI image: {e}")

    return jsonify({
        "image_id": image_id,
        "image_url": urls[0] if urls else None,
        "image_urls": urls,
        "image_base64": b64_first,
        "status": "done",
        "model": model,
        "size": size,
        "new_balance": new_balance,
        "raw": resp
    })

# ---- Proxy external images (OpenAI blobs) ----
@app.route("/api/proxy-image")
def api_proxy_image():
    url = request.args.get("u") or ""
    if not url:
        return jsonify({"error": "Missing url"}), 400

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return jsonify({"error": "Invalid scheme"}), 400
    host = parsed.hostname or ""
    if host not in ALLOWED_IMAGE_HOSTS:
        return jsonify({"error": "Host not allowed"}), 400

    try:
        r = requests.get(url, stream=True, timeout=30)
    except Exception as e:
        return jsonify({"error": f"Fetch failed: {e}"}), 502

    if not r.ok:
        return jsonify({"error": f"Upstream {r.status_code}"}), r.status_code

    content_type = r.headers.get("Content-Type", "application/octet-stream")
    return Response(r.content, status=200, mimetype=content_type)

# ---- Cache base64/data: images to local files and serve via GET ----
@app.route("/api/cache-image", methods=["POST", "OPTIONS"])
def api_cache_image():
    if request.method == "OPTIONS":
        return ("", 204)
    body = request.get_json(silent=True) or {}
    data_url = body.get("data_url") or ""
    if not data_url.startswith("data:"):
        return jsonify({"error": "data_url is required and must be a data URI"}), 400
    max_bytes = int(os.getenv("CACHE_IMAGE_MAX_BYTES", "5242880"))  # 5 MB default
    allowed_mimes = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
    try:
        header, b64data = data_url.split(",", 1)
        meta = header.split(";")[0]
        mime = meta.replace("data:", "") or "image/png"
        if mime.lower() not in allowed_mimes:
            return jsonify({"error": "mime not allowed"}), 400
        # quick length guard before decoding
        if (len(b64data) * 3) / 4 > max_bytes:
            return jsonify({"error": "image too large"}), 400
        ext = ".png" if "png" in mime else ".jpg"
        file_id = f"{int(time.time()*1000)}"
        file_path = CACHE_DIR / f"{file_id}{ext}"
        file_path.write_bytes(base64.b64decode(b64data))
    except Exception as e:
        return jsonify({"error": f"Failed to decode data URL: {e}"}), 400

    return jsonify({
        "url": f"/api/cache-image/{file_path.name}",
        "mime": mime
    })

@app.route("/api/cache-image/<path:filename>", methods=["GET"])
def api_cache_image_get(filename):
    target = CACHE_DIR / filename
    if not target.exists():
        return jsonify({"error": "Not found"}), 404
    return Response(target.read_bytes(), mimetype="image/png")

# ---- History persistence (DATABASE PRIMARY STORAGE) ----
@app.route("/api/history", methods=["GET", "POST", "OPTIONS"])
def api_history():
    if request.method == "OPTIONS":
        return ("", 204)

    user_id = g.user_id  # May be None for anonymous users

    if request.method == "GET":
        if USE_DB:
            conn = get_db_conn()
            if not conn:
                return jsonify({"error": "db_unavailable"}), 503
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Fetch history items (filtered by user if logged in)
                    if user_id:
                        cur.execute(f"""
                            SELECT id, item_type, status, stage, title, prompt,
                                   thumbnail_url, glb_url, image_url, payload, created_at
                            FROM {APP_SCHEMA}.history_items
                            WHERE identity_id = %s
                            ORDER BY created_at DESC;
                        """, (user_id,))
                    else:
                        # Anonymous: return all items without user_id (legacy data)
                        cur.execute(f"""
                            SELECT id, item_type, status, stage, title, prompt,
                                   thumbnail_url, glb_url, image_url, payload, created_at
                            FROM {APP_SCHEMA}.history_items
                            WHERE identity_id IS NULL
                            ORDER BY created_at DESC;
                        """)
                    rows = cur.fetchall()
                conn.close()
                print(f"[History] GET: Fetched {len(rows)} items from database")
                # Merge DB fields with payload for full item data
                items = []
                for r in rows:
                    item = r["payload"] if r["payload"] else {}
                    # Add DB fields to item
                    item["id"] = str(r["id"])
                    item["type"] = r["item_type"]
                    item["status"] = r["status"]
                    if r["stage"]: item["stage"] = r["stage"]
                    if r["title"]: item["title"] = r["title"]
                    if r["prompt"]: item["prompt"] = r["prompt"]
                    if r["thumbnail_url"]: item["thumbnail_url"] = r["thumbnail_url"]
                    if r["glb_url"]: item["glb_url"] = r["glb_url"]
                    if r["image_url"]: item["image_url"] = r["image_url"]
                    if r["created_at"]: item["created_at"] = int(r["created_at"].timestamp() * 1000)
                    items.append(item)
                # Log first few items for debugging
                for i, item in enumerate(items[:3]):
                    print(f"[History] Item {i}: title={item.get('title')}, thumbnail={item.get('thumbnail_url', 'None')[:60] if item.get('thumbnail_url') else 'None'}...")
                # DEV ONLY: sync to local JSON (no-op in production)
                save_history_store(items)
                return jsonify(items)
            except Exception as e:
                print(f"[History] DB read failed: {e}")
                try: conn.close()
                except Exception: pass
                return jsonify({"error": "db_query_failed"}), 503
        # DEV ONLY: fallback to local JSON (returns [] in production)
        return jsonify(load_history_store())

    try:
        payload = request.get_json(silent=True) or []
        if not isinstance(payload, list):
            return jsonify({"error": "Payload must be a list"}), 400

        db_ok = None
        db_errors = []
        # Track results for frontend retry logic
        updated_ids = []
        inserted_ids = []
        skipped_items = []  # [{client_id, reason}]

        if USE_DB:
            conn = get_db_conn()
            if conn:
                try:
                    with conn:
                        with conn.cursor() as cur:
                            for item in payload:
                                item_id = item.get("id") or item.get("job_id")
                                if not item_id:
                                    continue

                                # Check if this item already exists for this user
                                if user_id:
                                    cur.execute(f"""
                                        SELECT id FROM {APP_SCHEMA}.history_items
                                        WHERE (id::text = %s
                                           OR payload->>'original_job_id' = %s
                                           OR payload->>'original_id' = %s
                                           OR payload->>'job_id' = %s)
                                          AND identity_id = %s
                                        LIMIT 1
                                    """, (str(item_id), str(item_id), str(item_id), str(item_id), user_id))
                                else:
                                    cur.execute(f"""
                                        SELECT id FROM {APP_SCHEMA}.history_items
                                        WHERE (id::text = %s
                                           OR payload->>'original_job_id' = %s
                                           OR payload->>'original_id' = %s
                                           OR payload->>'job_id' = %s)
                                          AND identity_id IS NULL
                                        LIMIT 1
                                    """, (str(item_id), str(item_id), str(item_id), str(item_id)))
                                existing = cur.fetchone()
                                existing_id = existing[0] if existing else None

                                # Extract fields for the schema
                                item_type = item.get("type") or item.get("item_type") or "model"
                                status = item.get("status") or "pending"
                                stage = item.get("stage")
                                title = item.get("title")
                                prompt = item.get("prompt")
                                root_prompt = item.get("root_prompt")
                                thumbnail_url = item.get("thumbnail_url")
                                glb_url = item.get("glb_url")
                                image_url = item.get("image_url")

                                # Check if item has valid UUID id
                                if existing_id:
                                    use_id = str(existing_id)
                                else:
                                    try:
                                        uuid.UUID(str(item_id))
                                        use_id = str(item_id)
                                    except (ValueError, TypeError, AttributeError):
                                        use_id = str(uuid.uuid4())
                                        item["original_id"] = item_id

                                provider = "openai" if item_type == "image" else "meshy"
                                s3_user_id = user_id or "public"
                                if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
                                    thumbnail_url = ensure_s3_url_for_data_uri(
                                        thumbnail_url,
                                        "thumbnails",
                                        f"thumbnails/{s3_user_id}/{use_id}",
                                        user_id=user_id,
                                        name="thumbnail",
                                        provider=provider,
                                    )
                                if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
                                    image_url = ensure_s3_url_for_data_uri(
                                        image_url,
                                        "images",
                                        f"images/{s3_user_id}/{use_id}",
                                        user_id=user_id,
                                        name="image",
                                        provider=provider,
                                    )
                                item["thumbnail_url"] = thumbnail_url
                                item["image_url"] = image_url

                                item["id"] = use_id

                                if existing_id:
                                    # UPDATE existing row (model_id/image_id should already be set)
                                    cur.execute(
                                        f"""UPDATE {APP_SCHEMA}.history_items
                                           SET item_type = %s,
                                               status = COALESCE(%s, status),
                                               stage = COALESCE(%s, stage),
                                               title = COALESCE(%s, title),
                                               prompt = COALESCE(%s, prompt),
                                               root_prompt = COALESCE(%s, root_prompt),
                                               identity_id = COALESCE(%s, identity_id),
                                               thumbnail_url = COALESCE(%s, thumbnail_url),
                                               glb_url = COALESCE(%s, glb_url),
                                               image_url = COALESCE(%s, image_url),
                                               payload = %s,
                                               updated_at = NOW()
                                           WHERE id = %s;""",
                                        (item_type, status, stage, title, prompt, root_prompt, user_id,
                                         thumbnail_url, glb_url, image_url, json.dumps(item), use_id)
                                    )
                                    updated_ids.append(use_id)
                                else:
                                    # NEW INSERT: Must have model_id or image_id (XOR constraint)
                                    # Try to look up from item or DB
                                    model_id = item.get("model_id")
                                    image_id = item.get("image_id")
                                    lookup_reason = None
                                    if not model_id and not image_id:
                                        # Try to find the asset in DB
                                        model_id, image_id, lookup_reason = _lookup_asset_id_for_history(
                                            cur, item_type, item_id, glb_url, image_url, user_id, provider
                                        )
                                    # Validate XOR constraint
                                    if not _validate_history_item_asset_ids(model_id, image_id, f"bulk_sync:{item_id}"):
                                        # Determine skip reason
                                        if lookup_reason:
                                            skip_reason = lookup_reason
                                        elif model_id and image_id:
                                            skip_reason = "xor_violation"
                                        else:
                                            skip_reason = "missing_asset_reference"
                                        print(f"[History] Skipping item {item_id} - reason: {skip_reason}")
                                        skipped_items.append({"client_id": str(item_id), "reason": skip_reason})
                                        continue
                                    cur.execute(
                                        f"""INSERT INTO {APP_SCHEMA}.history_items (id, identity_id, item_type, status, stage, title, prompt,
                                               root_prompt, thumbnail_url, glb_url, image_url, model_id, image_id, payload)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s,
                                               %s, %s, %s, %s, %s, %s, %s)
                                           ON CONFLICT (id) DO UPDATE
                                           SET item_type = EXCLUDED.item_type,
                                               status = COALESCE(EXCLUDED.status, {APP_SCHEMA}.history_items.status),
                                               stage = COALESCE(EXCLUDED.stage, {APP_SCHEMA}.history_items.stage),
                                               title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.history_items.title),
                                               prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.history_items.prompt),
                                               root_prompt = COALESCE(EXCLUDED.root_prompt, {APP_SCHEMA}.history_items.root_prompt),
                                               identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.history_items.identity_id),
                                               thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.history_items.thumbnail_url),
                                               glb_url = COALESCE(EXCLUDED.glb_url, {APP_SCHEMA}.history_items.glb_url),
                                               image_url = COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.history_items.image_url),
                                               model_id = COALESCE(EXCLUDED.model_id, {APP_SCHEMA}.history_items.model_id),
                                               image_id = COALESCE(EXCLUDED.image_id, {APP_SCHEMA}.history_items.image_id),
                                               payload = EXCLUDED.payload,
                                               updated_at = NOW();""",
                                        (use_id, user_id, item_type, status, stage, title, prompt,
                                         root_prompt, thumbnail_url, glb_url, image_url, model_id, image_id, json.dumps(item))
                                    )
                                    inserted_ids.append(use_id)
                    conn.close()
                    print(f"[History] Bulk sync: updated={len(updated_ids)}, inserted={len(inserted_ids)}, skipped={len(skipped_items)}")
                    db_ok = True
                except Exception as e:
                    log_db_continue("history_bulk_write", e)
                    db_errors.append({"op": "history_bulk_write", "error": str(e)})
                    import traceback
                    traceback.print_exc()
                    try:
                        conn.close()
                    except Exception:
                        pass
                    db_ok = False
            else:
                db_ok = False
                db_errors.append({"op": "history_bulk_connect", "error": "db_unavailable"})

        # DEV ONLY: sync to local JSON (no-op in production)
        save_history_store(payload)
        return jsonify({
            "ok": True,
            "count": len(payload),
            "updated": updated_ids,
            "inserted": inserted_ids,
            "skipped": skipped_items if skipped_items else None,
            "db": db_ok,
            "db_errors": db_errors or None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Add single history item to database
@app.route("/api/history/item", methods=["POST", "OPTIONS"])
def api_history_item_add():
    if request.method == "OPTIONS":
        return ("", 204)

    user_id = g.user_id

    try:
        item = request.get_json(silent=True) or {}
        item_id = item.get("id") or item.get("job_id")
        if not item_id:
            return jsonify({"error": "Item ID required"}), 400

        # Always keep a copy of the ID on the payload for local fallback writes
        item["id"] = item_id
        db_ok = False
        db_errors = []

        if USE_DB:
            conn = get_db_conn()
            if conn:
                try:
                    with conn:
                        with conn.cursor() as cur:
                            # Check if this item already exists (for this user)
                            if user_id:
                                cur.execute(f"""
                                    SELECT id FROM {APP_SCHEMA}.history_items
                                    WHERE (id::text = %s
                                       OR payload->>'original_job_id' = %s
                                       OR payload->>'original_id' = %s
                                       OR payload->>'job_id' = %s)
                                      AND identity_id = %s
                                    LIMIT 1
                                """, (str(item_id), str(item_id), str(item_id), str(item_id), user_id))
                            else:
                                cur.execute(f"""
                                    SELECT id FROM {APP_SCHEMA}.history_items
                                    WHERE (id::text = %s
                                       OR payload->>'original_job_id' = %s
                                       OR payload->>'original_id' = %s
                                       OR payload->>'job_id' = %s)
                                      AND identity_id IS NULL
                                    LIMIT 1
                                """, (str(item_id), str(item_id), str(item_id), str(item_id)))
                            existing = cur.fetchone()
                            existing_id = existing[0] if existing else None

                            # Extract fields for the schema
                            item_type = item.get("type") or item.get("item_type") or "model"
                            status = item.get("status") or "pending"
                            stage = item.get("stage")
                            title = item.get("title")
                            prompt = item.get("prompt")
                            root_prompt = item.get("root_prompt")
                            thumbnail_url = item.get("thumbnail_url")
                            glb_url = item.get("glb_url")
                            image_url = item.get("image_url")

                            # Check if item has valid UUID id
                            if existing_id:
                                use_id = str(existing_id)
                            else:
                                try:
                                    uuid.UUID(str(item_id))
                                    use_id = str(item_id)
                                except (ValueError, TypeError, AttributeError):
                                    use_id = str(uuid.uuid4())
                                    item["original_id"] = item_id

                            provider = "openai" if item_type == "image" else "meshy"
                            s3_user_id = user_id or "public"
                            if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
                                thumbnail_url = ensure_s3_url_for_data_uri(
                                    thumbnail_url,
                                    "thumbnails",
                                    f"thumbnails/{s3_user_id}/{use_id}",
                                    user_id=user_id,
                                    name="thumbnail",
                                    provider=provider,
                                )
                            if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
                                image_url = ensure_s3_url_for_data_uri(
                                    image_url,
                                    "images",
                                    f"images/{s3_user_id}/{use_id}",
                                    user_id=user_id,
                                    name="image",
                                    provider=provider,
                                )
                            item["thumbnail_url"] = thumbnail_url
                            item["image_url"] = image_url

                            item["id"] = use_id

                            if existing_id:
                                # UPDATE existing row (model_id/image_id should already be set)
                                cur.execute(
                                    f"""UPDATE {APP_SCHEMA}.history_items
                                       SET item_type = %s,
                                           status = COALESCE(%s, status),
                                           stage = COALESCE(%s, stage),
                                           title = COALESCE(%s, title),
                                           prompt = COALESCE(%s, prompt),
                                           root_prompt = COALESCE(%s, root_prompt),
                                           identity_id = COALESCE(%s, identity_id),
                                           thumbnail_url = COALESCE(%s, thumbnail_url),
                                           glb_url = COALESCE(%s, glb_url),
                                           image_url = COALESCE(%s, image_url),
                                           payload = %s,
                                           updated_at = NOW()
                                       WHERE id = %s;""",
                                    (item_type, status, stage, title, prompt, root_prompt, user_id,
                                     thumbnail_url, glb_url, image_url, json.dumps(item), use_id)
                                )
                                db_ok = True
                                item_id = use_id
                            else:
                                # NEW INSERT: Must have model_id or image_id (XOR constraint)
                                # Try to look up from item or DB
                                model_id = item.get("model_id")
                                image_id = item.get("image_id")
                                lookup_reason = None
                                if not model_id and not image_id:
                                    # Try to find the asset in DB
                                    model_id, image_id, lookup_reason = _lookup_asset_id_for_history(
                                        cur, item_type, item_id, glb_url, image_url, user_id, provider
                                    )
                                # Validate XOR constraint
                                if not _validate_history_item_asset_ids(model_id, image_id, f"item_add:{item_id}"):
                                    # Determine skip reason for frontend
                                    if lookup_reason:
                                        skip_reason = lookup_reason
                                    elif model_id and image_id:
                                        skip_reason = "xor_violation"
                                    else:
                                        skip_reason = "missing_asset_reference"
                                    print(f"[History] Skipping item {item_id} - reason: {skip_reason}")
                                    db_errors.append({
                                        "op": "history_item_add",
                                        "error": f"Skipped: {skip_reason}",
                                        "skip_reason": skip_reason,
                                    })
                                else:
                                    cur.execute(
                                        f"""INSERT INTO {APP_SCHEMA}.history_items (id, identity_id, item_type, status, stage, title, prompt,
                                               root_prompt, thumbnail_url, glb_url, image_url, model_id, image_id, payload)
                                           VALUES (%s, %s, %s, %s, %s, %s, %s,
                                               %s, %s, %s, %s, %s, %s, %s)
                                           ON CONFLICT (id) DO UPDATE
                                           SET item_type = EXCLUDED.item_type,
                                               status = COALESCE(EXCLUDED.status, {APP_SCHEMA}.history_items.status),
                                               stage = COALESCE(EXCLUDED.stage, {APP_SCHEMA}.history_items.stage),
                                               title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.history_items.title),
                                               prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.history_items.prompt),
                                               root_prompt = COALESCE(EXCLUDED.root_prompt, {APP_SCHEMA}.history_items.root_prompt),
                                               identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.history_items.identity_id),
                                               thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.history_items.thumbnail_url),
                                               glb_url = COALESCE(EXCLUDED.glb_url, {APP_SCHEMA}.history_items.glb_url),
                                               image_url = COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.history_items.image_url),
                                               model_id = COALESCE(EXCLUDED.model_id, {APP_SCHEMA}.history_items.model_id),
                                               image_id = COALESCE(EXCLUDED.image_id, {APP_SCHEMA}.history_items.image_id),
                                               payload = EXCLUDED.payload,
                                               updated_at = NOW();""",
                                        (use_id, user_id, item_type, status, stage, title, prompt,
                                         root_prompt, thumbnail_url, glb_url, image_url, model_id, image_id, json.dumps(item))
                                    )
                                    db_ok = True
                                    item_id = use_id  # Return the actual UUID used
                    conn.close()
                except Exception as e:
                    log_db_continue("history_item_add", e)
                    db_errors.append({"op": "history_item_add", "error": str(e)})
                    try: conn.close()
                    except Exception: pass
            else:
                db_errors.append({"op": "history_item_add_connect", "error": "db_unavailable"})

        # Check if item was skipped (for frontend retry logic)
        skipped = None
        for err in db_errors:
            if err.get("skip_reason"):
                skipped = {"client_id": str(item_id), "reason": err["skip_reason"]}
                break

        local_ok = upsert_history_local(item, merge=False)
        return jsonify({
            "ok": db_ok or False,
            "id": item_id,
            "skipped": skipped,
            "db": db_ok,
            "db_errors": db_errors or None,
            "local": local_ok,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Update single history item in database
@app.route("/api/history/item/<item_id>", methods=["PATCH", "DELETE", "OPTIONS"])
def api_history_item_update(item_id):
    if request.method == "OPTIONS":
        return ("", 204)

    user_id = g.user_id

    # Validate UUID format
    try:
        uuid.UUID(str(item_id))
    except (ValueError, TypeError):
        # Not a valid UUID - might be an old-format ID
        pass

    if request.method == "DELETE":
        db_ok = False
        db_errors = []
        s3_deleted = 0
        if USE_DB:
            conn = get_db_conn()
            if not conn:
                db_errors.append({"op": "history_item_delete_connect", "error": "db_unavailable"})
                local_ok = delete_history_local(item_id)
                return jsonify({"ok": False, "error": "db_unavailable"}), 503
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    if user_id:
                        cur.execute(f"""
                            SELECT id, item_type, model_id, image_id, thumbnail_url, glb_url, image_url, payload
                            FROM {APP_SCHEMA}.history_items
                            WHERE id::text = %s AND (identity_id = %s OR identity_id IS NULL)
                            LIMIT 1
                        """, (str(item_id), user_id))
                    else:
                        cur.execute(f"""
                            SELECT id, item_type, model_id, image_id, thumbnail_url, glb_url, image_url, payload
                            FROM {APP_SCHEMA}.history_items
                            WHERE id::text = %s AND identity_id IS NULL
                            LIMIT 1
                        """, (str(item_id),))
                    row = cur.fetchone()
                if not row:
                    conn.close()
                    return jsonify({"ok": False, "error": "not_found"}), 404

                history_id = row["id"]
                model_id = row["model_id"]
                image_id = row["image_id"]
                payload = row["payload"] if row["payload"] else {}
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}

                row["payload"] = payload
                s3_keys = collect_s3_keys(row)

                try:
                    with conn:
                        with conn.cursor() as cur:
                            if user_id:
                                cur.execute(f"""
                                    DELETE FROM {APP_SCHEMA}.history_items
                                    WHERE id::text = %s AND (identity_id = %s OR identity_id IS NULL)
                                """, (str(item_id), user_id))
                            else:
                                cur.execute(f"""
                                    DELETE FROM {APP_SCHEMA}.history_items
                                    WHERE id::text = %s AND identity_id IS NULL
                                """, (str(item_id),))

                            if model_id:
                                cur.execute(f"DELETE FROM {APP_SCHEMA}.models WHERE id = %s", (model_id,))
                            if image_id:
                                cur.execute(f"DELETE FROM {APP_SCHEMA}.images WHERE id = %s", (image_id,))
                    db_ok = True
                except Exception as e:
                    log_db_continue("history_item_delete_db", e)
                    db_errors.append({"op": "history_item_delete_db", "error": str(e)})
                    conn.close()
                    local_ok = delete_history_local(item_id)
                    return jsonify({"ok": False, "error": "db_delete_failed"}), 500

                if s3_keys and AWS_BUCKET_MODELS:
                    try:
                        keys_list = [{"Key": k} for k in s3_keys if k]
                        for i in range(0, len(keys_list), 1000):
                            chunk = keys_list[i:i + 1000]
                            resp = s3.delete_objects(Bucket=AWS_BUCKET_MODELS, Delete={"Objects": chunk, "Quiet": True})
                            s3_deleted += len(resp.get("Deleted", []) or [])
                            errs = resp.get("Errors") or []
                            if errs:
                                raise RuntimeError(f"S3 delete errors: {errs}")
                    except Exception as e:
                        log_db_continue("history_item_delete_s3", e)
                        db_errors.append({"op": "history_item_delete_s3", "error": str(e)})
                        conn.close()
                        local_ok = delete_history_local(item_id)
                        return jsonify({"ok": False, "error": "s3_delete_failed"}), 500

                conn.close()
            except Exception as e:
                log_db_continue("history_item_delete_lookup", e)
                db_errors.append({"op": "history_item_delete_lookup", "error": str(e)})
                try:
                    conn.close()
                except Exception:
                    pass
                local_ok = delete_history_local(item_id)
                return jsonify({"ok": False, "error": "lookup_failed"}), 500

        local_ok = delete_history_local(item_id)
        return jsonify({"ok": True})

    if request.method == "PATCH":
        try:
            updates = request.get_json(silent=True) or {}
            db_ok = False
            db_errors = []
            if USE_DB:
                conn = get_db_conn()
                if conn:
                    try:
                        with conn:
                            with conn.cursor(row_factory=dict_row) as cur:
                                # Get existing item (verify user ownership)
                                if user_id:
                                    cur.execute(f"""SELECT id, payload FROM {APP_SCHEMA}.history_items
                                                   WHERE (id::text = %s
                                                      OR payload->>'original_id' = %s
                                                      OR payload->>'job_id' = %s)
                                                     AND (identity_id = %s OR identity_id IS NULL)
                                                   LIMIT 1;""", (str(item_id), str(item_id), str(item_id), user_id))
                                else:
                                    cur.execute(f"""SELECT id, payload FROM {APP_SCHEMA}.history_items
                                                   WHERE (id::text = %s
                                                      OR payload->>'original_id' = %s
                                                      OR payload->>'job_id' = %s)
                                                     AND identity_id IS NULL
                                                   LIMIT 1;""", (str(item_id), str(item_id), str(item_id)))
                                row = cur.fetchone()
                                if not row:
                                    return jsonify({"error": "Item not found or access denied"}), 404

                                # Merge updates into payload
                                existing = row["payload"] if row["payload"] else {}
                                existing.update(updates)
                                actual_id = row["id"]

                                # Extract fields that should be in columns
                                item_type = updates.get("type") or updates.get("item_type")
                                status = updates.get("status")
                                stage = updates.get("stage")
                                title = updates.get("title")
                                if isinstance(title, str):
                                    title_norm = title.strip().lower()
                                    if title_norm in ("", "untitled", "(untitled)"):
                                        title = None
                                prompt = updates.get("prompt")
                                thumbnail_url = updates.get("thumbnail_url")
                                glb_url = updates.get("glb_url")
                                image_url = updates.get("image_url")

                                provider = "openai" if item_type == "image" else "meshy"
                                s3_user_id = user_id or "public"
                                if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
                                    thumbnail_url = ensure_s3_url_for_data_uri(
                                        thumbnail_url,
                                        "thumbnails",
                                        f"thumbnails/{s3_user_id}/{actual_id}",
                                        user_id=user_id,
                                        name="thumbnail",
                                        provider=provider,
                                    )
                                    updates["thumbnail_url"] = thumbnail_url
                                if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
                                    image_url = ensure_s3_url_for_data_uri(
                                        image_url,
                                        "images",
                                        f"images/{s3_user_id}/{actual_id}",
                                        user_id=user_id,
                                        name="image",
                                        provider=provider,
                                    )
                                    updates["image_url"] = image_url
                                existing.update({k: v for k, v in updates.items() if k in {"thumbnail_url", "image_url"}})

                                # Update with column values if provided (with ownership check)
                                if user_id:
                                    cur.execute(
                                        f"""UPDATE {APP_SCHEMA}.history_items
                                           SET item_type = COALESCE(%s, item_type),
                                               status = COALESCE(%s, status),
                                               stage = COALESCE(%s, stage),
                                               title = COALESCE(%s, title),
                                               prompt = COALESCE(%s, prompt),
                                               thumbnail_url = COALESCE(%s, thumbnail_url),
                                               glb_url = COALESCE(%s, glb_url),
                                               image_url = COALESCE(%s, image_url),
                                               payload = %s,
                                               updated_at = NOW()
                                           WHERE id = %s AND (identity_id = %s OR identity_id IS NULL);""",
                                        (item_type, status, stage, title, prompt,
                                         thumbnail_url, glb_url, image_url,
                                         json.dumps(existing), actual_id, user_id)
                                    )
                                else:
                                    cur.execute(
                                        f"""UPDATE {APP_SCHEMA}.history_items
                                           SET item_type = COALESCE(%s, item_type),
                                               status = COALESCE(%s, status),
                                               stage = COALESCE(%s, stage),
                                               title = COALESCE(%s, title),
                                               prompt = COALESCE(%s, prompt),
                                               thumbnail_url = COALESCE(%s, thumbnail_url),
                                               glb_url = COALESCE(%s, glb_url),
                                               image_url = COALESCE(%s, image_url),
                                               payload = %s,
                                               updated_at = NOW()
                                           WHERE id = %s AND identity_id IS NULL;""",
                                        (item_type, status, stage, title, prompt,
                                         thumbnail_url, glb_url, image_url,
                                         json.dumps(existing), actual_id)
                                    )
                        conn.close()
                        db_ok = True
                    except Exception as e:
                        log_db_continue("history_item_update", e)
                        db_errors.append({"op": "history_item_update", "error": str(e)})
                        try: conn.close()
                        except Exception: pass
                else:
                    db_errors.append({"op": "history_item_update_connect", "error": "db_unavailable"})

            # DEV ONLY: persist updates to local JSON (no-op in production)
            local_ok = False
            if LOCAL_DEV_MODE:
                existing_local = None
                arr = load_history_store()
                if isinstance(arr, list):
                    for entry in arr:
                        if isinstance(entry, dict) and _local_history_id(entry) == item_id:
                            existing_local = entry
                            break
                merged_local = {**(existing_local or {}), **updates, "id": item_id}
                local_ok = upsert_history_local(merged_local, merge=True)

            return jsonify({"ok": True, "id": item_id, "db": db_ok, "db_errors": db_errors or None, "local": local_ok})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

# Entrypoint
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # For Render, use: gunicorn 3dprint-backend:app
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")))
