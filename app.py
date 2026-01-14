import os, json, time, base64, uuid
import boto3
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any
from urllib.parse import urlparse
from datetime import datetime

load_dotenv()

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
from flask import Flask, request, jsonify, Response, abort, g
from flask_cors import CORS

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

import re

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
        "model/obj": ".obj",
        "model/stl": ".stl",
    }
    return ext_map.get(content_type, "")

def upload_bytes_to_s3(data_bytes: bytes, content_type: str = "application/octet-stream", prefix: str = "models", name: str = None, user_id: str = None) -> str:
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

    print(f"[S3] Uploading {len(data_bytes)} bytes to bucket={AWS_BUCKET_MODELS}, key={key}, content_type={content_type}")
    try:
        s3.put_object(
            Bucket=AWS_BUCKET_MODELS,
            Key=key,
            Body=data_bytes,
            ContentType=content_type,
            ACL='public-read',  # Make the object publicly readable
        )
        s3_url = f"https://{AWS_BUCKET_MODELS}.s3.{AWS_REGION}.amazonaws.com/{key}"
        print(f"[S3] SUCCESS: Uploaded {len(data_bytes)} bytes -> {s3_url}")
        return s3_url
    except Exception as e:
        print(f"[S3] ERROR: put_object failed for {key}: {e}")
        print(f"[S3] HINT: If you see AccessControlListNotSupported, disable 'Block public access' in S3 bucket settings")
        import traceback
        traceback.print_exc()
        raise

def upload_url_to_s3(url: str, content_type: str = None, prefix: str = "models", name: str = None, user_id: str = None) -> str:
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
    print(f"[S3] Downloading from URL: {url[:100]}...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    ct = content_type or resp.headers.get("Content-Type", "application/octet-stream")
    print(f"[S3] Downloaded {len(resp.content)} bytes, content-type={ct}")
    return upload_bytes_to_s3(resp.content, ct, prefix, name, user_id)

def safe_upload_to_s3(url: str, content_type: str, prefix: str, name: str = None, user_id: str = None) -> str:
    """
    Safely upload URL to S3, returning original URL if S3 upload fails or user_id missing.
    Also handles base64 data URLs.

    Args:
        url: URL to upload (or base64 data URL)
        content_type: MIME type
        prefix: folder prefix
        name: optional human-readable name to include in the S3 key
        user_id: user UUID for namespacing (uses 'anonymous' folder if missing)
    """
    print(f"[S3] safe_upload_to_s3 called: prefix={prefix}, user_id={user_id}, name={name}, url={url[:60] if url else 'None'}...")
    if not url:
        print(f"[S3] SKIP: No URL provided for {prefix}")
        return url
    if not AWS_BUCKET_MODELS:
        msg = "[S3] SKIP: AWS_BUCKET_MODELS not configured, returning original URL"
        if REQUIRE_AWS_UPLOADS:
            raise RuntimeError(msg)
        print(msg)
        return url
    if REQUIRE_AWS_UPLOADS and (not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY):
        raise RuntimeError("[S3] AWS credentials not configured")
    # Allow anonymous uploads to prevent asset loss (Meshy URLs expire)
    # Use "anonymous" folder for unauthenticated users
    if not user_id:
        user_id = "anonymous"
        print(f"[S3] INFO: No user_id provided, using 'anonymous' folder for {prefix}")
    try:
        # Handle base64 data URLs
        if url.startswith("data:"):
            s3_url = upload_base64_to_s3(url, prefix, name, user_id)
            print(f"[S3] SUCCESS: Uploaded base64 {prefix} -> {s3_url}")
            return s3_url
        # Handle regular URLs
        s3_url = upload_url_to_s3(url, content_type, prefix, name, user_id)
        print(f"[S3] SUCCESS: Uploaded {prefix}: {url[:60]}... -> {s3_url}")
        return s3_url
    except Exception as e:
        # With AWS configured, treat upload failures as fatal to avoid silent Meshy fallbacks
        print(f"[S3] ERROR: Failed to upload {prefix}: {e}")
        import traceback
        traceback.print_exc()
        raise

def upload_base64_to_s3(data_url: str, prefix: str = "images", name: str = None, user_id: str = None) -> str:
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
        return upload_bytes_to_s3(image_bytes, mime, prefix, name, user_id)
    except Exception as e:
        print(f"[S3] Failed to parse/upload base64: {e}")
        raise

MESHY_API_KEY  = os.getenv("MESHY_API_KEY", "").strip()
MESHY_API_BASE = os.getenv("MESHY_API_BASE", "https://api.meshy.ai").rstrip("/")

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
# Example .env for production:
#   ALLOWED_ORIGINS=https://hub.yourdomain.com,https://3d.yourdomain.com,https://yourdomain.com
#
DEV_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:5173",      # Vite default
    "http://localhost:8080",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
]

# Parse ALLOWED_ORIGINS from env (comma-separated)
_env_origins = os.getenv("ALLOWED_ORIGINS", "").strip()
ALLOW_ALL_ORIGINS = False  # Flag for wildcard mode

if _env_origins == "*":
    # Wildcard mode: allow all origins dynamically
    # NOTE: With credentials=True, we can't use literal "*" - must echo the requesting origin
    ALLOW_ALL_ORIGINS = True
    import re
    ALLOWED_ORIGINS = [
        # Match any origin - we'll echo it back
        re.compile(r".*"),
    ]
    print("[CORS] Wildcard mode enabled - allowing all origins")
elif _env_origins:
    ALLOWED_ORIGINS = [o.strip() for o in _env_origins.split(",") if o.strip()]
elif IS_DEV:
    # Dev mode with no explicit origins: allow localhost
    ALLOWED_ORIGINS = DEV_ORIGINS
else:
    # Production with no explicit origins: block by default for safety
    ALLOWED_ORIGINS = []
    print("[CORS] ERROR: ALLOWED_ORIGINS not set in production; requests will be blocked until configured.")

print(f"[CORS] Allowed origins: {ALLOWED_ORIGINS}")

app = Flask(__name__)
CORS(
    app,
    resources={r"/api/*": {"origins": ALLOWED_ORIGINS}},
    supports_credentials=True,  # Required for httpOnly cookies
    allow_headers=["Content-Type", "Authorization"],
    expose_headers=["Content-Type"],
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
)

@app.before_request
def _set_anonymous_user():
    # Auth removed; keep request user_id consistently set to None.
    g.user_id = None
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
        with conn, conn.cursor() as cur:
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
        "rig": "MESHY_REFINE",
        "rigging": "MESHY_REFINE",
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

        with conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"""
                INSERT INTO {APP_SCHEMA}.history_items (
                    id, identity_id, item_type, status, stage,
                    title, prompt, root_prompt,
                    thumbnail_url, glb_url, image_url,
                    payload
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s
                )
                ON CONFLICT (id) DO UPDATE
                SET status = EXCLUDED.status,
                    stage = COALESCE(EXCLUDED.stage, {APP_SCHEMA}.history_items.stage),
                    title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.history_items.title),
                    prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.history_items.prompt),
                    root_prompt = COALESCE(EXCLUDED.root_prompt, {APP_SCHEMA}.history_items.root_prompt),
                    identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.history_items.identity_id),
                    thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.history_items.thumbnail_url),
                    glb_url = COALESCE(EXCLUDED.glb_url, {APP_SCHEMA}.history_items.glb_url),
                    image_url = COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.history_items.image_url),
                    payload = EXCLUDED.payload,
                    updated_at = NOW()
            """, (
                history_id,
                user_id,
                item_type,
                "processing",
                stage,
                title,
                prompt,
                root_prompt,
                thumbnail_url,
                glb_url,
                image_url,
                json.dumps(payload),
            ))

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
                        related_history_id = COALESCE(%s, related_history_id),
                        updated_at = NOW()
                    WHERE id = %s
                """, (user_id, provider, action_code, progress, history_id, existing["id"]))
            else:
                cur.execute(f"""
                    INSERT INTO {APP_SCHEMA}.active_jobs (
                        id, identity_id, provider, action_code, upstream_job_id,
                        status, progress, related_history_id
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        'running', %s, %s
                    )
                """, (str(uuid.uuid4()), user_id, provider, action_code, job_id, progress, history_id))
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
            # Check if this image already exists for this user (prevent duplicates)
            if user_id:
                cur.execute(f"""
                    SELECT id FROM {APP_SCHEMA}.history_items
                    WHERE payload->>'original_id' = %s AND identity_id = %s
                    LIMIT 1
                """, (image_id, user_id))
            else:
                cur.execute(f"""
                    SELECT id FROM {APP_SCHEMA}.history_items
                    WHERE payload->>'original_id' = %s AND identity_id IS NULL
                    LIMIT 1
                """, (image_id,))
            existing = cur.fetchone()
            if existing:
                print(f"[DB] Image {image_id} already exists for user {user_id}, skipping duplicate")
                conn.close()
                return True

            # Upload images to S3 for permanent storage (OpenAI URLs expire, base64 needs storage)
            s3_name = prompt or "image"
            if image_url:
                image_url = safe_upload_to_s3(image_url, "image/png", "images", s3_name, user_id=user_id)
            if image_urls:
                image_urls = [
                    safe_upload_to_s3(url, "image/png", "images", f"{s3_name}_{i}", user_id=user_id) if url else url
                    for i, url in enumerate(image_urls)
                ]

            # Parse size for width/height
            width, height = 1024, 1024
            if size and 'x' in size:
                parts = size.split('x')
                try:
                    width, height = int(parts[0]), int(parts[1])
                except ValueError:
                    pass

            # Generate a proper UUID for the history item
            history_uuid = str(uuid.uuid4())
            image_uuid = str(uuid.uuid4())

            # Store extra metadata in payload JSONB
            payload = {
                "original_id": image_id,
                "ai_model": ai_model,
                "size": size,
                "image_urls": image_urls or [image_url],
            }

            title = (prompt[:50] if prompt else "Generated Image")

            # Insert image row
            cur.execute(f"""
                INSERT INTO {APP_SCHEMA}.images (
                    id, identity_id,
                    title, prompt,
                    provider, upstream_id, status,
                    image_url, thumbnail_url,
                    width, height,
                    meta
                ) VALUES (
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s
                )
                RETURNING id
            """, (
                image_uuid,
                user_id,
                title,
                prompt,
                "openai",
                image_id,
                "ready",
                image_url,
                image_url,
                width,
                height,
                json.dumps({
                    "prompt": prompt,
                    "ai_model": ai_model,
                    "size": size,
                    "format": "png",
                    "image_urls": image_urls or [image_url],
                }),
            ))
            returned_image_id = cur.fetchone()[0]

            # Insert history row
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
                RETURNING id
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
            _ = cur.fetchone()[0]

        conn.close()
        print(f"[DB] Saved image {image_id} -> {history_uuid} to normalized tables (user_id={user_id})")
        return True
    except Exception as e:
        print(f"[DB] Failed to save image {image_id}: {e}")
        import traceback
        traceback.print_exc()
        try:
            conn.close()
        except Exception:
            pass
        return False

def save_finished_job_to_normalized_db(job_id: str, status_data: dict, job_meta: dict, job_type: str = 'model', user_id: str = None):
    """
    Save finished job data to normalized tables (history_items, models, images).
    Called when a job status becomes 'done'.
    Uses the user's schema with UUID primary keys and correct column names.
    user_id is extracted from job_meta if not provided.
    """
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
            # Check if this job already has S3 URLs - if so, skip re-upload
            # Also verify user ownership
            if user_id:
                cur.execute(f"""
                    SELECT id, thumbnail_url, glb_url FROM {APP_SCHEMA}.history_items
                    WHERE id::text = %s AND (identity_id = %s OR identity_id IS NULL)
                    LIMIT 1
                """, (job_id, user_id))
            else:
                cur.execute(f"""
                    SELECT id, thumbnail_url, glb_url FROM {APP_SCHEMA}.history_items
                    WHERE id::text = %s AND identity_id IS NULL
                    LIMIT 1
                """, (job_id,))
            existing = cur.fetchone()
            if existing:
                existing_id = existing["id"]
                existing_thumb = existing["thumbnail_url"]
                existing_glb = existing["glb_url"]
                # Check if existing entry already has S3 URLs
                has_s3 = any(
                    url and 's3.' in url and 'amazonaws.com' in url
                    for url in [existing_thumb, existing_glb] if url
                )
                if has_s3:
                    print(f"[DB] Job {job_id} already exists with S3 URLs (id={existing_id}), skipping")
                    conn.close()
                    return True
                else:
                    print(f"[DB] Job {job_id} exists with Meshy URLs, will update to S3 version...")

            # Merge status_data and job_meta
            glb_url = status_data.get("glb_url") or status_data.get("textured_glb_url")
            thumbnail_url = status_data.get("thumbnail_url")
            model_urls = status_data.get("model_urls") or {}
            textured_model_urls = status_data.get("textured_model_urls") or {}
            textured_glb_url = status_data.get("textured_glb_url")
            rigged_glb_url = status_data.get("rigged_character_glb_url")
            rigged_fbx_url = status_data.get("rigged_character_fbx_url")
            texture_urls = status_data.get("texture_urls") or []

            # Get the name for S3 files from prompt or title
            s3_name = job_meta.get("prompt") or job_meta.get("title") or "model"

            print(f"[DB] save_finished_job: job_id={job_id}, job_type={job_type}")
            print(f"[DB] Input URLs from Meshy:")
            print(f"[DB]   glb_url: {glb_url[:80] if glb_url else 'None'}...")
            print(f"[DB]   thumbnail_url: {thumbnail_url[:80] if thumbnail_url else 'None'}...")
            print(f"[DB]   textured_glb_url: {textured_glb_url[:80] if textured_glb_url else 'None'}...")
            print(f"[DB] job_meta: title={job_meta.get('title')}, prompt={job_meta.get('prompt', '')[:50]}...")
            print(f"[DB] S3 filename will use: {s3_name[:50]}...")

            # Upload ALL URLs to S3 for permanent storage (Meshy URLs expire)
            if glb_url:
                glb_url = safe_upload_to_s3(glb_url, "model/gltf-binary", "models", s3_name, user_id=user_id)
            if thumbnail_url:
                thumbnail_url = safe_upload_to_s3(thumbnail_url, "image/png", "thumbnails", s3_name, user_id=user_id)
            if textured_glb_url:
                textured_glb_url = safe_upload_to_s3(textured_glb_url, "model/gltf-binary", "models", f"{s3_name}_textured", user_id=user_id)
            if rigged_glb_url:
                rigged_glb_url = safe_upload_to_s3(rigged_glb_url, "model/gltf-binary", "models", f"{s3_name}_rigged", user_id=user_id)
            if rigged_fbx_url:
                rigged_fbx_url = safe_upload_to_s3(rigged_fbx_url, "application/octet-stream", "models", f"{s3_name}_rigged_fbx", user_id=user_id)

            # Prefer textured output as the canonical model when available
            primary_glb_url = textured_glb_url or glb_url

            # Upload texture images to S3
            if texture_urls and isinstance(texture_urls, list):
                texture_urls = [
                    safe_upload_to_s3(url, "image/png", "textures", f"{s3_name}_tex{i}", user_id=user_id) if url else url
                    for i, url in enumerate(texture_urls)
                ]

            # Upload model format URLs to S3
            for fmt in list(model_urls.keys()):
                if model_urls[fmt]:
                    model_urls[fmt] = safe_upload_to_s3(model_urls[fmt], "application/octet-stream", "models", f"{s3_name}_{fmt}", user_id=user_id)
            for fmt in list(textured_model_urls.keys()):
                if textured_model_urls[fmt]:
                    textured_model_urls[fmt] = safe_upload_to_s3(textured_model_urls[fmt], "application/octet-stream", "models", f"{s3_name}_textured_{fmt}", user_id=user_id)

            # Determine item type
            item_type = 'model'
            if job_type in ('image', 'openai_image'):
                item_type = 'image'

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
                "model_urls": model_urls,
                "textured_model_urls": textured_model_urls,
            }

            # Log final URLs after S3 upload
            final_title = job_meta.get("title") or (job_meta.get("prompt", "")[:50] if job_meta.get("prompt") else DEFAULT_MODEL_TITLE)
            print(f"[DB] Final URLs after S3 upload:")
            print(f"[DB]   glb_url: {primary_glb_url[:80] if primary_glb_url else 'None'}...")
            print(f"[DB]   thumbnail_url: {thumbnail_url[:80] if thumbnail_url else 'None'}...")
            print(f"[DB]   title: {final_title}")
            print(f"[DB] Inserting history_items with id={history_uuid}")

            final_stage = status_data.get("stage") or job_meta.get("stage") or 'preview'
            final_prompt = job_meta.get("prompt")
            root_prompt = job_meta.get("root_prompt") or final_prompt

            model_id = None
            if primary_glb_url or model_urls or textured_model_urls or rigged_glb_url:
                provider = _map_provider(job_type)
                model_meta = {
                    "textured_glb_url": textured_glb_url,
                    "rigged_character_glb_url": rigged_glb_url,
                    "rigged_fbx_url": rigged_fbx_url,
                    "texture_urls": texture_urls,
                    "model_urls": model_urls,
                    "textured_model_urls": textured_model_urls,
                    "stage": final_stage,
                }

                if user_id:
                    cur.execute(f"""
                        SELECT id FROM {APP_SCHEMA}.models
                        WHERE provider = %s AND upstream_job_id = %s
                          AND (identity_id = %s OR identity_id IS NULL)
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (provider, job_id, user_id))
                else:
                    cur.execute(f"""
                        SELECT id FROM {APP_SCHEMA}.models
                        WHERE provider = %s AND upstream_job_id = %s
                          AND identity_id IS NULL
                        ORDER BY created_at DESC
                        LIMIT 1
                    """, (provider, job_id))
                existing_model = cur.fetchone()

                if existing_model:
                    model_id = existing_model["id"]
                    cur.execute(f"""
                        UPDATE {APP_SCHEMA}.models
                        SET identity_id = COALESCE(%s, identity_id),
                            title = COALESCE(%s, title),
                            prompt = COALESCE(%s, prompt),
                            root_prompt = COALESCE(%s, root_prompt),
                            status = 'ready',
                            glb_url = %s,
                            thumbnail_url = %s,
                            meta = %s,
                            updated_at = NOW()
                        WHERE id = %s
                    """, (
                        user_id,
                        final_title,
                        final_prompt,
                        root_prompt,
                        primary_glb_url,
                        thumbnail_url,
                        json.dumps(model_meta),
                        model_id,
                    ))
                else:
                    cur.execute(f"""
                        INSERT INTO {APP_SCHEMA}.models (
                            id, identity_id,
                            title, prompt, root_prompt,
                            provider, upstream_job_id,
                            status,
                            glb_url, thumbnail_url,
                            meta
                        ) VALUES (
                            %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s,
                            %s, %s,
                            %s
                        )
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
                        primary_glb_url,
                        thumbnail_url,
                        json.dumps(model_meta),
                    ))
                    model_id = cur.fetchone()[0]

            cur.execute(f"""
                INSERT INTO {APP_SCHEMA}.history_items (
                    id, identity_id, item_type, status, stage,
                    title, prompt, root_prompt,
                    thumbnail_url, glb_url, image_url,
                    model_id,
                    payload
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
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
                    image_url = COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.history_items.image_url),
                    model_id = COALESCE(EXCLUDED.model_id, {APP_SCHEMA}.history_items.model_id),
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
                thumbnail_url,
                primary_glb_url,
                None,
                model_id,
                json.dumps(payload),
            ))
            returned_id = cur.fetchone()[0]
            print(f"[DB] Upserted history_items with id={returned_id}")

        conn.close()
        print(f"[DB] Saved finished job {job_id} -> {history_uuid} to normalized tables")
        # Return the S3 URLs so the calling endpoint can use them in the API response
        return {
            "success": True,
            "glb_url": primary_glb_url,
            "thumbnail_url": thumbnail_url,
            "textured_glb_url": textured_glb_url,
            "rigged_character_glb_url": rigged_glb_url,
            "rigged_character_fbx_url": rigged_fbx_url,
            "texture_urls": texture_urls,
            "model_urls": model_urls,
            "textured_model_urls": textured_model_urls,
        }
    except Exception as e:
        print(f"[DB] Failed to save finished job {job_id}: {e}")
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
        return (
            container.get("glb")
            or container.get("textured_glb")
            or container.get("textured")
            or container.get("usdz")
            or container.get("obj")
        )

    for c in containers:
        if not isinstance(c, dict):
            continue
        if not model_urls and isinstance(c.get("model_urls"), dict):
            model_urls = c.get("model_urls") or {}
        if not textured_model_urls and isinstance(c.get("textured_model_urls"), dict):
            textured_model_urls = c.get("textured_model_urls") or {}
        # Some responses put outputs in a nested "output" dict
        if not model_urls and isinstance(c.get("output_model_urls"), dict):
            model_urls = c.get("output_model_urls") or {}
        if not textured_model_urls and isinstance(c.get("output_textured_model_urls"), dict):
            textured_model_urls = c.get("output_textured_model_urls") or {}
        if not textured_glb_url and c.get("textured_glb_url"):
            textured_glb_url = c.get("textured_glb_url")
        if not rigged_glb and c.get("rigged_character_glb_url"):
            rigged_glb = c.get("rigged_character_glb_url")
        if not rigged_fbx and c.get("rigged_character_fbx_url"):
            rigged_fbx = c.get("rigged_character_fbx_url")

        glb_candidates.extend([
            url for url in [
                # Prioritize textured models for texture jobs
                c.get("textured_glb_url"),
                c.get("textured_model_url"),
                pick_url(c.get("textured_model_urls") or {}),
                # Then regular models
                c.get("glb_url"),
                c.get("model_url"),
                c.get("output_model_url"),
                c.get("mesh_url"),
                c.get("mesh_download_url"),
                c.get("gltf_url"),
                c.get("gltf_download_url"),
                c.get("usdz_url"),
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
        or pick_url(textured_model_urls)
        or next((u for u in glb_candidates if u), None)
        or pick_url(model_urls)
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
    status_map = {
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
    st_raw = (_pick_first(containers, ["status", "task_status"]) or "").upper()
    status = status_map.get(st_raw, st_raw.lower() or "pending")
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
    status_map = {
        "PENDING": "pending",
        "IN_PROGRESS": "running",
        "SUCCEEDED": "done",
        "FAILED": "failed",
        "CANCELED": "failed",
        "COMPLETED": "done",
        "FINISHED": "done",
        "SUCCESS": "done",
        "CANCELLED": "failed",
        "TIMEOUT": "failed",
    }
    st_raw = (_pick_first(containers, ["status", "task_status"]) or "").upper()
    status = status_map.get(st_raw, st_raw.lower() or "pending")
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
            one = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM timrx_app.history_items;")
            count = cur.fetchone()[0]
        conn.close()
        return jsonify({"ok": True, "select_1": one, "history_items_count": count})
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

    body = request.get_json(silent=True) or {}
    log_event("text-to-3d/start:incoming", body)
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "prompt required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

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
            return jsonify({"error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 502

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
    }
    save_store(store)

    # Save to DB for recovery
    save_active_job_to_db(job_id, "text-to-3d", "preview", store[job_id], user_id)

    return jsonify({"job_id": job_id})

# ---- Refine from preview ----
@app.route("/api/text-to-3d/refine", methods=["POST", "OPTIONS"])
def api_text_to_3d_refine():
    if request.method == "OPTIONS":
        return ("", 204)

    user_id = g.user_id  # May be None for anonymous users

    body = request.get_json(silent=True) or {}
    log_event("text-to-3d/refine:incoming", body)
    preview_task_id_input = (body.get("preview_task_id") or "").strip()
    if not preview_task_id_input:
        return jsonify({"error": "preview_task_id required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

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
            return jsonify({"error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 502

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
    }
    save_store(store)

    # Save to DB for recovery
    save_active_job_to_db(job_id, "text-to-3d", "refine", store[job_id], user_id)

    return jsonify({"job_id": job_id})

# ---- (Soft) Remesh start (re-run preview with flags) ----
@app.route("/api/text-to-3d/remesh-start", methods=["POST", "OPTIONS"])
def api_text_to_3d_remesh_start():
    if request.method == "OPTIONS":
        return ("", 204)

    body = request.get_json(silent=True) or {}
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "prompt required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

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
    }
    save_store(store)
    return jsonify({"job_id": job_id})

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

    body = request.get_json(silent=True) or {}
    log_event("mesh/remesh:incoming", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"error": err}), 400

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

        return jsonify({"job_id": job_id})
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

    return jsonify(out)

# ---- Meshy Retexture ----
@app.route("/api/mesh/retexture", methods=["POST", "OPTIONS"])
def api_mesh_retexture():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    user_id = g.user_id

    body = request.get_json(silent=True) or {}
    log_event("mesh/retexture:incoming", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"error": err}), 400

    prompt = (body.get("text_style_prompt") or "").strip()
    style_img = (body.get("image_style_url") or "").strip()
    if not prompt and not style_img:
        return jsonify({"error": "text_style_prompt or image_style_url required"}), 400

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

        return jsonify({"job_id": job_id})
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

    return jsonify(out)

# ---- Meshy Rigging ----
@app.route("/api/mesh/rigging", methods=["POST", "OPTIONS"])
def api_mesh_rigging():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    user_id = g.user_id

    body = request.get_json(silent=True) or {}
    log_event("mesh/rigging:incoming", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"error": err}), 400

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

        return jsonify({"job_id": job_id})
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

    return jsonify(out)

# ---- Meshy Image to 3D ----
@app.route("/api/image-to-3d/start", methods=["POST", "OPTIONS"])
def api_image_to_3d_start():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    user_id = g.user_id

    body = request.get_json(silent=True) or {}
    log_event("image-to-3d/start:incoming", body)
    image_url = (body.get("image_url") or "").strip()
    if not image_url:
        return jsonify({"error": "image_url required"}), 400

    prompt = (body.get("prompt") or "").strip()
    s3_name = prompt if prompt else "image_to_3d_source"

    # Upload source image to S3 for permanent storage with readable name
    # safe_upload_to_s3 handles anonymous uploads (user_id=None -> "anonymous" folder)
    s3_image_url = image_url
    if AWS_BUCKET_MODELS:
        try:
            s3_image_url = safe_upload_to_s3(image_url, "image/png", "source_images", s3_name, user_id=user_id)
            print(f"[image-to-3d] Uploaded source image to S3: {s3_image_url}")
        except Exception as e:
            print(f"[image-to-3d] Failed to upload source image to S3: {e}, using original URL")

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

        return jsonify({"job_id": job_id})
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
    if urls:
        image_id = f"img_{int(time.time() * 1000)}"
        save_image_to_normalized_db(
            image_id=image_id,
            image_url=urls[0],
            prompt=prompt,
            ai_model=model,
            size=size,
            image_urls=urls,
            user_id=user_id
        )

    return jsonify({
        "image_url": urls[0] if urls else None,
        "image_urls": urls,
        "image_base64": b64_first,
        "status": "done",
        "model": model,
        "size": size,
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

        if USE_DB:
            conn = get_db_conn()
            if conn:
                try:
                    with conn:
                        with conn.cursor() as cur:
                            skipped = 0
                            saved = 0
                            for item in payload:
                                item_id = item.get("id") or item.get("job_id")
                                if not item_id:
                                    continue

                                # Check if this item already exists with S3 URLs (for this user)
                                if user_id:
                                    cur.execute(f"""
                                        SELECT id, thumbnail_url, glb_url, image_url FROM {APP_SCHEMA}.history_items
                                        WHERE (id::text = %s
                                           OR payload->>'original_job_id' = %s
                                           OR payload->>'original_id' = %s
                                           OR payload->>'job_id' = %s)
                                          AND identity_id = %s
                                        LIMIT 1
                                    """, (str(item_id), str(item_id), str(item_id), str(item_id), user_id))
                                else:
                                    cur.execute(f"""
                                        SELECT id, thumbnail_url, glb_url, image_url FROM {APP_SCHEMA}.history_items
                                        WHERE (id::text = %s
                                           OR payload->>'original_job_id' = %s
                                           OR payload->>'original_id' = %s
                                           OR payload->>'job_id' = %s)
                                          AND identity_id IS NULL
                                        LIMIT 1
                                    """, (str(item_id), str(item_id), str(item_id), str(item_id)))
                                existing = cur.fetchone()

                                if existing:
                                    _, existing_thumb, existing_glb, existing_img = existing
                                    has_s3 = any(
                                        url and 's3.' in url and 'amazonaws.com' in url
                                        for url in [existing_thumb, existing_glb, existing_img]
                                        if url
                                    )
                                    if has_s3:
                                        skipped += 1
                                        continue  # Skip - already saved with S3 URLs

                                # Extract fields for the schema
                                item_type = item.get("type") or item.get("item_type") or "model"
                                status = item.get("status") or "pending"
                                stage = item.get("stage")
                                title = item.get("title")
                                prompt = item.get("prompt")
                                thumbnail_url = item.get("thumbnail_url")
                                glb_url = item.get("glb_url")
                                image_url = item.get("image_url")

                                # Check if item has valid UUID id
                                try:
                                    # Validate as UUID
                                    uuid.UUID(str(item_id))
                                    use_id = str(item_id)
                                except (ValueError, TypeError, AttributeError):
                                    # Generate new UUID, store original id in payload
                                    use_id = str(uuid.uuid4())
                                    item["original_id"] = item_id

                                # Preserve S3 URLs - don't overwrite with Meshy URLs
                                cur.execute(
                                    f"""INSERT INTO {APP_SCHEMA}.history_items (id, identity_id, item_type, status, stage, title, prompt,
                                           thumbnail_url, glb_url, image_url, payload)
                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                       ON CONFLICT (id) DO UPDATE
                                       SET item_type = EXCLUDED.item_type,
                                           status = EXCLUDED.status,
                                           stage = EXCLUDED.stage,
                                           title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.history_items.title),
                                           prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.history_items.prompt),
                                           identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.history_items.identity_id),
                                           -- Keep S3 URLs if they exist, otherwise use new value
                                           thumbnail_url = CASE
                                               WHEN {APP_SCHEMA}.history_items.thumbnail_url LIKE '%%s3.%%amazonaws.com%%' THEN {APP_SCHEMA}.history_items.thumbnail_url
                                               ELSE COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.history_items.thumbnail_url)
                                           END,
                                           glb_url = CASE
                                               WHEN {APP_SCHEMA}.history_items.glb_url LIKE '%%s3.%%amazonaws.com%%' THEN {APP_SCHEMA}.history_items.glb_url
                                               ELSE COALESCE(EXCLUDED.glb_url, {APP_SCHEMA}.history_items.glb_url)
                                           END,
                                           image_url = CASE
                                               WHEN {APP_SCHEMA}.history_items.image_url LIKE '%%s3.%%amazonaws.com%%' THEN {APP_SCHEMA}.history_items.image_url
                                               ELSE COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.history_items.image_url)
                                           END,
                                           payload = EXCLUDED.payload,
                                           updated_at = NOW();""",
                                    (use_id, user_id, item_type, status, stage, title, prompt,
                                     thumbnail_url, glb_url, image_url, json.dumps(item))
                                )
                                saved += 1
                    conn.close()
                    print(f"[History] Saved {saved} items to DB, skipped {skipped} with S3 URLs")
                except Exception as e:
                    print(f"[History] DB write failed: {e}")
                    import traceback
                    traceback.print_exc()
                    try:
                        conn.close()
                    except Exception:
                        pass

        # DEV ONLY: sync to local JSON (no-op in production)
        save_history_store(payload)
        return jsonify({"ok": True, "count": len(payload)})
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

        if USE_DB:
            conn = get_db_conn()
            if conn:
                try:
                    with conn:
                        with conn.cursor() as cur:
                            # Check if this item already exists with S3 URLs (for this user)
                            if user_id:
                                cur.execute(f"""
                                    SELECT id, thumbnail_url, glb_url, image_url FROM {APP_SCHEMA}.history_items
                                    WHERE (id::text = %s
                                       OR payload->>'original_job_id' = %s
                                       OR payload->>'original_id' = %s
                                       OR payload->>'job_id' = %s)
                                      AND identity_id = %s
                                    LIMIT 1
                                """, (str(item_id), str(item_id), str(item_id), str(item_id), user_id))
                            else:
                                cur.execute(f"""
                                    SELECT id, thumbnail_url, glb_url, image_url FROM {APP_SCHEMA}.history_items
                                    WHERE (id::text = %s
                                       OR payload->>'original_job_id' = %s
                                       OR payload->>'original_id' = %s
                                       OR payload->>'job_id' = %s)
                                      AND identity_id IS NULL
                                    LIMIT 1
                                """, (str(item_id), str(item_id), str(item_id), str(item_id)))
                            existing = cur.fetchone()

                            if existing:
                                # Check if existing entry has S3 URLs - if so, skip to avoid overwriting
                                _, existing_thumb, existing_glb, existing_img = existing
                                has_s3 = any(
                                    url and 's3.' in url and 'amazonaws.com' in url
                                    for url in [existing_thumb, existing_glb, existing_img]
                                    if url
                                )
                                if has_s3:
                                    print(f"[History] Item {item_id} already exists with S3 URLs for user {user_id}, skipping")
                                    conn.close()
                                    return jsonify({"ok": True, "id": item_id, "skipped": True, "reason": "already_saved_with_s3"})

                            # Extract fields for the schema
                            item_type = item.get("type") or item.get("item_type") or "model"
                            status = item.get("status") or "pending"
                            stage = item.get("stage")
                            title = item.get("title")
                            prompt = item.get("prompt")
                            thumbnail_url = item.get("thumbnail_url")
                            glb_url = item.get("glb_url")
                            image_url = item.get("image_url")

                            # Check if item has valid UUID id
                            try:
                                uuid.UUID(str(item_id))
                                use_id = str(item_id)
                            except (ValueError, TypeError, AttributeError):
                                use_id = str(uuid.uuid4())
                                item["original_id"] = item_id

                            # Preserve S3 URLs - don't overwrite with Meshy URLs
                            cur.execute(
                                f"""INSERT INTO {APP_SCHEMA}.history_items (id, identity_id, item_type, status, stage, title, prompt,
                                       thumbnail_url, glb_url, image_url, payload)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                   ON CONFLICT (id) DO UPDATE
                                   SET item_type = EXCLUDED.item_type,
                                       status = EXCLUDED.status,
                                       stage = EXCLUDED.stage,
                                       title = COALESCE(EXCLUDED.title, {APP_SCHEMA}.history_items.title),
                                       prompt = COALESCE(EXCLUDED.prompt, {APP_SCHEMA}.history_items.prompt),
                                       identity_id = COALESCE(EXCLUDED.identity_id, {APP_SCHEMA}.history_items.identity_id),
                                       -- Keep S3 URLs if they exist, otherwise use new value
                                       thumbnail_url = CASE
                                           WHEN {APP_SCHEMA}.history_items.thumbnail_url LIKE '%%s3.%%amazonaws.com%%' THEN {APP_SCHEMA}.history_items.thumbnail_url
                                           ELSE COALESCE(EXCLUDED.thumbnail_url, {APP_SCHEMA}.history_items.thumbnail_url)
                                       END,
                                       glb_url = CASE
                                           WHEN {APP_SCHEMA}.history_items.glb_url LIKE '%%s3.%%amazonaws.com%%' THEN {APP_SCHEMA}.history_items.glb_url
                                           ELSE COALESCE(EXCLUDED.glb_url, {APP_SCHEMA}.history_items.glb_url)
                                       END,
                                       image_url = CASE
                                           WHEN {APP_SCHEMA}.history_items.image_url LIKE '%%s3.%%amazonaws.com%%' THEN {APP_SCHEMA}.history_items.image_url
                                           ELSE COALESCE(EXCLUDED.image_url, {APP_SCHEMA}.history_items.image_url)
                                       END,
                                       payload = EXCLUDED.payload,
                                       updated_at = NOW();""",
                                (use_id, user_id, item_type, status, stage, title, prompt,
                                 thumbnail_url, glb_url, image_url, json.dumps(item))
                            )
                            db_ok = True
                            item_id = use_id  # Return the actual UUID used
                    conn.close()
                except Exception as e:
                    print(f"[History] DB add item failed: {e}")
                    try: conn.close()
                    except Exception: pass

        local_ok = upsert_history_local(item, merge=False)
        return jsonify({"ok": True, "id": item_id, "db": db_ok, "local": local_ok})
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
        if USE_DB:
            conn = get_db_conn()
            if conn:
                try:
                    with conn:
                        with conn.cursor() as cur:
                            # Delete by UUID id (only if user owns it or user_id is NULL)
                            if user_id:
                                cur.execute(f"""DELETE FROM {APP_SCHEMA}.history_items
                                               WHERE id::text = %s AND (identity_id = %s OR identity_id IS NULL);""",
                                            (str(item_id), user_id))
                                cur.execute(f"""DELETE FROM {APP_SCHEMA}.history_items
                                               WHERE (payload->>'original_id' = %s OR payload->>'job_id' = %s)
                                                 AND (identity_id = %s OR identity_id IS NULL);""",
                                            (str(item_id), str(item_id), user_id))
                            else:
                                # Anonymous user can only delete items without user_id
                                cur.execute(f"""DELETE FROM {APP_SCHEMA}.history_items
                                               WHERE id::text = %s AND identity_id IS NULL;""", (str(item_id),))
                                cur.execute(f"""DELETE FROM {APP_SCHEMA}.history_items
                                               WHERE (payload->>'original_id' = %s OR payload->>'job_id' = %s)
                                                 AND identity_id IS NULL;""", (str(item_id), str(item_id)))
                    conn.close()
                    db_ok = True
                except Exception as e:
                    print(f"[History] DB delete failed for {item_id}: {e}")
                    try: conn.close()
                    except Exception: pass
        local_ok = delete_history_local(item_id)
        return jsonify({"ok": True, "deleted": item_id, "db": db_ok, "local": local_ok})

    if request.method == "PATCH":
        try:
            updates = request.get_json(silent=True) or {}
            db_ok = False
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
                                prompt = updates.get("prompt")
                                thumbnail_url = updates.get("thumbnail_url")
                                glb_url = updates.get("glb_url")
                                image_url = updates.get("image_url")

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
                        print(f"[History] DB update failed for {item_id}: {e}")
                        try: conn.close()
                        except Exception: pass

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

            return jsonify({"ok": True, "id": item_id, "db": db_ok, "local": local_ok})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

# Entrypoint
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # For Render, use: gunicorn 3dprint-backend:app
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")))
