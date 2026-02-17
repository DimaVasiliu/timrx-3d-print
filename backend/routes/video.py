"""
Video Generation Routes Blueprint.
----------------------------------
Registered under /api/_mod and /api for compatibility.

Veo Endpoints (Google Vertex/AI Studio with fallback):
- POST /video/generate   - Unified start (text2video or image2video) — legacy
- POST /video/text        - Text → short cinematic clip
- POST /video/animate     - Image → animated video clip
- GET  /video/status/<job_id>          - Poll job status (canonical)
- GET  /video/generate/status/<job_id> - Poll job status (legacy alias)

Veo 3.1 Constraints:
- aspectRatio: ONLY "16:9" or "9:16" (NO "1:1" for video)
- resolution: "720p", "1080p", "4k" (lowercase 4k)
- durationSeconds: 4, 6, 8 (integers, NOT strings!)
- CRITICAL: 1080p/4k requires durationSeconds=8
"""

from __future__ import annotations

import uuid
from flask import Blueprint, jsonify, request

from backend.db import USE_DB, get_conn, Tables
from backend.middleware import with_session
from backend.services.async_dispatch import get_executor
from backend.services.credits_helper import start_paid_job
from backend.services.expense_guard import ExpenseGuard
from backend.services.identity_service import require_identity
from backend.services.job_service import create_internal_job_row, load_store, save_store
from backend.services.gemini_video_service import (
    validate_video_params,
    GeminiValidationError,
)
from backend.services.vertex_video_service import check_vertex_resolution
from backend.services.video_router import video_router, resolve_video_provider
from backend.services.video_prompts import (
    normalize_text_prompt,
    normalize_motion_prompt,
    get_style_presets,
    get_motion_presets,
)
from backend.services.pricing_service import (
    get_video_action_code,
    get_video_credit_cost,
)
from backend.utils.helpers import now_s, log_event

bp = Blueprint("video", __name__)


# Map UI duration values to Veo allowed values (integers!)
DURATION_MAP = {
    4: 4, 5: 4, 6: 6, 7: 6, 8: 8, 10: 8,
    "4": 4, "5": 4, "6": 6, "7": 6, "8": 8, "10": 8,
}


@bp.route("/video/generate", methods=["POST", "OPTIONS"])
@with_session
def generate_video():
    """
    Start a video generation job using Gemini Veo 3.1.

    Request body:
    {
        "provider": "google",           # Provider (only google supported)
        "task": "text2video",           # "text2video" or "image2video"
        "prompt": "A serene forest...", # Required for text2video
        "image_data": "base64...",      # Required for image2video
        "duration_sec": 6,              # Duration: 4, 6, or 8 seconds
        "aspect_ratio": "16:9",         # "16:9" or "9:16" (NO "1:1")
        "resolution": "720p",           # "720p", "1080p", "4k"
        "motion": "Camera slowly...",   # Motion description (for image2video)
        "negative_prompt": "...",       # Optional: what to avoid
        "seed": 12345                   # Optional: random seed
    }

    CRITICAL CONSTRAINTS:
    - Video aspect_ratio must be "16:9" or "9:16" (1:1 not supported)
    - Resolution "1080p" or "4k" REQUIRES duration of 8 seconds
    - For shorter videos (4s, 6s), use resolution "720p"

    Response (success):
    {
        "ok": true,
        "job_id": "uuid",
        "status": "queued",
        "new_balance": 100
    }

    Response (error):
    {
        "error": "<machine_code>",
        "message": "<human readable>",
        "details": {...}
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    # Fail-fast: Check if any video provider is configured (router handles fallback)
    available_providers = video_router.get_available_providers()
    if not available_providers:
        return jsonify({
            "error": "video_not_configured",
            "message": "No video generation providers are configured",
            "details": {"hint": "Set GEMINI_API_KEY environment variable"}
        }), 500

    # Require authentication
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    # Parse request body
    body = request.get_json(silent=True) or {}

    provider = (body.get("provider") or "video").lower()
    task = (body.get("task") or "text2video").lower()

    # Validate task type
    if task not in ("text2video", "image2video"):
        return jsonify({
            "error": "invalid_params",
            "message": "task must be 'text2video' or 'image2video'",
            "field": "task",
            "allowed": ["text2video", "image2video"]
        }), 400

    # Get parameters from request
    raw_duration = body.get("duration_sec") or body.get("durationSeconds") or 6
    aspect_ratio = body.get("aspect_ratio") or body.get("aspectRatio") or "16:9"
    resolution = body.get("resolution") or "720p"
    motion = (body.get("motion") or "").strip()
    negative_prompt = (body.get("negative_prompt") or body.get("negativePrompt") or "").strip()
    seed = body.get("seed")

    # Map UI duration to Veo allowed values (must be integer!)
    duration_seconds = DURATION_MAP.get(raw_duration, 6)

    # Normalize resolution (4K -> 4k)
    if resolution == "4K":
        resolution = "4k"

    # Validate video parameters STRICTLY - reject invalid combinations
    # Valid combinations:
    # - 720p: 4s, 6s, 8s
    # - 1080p: 8s only
    # - 4k: 8s only
    try:
        aspect_ratio, resolution, duration_seconds = validate_video_params(
            aspect_ratio, resolution, duration_seconds
        )
    except GeminiValidationError as e:
        # Return 400 with clear error message for invalid combinations
        print(f"[VIDEO] Validation failed: {e.message}")
        return jsonify({
            "ok": False,
            "error": "invalid_params",
            "message": e.message,
            "field": e.field,
            "value": e.value,
            "allowed": e.allowed,
        }), 400

    # Task-specific validation
    if task == "text2video":
        prompt = (body.get("prompt") or "").strip()
        if not prompt:
            return jsonify({
                "error": "invalid_params",
                "message": "prompt is required for text2video",
                "field": "prompt"
            }), 400
        image_data = None
    else:  # image2video
        image_data = body.get("image_data") or body.get("image") or ""
        if not image_data:
            return jsonify({
                "error": "invalid_params",
                "message": "image_data is required for image2video",
                "field": "image_data"
            }), 400
        prompt = motion or "Animate this image with natural, smooth motion"

    # Stability guardrails: check API limits and concurrent jobs
    guard_error = ExpenseGuard.check_video_request(duration_seconds=duration_seconds)
    if guard_error:
        return guard_error

    # Idempotency check: return cached response if duplicate
    idempotency_key = ExpenseGuard.compute_idempotency_key(
        identity_id or "", task, prompt,
        aspect_ratio=aspect_ratio, resolution=resolution, duration_seconds=duration_seconds
    )
    cached = ExpenseGuard.is_duplicate_request(idempotency_key)
    if cached:
        return jsonify(cached)

    # Generate internal job ID
    internal_job_id = str(uuid.uuid4())

    # Determine action key for credits - use variant code based on duration/resolution
    # Format: video_text_generate_4s_720p or video_image_animate_8s_4k (lowercase canonical)
    action_key = get_video_action_code(task, duration_seconds, resolution)
    expected_cost = get_video_credit_cost(duration_seconds, resolution)

    # PRE-FLIGHT: Check Vertex resolution capability BEFORE reserving credits
    # This prevents reserve -> fail -> release cycle for 4k on non-allowlisted projects
    primary_provider = available_providers[0].name if available_providers else None
    if primary_provider == "vertex":
        resolution_ok, resolution_err = check_vertex_resolution(resolution)
        if not resolution_ok:
            print(f"[VIDEO] Vertex resolution check failed: {resolution_err}")
            return jsonify({
                "ok": False,
                "error": "vertex_resolution_not_allowed",
                "message": resolution_err,
                "field": "resolution",
                "value": resolution,
                "allowed": ["720p", "1080p"],
            }), 400

    print(f"[VIDEO] Reserving credits: action_code={action_key} cost={expected_cost} duration={duration_seconds}s resolution={resolution}")

    # Reserve credits
    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {
            "task": task,
            "provider": provider,
            "prompt": prompt[:100] if prompt else None,
            "duration_seconds": duration_seconds,
            "resolution": resolution,
            "expected_cost": expected_cost,
        },
    )
    if credit_error:
        return credit_error

    # Build store metadata
    store_meta = {
        "stage": "video",
        "created_at": now_s() * 1000,
        "task": task,
        "provider": provider,
        "prompt": prompt,
        "duration_seconds": duration_seconds,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "motion": motion,
        "negative_prompt": negative_prompt,
        "seed": seed,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
    }

    # Persist to job store immediately so status polling works
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    # Create job row in database
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider=provider,
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    # Build payload for async dispatch
    payload = {
        "task": task,
        "prompt": prompt,
        "image_data": image_data,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "duration_seconds": duration_seconds,
        "motion": motion,
        "negative_prompt": negative_prompt,
        "seed": seed,
    }

    # Import here to avoid circular imports
    from backend.services.async_dispatch import dispatch_gemini_video_async

    # Register active job for concurrent limit tracking
    ExpenseGuard.register_active_job(internal_job_id)

    # Dispatch async task
    get_executor().submit(
        dispatch_gemini_video_async,
        internal_job_id,
        identity_id,
        reservation_id,
        payload,
        store_meta,
    )

    log_event("video/generate:dispatched", {"internal_job_id": internal_job_id, "task": task})

    # D1: Return fast — skip balance query (frontend caches wallet separately)
    response_data = {
        "ok": True,
        "job_id": internal_job_id,
        "video_id": internal_job_id,
        "reservation_id": reservation_id,
        "status": "queued",
        "task": task,
        "provider": provider,
        "params": {
            "aspect_ratio": aspect_ratio,
            "resolution": resolution,
            "duration_seconds": duration_seconds,
        },
        "source": "modular",
    }

    # Cache response for idempotency
    ExpenseGuard.cache_response(idempotency_key, response_data)

    return jsonify(response_data)


def _dispatch_video_job(
    identity_id: str,
    task: str,
    prompt: str,
    image_data: str | None,
    aspect_ratio: str,
    resolution: str,
    duration_seconds: int,
    motion: str,
    negative_prompt: str,
    seed: int | None,
    style_preset: str | None = None,
    motion_preset: str | None = None,
):
    """
    Shared helper: validate, reserve credits, create job row, dispatch async, return response.

    Returns a Flask response tuple.
    """
    # Validate video parameters STRICTLY - reject invalid combinations
    # Valid combinations:
    # - 720p: 4s, 6s, 8s
    # - 1080p: 8s only
    # - 4k: 8s only
    try:
        aspect_ratio, resolution, duration_seconds = validate_video_params(
            aspect_ratio, resolution, duration_seconds
        )
    except GeminiValidationError as e:
        # Return 400 with clear error message for invalid combinations
        print(f"[VIDEO] Validation failed: {e.message}")
        return jsonify({
            "ok": False,
            "error": "invalid_params",
            "message": e.message,
            "field": e.field,
            "value": e.value,
            "allowed": e.allowed,
        }), 400

    internal_job_id = str(uuid.uuid4())

    # Use variant action code based on duration/resolution
    action_key = get_video_action_code(task, duration_seconds, resolution)
    expected_cost = get_video_credit_cost(duration_seconds, resolution)

    # PRE-FLIGHT: Check Vertex resolution capability BEFORE reserving credits
    # This prevents reserve -> fail -> release cycle for 4k on non-allowlisted projects
    available_providers = video_router.get_available_providers()
    primary_provider = available_providers[0].name if available_providers else None

    if primary_provider == "vertex":
        resolution_ok, resolution_err = check_vertex_resolution(resolution)
        if not resolution_ok:
            print(f"[VIDEO] Vertex resolution check failed: {resolution_err}")
            return jsonify({
                "ok": False,
                "error": "vertex_resolution_not_allowed",
                "message": resolution_err,
                "field": "resolution",
                "value": resolution,
                "allowed": ["720p", "1080p"],
            }), 400

    print(f"[VIDEO] Reserving credits: action_code={action_key} cost={expected_cost} duration={duration_seconds}s resolution={resolution}")

    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {
            "task": task,
            "prompt": prompt[:100] if prompt else None,
            "duration_seconds": duration_seconds,
            "resolution": resolution,
            "style_preset": style_preset,
            "motion_preset": motion_preset,
            "expected_cost": expected_cost,
        },
    )
    if credit_error:
        return credit_error

    store_meta = {
        "stage": "video",
        "created_at": now_s() * 1000,
        "task": task,
        "prompt": prompt,
        "duration_seconds": duration_seconds,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "motion": motion,
        "negative_prompt": negative_prompt,
        "seed": seed,
        "style_preset": style_preset,
        "motion_preset": motion_preset,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
    }

    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="video",  # Generic — actual provider determined by router during dispatch
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    payload = {
        "task": task,
        "prompt": prompt,
        "image_data": image_data,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "duration_seconds": duration_seconds,
        "motion": motion,
        "negative_prompt": negative_prompt,
        "seed": seed,
    }

    from backend.services.async_dispatch import dispatch_gemini_video_async

    get_executor().submit(
        dispatch_gemini_video_async,
        internal_job_id,
        identity_id,
        reservation_id,
        payload,
        store_meta,
    )

    log_event(f"video/{task}:dispatched", {"internal_job_id": internal_job_id})

    # D1: Return fast — skip balance query (frontend caches wallet separately)
    return jsonify({
        "ok": True,
        "job_id": internal_job_id,
        "video_id": internal_job_id,
        "reservation_id": reservation_id,
        "status": "queued",
        "task": task,
        "params": {
            "aspect_ratio": aspect_ratio,
            "resolution": resolution,
            "duration_seconds": duration_seconds,
        },
    })


# ── POST /video/text — Text → short cinematic clip ───────────
@bp.route("/video/text", methods=["POST", "OPTIONS"])
@with_session
def video_text():
    """
    Generate a short cinematic video clip from a text prompt.

    Request body:
    {
        "prompt": "A serene forest at sunset, cinematic slow motion",
        "seconds": 6,                   # 4, 6, or 8
        "aspect_ratio": "16:9",         # "16:9" or "9:16"
        "style_preset": "cinematic"     # optional style hint
    }

    Returns immediately: { ok:true, job_id, reservation_id }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    available_providers = video_router.get_available_providers()
    if not available_providers:
        return jsonify({
            "error": "video_not_configured",
            "message": "No video generation providers are configured",
        }), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}

    raw_prompt = (body.get("prompt") or "").strip()
    if not raw_prompt:
        return jsonify({"error": "invalid_params", "message": "prompt is required", "field": "prompt"}), 400

    raw_duration = body.get("seconds") or body.get("duration_sec") or 6
    duration_seconds = DURATION_MAP.get(raw_duration, 6)
    aspect_ratio = body.get("aspect_ratio") or "16:9"
    resolution = body.get("resolution") or "720p"
    if resolution == "4K":
        resolution = "4k"
    negative_prompt = (body.get("negative_prompt") or "").strip()
    seed = body.get("seed")
    style_preset = body.get("style_preset")

    # C1: Normalize prompt with cinematic style instructions
    prompt = normalize_text_prompt(raw_prompt, style_preset, duration_seconds)

    return _dispatch_video_job(
        identity_id=identity_id,
        task="text2video",
        prompt=prompt,
        image_data=None,
        aspect_ratio=aspect_ratio,
        resolution=resolution,
        duration_seconds=duration_seconds,
        motion="",
        negative_prompt=negative_prompt,
        seed=seed,
        style_preset=style_preset,
    )


# ── POST /video/animate — Image → animated video clip ────────
@bp.route("/video/animate", methods=["POST", "OPTIONS"])
@with_session
def video_animate():
    """
    Animate a single image into a short video clip.

    Request body:
    {
        "image_url": "https://...",     # OR image_id OR image_data (base64)
        "prompt": "Gentle zoom out...", # optional motion description
        "seconds": 6,                   # 4, 6, or 8
        "motion_preset": "zoom_out"     # optional motion hint
    }

    Returns immediately: { ok:true, job_id, reservation_id }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    available_providers = video_router.get_available_providers()
    if not available_providers:
        return jsonify({
            "error": "video_not_configured",
            "message": "No video generation providers are configured",
        }), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}

    # C2: Accept image_data, image_url, image_id (DB lookup), or raw image
    image_data = body.get("image_data") or body.get("image_url") or body.get("image") or ""
    image_id = body.get("image_id")

    # If image_id provided, fetch the URL from our images table
    if not image_data and image_id:
        image_data = _resolve_image_id(image_id)

    if not image_data:
        return jsonify({"error": "invalid_params", "message": "image_data, image_url, or image_id is required", "field": "image_data"}), 400

    raw_user_prompt = (body.get("prompt") or body.get("motion") or "").strip()
    raw_duration = body.get("seconds") or body.get("duration_sec") or 6
    duration_seconds = DURATION_MAP.get(raw_duration, 6)
    aspect_ratio = body.get("aspect_ratio") or "16:9"
    resolution = body.get("resolution") or "720p"
    if resolution == "4K":
        resolution = "4k"
    negative_prompt = (body.get("negative_prompt") or "").strip()
    seed = body.get("seed")
    motion_preset = body.get("motion_preset")

    # C2: Normalize motion prompt with preset
    prompt = normalize_motion_prompt(raw_user_prompt, motion_preset)

    return _dispatch_video_job(
        identity_id=identity_id,
        task="image2video",
        prompt=prompt,
        image_data=image_data,
        aspect_ratio=aspect_ratio,
        resolution=resolution,
        duration_seconds=duration_seconds,
        motion=prompt,
        negative_prompt=negative_prompt,
        seed=seed,
        motion_preset=motion_preset,
    )


# ── Helper: resolve image_id → image_url from DB ─────────────
def _resolve_image_id(image_id: str) -> str | None:
    """Look up an image URL from our images table by image_id."""
    if not USE_DB or not image_id:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT image_url FROM {Tables.IMAGES}
                    WHERE id::text = %s AND deleted_at IS NULL
                    LIMIT 1
                    """,
                    (image_id,),
                )
                row = cur.fetchone()
        if row and row.get("image_url"):
            print(f"[VIDEO] Resolved image_id={image_id} → {row['image_url'][:80]}...")
            return row["image_url"]
    except Exception as e:
        print(f"[VIDEO] Error resolving image_id={image_id}: {e}")
    return None


# ── GET /video/presets — Available style & motion presets ─────
@bp.route("/video/presets", methods=["GET", "OPTIONS"])
def video_presets():
    """Return available style and motion presets for the frontend."""
    if request.method == "OPTIONS":
        return ("", 204)
    return jsonify({
        "ok": True,
        "style_presets": get_style_presets(),
        "motion_presets": get_motion_presets(),
    })


# ── GET /video/status/<job_id> — Canonical status endpoint ───
@bp.route("/video/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def video_status_canonical(job_id: str):
    """Canonical status endpoint (delegates to shared handler)."""
    return _video_status_handler(job_id)


# ── GET /video/generate/status/<job_id> — Legacy alias ───────
@bp.route("/video/generate/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def video_status(job_id: str):
    """Legacy status endpoint (delegates to shared handler)."""
    return _video_status_handler(job_id)


# ── Shared status handler ────────────────────────────────────
def _video_status_handler(job_id: str):
    """
    Get the status of a video generation job.

    Returns:
        queued      — waiting to start
        processing  — generating (with progress %)
        done        — ready with video_url, thumbnail_url
        failed      — error with message
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    # Try job store first
    store = load_store()
    meta = store.get(job_id) or {}

    # Check database for job status
    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, status, error_message, meta
                        FROM {Tables.JOBS}
                        WHERE id::text = %s AND identity_id = %s
                        LIMIT 1
                        """,
                        (job_id, identity_id),
                    )
                    job = cur.fetchone()

            if job:
                job_meta = job.get("meta") or {}
                if isinstance(job_meta, str):
                    try:
                        job_meta = __import__('json').loads(job_meta)
                    except Exception:
                        job_meta = {}

                if job["status"] == "queued":
                    return jsonify({
                        "ok": True,
                        "status": "queued",
                        "job_id": job_id,
                        "message": "Video generation queued...",
                        "progress": 0,
                    })

                if job["status"] == "quota_queued":
                    return jsonify({
                        "ok": True,
                        "status": "queued",
                        "job_id": job_id,
                        "message": "Waiting for provider quota to reset — your video is queued and will be processed automatically.",
                        "progress": 0,
                        "quota_queued": True,
                    })

                if job["status"] == "processing":
                    progress = meta.get("progress") or job_meta.get("progress") or 0
                    return jsonify({
                        "ok": True,
                        "status": "processing",
                        "job_id": job_id,
                        "message": "Generating video...",
                        "progress": progress,
                    })

                if job["status"] == "failed":
                    error_msg = job.get("error_message", "Video generation failed")
                    error_code = "video_failed"
                    # Parse error code from "code: message" format
                    if error_msg and ":" in error_msg:
                        parts = error_msg.split(":", 1)
                        candidate = parts[0].strip()
                        # Accept any machine-readable code (snake_case, no spaces)
                        if candidate and " " not in candidate:
                            error_code = candidate
                            error_msg = parts[1].strip()

                    # Surface user-friendly message for filtered errors
                    user_message = job_meta.get("user_message") if error_code == "provider_filtered_third_party" else None

                    resp = {
                        "ok": False,
                        "status": "failed",
                        "job_id": job_id,
                        "error": error_code,
                        "message": user_message or error_msg,
                    }
                    if user_message:
                        resp["user_message"] = user_message
                    return jsonify(resp)

                if job["status"] == "ready":
                    video_url = meta.get("video_url") or job_meta.get("video_url")
                    thumbnail_url = meta.get("thumbnail_url") or job_meta.get("thumbnail_url")
                    # Include new_balance so frontend can update credits display
                    new_balance = meta.get("new_balance") or job_meta.get("new_balance")

                    response_data = {
                        "ok": True,
                        "status": "done",
                        "job_id": job_id,
                        "video_id": job_id,
                        "video_url": video_url,
                        "thumbnail_url": thumbnail_url,
                        "duration_seconds": meta.get("duration_seconds") or job_meta.get("duration_seconds"),
                        "resolution": meta.get("resolution") or job_meta.get("resolution"),
                        "provider": meta.get("provider") or job_meta.get("provider") or "google",
                    }
                    # Only include new_balance if present (backwards compatible)
                    if new_balance is not None:
                        response_data["new_balance"] = new_balance
                    return jsonify(response_data)

        except Exception as e:
            print(f"[VIDEO STATUS] Error checking job {job_id}: {e}")

    # Fallback to job store
    if meta.get("status") == "done":
        response_data = {
            "ok": True,
            "status": "done",
            "job_id": job_id,
            "video_id": job_id,
            "video_url": meta.get("video_url"),
            "thumbnail_url": meta.get("thumbnail_url"),
            "duration_seconds": meta.get("duration_seconds"),
            "resolution": meta.get("resolution"),
            "provider": meta.get("provider", "google"),
        }
        # Include new_balance if present (for frontend credit display)
        new_balance = meta.get("new_balance")
        if new_balance is not None:
            response_data["new_balance"] = new_balance
        return jsonify(response_data)

    if meta.get("status") == "failed":
        error_msg = meta.get("error", "Video generation failed")
        error_code = meta.get("error_code", "gemini_video_failed")
        resp = {
            "ok": False,
            "status": "failed",
            "job_id": job_id,
            "error": error_code,
            "message": error_msg,
        }
        if error_code == "provider_filtered_third_party":
            resp["user_message"] = error_msg
        return jsonify(resp)

    if meta.get("status") == "quota_queued":
        return jsonify({
            "ok": True,
            "status": "queued",
            "job_id": job_id,
            "progress": 0,
            "message": "Waiting for provider quota to reset — your video is queued and will be processed automatically.",
            "quota_queued": True,
        })

    if meta.get("status") in ("queued", "processing"):
        return jsonify({
            "ok": True,
            "status": meta.get("status"),
            "job_id": job_id,
            "progress": meta.get("progress", 0),
            "message": "Generating video...",
        })

    # Store has an entry but with an unexpected status — treat as in-flight
    if meta:
        return jsonify({
            "ok": True,
            "status": "processing",
            "job_id": job_id,
            "progress": meta.get("progress", 0),
            "message": "Generating video...",
        })

    # Last resort: check DB without identity constraint (handles edge cases)
    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, status, error_message, meta
                        FROM {Tables.JOBS}
                        WHERE id::text = %s
                        LIMIT 1
                        """,
                        (job_id,),
                    )
                    job = cur.fetchone()
            if job:
                status = job["status"]
                if status == "ready":
                    jm = job.get("meta") or {}
                    if isinstance(jm, str):
                        try:
                            jm = __import__('json').loads(jm)
                        except Exception:
                            jm = {}
                    response_data = {
                        "ok": True,
                        "status": "done",
                        "job_id": job_id,
                        "video_id": job_id,
                        "video_url": jm.get("video_url"),
                        "thumbnail_url": jm.get("thumbnail_url"),
                    }
                    # Include new_balance if present
                    new_balance = jm.get("new_balance")
                    if new_balance is not None:
                        response_data["new_balance"] = new_balance
                    return jsonify(response_data)
                if status == "failed":
                    return jsonify({
                        "ok": False,
                        "status": "failed",
                        "job_id": job_id,
                        "error": "video_failed",
                        "message": job.get("error_message") or "Video generation failed",
                    })
                # queued / processing / other — return in-flight
                return jsonify({
                    "ok": True,
                    "status": status if status in ("queued", "processing") else "processing",
                    "job_id": job_id,
                    "progress": 0,
                    "message": "Generating video...",
                })
        except Exception as e:
            print(f"[VIDEO STATUS] Fallback DB check error for {job_id}: {e}")

    return jsonify({
        "error": "job_not_found",
        "message": f"No video job found with ID: {job_id}"
    }), 404


# ── POST /video/admin/process-queue — Admin: trigger queued jobs ──
@bp.route("/video/admin/process-queue", methods=["POST", "OPTIONS"])
@with_session
def video_admin_process_queue():
    """
    Admin trigger: immediately attempt to process quota-queued video jobs.

    Returns:
    {
        "ok": true,
        "dispatched": 3,
        "queue_remaining": 1
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    from backend.services.video_queue import video_queue

    dispatched = video_queue.process_queue_now()

    return jsonify({
        "ok": True,
        "dispatched": dispatched,
        "queue_remaining": video_queue.size,
    })


# ── GET /video/admin/provider-info — Show current video provider config ──
@bp.route("/video/admin/provider-info", methods=["GET", "OPTIONS"])
@with_session
def video_admin_provider_info():
    """
    Admin endpoint: Show current video provider configuration.

    Returns:
    {
        "ok": true,
        "video_provider": "vertex",
        "video_quality": "fast",
        "vertex_model": "veo-3.1-fast-generate-001",
        "providers": [
            {"name": "vertex", "configured": true, "primary": true},
            {"name": "google", "configured": true, "primary": false}
        ]
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    from backend.config import config

    providers_info = []
    available = video_router.get_available_providers()
    available_names = [p.name for p in available]

    for idx, provider in enumerate(video_router.providers):
        configured, err = provider.is_configured()
        providers_info.append({
            "name": provider.name,
            "configured": configured,
            "primary": idx == 0,
            "error": err if not configured else None,
        })

    return jsonify({
        "ok": True,
        "video_provider": getattr(config, 'VIDEO_PROVIDER', 'vertex'),
        "video_quality": getattr(config, 'VIDEO_QUALITY', 'fast'),
        "vertex_model": getattr(config, 'VERTEX_VEO_MODEL', 'unknown'),
        "vertex_location": getattr(config, 'VERTEX_LOCATION', 'us-central1'),
        "active_providers": available_names,
        "providers": providers_info,
    })


# ── POST /video/admin/smoke-test — Quick Veo test (no credits) ──
@bp.route("/video/admin/smoke-test", methods=["POST", "OPTIONS"])
@with_session
def video_admin_smoke_test():
    """
    Admin smoke test: Start a fast Veo job and return operation name.

    This is for testing the Vertex AI integration without going through
    the full credit reservation flow. Only available to admins.

    Request body (optional):
    {
        "provider": "vertex",           # "vertex" or "aistudio"
        "prompt": "A cat on a beach"    # Optional, uses default if not provided
    }

    Returns:
    {
        "ok": true,
        "provider": "vertex",
        "operation_name": "projects/.../operations/...",
        "model": "veo-3.1-fast-generate-001"
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    # Check admin access (optional - remove if you want any authenticated user)
    from backend.config import config
    # For now, allow any authenticated user to run smoke test

    body = request.get_json(silent=True) or {}
    provider_name = (body.get("provider") or "vertex").lower()
    prompt = body.get("prompt") or "A serene mountain lake at sunrise, calm water reflections, cinematic"

    # Get the requested provider
    provider = video_router.get_provider(provider_name)
    if not provider:
        return jsonify({
            "ok": False,
            "error": "invalid_provider",
            "message": f"Unknown provider: {provider_name}",
            "available": [p.name for p in video_router.providers],
        }), 400

    configured, config_err = provider.is_configured()
    if not configured:
        return jsonify({
            "ok": False,
            "error": "provider_not_configured",
            "message": f"Provider {provider_name} is not configured: {config_err}",
        }), 400

    try:
        # Start a short, low-cost video job for testing
        result = provider.start_text_to_video(
            prompt=prompt,
            aspect_ratio="16:9",
            resolution="720p",
            duration_seconds=4,  # Shortest duration for quick test
        )

        operation_name = result.get("operation_name")

        return jsonify({
            "ok": True,
            "provider": provider_name,
            "operation_name": operation_name,
            "model": getattr(config, 'VERTEX_VEO_MODEL', 'unknown') if provider_name == "vertex" else "veo-3.1-generate-preview",
            "prompt": prompt,
            "hint": f"Poll status with: GET /api/_mod/video/admin/smoke-test/status?op={operation_name[:50]}...",
        })

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": "smoke_test_failed",
            "message": str(e),
            "provider": provider_name,
        }), 500


# ── GET /video/admin/smoke-test/status — Poll smoke test operation ──
@bp.route("/video/admin/smoke-test/status", methods=["GET", "OPTIONS"])
@with_session
def video_admin_smoke_test_status():
    """
    Poll a smoke test operation for status.

    Query params:
    - op: Operation name from smoke test
    - provider: "vertex" or "aistudio" (default: vertex)

    Returns:
    {
        "ok": true,
        "status": "processing" | "done" | "failed",
        "progress": 45,
        "video_url": "..." (if done)
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    operation_name = request.args.get("op")
    if not operation_name:
        return jsonify({
            "ok": False,
            "error": "missing_param",
            "message": "Query param 'op' (operation name) is required",
        }), 400

    provider_name = request.args.get("provider", "vertex").lower()
    provider = video_router.get_provider(provider_name)
    if not provider:
        return jsonify({
            "ok": False,
            "error": "invalid_provider",
            "message": f"Unknown provider: {provider_name}",
        }), 400

    try:
        result = provider.check_status(operation_name)

        response = {
            "ok": True,
            "provider": provider_name,
            "status": result.get("status"),
            "progress": result.get("progress", 0),
        }

        if result.get("video_url"):
            response["video_url"] = result["video_url"]

        if result.get("error"):
            response["error"] = result.get("error")
            response["message"] = result.get("message")

        return jsonify(response)

    except Exception as e:
        return jsonify({
            "ok": False,
            "error": "status_check_failed",
            "message": str(e),
        }), 500
