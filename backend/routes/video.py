"""
Video Generation Routes Blueprint (Veo 3.1).
----------------------------------
Registered under /api/_mod and /api for compatibility.

Endpoints:
- POST /video/generate - Start video generation (text2video or image2video)
- GET /video/generate/status/<job_id> - Poll job status

Veo 3.1 Constraints:
- aspectRatio: ONLY "16:9" or "9:16" (NO "1:1" for video)
- resolution: "720p", "1080p", "4k" (lowercase 4k)
- durationSeconds: "4", "6", "8"
- CRITICAL: 1080p/4k requires durationSeconds="8"
"""

from __future__ import annotations

import uuid
from flask import Blueprint, jsonify, request

from backend.config import config
from backend.db import USE_DB, get_conn, Tables
from backend.middleware import with_session
from backend.services.async_dispatch import get_executor
from backend.services.credits_helper import get_current_balance, start_paid_job
from backend.services.identity_service import require_identity
from backend.services.job_service import create_internal_job_row, load_store, save_store
from backend.services.gemini_video_service import (
    check_gemini_configured,
    validate_video_params,
    GeminiValidationError,
    ALLOWED_VIDEO_ASPECT_RATIOS,
    ALLOWED_RESOLUTIONS,
    ALLOWED_DURATIONS,
)
from backend.utils.helpers import now_s, log_event

bp = Blueprint("video", __name__)


# Map UI duration values to Veo allowed values
DURATION_MAP = {
    4: "4", 5: "4", 6: "6", 7: "6", 8: "8", 10: "8",
    "4": "4", "5": "4", "6": "6", "7": "6", "8": "8", "10": "8",
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

    # Fail-fast: Check if Gemini is configured
    is_configured, config_error = check_gemini_configured()
    if not is_configured:
        return jsonify({
            "error": "gemini_not_configured",
            "message": "Set GEMINI_API_KEY environment variable",
            "details": {"hint": config_error}
        }), 500

    # Require authentication
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    # Parse request body
    body = request.get_json(silent=True) or {}

    provider = (body.get("provider") or "google").lower()
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

    # Map UI duration to Veo allowed values
    duration_seconds = DURATION_MAP.get(raw_duration, "6")

    # Normalize resolution (4K -> 4k)
    if resolution == "4K":
        resolution = "4k"

    # Validate video parameters BEFORE reserving credits
    try:
        aspect_ratio, resolution, duration_seconds = validate_video_params(
            aspect_ratio, resolution, duration_seconds
        )
    except GeminiValidationError as e:
        return jsonify({
            "error": "invalid_params",
            "message": e.message,
            "field": e.field,
            "allowed": e.allowed
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

    # Generate internal job ID
    internal_job_id = str(uuid.uuid4())

    # Determine action key for credits
    action_key = "video"

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
    from backend.services.async_dispatch import _dispatch_gemini_video_async

    # Dispatch async task
    get_executor().submit(
        _dispatch_gemini_video_async,
        internal_job_id,
        identity_id,
        reservation_id,
        payload,
        store_meta,
    )

    log_event("video/generate:dispatched", {"internal_job_id": internal_job_id, "task": task})

    # Return response
    balance_info = get_current_balance(identity_id) if identity_id else None
    return jsonify({
        "ok": True,
        "job_id": internal_job_id,
        "video_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "task": task,
        "provider": provider,
        "params": {
            "aspect_ratio": aspect_ratio,
            "resolution": resolution,
            "duration_seconds": duration_seconds,
        },
        "source": "modular",
    })


@bp.route("/video/generate/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def video_status(job_id: str):
    """
    Get the status of a video generation job.

    Response (queued/processing):
    {
        "ok": true,
        "status": "processing",
        "job_id": "uuid",
        "progress": 50,
        "message": "Generating video..."
    }

    Response (done):
    {
        "ok": true,
        "status": "done",
        "job_id": "uuid",
        "video_id": "uuid",
        "video_url": "https://..."
    }

    Response (failed):
    {
        "ok": false,
        "status": "failed",
        "job_id": "uuid",
        "error": "<machine_code>",
        "message": "<human readable>"
    }
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
                    # Parse error code if present
                    error_code = "gemini_video_failed"
                    if error_msg and error_msg.startswith("gemini_"):
                        parts = error_msg.split(":", 1)
                        error_code = parts[0]
                        error_msg = parts[1].strip() if len(parts) > 1 else error_msg

                    return jsonify({
                        "ok": False,
                        "status": "failed",
                        "job_id": job_id,
                        "error": error_code,
                        "message": error_msg,
                    })

                if job["status"] == "ready":
                    video_url = meta.get("video_url") or job_meta.get("video_url")
                    thumbnail_url = meta.get("thumbnail_url") or job_meta.get("thumbnail_url")

                    return jsonify({
                        "ok": True,
                        "status": "done",
                        "job_id": job_id,
                        "video_id": job_id,
                        "video_url": video_url,
                        "thumbnail_url": thumbnail_url,
                        "duration_seconds": meta.get("duration_seconds") or job_meta.get("duration_seconds"),
                        "resolution": meta.get("resolution") or job_meta.get("resolution"),
                        "provider": "google",
                    })

        except Exception as e:
            print(f"[VIDEO STATUS] Error checking job {job_id}: {e}")

    # Fallback to job store
    if meta.get("status") == "done":
        return jsonify({
            "ok": True,
            "status": "done",
            "job_id": job_id,
            "video_id": job_id,
            "video_url": meta.get("video_url"),
            "thumbnail_url": meta.get("thumbnail_url"),
            "duration_seconds": meta.get("duration_seconds"),
            "resolution": meta.get("resolution"),
            "provider": "google",
        })

    if meta.get("status") == "failed":
        error_msg = meta.get("error", "Video generation failed")
        error_code = meta.get("error_code", "gemini_video_failed")
        return jsonify({
            "ok": False,
            "status": "failed",
            "job_id": job_id,
            "error": error_code,
            "message": error_msg,
        })

    if meta.get("status") in ("queued", "processing"):
        return jsonify({
            "ok": True,
            "status": meta.get("status"),
            "job_id": job_id,
            "progress": meta.get("progress", 0),
            "message": "Generating video...",
        })

    return jsonify({
        "error": "job_not_found",
        "message": f"No video job found with ID: {job_id}"
    }), 404
