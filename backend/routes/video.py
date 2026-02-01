"""
Video Generation Routes Blueprint.
----------------------------------
Registered under /api/_mod and /api for compatibility.

Endpoints:
- POST /video/generate - Start video generation (text2video or image2video)
- GET /video/generate/status/<job_id> - Poll job status
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
from backend.utils.helpers import now_s, log_event

bp = Blueprint("video", __name__)


@bp.route("/video/generate", methods=["POST", "OPTIONS"])
@with_session
def generate_video():
    """
    Start a video generation job.

    Request body:
    {
        "provider": "google",           # Provider: google, openai, etc.
        "task": "text2video",           # Task type: text2video or image2video
        "prompt": "A serene forest...", # Required for text2video
        "image_data": "base64...",      # Required for image2video (base64 or data URL)
        "duration_sec": 5,              # Video duration: 5 or 10
        "fps": 24,                      # Frames per second: 24, 30, 60
        "aspect_ratio": "16:9",         # Aspect ratio: 16:9, 9:16, 1:1
        "resolution": "1080p",          # Resolution: 720p, 1080p, 4K
        "motion": "Camera slowly...",   # Motion description (for image2video)
        "audio": false,                 # Generate audio
        "loop_seamlessly": false        # Loop seamlessly
    }

    Response:
    {
        "ok": true,
        "job_id": "uuid",
        "video_id": "uuid",
        "reservation_id": "uuid",
        "status": "queued",
        "new_balance": 100
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    # Check API key configuration
    google_api_key = getattr(config, 'GOOGLE_API_KEY', None)
    if not google_api_key:
        return jsonify({"error": "GOOGLE_API_KEY not configured"}), 503

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
        return jsonify({"error": "Invalid task type. Must be 'text2video' or 'image2video'"}), 400

    # Get common parameters with defaults
    duration_sec = int(body.get("duration_sec") or 5)
    fps = int(body.get("fps") or 24)
    aspect_ratio = body.get("aspect_ratio") or "16:9"
    resolution = body.get("resolution") or "1080p"
    motion = (body.get("motion") or "").strip()
    audio = bool(body.get("audio"))
    loop_seamlessly = bool(body.get("loop_seamlessly"))

    # Validate duration
    if duration_sec not in (5, 10):
        duration_sec = 5

    # Validate fps
    if fps not in (24, 30, 60):
        fps = 24

    # Validate aspect ratio
    if aspect_ratio not in ("16:9", "9:16", "1:1"):
        aspect_ratio = "16:9"

    # Validate resolution
    if resolution not in ("720p", "1080p", "4K"):
        resolution = "1080p"

    # Task-specific validation
    if task == "text2video":
        prompt = (body.get("prompt") or "").strip()
        if not prompt:
            return jsonify({"error": "prompt is required for text2video"}), 400
        image_data = None
    else:  # image2video
        image_data = body.get("image_data") or body.get("image") or ""
        if not image_data:
            return jsonify({"error": "image_data is required for image2video"}), 400
        prompt = motion  # Use motion as the prompt for image-to-video

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
            "duration_sec": duration_sec,
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
        "duration_sec": duration_sec,
        "fps": fps,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "motion": motion,
        "audio": audio,
        "loop_seamlessly": loop_seamlessly,
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
        "duration_sec": duration_sec,
        "fps": fps,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "motion": motion,
        "audio": audio,
        "loop_seamlessly": loop_seamlessly,
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
    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": internal_job_id,
        "video_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "task": task,
        "provider": provider,
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
        "video_url": "https://...",
        "thumbnail_url": "https://..."
    }

    Response (failed):
    {
        "ok": false,
        "status": "failed",
        "job_id": "uuid",
        "error": "Error message"
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
                    return jsonify({
                        "ok": False,
                        "status": "failed",
                        "job_id": job_id,
                        "error": job.get("error_message", "Video generation failed"),
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
                        "duration_sec": meta.get("duration_sec") or job_meta.get("duration_sec"),
                        "resolution": meta.get("resolution") or job_meta.get("resolution"),
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
            "duration_sec": meta.get("duration_sec"),
            "resolution": meta.get("resolution"),
        })

    if meta.get("status") == "failed":
        return jsonify({
            "ok": False,
            "status": "failed",
            "job_id": job_id,
            "error": meta.get("error", "Video generation failed"),
        })

    if meta.get("status") in ("queued", "processing"):
        return jsonify({
            "ok": True,
            "status": meta.get("status"),
            "job_id": job_id,
            "progress": meta.get("progress", 0),
            "message": "Generating video...",
        })

    return jsonify({"error": "Job not found"}), 404
