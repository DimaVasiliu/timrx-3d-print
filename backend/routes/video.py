"""
Video Generation Routes Blueprint.
----------------------------------
Registered under /api/_mod and /api for compatibility.

Endpoints:
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
from backend.services.identity_service import require_identity
from backend.services.job_service import create_internal_job_row, load_store, save_store
from backend.services.gemini_video_service import (
    validate_video_params,
    GeminiValidationError,
)
from backend.services.video_router import video_router
from backend.services.video_prompts import (
    normalize_text_prompt,
    normalize_motion_prompt,
    get_style_presets,
    get_motion_presets,
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
            "details": {"hint": "Set GEMINI_API_KEY or RUNWAY_API_KEY environment variable"}
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

    # Normalize video parameters (provider-agnostic — each provider
    # handles its own ratio/duration constraints via mapping).
    try:
        aspect_ratio, resolution, duration_seconds = validate_video_params(
            aspect_ratio, resolution, duration_seconds
        )
    except GeminiValidationError:
        # Gemini-specific validation failed — may still be valid for other providers.
        # Normalize duration to int and let the router pick the right provider.
        try:
            if isinstance(duration_seconds, str):
                duration_seconds = int(duration_seconds.lower().replace("sec", "").replace("s", "").strip())
            else:
                duration_seconds = int(duration_seconds)
        except (ValueError, TypeError):
            duration_seconds = 6
        if duration_seconds not in (2, 4, 6, 8, 10):
            duration_seconds = 6

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

    # Determine action key for credits (granular per task type)
    action_key = "text2video" if task == "text2video" else "image2video"

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
    from backend.services.async_dispatch import dispatch_gemini_video_async

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
    return jsonify({
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
    })


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
    # Normalize video parameters (provider-agnostic — each provider
    # handles its own ratio/duration constraints via mapping).
    try:
        aspect_ratio, resolution, duration_seconds = validate_video_params(
            aspect_ratio, resolution, duration_seconds
        )
    except GeminiValidationError:
        # Gemini-specific validation failed — may still be valid for other providers.
        # Normalize duration to int and let the router pick the right provider.
        try:
            if isinstance(duration_seconds, str):
                duration_seconds = int(duration_seconds.lower().replace("sec", "").replace("s", "").strip())
            else:
                duration_seconds = int(duration_seconds)
        except (ValueError, TypeError):
            duration_seconds = 6
        if duration_seconds not in (2, 4, 6, 8, 10):
            duration_seconds = 6

    internal_job_id = str(uuid.uuid4())
    action_key = "text2video" if task == "text2video" else "image2video"

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
                        "provider": meta.get("provider") or job_meta.get("provider") or "google",
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
            "provider": meta.get("provider", "google"),
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
                    return jsonify({
                        "ok": True,
                        "status": "done",
                        "job_id": job_id,
                        "video_id": job_id,
                        "video_url": jm.get("video_url"),
                        "thumbnail_url": jm.get("thumbnail_url"),
                    })
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
