"""
Video Generation Routes Blueprint.
----------------------------------
Registered under /api/_mod and /api for compatibility.

Active providers:
- vertex   (Veo 3.1)       — durations 4/6/8s, aspects 16:9/9:16, resolutions 720p/1080p/4k
- seedance (Seedance 2.0)  — durations 5/10/15s, aspects 16:9/9:16/1:1, tiers fast/preview

Endpoints:
- POST /video/generate   — Unified start (text2video or image2video) — legacy
- POST /video/text       — Text → short cinematic clip
- POST /video/animate    — Image → animated video clip
- GET  /video/status/<job_id>          — Poll job status (canonical)
- GET  /video/generate/status/<job_id> — Poll job status (legacy alias)
"""

from __future__ import annotations

import uuid
from flask import Blueprint, jsonify, request

from backend.db import USE_DB, get_conn, Tables
from backend.middleware import with_session
from backend.services.async_dispatch import get_executor
from backend.services.credits_helper import start_paid_job, release_job_credits
from backend.services.expense_guard import ExpenseGuard
from backend.services.identity_service import require_identity
from backend.services.job_service import create_internal_job_row, load_store, save_store
from backend.services.gemini_video_service import (
    validate_video_params,
    GeminiValidationError,
)
from backend.services.vertex_video_service import check_vertex_resolution
from backend.services.video_router import (
    video_router,
    resolve_video_provider,
    normalize_provider_name,
)
from backend.services.video_providers.seedance_provider import (
    normalize_seedance_params,
)
from backend.services.video_providers.vertex_provider import (
    normalize_vertex_params,
)
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
from backend.services.video_limits import validate_video_rate_limits
from backend.utils.helpers import now_s, log_event

bp = Blueprint("video", __name__)


@bp.route("/video/generate", methods=["POST", "OPTIONS"])
@with_session
def generate_video():
    """
    Legacy unified endpoint: start text2video or image2video.

    Delegates to _dispatch_video_job after provider-specific normalization.
    New code should use POST /video/text or POST /video/animate instead.
    """
    if request.method == "OPTIONS":
        return ("", 204)

    available_providers = video_router.get_available_providers()
    if not available_providers:
        return jsonify({
            "error": "video_not_configured",
            "message": "No video generation providers are configured",
            "details": {"hint": "Set GEMINI_API_KEY environment variable"}
        }), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}

    provider = normalize_provider_name(body.get("provider"))
    task = (body.get("task") or "text2video").lower()

    if task not in ("text2video", "image2video"):
        return jsonify({
            "error": "invalid_params",
            "message": "task must be 'text2video' or 'image2video'",
            "field": "task",
            "allowed": ["text2video", "image2video"]
        }), 400

    # Raw parameters from request
    raw_duration = body.get("duration_sec") or body.get("durationSeconds") or 6
    aspect_ratio = body.get("aspect_ratio") or body.get("aspectRatio") or "16:9"
    resolution = body.get("resolution") or "720p"
    motion = (body.get("motion") or "").strip()
    negative_prompt = (body.get("negative_prompt") or body.get("negativePrompt") or "").strip()
    seed = body.get("seed")

    seedance_variant = None
    seedance_tier = "fast"

    # ── Provider-specific normalization ──
    if provider == "seedance":
        sc = normalize_seedance_params(
            duration_seconds=raw_duration,
            aspect_ratio=aspect_ratio,
            seedance_variant=body.get("seedance_variant"),
        )
        duration_seconds = sc["duration_seconds"]
        aspect_ratio = sc["aspect_ratio"]
        seedance_variant = sc["task_type"]
        seedance_tier = sc["tier"]
        resolution = "720p"  # Seedance has no resolution concept
    else:
        vc = normalize_vertex_params(
            duration_seconds=raw_duration,
            aspect_ratio=aspect_ratio,
            resolution=resolution,
        )
        duration_seconds = vc["duration_seconds"]
        aspect_ratio = vc["aspect_ratio"]
        resolution = vc["resolution"]

    # Task-specific validation
    if task == "text2video":
        prompt = (body.get("prompt") or "").strip()
        if not prompt:
            return jsonify({"error": "invalid_params", "message": "prompt is required for text2video", "field": "prompt"}), 400
        image_data = None
    else:
        image_data = body.get("image_data") or body.get("image") or ""
        if not image_data:
            return jsonify({"error": "invalid_params", "message": "image_data is required for image2video", "field": "image_data"}), 400
        prompt = motion or "Animate this image with natural, smooth motion"

    return _dispatch_video_job(
        identity_id=identity_id or "",
        task=task,
        prompt=prompt,
        image_data=image_data,
        aspect_ratio=aspect_ratio,
        resolution=resolution,
        duration_seconds=duration_seconds,
        motion=motion,
        negative_prompt=negative_prompt,
        seed=seed,
        provider=provider,
        seedance_variant=seedance_variant,
        seedance_tier=seedance_tier,
    )


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
    provider: str = "vertex",
    seedance_variant: str | None = None,
    seedance_tier: str = "fast",
):
    """
    Shared helper: validate, reserve credits, create job row, dispatch async, return response.

    Expects already-normalized provider-specific parameters (callers must
    run normalize_seedance_params or normalize_vertex_params before calling).
    """
    # Dynamic provider routing — auto-select cheaper provider under load
    from backend.services.video_limits import select_video_provider
    provider = select_video_provider(provider)

    # Vertex: final validation via gemini_video_service (catches edge cases)
    if provider == "vertex":
        try:
            aspect_ratio, resolution, duration_seconds = validate_video_params(
                aspect_ratio, resolution, duration_seconds
            )
        except GeminiValidationError as e:
            print(f"[VIDEO] Vertex validation failed: {e.message}")
            return jsonify({
                "ok": False,
                "error": "invalid_params",
                "message": e.message,
                "field": e.field,
                "value": e.value,
                "allowed": e.allowed,
            }), 400

        # Pre-flight: reject 4k if Vertex project is not allowlisted
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

    internal_job_id = str(uuid.uuid4())

    # Credit calculation
    action_key = get_video_action_code(task, duration_seconds, resolution, provider=provider, seedance_tier=seedance_tier)
    expected_cost = get_video_credit_cost(duration_seconds, resolution, provider=provider, seedance_tier=seedance_tier)

    if expected_cost <= 0:
        print(f"[VIDEO] REJECTED: action_key={action_key} resolved to 0 credits")
        return jsonify({
            "ok": False,
            "error": "unknown_action_code",
            "message": f"Unknown video action code: {action_key}. Cannot determine cost.",
        }), 400

    # RATE LIMITS: concurrency, hourly cap, cooldown, spend guardrails
    rate_error = validate_video_rate_limits(
        identity_id,
        provider=provider,
        duration_seconds=duration_seconds,
        seedance_tier=seedance_tier,
    )
    if rate_error:
        return rate_error

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
        "provider": provider,
        "prompt": prompt,
        "duration_seconds": duration_seconds,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "motion": motion,
        "negative_prompt": negative_prompt,
        "seed": seed,
        "style_preset": style_preset,
        "motion_preset": motion_preset,
        "seedance_variant": seedance_variant,
        "seedance_tier": seedance_tier if provider == "seedance" else None,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
    }

    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    # Part 8: Priority queue — derive priority from user tier
    from backend.services.video_limits import get_job_priority
    job_priority = get_job_priority(identity_id)

    # FAIL-CLOSED: Job row MUST exist before dispatching upstream provider task.
    # If the insert fails (e.g. FK constraint on action_code), abort and release credits.
    job_created = create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider=provider,
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
        priority=str(job_priority),
        stage="video",
    )
    if not job_created:
        print(f"[VIDEO] CRITICAL: Job row creation failed for {internal_job_id}, aborting dispatch. action_key={action_key}")
        # Release reserved credits
        if reservation_id:
            try:
                release_job_credits(reservation_id, job_id=internal_job_id, reason="job_row_creation_failed")
            except Exception as rel_err:
                print(f"[VIDEO] ERROR releasing reservation {reservation_id}: {rel_err}")
        # Clean up in-memory store
        store.pop(internal_job_id, None)
        save_store(store)
        return jsonify({
            "ok": False,
            "error": "internal_job_creation_failed",
            "message": "Failed to create internal job record. Credits have been released. Please try again.",
        }), 500

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
        "seedance_variant": seedance_variant,
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
    from backend.services.video_limits import get_estimated_render_time
    rtime = get_estimated_render_time(provider)
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
        "estimated_duration_seconds": rtime["estimated_duration_seconds"],
    })


# ── POST /video/text — Text → short cinematic clip ───────────
@bp.route("/video/text", methods=["POST", "OPTIONS"])
@with_session
def video_text():
    """
    Generate a short cinematic video clip from a text prompt.

    Provider-specific normalization happens here so _dispatch_video_job
    receives clean, validated parameters.
    """
    if request.method == "OPTIONS":
        return ("", 204)

    available_providers = video_router.get_available_providers()
    if not available_providers:
        return jsonify({"error": "video_not_configured", "message": "No video generation providers are configured"}), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}

    raw_prompt = (body.get("prompt") or "").strip()
    if not raw_prompt:
        return jsonify({"error": "invalid_params", "message": "prompt is required", "field": "prompt"}), 400

    provider = normalize_provider_name(body.get("provider"))
    raw_duration = body.get("seconds") or body.get("duration_sec") or (5 if provider == "seedance" else 6)
    aspect_ratio = body.get("aspect_ratio") or "16:9"
    resolution = body.get("resolution") or "720p"
    negative_prompt = (body.get("negative_prompt") or "").strip()
    seed = body.get("seed")
    style_preset = body.get("style_preset")

    seedance_variant = None
    seedance_tier = "fast"

    if provider == "seedance":
        sc = normalize_seedance_params(
            duration_seconds=raw_duration,
            aspect_ratio=aspect_ratio,
            seedance_variant=body.get("seedance_variant"),
        )
        duration_seconds = sc["duration_seconds"]
        aspect_ratio = sc["aspect_ratio"]
        seedance_variant = sc["task_type"]
        seedance_tier = sc["tier"]
        resolution = "720p"
        prompt = raw_prompt  # No style normalization for Seedance
    else:
        vc = normalize_vertex_params(
            duration_seconds=raw_duration,
            aspect_ratio=aspect_ratio,
            resolution=resolution,
        )
        duration_seconds = vc["duration_seconds"]
        aspect_ratio = vc["aspect_ratio"]
        resolution = vc["resolution"]
        prompt = normalize_text_prompt(raw_prompt, style_preset, duration_seconds)

    return _dispatch_video_job(
        identity_id=identity_id or "",
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
        provider=provider,
        seedance_variant=seedance_variant,
        seedance_tier=seedance_tier,
    )


# ── POST /video/animate — Image → animated video clip ────────
@bp.route("/video/animate", methods=["POST", "OPTIONS"])
@with_session
def video_animate():
    """
    Animate a single image into a short video clip.

    Provider-specific normalization happens here so _dispatch_video_job
    receives clean, validated parameters.
    """
    if request.method == "OPTIONS":
        return ("", 204)

    available_providers = video_router.get_available_providers()
    if not available_providers:
        return jsonify({"error": "video_not_configured", "message": "No video generation providers are configured"}), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}

    image_data = body.get("image_data") or body.get("image_url") or body.get("image") or ""
    image_id = body.get("image_id")

    if not image_data and image_id:
        image_data = _resolve_image_id(image_id, identity_id)

    if not image_data:
        return jsonify({"error": "invalid_params", "message": "image_data, image_url, or image_id is required", "field": "image_data"}), 400

    provider = normalize_provider_name(body.get("provider"))
    raw_user_prompt = (body.get("prompt") or body.get("motion") or "").strip()
    raw_duration = body.get("seconds") or body.get("duration_sec") or (5 if provider == "seedance" else 6)
    aspect_ratio = body.get("aspect_ratio") or "16:9"
    resolution = body.get("resolution") or "720p"
    negative_prompt = (body.get("negative_prompt") or "").strip()
    seed = body.get("seed")
    motion_preset = body.get("motion_preset")

    seedance_variant = None
    seedance_tier = "fast"

    if provider == "seedance":
        sc = normalize_seedance_params(
            duration_seconds=raw_duration,
            aspect_ratio=aspect_ratio,
            seedance_variant=body.get("seedance_variant"),
        )
        duration_seconds = sc["duration_seconds"]
        aspect_ratio = sc["aspect_ratio"]
        seedance_variant = sc["task_type"]
        seedance_tier = sc["tier"]
        resolution = "720p"
        prompt = raw_user_prompt or "Animate this image with natural, smooth motion"
    else:
        vc = normalize_vertex_params(
            duration_seconds=raw_duration,
            aspect_ratio=aspect_ratio,
            resolution=resolution,
        )
        duration_seconds = vc["duration_seconds"]
        aspect_ratio = vc["aspect_ratio"]
        resolution = vc["resolution"]
        prompt = normalize_motion_prompt(raw_user_prompt, motion_preset)

    return _dispatch_video_job(
        identity_id=identity_id or "",
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
        provider=provider,
        seedance_variant=seedance_variant,
        seedance_tier=seedance_tier,
    )


# ── Helper: resolve image_id → image_url from DB ─────────────
def _resolve_image_id(image_id: str, identity_id: str | None) -> str | None:
    """Look up an image URL from our images table by image_id, scoped to the requesting user."""
    if not USE_DB or not image_id or not identity_id:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT image_url FROM {Tables.IMAGES}
                    WHERE id::text = %s AND identity_id = %s AND deleted_at IS NULL
                    LIMIT 1
                    """,
                    (image_id, identity_id),
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


# ── Live PiAPI re-check helpers for orphaned Seedance jobs ───
_STALE_THRESHOLD_SECONDS = 10 * 60  # 10 minutes without DB update → stale


def _estimate_video_progress(job_row, job_meta, estimated_duration: int) -> int:
    """Estimate video generation progress from elapsed time (asymptotic to 95%)."""
    import math
    from datetime import datetime, timezone

    # Use processing_started_at > dispatched_at > created_at
    started = job_meta.get("processing_started_at") or job_meta.get("dispatched_at")
    if not started:
        created = job_row.get("created_at")
        if created and hasattr(created, 'timestamp'):
            started = created.timestamp()

    if not started:
        return 0

    now = datetime.now(timezone.utc).timestamp()
    if isinstance(started, (int, float)):
        elapsed = now - started
    else:
        try:
            elapsed = now - started.timestamp()
        except Exception:
            return 0

    if elapsed <= 0 or estimated_duration <= 0:
        return 0

    # Asymptotic curve: approaches 95% as elapsed -> estimated_duration
    # progress = 95 * (1 - e^(-2 * elapsed / estimated_duration))
    ratio = elapsed / estimated_duration
    progress = int(95 * (1 - math.exp(-2 * ratio)))
    return max(1, min(progress, 95))


def _is_stale_seedance_job(job_row, job_meta) -> bool:
    """Return True if this looks like a Seedance job whose polling thread died."""
    provider = job_meta.get("provider", "")
    if provider != "seedance":
        return False
    upstream_id = job_meta.get("upstream_id") or job_meta.get("operation_name")
    if not upstream_id:
        return False
    updated_at = job_row.get("updated_at")
    if not updated_at:
        return True
    from datetime import datetime, timezone
    if hasattr(updated_at, 'tzinfo') and updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - updated_at).total_seconds()
    return age > _STALE_THRESHOLD_SECONDS


def _try_live_seedance_check(job_id: str, job_meta: dict, identity_id: str | None):
    """
    One-shot live check against PiAPI for an orphaned Seedance job.

    If PiAPI says done → finalize (upload to S3, update DB, update history).
    If PiAPI says failed → mark failed.
    If still processing → return None (let normal status response handle it).
    """
    upstream_id = job_meta.get("upstream_id") or job_meta.get("operation_name")
    if not upstream_id:
        return None

    try:
        from backend.services.seedance_service import check_seedance_status
        status_resp = check_seedance_status(upstream_id)
    except Exception as e:
        print(f"[VIDEO STATUS] Live PiAPI check failed for {job_id}: {e}")
        return None

    status = status_resp.get("status")

    if status == "done":
        video_url = status_resp.get("video_url")
        if not video_url:
            return None

        print(f"[VIDEO STATUS] Live check: job {job_id} completed on PiAPI! Finalizing now.")

        # Finalize in background to not block the status response
        try:
            from backend.services.async_dispatch import (
                _finalize_video_success,
                load_store,
            )
            store = load_store()
            store_meta = store.get(job_id) or job_meta
            reservation_id = store_meta.get("reservation_id") or job_meta.get("reservation_id")

            _finalize_video_success(
                job_id, identity_id or "", reservation_id,
                video_url, store_meta, provider_name="seedance",
            )
        except Exception as e:
            print(f"[VIDEO STATUS] Live finalize failed for {job_id}: {e}")

        return jsonify({
            "ok": True,
            "status": "done",
            "job_id": job_id,
            "video_url": video_url,
            "message": "Video ready",
        })

    if status == "failed":
        return _live_check_mark_failed(job_id, job_meta, status_resp)

    # Still pending/processing on PiAPI — return None to use normal response
    return None


def _live_check_mark_failed(job_id, job_meta, status_resp):
    """Handle a live PiAPI check that returned failed."""
    error_code = status_resp.get("error", "seedance_generation_failed")
    error_msg = status_resp.get("message", "Seedance generation failed")
    print(f"[VIDEO STATUS] Live check: job {job_id} failed on PiAPI: {error_code}")
    try:
        from backend.services.async_dispatch import update_job_status_failed, ExpenseGuard
        reservation_id = job_meta.get("reservation_id")
        if reservation_id:
            from backend.services.credits_helper import release_job_credits
            release_job_credits(reservation_id, error_code, job_id)
        update_job_status_failed(job_id, f"{error_code}: {error_msg}")
        ExpenseGuard.unregister_active_job(job_id)
    except Exception as e:
        print(f"[VIDEO STATUS] Live fail-update error for {job_id}: {e}")
    return jsonify({
        "ok": False, "status": "failed", "job_id": job_id,
        "error": error_code, "message": error_msg,
    })


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
                        SELECT id, status, error_message, meta, updated_at
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
                    from backend.services.video_limits import get_queue_position, get_estimated_render_time
                    qpos = get_queue_position(job_id)
                    provider_hint = meta.get("provider") or job_meta.get("provider") or "vertex"
                    rtime = get_estimated_render_time(provider_hint)
                    return jsonify({
                        "ok": True,
                        "status": "queued",
                        "job_id": job_id,
                        "message": f"Queue position #{qpos['queue_position']}",
                        "progress": 0,
                        "queue_position": qpos["queue_position"],
                        "estimated_start_seconds": qpos["estimated_start_seconds"],
                        "estimated_duration_seconds": rtime["estimated_duration_seconds"],
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

                # ── Live PiAPI re-check for orphaned Seedance jobs ──
                # If the backend polling thread died (deploy/crash) and the job
                # has been stuck for >10 min, check PiAPI directly and finalize.
                if job["status"] in ("provider_pending", "processing") and _is_stale_seedance_job(job, job_meta):
                    live_result = _try_live_seedance_check(job_id, job_meta, identity_id)
                    if live_result:
                        return live_result

                if job["status"] == "provider_pending":
                    pending_secs = job_meta.get("pending_seconds", 0)
                    msg = "Queued with provider" if pending_secs < 120 else "Provider queue busy — your video is still queued"
                    return jsonify({
                        "ok": True,
                        "status": "provider_pending",
                        "job_id": job_id,
                        "message": msg,
                        "progress": 0,
                        "pending_seconds": pending_secs,
                        "provider_status": job_meta.get("provider_status", "pending"),
                    })

                if job["status"] in ("processing", "provider_processing"):
                    real_progress = meta.get("progress") or job_meta.get("progress") or 0
                    provider_hint = meta.get("provider") or job_meta.get("provider") or "vertex"
                    from backend.services.video_limits import get_estimated_render_time
                    rtime = get_estimated_render_time(provider_hint)

                    # Estimate progress from elapsed time if provider gives 0
                    progress = real_progress
                    if progress == 0:
                        progress = _estimate_video_progress(job, job_meta, rtime["estimated_duration_seconds"])

                    # Format time estimate as minutes if > 90s
                    lo, hi = rtime['estimated_min_seconds'], rtime['estimated_max_seconds']
                    if lo >= 90:
                        time_hint = f"{lo // 60}–{hi // 60} min"
                    else:
                        time_hint = f"{lo}–{hi}s"

                    return jsonify({
                        "ok": True,
                        "status": "processing",
                        "job_id": job_id,
                        "message": f"Rendering (estimated {time_hint})",
                        "progress": progress,
                        "estimated_duration_seconds": rtime["estimated_duration_seconds"],
                    })

                if job["status"] in ("failed", "provider_stalled"):
                    error_msg = job.get("error_message", "Video generation failed")
                    error_code = job_meta.get("error_code") or "video_failed"

                    # Fallback: parse error code from "code: message" format
                    if error_code == "video_failed" and error_msg and ":" in error_msg:
                        parts = error_msg.split(":", 1)
                        candidate = parts[0].strip()
                        if candidate and " " not in candidate:
                            error_code = candidate
                            error_msg = parts[1].strip()

                    # Use structured failure_reason from meta if available
                    failure_reason = job_meta.get("failure_reason") or error_msg

                    # Surface user-friendly message for filtered errors
                    user_message = job_meta.get("user_message") if error_code == "provider_filtered_third_party" else None

                    resp = {
                        "ok": False,
                        "status": "failed",
                        "job_id": job_id,
                        "error": error_code,
                        "message": user_message or failure_reason,
                    }
                    if user_message:
                        resp["user_message"] = user_message
                    if job["status"] == "provider_stalled":
                        resp["provider_stalled"] = True
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

    if meta.get("status") in ("failed", "provider_stalled"):
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
        if meta.get("status") == "provider_stalled":
            resp["provider_stalled"] = True
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

    if meta.get("status") == "provider_pending":
        pending_secs = meta.get("pending_seconds", 0)
        msg = "Queued with provider" if pending_secs < 120 else "Provider queue busy — your video is still queued"
        return jsonify({
            "ok": True,
            "status": "provider_pending",
            "job_id": job_id,
            "message": msg,
            "progress": 0,
            "pending_seconds": pending_secs,
            "provider_status": meta.get("provider_status", "pending"),
        })

    if meta.get("status") in ("queued", "processing"):
        provider_hint = meta.get("provider", "vertex")
        from backend.services.video_limits import get_estimated_render_time
        rtime = get_estimated_render_time(provider_hint)
        real_progress = meta.get("progress", 0)
        # Estimate progress from elapsed time for store-based fallback
        progress = real_progress
        if progress == 0 and meta.get("status") == "processing":
            import math, time as _time
            from datetime import datetime, timezone
            started = meta.get("processing_started_at") or meta.get("dispatched_at") or meta.get("started_at")
            if started and isinstance(started, (int, float)):
                elapsed = _time.time() - started
                est = rtime["estimated_duration_seconds"]
                if elapsed > 0 and est > 0:
                    progress = max(1, min(int(95 * (1 - math.exp(-2 * elapsed / est))), 95))
        return jsonify({
            "ok": True,
            "status": meta.get("status"),
            "job_id": job_id,
            "progress": progress,
            "message": "Generating video...",
            "estimated_duration_seconds": rtime["estimated_duration_seconds"],
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
