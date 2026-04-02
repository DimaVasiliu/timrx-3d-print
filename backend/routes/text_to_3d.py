"""
Text-to-3D Routes Blueprint (Modular)
------------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

import uuid

from flask import Blueprint, jsonify, request, g

from backend.config import ACTION_KEYS, MESHY_API_KEY
from backend.utils import derive_display_title
from backend.db import USE_DB, get_conn
from backend.middleware import with_session, with_session_readonly
from backend.services.async_dispatch import (
    _dispatch_meshy_refine_async,
    _dispatch_meshy_text_to_3d_async,
    get_executor,
    update_job_with_upstream_id,
)
from backend.services.credits_helper import (
    finalize_job_credits,
    get_current_balance,
    release_job_credits,
    start_paid_job,
)
from backend.services.identity_service import require_identity
from backend.services.job_service import (
    _update_job_status_failed,
    _update_job_status_ready,
    create_internal_job_row,
    get_job_metadata,
    load_store,
    resolve_meshy_job_id,
    save_store,
    verify_job_ownership,
)
from backend.services.meshy_service import mesh_get, mesh_post, normalize_status, MeshyTaskNotFoundError, terminalize_expired_meshy_job
from backend.services.s3_service import save_finished_job_to_normalized_db
from backend.services.history_service import get_canonical_model_row
from backend.utils.helpers import clamp_int, log_event, log_status_summary, normalize_license, now_s

bp = Blueprint("text_to_3d", __name__)

# ── Status response cache ──
# Uses the shared thread-safe status cache service (same as all other routes).
import time as _time
from backend.services.status_cache import get_cached_status as _get_cached_status, cache_status as _set_status_cache, invalidate_status as _clear_status_cache

# ── Finalization short-circuit (TTL-based) ──
# Prevents duplicate finalize calls from burning 3-4 pool connections each.
# The DB-level idempotency guard (FOR UPDATE lock) still exists as the
# authoritative check; this in-memory cache avoids hitting the DB for the
# common duplicate-poll case.
# TTL-based instead of capped-set: entries expire after 30 min, which is
# far longer than any polling cycle.  No cap needed — TTL naturally bounds
# memory (at 10 jobs/min, 30min = ~300 entries max).
_finalized_jobs: dict = {}  # job_id -> monotonic timestamp
_FINALIZED_TTL = 1800  # 30 minutes


@bp.route("/text-to-3d/start", methods=["POST", "OPTIONS"])
@with_session
def text_to_3d_start_mod():
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("text-to-3d/start:incoming[mod]", body)
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"ok": False, "error": "prompt required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["text-to-3d-preview"]
    ai_model = body.get("model") or "latest"

    # Block removed Meshy 4
    if ai_model in ("meshy-4", "meshy4"):
        return jsonify({"ok": False, "error": "Meshy 4 is no longer supported. Use meshy-5 or latest."}), 400

    payload = {
        "mode": "preview",
        "prompt": prompt,
        "ai_model": ai_model,
    }

    symmetry_mode = (body.get("symmetry_mode") or "").strip().lower()
    if symmetry_mode in {"off", "auto", "on"}:
        payload["symmetry_mode"] = symmetry_mode

    # pose_mode replaces deprecated is_a_t_pose (enum: "", "a-pose", "t-pose")
    pose_mode = (body.get("pose_mode") or "").strip().lower()
    if pose_mode in {"a-pose", "t-pose"}:
        payload["pose_mode"] = pose_mode

    # New Meshy params
    model_type = (body.get("model_type") or "").strip().lower()
    if model_type in {"standard", "lowpoly"}:
        payload["model_type"] = model_type

    if body.get("should_remesh") is not None:
        payload["should_remesh"] = bool(body["should_remesh"])
    if body.get("should_texture") is not None:
        payload["should_texture"] = bool(body["should_texture"])
    # NOTE: enable_pbr is NOT valid for text-to-3d preview.
    # It belongs in: refine flows and image-to-3d (when should_texture=true).

    license_choice = normalize_license(body.get("license"))
    batch_count = clamp_int(body.get("batch_count"), 1, 8, 1)
    batch_slot = clamp_int(body.get("batch_slot"), 1, batch_count, 1)
    batch_group_id = (body.get("batch_group_id") or "").strip() or None

    job_meta = {
        "prompt": prompt,
        "root_prompt": prompt,
        "title": prompt[:50] if prompt else None,
        "stage": "preview",
        "model": ai_model,
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "pose_mode": pose_mode,
        "batch_count": batch_count,
        "batch_slot": batch_slot,
        "batch_group_id": batch_group_id,
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    store_meta = {
        "stage": "preview",
        "prompt": prompt,
        "title": derive_display_title(prompt, None),
        "root_prompt": prompt,
        "model": ai_model,
        "created_at": now_s() * 1000,
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "pose_mode": pose_mode,
        "batch_count": batch_count,
        "batch_slot": batch_slot,
        "batch_group_id": batch_group_id,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
    }

    # Persist immediately so status polling can return queued while dispatch runs
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    # Persist job row so status polling works across workers
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    get_executor().submit(
        _dispatch_meshy_text_to_3d_async,
        internal_job_id,
        identity_id,
        reservation_id,
        payload,
        store_meta,
    )

    log_event("text-to-3d/start:dispatched[mod]", {"internal_job_id": internal_job_id})

    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "source": "modular",
    })


@bp.route("/text-to-3d/refine", methods=["POST", "OPTIONS"])
@with_session
def text_to_3d_refine_mod():
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("text-to-3d/refine:incoming[mod]", body)
    preview_task_id_input = (body.get("preview_task_id") or "").strip()
    if not preview_task_id_input:
        return jsonify({"ok": False, "error": "preview_task_id required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    preview_task_id = resolve_meshy_job_id(preview_task_id_input)

    # If resolution returned a different value, it found the Meshy upstream ID
    resolved = preview_task_id != preview_task_id_input
    print(
        f"[REFINE_RESOLVE] incoming={preview_task_id_input} "
        f"resolved={'yes' if resolved else 'no'} "
        f"meshy_preview_id={preview_task_id}"
    )

    # If resolution returned the same value (no upstream mapping found),
    # the input might be an internal TimrX job UUID. Try one more DB lookup
    # to find the Meshy task ID via the jobs table.
    if not resolved and USE_DB:
        try:
            from backend.db import get_conn, Tables
            from psycopg.rows import dict_row
            with get_conn() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Direct jobs lookup: input is internal UUID → get upstream
                    cur.execute(
                        f"SELECT upstream_job_id FROM {Tables.JOBS} WHERE id::text = %s AND upstream_job_id IS NOT NULL LIMIT 1",
                        (preview_task_id_input,),
                    )
                    row = cur.fetchone()
                    if row:
                        preview_task_id = row["upstream_job_id"]
                        resolved = True
                        print(f"[REFINE_RESOLVE] fallback jobs lookup: {preview_task_id_input} -> {preview_task_id}")
                    else:
                        # Input might be a Meshy task ID that's valid on the provider side
                        # Verify it exists as an upstream_job_id somewhere
                        cur.execute(
                            f"""
                            SELECT 1 FROM {Tables.JOBS} WHERE upstream_job_id = %s
                            UNION ALL
                            SELECT 1 FROM {Tables.MODELS} WHERE upstream_job_id = %s
                            LIMIT 1
                            """,
                            (preview_task_id_input, preview_task_id_input),
                        )
                        if cur.fetchone():
                            print(f"[REFINE_RESOLVE] input is already a valid Meshy task ID: {preview_task_id_input}")
                            resolved = True
                        else:
                            print(f"[REFINE_RESOLVE] FAILED: no upstream mapping for {preview_task_id_input}")
        except Exception as e:
            print(f"[REFINE_RESOLVE] fallback error: {e}")

    if not resolved:
        return jsonify({
            "ok": False,
            "error": "Preview task ID not found or not yet ready. Ensure the preview completed successfully before refining.",
            "code": "PREVIEW_TASK_NOT_FOUND",
        }), 400

    # ── Preflight: verify the preview task still exists on Meshy ──────────
    # Meshy expires preview tasks after ~7 days.  Without this check, the
    # async dispatch burns ~18 s retrying before failing.  Fail fast instead.
    try:
        from backend.services.meshy_service import mesh_get, MeshyTaskNotFoundError
        try:
            upstream = mesh_get(f"/openapi/v2/text-to-3d/{preview_task_id}")
            upstream_status = (upstream.get("status") or "").upper()
            print(f"[REFINE:PREFLIGHT] task_id={preview_task_id} status={upstream_status}")
            if upstream_status == "FAILED":
                return jsonify({
                    "ok": False,
                    "error": "Preview task not found — the preview generation failed. Please generate a new preview first.",
                    "code": "PREVIEW_FAILED_UPSTREAM",
                }), 400
        except MeshyTaskNotFoundError:
            print(f"[REFINE:PREFLIGHT] task_id={preview_task_id} EXPIRED (404 from provider)")
            return jsonify({
                "ok": False,
                "error": "Preview task not found — this model's source data has expired. Please generate a new preview first.",
                "code": "PREVIEW_EXPIRED_UPSTREAM",
            }), 400
    except Exception as e:
        # Network error or import issue — don't block, let the dispatch retry logic handle it
        print(f"[REFINE:PREFLIGHT] check failed ({e}) — proceeding without validation")

    store = load_store()
    preview_meta = get_job_metadata(preview_task_id_input, store)
    if not preview_meta.get("prompt"):
        preview_meta = get_job_metadata(preview_task_id, store)
    original_prompt = preview_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = preview_meta.get("root_prompt") or original_prompt
    texture_prompt = (body.get("texture_prompt") or "").strip() or None
    texture_image_url = (body.get("texture_image_url") or body.get("image_style_url") or "").strip() or None
    enable_pbr = bool(body.get("enable_pbr", True))
    ai_model = (body.get("ai_model") or body.get("model") or "latest").strip() or "latest"
    remove_lighting = None
    if body.get("remove_lighting") is not None:
        remove_lighting = bool(body.get("remove_lighting"))
    texture_style_mode = "image" if texture_image_url else "text"
    # Derive title from prompt/root_prompt - derive_display_title handles generic titles automatically
    explicit_title = body.get("title") or preview_meta.get("title")
    title = derive_display_title(original_prompt, explicit_title, root_prompt=root_prompt)

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["text-to-3d-refine"]
    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "refine",
        "preview_task_id": preview_task_id,
        "enable_pbr": enable_pbr,
        "ai_model": ai_model,
        "texture_style_mode": texture_style_mode,
        "uses_image_style": texture_style_mode == "image",
    }
    if texture_prompt:
        job_meta["texture_prompt"] = texture_prompt
    if remove_lighting is not None:
        job_meta["remove_lighting"] = remove_lighting

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    payload = {
        "mode": "refine",
        "preview_task_id": preview_task_id,
        "enable_pbr": enable_pbr,
        "ai_model": ai_model,
    }
    if texture_prompt:
        payload["texture_prompt"] = texture_prompt
    if texture_image_url:
        payload["texture_image_url"] = texture_image_url
    if remove_lighting is not None:
        payload["remove_lighting"] = remove_lighting

    store_meta = {
        "stage": "refine",
        "preview_task_id": preview_task_id,
        "created_at": now_s() * 1000,
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "texture_prompt": texture_prompt,
        "enable_pbr": enable_pbr,
        "ai_model": ai_model,
        "texture_style_mode": texture_style_mode,
        "uses_image_style": texture_style_mode == "image",
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
    }
    if remove_lighting is not None:
        store_meta["remove_lighting"] = remove_lighting

    # Persist immediately so status polling can return queued while dispatch runs
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    # Persist job row so status polling works across workers
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=original_prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    get_executor().submit(
        _dispatch_meshy_refine_async,
        internal_job_id,
        identity_id,
        reservation_id,
        payload,
        store_meta,
    )

    log_event("text-to-3d/refine:dispatched[mod]", {"internal_job_id": internal_job_id})

    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "source": "modular",
    })


@bp.route("/text-to-3d/remesh-start", methods=["POST", "OPTIONS"])
@with_session
def text_to_3d_remesh_start_mod():
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"ok": False, "error": "prompt required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["remesh"]
    ai_model = body.get("model") or "latest"
    if ai_model in ("meshy-4", "meshy4"):
        return jsonify({"ok": False, "error": "Meshy 4 is no longer supported."}), 400

    payload = {
        "mode": "preview",
        "prompt": prompt,
        "ai_model": ai_model,
        "topology": "triangle",
        "should_remesh": True,
        "target_polycount": body.get("target_polycount", 45000),
    }

    symmetry_mode = (body.get("symmetry_mode") or "").strip().lower()
    if symmetry_mode in {"off", "auto", "on"}:
        payload["symmetry_mode"] = symmetry_mode

    pose_mode = (body.get("pose_mode") or "").strip().lower()
    if pose_mode in {"a-pose", "t-pose"}:
        payload["pose_mode"] = pose_mode

    license_choice = normalize_license(body.get("license"))
    batch_count = clamp_int(body.get("batch_count"), 1, 8, 1)
    batch_slot = clamp_int(body.get("batch_slot"), 1, batch_count, 1)

    title = derive_display_title(prompt, None)
    job_meta = {
        "prompt": prompt,
        "root_prompt": prompt,
        "title": title,
        "stage": "preview",
        "model": ai_model,
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "pose_mode": pose_mode,
        "batch_count": batch_count,
        "batch_slot": batch_slot,
        "remesh_like": True,
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    try:
        resp = mesh_post("/openapi/v2/text-to-3d", payload)
        meshy_task_id = resp.get("result")
        if not meshy_task_id:
            release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            print(f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} error=no_task_id_in_response raw={resp}")
            return jsonify({"ok": False, "error": "MODEL_GENERATION_FAILED", "message": "3D model generation failed. Please try again."}), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_provider_error, MODEL_GENERATION_FAILED
        return jsonify(sanitize_provider_error(
            provider="meshy", error=e, job_id=internal_job_id,
            code=MODEL_GENERATION_FAILED,
        )), 502

    # Use internal_job_id (not meshy_task_id) for credit finalization tracking
    finalize_job_credits(reservation_id, internal_job_id, identity_id)

    store_meta = {
        "stage": "preview",
        "prompt": prompt,
        "root_prompt": prompt,
        "title": title,
        "model": ai_model,
        "created_at": now_s() * 1000,
        "remesh_like": True,
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "pose_mode": pose_mode,
        "batch_count": batch_count,
        "batch_slot": batch_slot,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "upstream_job_id": meshy_task_id,
    }

    store = load_store()
    store[meshy_task_id] = store_meta
    store[internal_job_id] = store_meta  # Also index by internal ID for status lookup
    save_store(store)

    # Persist job row so status polling works across workers
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="processing",
    )
    # Update with upstream job ID since we have it immediately (synchronous call)
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": meshy_task_id,
        "internal_job_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "source": "modular",
    })


@bp.route("/text-to-3d/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def text_to_3d_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("text-to-3d/status:incoming[mod]", {"job_id": job_id})
    if not job_id:
        return jsonify({"error": "job_id required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Short-circuit: return cached response if within TTL (avoids all DB work)
    cached_resp = _get_cached_status(job_id)
    if cached_resp is not None:
        return jsonify(cached_resp)

    identity_id = g.identity_id
    meshy_job_id = job_id
    internal_job = None
    ownership_verified = False  # Track if we already verified ownership via DB

    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, upstream_job_id, error_message, meta, reservation_id
                        FROM timrx_billing.jobs
                        WHERE id::text = %s AND identity_id = %s
                        LIMIT 1
                        """,
                        (job_id, identity_id),
                    )
                    internal_job = cur.fetchone()

            if internal_job:
                ownership_verified = True  # Found in jobs table with matching identity_id
                job_meta = internal_job.get("meta") or {}
                if isinstance(job_meta, str):
                    try:
                        job_meta = __import__('json').loads(job_meta)
                    except Exception:
                        job_meta = {}
                stage_hint = job_meta.get("stage") or "preview"

                if internal_job["status"] == "queued":
                    return jsonify({
                        "status": "queued",
                        "pct": 0,
                        "stage": stage_hint,
                        "message": "Job is being dispatched to provider...",
                        "job_id": job_id,
                    })

                if internal_job["status"] == "failed":
                    return jsonify({
                        "status": "failed",
                        "error": internal_job.get("error_message", "Job failed"),
                        "job_id": job_id,
                    })

                if internal_job["upstream_job_id"]:
                    meshy_job_id = internal_job["upstream_job_id"]
                else:
                    return jsonify({
                        "status": "pending",
                        "pct": 0,
                        "stage": stage_hint,
                        "message": "Waiting for provider response...",
                        "job_id": job_id,
                    })
        except Exception as e:
            print(f"[STATUS][mod] Error checking internal job {job_id}: {e}")

    # If still not verified, check local store for queued jobs (before dispatch finishes)
    if not ownership_verified:
        store = load_store()
        store_meta = store.get(job_id) or {}
        if store_meta:
            job_user_id = store_meta.get("identity_id") or store_meta.get("user_id")
            if identity_id and job_user_id and job_user_id != identity_id:
                return jsonify({"error": "Job not found or access denied"}), 404

            upstream_hint = store_meta.get("upstream_job_id") or store_meta.get("meshy_task_id")
            stage_hint = store_meta.get("stage") or "preview"
            if not upstream_hint:
                return jsonify({
                    "status": "queued",
                    "pct": 0,
                    "stage": stage_hint,
                    "message": "Job is being dispatched to provider...",
                    "job_id": job_id,
                })
            meshy_job_id = upstream_hint
            ownership_verified = True

    # Skip verify_job_ownership if we already verified via timrx_billing.jobs query
    if not ownership_verified and not verify_job_ownership(meshy_job_id, identity_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v2/text-to-3d/{meshy_job_id}")
        log_event("text-to-3d/status:meshy-resp[mod]", ms)
    except MeshyTaskNotFoundError:
        print(f"[MESHY] Task expired: text-to-3d job_id={job_id} meshy_id={meshy_job_id}")
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({"status": "failed", "error": "TASK_EXPIRED", "message": "This generation has expired on the provider."}), 200
    except Exception as e:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={meshy_job_id} error={e}")
        return jsonify({"error": "MODEL_GENERATION_FAILED", "message": "Failed to fetch job status. Please try again."}), 502

    out = normalize_status(ms)
    log_status_summary("text-to-3d[mod]", job_id, out)

    store = load_store()
    meta = (store.get(job_id) or store.get(meshy_job_id)
            or get_job_metadata(meshy_job_id, store) or get_job_metadata(job_id, store) or {})
    if identity_id and not meta.get("identity_id"):
        meta["identity_id"] = identity_id
        meta["user_id"] = identity_id

    for key in ("batch_count", "batch_slot", "batch_group_id", "license", "symmetry_mode", "pose_mode"):
        if key in meta and key not in out:
            out[key] = meta.get(key)
    meta.update({"last_status": out["status"], "last_pct": out["pct"], "stage": out["stage"]})
    if out.get("glb_url"):
        meta["glb_url"] = out["glb_url"]
    if out.get("thumbnail_url"):
        meta["thumbnail_url"] = out["thumbnail_url"]
    store[job_id] = meta
    save_store(store)

    # Auto-refund on async failure
    if out["status"] == "failed":
        try:
            from backend.services.credits_helper import refund_failed_job
            refund_failed_job(job_id)
        except Exception as e:
            print(f"[text-to-3d/status] auto-refund failed: {e}")

    if out["status"] == "done" and (out.get("glb_url") or out.get("thumbnail_url")):
        user_id = meta.get("identity_id") or meta.get("user_id") or getattr(g, 'identity_id', None)
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type="text-to-3d", user_id=user_id)

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

            # Resolve reservation_id from store meta first, then fall back to
            # the DB job row's JSONB meta or reservation_id column.  This covers
            # the case where the in-memory store was lost (server restart) but
            # the DB row was persisted before dispatch.
            reservation_id = meta.get("reservation_id")
            if not reservation_id and internal_job:
                _db_meta = internal_job.get("meta") or {}
                if isinstance(_db_meta, str):
                    try:
                        _db_meta = __import__('json').loads(_db_meta)
                    except Exception:
                        _db_meta = {}
                reservation_id = _db_meta.get("reservation_id") or internal_job.get("reservation_id")
            # internal_job_id from store meta, or from the DB job row if store
            # was lost (worker restart / cross-worker status poll)
            internal_job_id = (
                meta.get("internal_job_id")
                or (str(internal_job["id"]) if internal_job else None)
            )
            user_id = meta.get("identity_id") or meta.get("user_id") or getattr(g, 'identity_id', None)
            if reservation_id:
                effective_job_id = internal_job_id or job_id
                # Short-circuit: skip DB-heavy finalize if already done in-process
                _fin_ts = _finalized_jobs.get(effective_job_id)
                if _fin_ts is None or (_time.monotonic() - _fin_ts) > _FINALIZED_TTL:
                    finalize_job_credits(reservation_id, effective_job_id, user_id)
                    _finalized_jobs[effective_job_id] = _time.monotonic()

            if internal_job_id:
                _update_job_status_ready(
                    internal_job_id,
                    upstream_job_id=meshy_job_id,
                    model_id=s3_result.get("model_id"),
                    glb_url=s3_result.get("glb_url"),
                )
                # Include generation duration in the response for frontend display
                try:
                    from backend.db import query_one as _q1
                    _dur_row = _q1(
                        f"SELECT generation_duration_ms FROM {Tables.JOBS} WHERE id = %s",
                        (internal_job_id,),
                    )
                    if _dur_row and _dur_row.get("generation_duration_ms"):
                        out["generation_duration_ms"] = _dur_row["generation_duration_ms"]
                except Exception:
                    pass  # Non-critical

    # If DB has the finalized model, prefer S3 URLs for frontend rendering.
    if USE_DB and identity_id:
        try:
            canonical = get_canonical_model_row(
                identity_id,
                upstream_job_id=job_id,
                alt_upstream_job_id=meshy_job_id,
            )
            if canonical:
                if canonical.get("glb_url"):
                    out["glb_url"] = canonical["glb_url"]
                    if out.get("textured_glb_url"):
                        out["textured_glb_url"] = canonical["glb_url"]
                    if out.get("rigged_character_glb_url"):
                        out["rigged_character_glb_url"] = canonical["glb_url"]
                if canonical.get("thumbnail_url"):
                    out["thumbnail_url"] = canonical["thumbnail_url"]
                if canonical.get("model_urls"):
                    out["model_urls"] = canonical["model_urls"]
                if canonical.get("textured_model_urls"):
                    out["textured_model_urls"] = canonical["textured_model_urls"]
        except Exception as e:
            print(f"[text-to-3d][mod] DB lookup for finalized model failed: {e}")

    if out["status"] == "failed":
        reservation_id = meta.get("reservation_id")
        internal_job_id = meta.get("internal_job_id")
        error_msg = out.get("message") or out.get("error") or "Provider job failed"

        if reservation_id:
            release_job_credits(reservation_id, "provider_job_failed", job_id)

        if internal_job_id:
            _update_job_status_failed(internal_job_id, error_msg)

    # Cache status responses via the shared thread-safe cache.
    # Terminal states (done/failed) get a longer TTL since they won't change.
    _set_status_cache(job_id, out, is_terminal=(out.get("status") in ("done", "failed")))

    return jsonify(out)


@bp.route("/text-to-3d/list", methods=["GET", "OPTIONS"])
@with_session_readonly
def text_to_3d_list_mod():
    """List text-to-3d jobs for the current authenticated user only."""
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    store = load_store()
    user_jobs = [
        jid for jid, meta in store.items()
        if isinstance(meta, dict)
        and (meta.get("identity_id") or meta.get("user_id")) == identity_id
    ]
    return jsonify(user_jobs)
