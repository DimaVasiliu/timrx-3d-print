"""
Rigging & Animation Routes Blueprint (Modular)
-----------------------------------------------
Registered under /api/_mod.

Routes:
  POST /rig/start                          — start a rigging task
  GET  /rig/status/<job_id>                — poll rigging status
  GET  /rig/stream/<job_id>                — SSE stream rigging progress
  POST /rig/animate                        — start an animation task
  GET  /rig/animate/status/<job_id>        — poll animation status
  GET  /rig/animate/stream/<job_id>        — SSE stream animation progress
  GET  /rig/animations/library             — animation library catalog
"""

from __future__ import annotations

import json
import uuid

from flask import Blueprint, Response, jsonify, request, g

from backend.config import ACTION_KEYS, MESHY_API_KEY
from backend.db import USE_DB
from backend.middleware import with_session
from backend.services.async_dispatch import update_job_with_upstream_id
from backend.services.credits_helper import (
    finalize_job_credits,
    get_current_balance,
    release_job_credits,
    start_paid_job,
)
from backend.services.identity_service import require_identity
from backend.services.job_service import (
    create_internal_job_row,
    get_job_metadata,
    load_store,
    save_store,
    verify_job_ownership_detailed,
)
from backend.services.meshy_service import build_source_payload
from backend.services.rigging_service import (
    create_rigging_task,
    get_rigging_task,
    stream_rigging_task,
    normalize_rigging_response,
    create_animation_task,
    get_animation_task,
    stream_animation_task,
    normalize_animation_response,
)
from backend.utils.helpers import log_event, log_status_summary, now_s

bp = Blueprint("rigging", __name__)


# ─── POST /rig/start ────────────────────────────────────────────────────────

@bp.route("/rig/start", methods=["POST", "OPTIONS"])
@with_session
def rig_start():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("rig/start:incoming", body)

    source, err = build_source_payload(body, identity_id=identity_id)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    # Parse optional height_meters (default 1.7)
    try:
        height_meters = float(body.get("height_meters", 1.7))
        if height_meters <= 0:
            height_meters = 1.7
    except (TypeError, ValueError):
        height_meters = 1.7

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["rigging"]

    job_meta = {
        "stage": "rig",
        "height_meters": height_meters,
        "source_task_id": source.get("input_task_id"),
        "model_url": source.get("model_url"),
    }

    reservation_id, credit_error = start_paid_job(
        identity_id, action_key, internal_job_id, job_meta
    )
    if credit_error:
        return credit_error

    # Persist job row so status polling / ownership checks work
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt="",
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    try:
        resp = create_rigging_task(source, height_meters=height_meters)
        log_event("rig/start:meshy-resp", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            print(
                f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} "
                f"error=no_task_id_in_response raw={resp}"
            )
            return jsonify({
                "ok": False,
                "error": "MODEL_GENERATION_FAILED",
                "message": "Rigging failed. Please try again.",
            }), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_provider_error, MODEL_GENERATION_FAILED
        # Surface actionable face-limit errors instead of generic message
        err_str = str(e)
        user_msg = None
        if "face limit" in err_str.lower() or "exceeds the" in err_str.lower():
            user_msg = "Model has too many faces for rigging (max 300K). Please remesh the model first, then try rigging again."
        return jsonify(sanitize_provider_error(
            provider="meshy", error=e, job_id=internal_job_id,
            code=MODEL_GENERATION_FAILED,
            message=user_msg,
        )), 502

    finalize_job_credits(reservation_id, internal_job_id, identity_id)
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

    store = load_store()
    store[meshy_task_id] = {
        "stage": "rig",
        "created_at": now_s() * 1000,
        "height_meters": height_meters,
        "source_task_id": source.get("input_task_id"),
        "model_url": source.get("model_url"),
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
    }
    save_store(store)

    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": meshy_task_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "source": "modular",
    })


# ─── GET /rig/status/<job_id> ───────────────────────────────────────────────

@bp.route("/rig/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def rig_status(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("rig/status:incoming", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id = g.identity_id
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    try:
        ms = get_rigging_task(job_id)
        log_event("rig/status:meshy-resp", ms)
    except Exception as e:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} error={e}")
        return jsonify({
            "error": "MODEL_GENERATION_FAILED",
            "message": "Failed to fetch rigging status. Please try again.",
        }), 502

    out = normalize_rigging_response(ms)
    log_status_summary("rig/status", job_id, out)

    # Persist rigging outputs when done
    if out["status"] == "done" and (
        out.get("rigged_character_glb_url") or out.get("rigged_character_fbx_url")
    ):
        try:
            from backend.services.s3_service import save_finished_job_to_normalized_db

            store = load_store()
            meta = get_job_metadata(job_id, store)
            if identity_id and not meta.get("identity_id"):
                meta["identity_id"] = identity_id
                meta["user_id"] = identity_id

            user_id = (
                meta.get("identity_id")
                or meta.get("user_id")
                or getattr(g, "identity_id", None)
            )
            s3_result = save_finished_job_to_normalized_db(
                job_id, out, meta, job_type="rig", user_id=user_id
            )
            if s3_result and s3_result.get("success"):
                if s3_result.get("rigged_character_glb_url"):
                    out["rigged_character_glb_url"] = s3_result["rigged_character_glb_url"]
                if s3_result.get("rigged_character_fbx_url"):
                    out["rigged_character_fbx_url"] = s3_result["rigged_character_fbx_url"]
                if s3_result.get("glb_url"):
                    out["glb_url"] = s3_result["glb_url"]
                if s3_result.get("thumbnail_url"):
                    out["thumbnail_url"] = s3_result["thumbnail_url"]
                if s3_result.get("db_ok") is False:
                    out["db_ok"] = False
                    out["db_errors"] = s3_result.get("db_errors")
        except Exception as e:
            print(f"[rig/status] persist failed: {e}")

    return jsonify(out)


# ─── GET /rig/stream/<job_id> — SSE proxy for rigging ──────────────────────

@bp.route("/rig/stream/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def rig_stream(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id = g.identity_id
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    def generate():
        try:
            for line in stream_rigging_task(job_id):
                if line:
                    yield line.decode("utf-8", errors="replace") + "\n"
                else:
                    yield "\n"
        except Exception as e:
            error_event = f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
            yield error_event

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ─── POST /rig/animate ──────────────────────────────────────────────────────

@bp.route("/rig/animate", methods=["POST", "OPTIONS"])
@with_session
def rig_animate():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("rig/animate:incoming", body)

    # Accept both old field name (rigging_task_id) and correct Meshy name (rig_task_id)
    rig_task_id = (body.get("rig_task_id") or body.get("rigging_task_id") or "").strip()

    # action_id must be an integer (Meshy animation library ID)
    raw_action_id = body.get("action_id")
    if raw_action_id is None:
        return jsonify({"ok": False, "error": "action_id required (integer)"}), 400
    try:
        action_id = int(raw_action_id)
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "action_id must be an integer"}), 400

    if not rig_task_id:
        return jsonify({"ok": False, "error": "rig_task_id required"}), 400

    # Optional post_process
    post_process = body.get("post_process")
    if post_process and not isinstance(post_process, dict):
        post_process = None

    # Resolve internal IDs to Meshy IDs when needed
    try:
        from backend.services.job_service import resolve_meshy_job_id, verify_job_ownership
        resolved_id = resolve_meshy_job_id(rig_task_id)
        if not verify_job_ownership(resolved_id, identity_id):
            return jsonify({"ok": False, "error": "Job not found or access denied"}), 403
        rig_task_id = resolved_id
    except Exception:
        pass  # Use rig_task_id as-is

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["animation"]

    job_meta = {
        "stage": "animate",
        "rig_task_id": rig_task_id,
        "action_id": action_id,
    }
    if post_process:
        job_meta["post_process"] = post_process

    reservation_id, credit_error = start_paid_job(
        identity_id, action_key, internal_job_id, job_meta
    )
    if credit_error:
        return credit_error

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt="",
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    try:
        resp = create_animation_task(rig_task_id, action_id, post_process=post_process)
        log_event("rig/animate:meshy-resp", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            print(
                f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} "
                f"error=no_task_id_in_response raw={resp}"
            )
            return jsonify({
                "ok": False,
                "error": "MODEL_GENERATION_FAILED",
                "message": "Animation failed. Please try again.",
            }), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_provider_error, MODEL_GENERATION_FAILED
        return jsonify(sanitize_provider_error(
            provider="meshy", error=e, job_id=internal_job_id,
            code=MODEL_GENERATION_FAILED,
        )), 502

    finalize_job_credits(reservation_id, internal_job_id, identity_id)
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

    store = load_store()
    store[meshy_task_id] = {
        "stage": "animate",
        "created_at": now_s() * 1000,
        "rig_task_id": rig_task_id,
        "action_id": action_id,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
    }
    save_store(store)

    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": meshy_task_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "source": "modular",
    })


# ─── GET /rig/animate/status/<job_id> ───────────────────────────────────────

@bp.route("/rig/animate/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def rig_animate_status(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("rig/animate/status:incoming", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id = g.identity_id
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    try:
        ms = get_animation_task(job_id)
        log_event("rig/animate/status:meshy-resp", ms)
    except Exception as e:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} error={e}")
        return jsonify({
            "error": "MODEL_GENERATION_FAILED",
            "message": "Failed to fetch animation status. Please try again.",
        }), 502

    out = normalize_animation_response(ms)
    log_status_summary("rig/animate/status", job_id, out)

    # Persist animation outputs when done (same pattern as rigging)
    if out["status"] == "done" and (
        out.get("animation_glb_url") or out.get("animation_fbx_url")
    ):
        try:
            from backend.services.s3_service import save_finished_job_to_normalized_db

            store = load_store()
            meta = get_job_metadata(job_id, store)
            if identity_id and not meta.get("identity_id"):
                meta["identity_id"] = identity_id
                meta["user_id"] = identity_id

            user_id = (
                meta.get("identity_id")
                or meta.get("user_id")
                or getattr(g, "identity_id", None)
            )
            s3_result = save_finished_job_to_normalized_db(
                job_id, out, meta, job_type="animate", user_id=user_id
            )
            if s3_result and s3_result.get("success"):
                for key in (
                    "animation_glb_url", "animation_fbx_url",
                    "processed_usdz_url", "processed_armature_fbx_url",
                    "processed_animation_fps_fbx_url",
                    "glb_url", "thumbnail_url",
                ):
                    if s3_result.get(key):
                        out[key] = s3_result[key]
                if s3_result.get("db_ok") is False:
                    out["db_ok"] = False
                    out["db_errors"] = s3_result.get("db_errors")
        except Exception as e:
            print(f"[rig/animate/status] persist failed: {e}")

    return jsonify(out)


# ─── GET /rig/animate/stream/<job_id> — SSE proxy for animation ─────────────

@bp.route("/rig/animate/stream/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def rig_animate_stream(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id = g.identity_id
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    def generate():
        try:
            for line in stream_animation_task(job_id):
                if line:
                    yield line.decode("utf-8", errors="replace") + "\n"
                else:
                    yield "\n"
        except Exception as e:
            error_event = f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"
            yield error_event

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ─── GET /rig/animations/library — curated animation catalog ───────────────

@bp.route("/rig/animations/library", methods=["GET", "OPTIONS"])
@with_session
def animation_library():
    if request.method == "OPTIONS":
        return ("", 204)

    from backend.services.animation_library import get_animation_library
    catalog = get_animation_library()

    # Optional filtering
    category = request.args.get("category", "").strip()
    search = request.args.get("q", "").strip().lower()
    enabled_only = request.args.get("enabled", "true").lower() != "false"

    items = catalog
    if enabled_only:
        items = [a for a in items if a.get("enabled", True)]
    if category:
        items = [a for a in items if a.get("category", "").lower() == category.lower()]
    if search:
        items = [
            a for a in items
            if search in a.get("name", "").lower()
            or search in a.get("category", "").lower()
            or search in a.get("subcategory", "").lower()
            or any(search in t.lower() for t in a.get("tags", []))
        ]

    return jsonify({"ok": True, "count": len(items), "items": items})
