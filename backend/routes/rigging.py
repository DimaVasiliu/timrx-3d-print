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
from backend.middleware import with_session, with_session_readonly
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
from backend.services.meshy_service import build_source_payload, MeshyTaskNotFoundError, terminalize_expired_meshy_job
from backend.services.status_cache import get_cached_status, cache_status
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


# ─── POST /rig/preflight ─────────────────────────────────────────────────────

@bp.route("/rig/preflight", methods=["POST", "OPTIONS"])
@with_session
def rig_preflight():
    """
    Pre-flight check before rigging submission.

    Validates:
    - Source model resolution (input_task_id or model_url)
    - Face count against Meshy's 300K limit
    - Whether the source model still exists upstream
    - Whether model is already rigged

    Returns riggable=true/false with reason + recommended_action.
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    print(f"[RIG_PREFLIGHT] identity={identity_id} body_keys={list(body.keys())}")

    # Resolve source model
    source, err = build_source_payload(body, identity_id=identity_id, prefer="input_task_id")
    if err:
        print(f"[RIG_PREFLIGHT] source_resolution_failed: {err}")
        return jsonify({
            "ok": True,
            "riggable": False,
            "reason": f"Source model not available: {err}",
            "recommended_action": "unsupported",
            "source": None,
            "face_count": None,
            "vertex_count": None,
        })

    result = {
        "ok": True,
        "riggable": True,
        "reason": None,
        "recommended_action": "proceed",
        "source": {
            "input_task_id": source.get("input_task_id"),
            "model_url": source.get("model_url"),
        },
        "face_count": None,
        "vertex_count": None,
        "already_rigged": False,
    }

    # Check if the source is already a rigged model
    source_task_id = source.get("input_task_id")
    if source_task_id and USE_DB:
        try:
            from backend.db import get_conn, Tables
            from psycopg.rows import dict_row
            with get_conn() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Check if this task ID is already a rig result
                    cur.execute(
                        f"""
                        SELECT payload->>'stage' as stage
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE identity_id = %s
                          AND (payload->>'original_job_id' = %s
                               OR payload->>'source_task_id' = %s)
                          AND payload->>'stage' = 'rig'
                        LIMIT 1
                        """,
                        (identity_id, source_task_id, source_task_id),
                    )
                    if cur.fetchone():
                        result["already_rigged"] = True
                        print(f"[RIG_PREFLIGHT] model already rigged: task={source_task_id}")
        except Exception as e:
            print(f"[RIG_PREFLIGHT] already-rigged check failed: {e}")

    # Try to get face count from Meshy task metadata
    if source_task_id:
        try:
            from backend.services.meshy_service import mesh_get
            task_info = mesh_get(f"/openapi/v1/text-to-3d/{source_task_id}")
            # Meshy may return topology info in task metadata
            face_count = None
            vertex_count = None
            # Check output containers for topology info
            for container_key in ["output", "result", ""]:
                container = task_info.get(container_key, task_info) if container_key else task_info
                if isinstance(container, dict):
                    face_count = face_count or container.get("face_count") or container.get("faces")
                    vertex_count = vertex_count or container.get("vertex_count") or container.get("vertices")
            if face_count:
                result["face_count"] = int(face_count)
                result["vertex_count"] = int(vertex_count) if vertex_count else None
                if result["face_count"] > 300000:
                    result["riggable"] = False
                    result["reason"] = f"Model has {result['face_count']:,} faces (limit: 300,000). Remesh the model first."
                    result["recommended_action"] = "remesh_first"
                    print(f"[RIG_PREFLIGHT] too_many_faces: {result['face_count']} > 300K")
            print(f"[RIG_PREFLIGHT] source={source_task_id} faces={face_count} vertices={vertex_count} riggable={result['riggable']}")
        except Exception as e:
            # Non-fatal: can't get face count from this endpoint, proceed anyway
            err_str = str(e).lower()
            if "not found" in err_str or "404" in err_str:
                # Try image-to-3d endpoint
                try:
                    task_info = mesh_get(f"/openapi/v1/image-to-3d/{source_task_id}")
                    print(f"[RIG_PREFLIGHT] found via image-to-3d: task={source_task_id}")
                except Exception:
                    print(f"[RIG_PREFLIGHT] task_lookup_failed (may be fine for uploaded models): {e}")
            else:
                print(f"[RIG_PREFLIGHT] task_info_error: {e}")

    print(f"[RIG_PREFLIGHT] result: riggable={result['riggable']} action={result['recommended_action']} faces={result['face_count']}")
    return jsonify(result)


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
    print(f"[RIG_SUBMIT] identity={identity_id} keys={list(body.keys())}")

    # Prefer input_task_id for rigging — Meshy's original model retains textures.
    # S3 model_url is often a decimated/remeshed GLB without materials.
    # Falls back to model_url automatically if input_task_id resolution fails.
    source, err = build_source_payload(body, identity_id=identity_id, prefer="input_task_id")
    if err:
        print(f"[RIG_SOURCE] FAILED identity={identity_id} error={err}")
        return jsonify({"ok": False, "error": err}), 400
    print(f"[RIG_SOURCE] resolved: input_task_id={source.get('input_task_id')} model_url={'yes' if source.get('model_url') else 'no'}")

    # Parse optional height_meters (default 1.7)
    try:
        height_meters = float(body.get("height_meters", 1.7))
        if height_meters <= 0:
            height_meters = 1.7
    except (TypeError, ValueError):
        height_meters = 1.7

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["rigging"]

    # Accept optional prompt/title from frontend for title persistence
    rig_prompt = (body.get("prompt") or body.get("title") or "").strip()

    job_meta = {
        "stage": "rig",
        "height_meters": height_meters,
        "source_task_id": source.get("input_task_id"),
        "model_url": source.get("model_url"),
        "prompt": rig_prompt or None,
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
        err_str = str(e).lower()

        # Auto-retry with model_url when input_task_id expired on Meshy
        if ("input task not found" in err_str or "task not found" in err_str) and source.get("input_task_id"):
            fallback_url = body.get("model_url", "").strip()
            if fallback_url:
                print(f"[RIG_SUBMIT] input_task_id expired, retrying with model_url")
                fallback_source, fallback_err = build_source_payload(
                    {"model_url": fallback_url}, identity_id=identity_id, prefer="model_url"
                )
                if not fallback_err and fallback_source:
                    try:
                        resp = create_rigging_task(fallback_source, height_meters=height_meters)
                        meshy_task_id = resp.get("result") or resp.get("id")
                        if meshy_task_id:
                            print(f"[RIG_SUBMIT] model_url fallback succeeded: {meshy_task_id}")
                            # Skip to the success path below
                            source = fallback_source
                        else:
                            meshy_task_id = None
                    except Exception as retry_err:
                        print(f"[RIG_SUBMIT] model_url fallback also failed: {retry_err}")
                        meshy_task_id = None
                else:
                    meshy_task_id = None

                if meshy_task_id:
                    # Fall through to normal success path (after the except block)
                    pass
                else:
                    release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
                    return jsonify({
                        "ok": False,
                        "error": "MODEL_GENERATION_FAILED",
                        "message": "The source model has expired on Meshy and the fallback URL also failed. Please generate a new model or upload a GLB file directly.",
                    }), 502
            else:
                release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
                return jsonify({
                    "ok": False,
                    "error": "MODEL_GENERATION_FAILED",
                    "message": "The source model has expired or is no longer available on Meshy. Please generate a new model or upload a GLB file directly.",
                }), 502
        else:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
            from backend.services.error_sanitizer import sanitize_provider_error, MODEL_GENERATION_FAILED
            user_msg = None
            if "face limit" in err_str or "exceeds the" in err_str:
                user_msg = "Model has too many faces for rigging (max 300K). Please remesh the model first, then try rigging again."
            elif "pose estimation" in err_str:
                user_msg = "Rigging failed: could not detect a humanoid pose. Make sure the model is a clear bipedal character with visible limbs."
            elif "400" in err_str and "model_url" in err_str:
                user_msg = "The model URL is not accessible. Please try uploading the file directly."
            return jsonify(sanitize_provider_error(
                provider="meshy", error=e, job_id=internal_job_id,
                code=MODEL_GENERATION_FAILED,
                message=user_msg,
            )), 502

    # Credits: do NOT finalize now — rigging is async.
    # Finalize on terminal success in status endpoint; release on failure.
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

    store = load_store()
    store[meshy_task_id] = {
        "stage": "rig",
        "created_at": now_s() * 1000,
        "height_meters": height_meters,
        "source_task_id": source.get("input_task_id"),
        "model_url": source.get("model_url"),
        "prompt": rig_prompt or None,
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
@with_session_readonly
def rig_status(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Short-circuit: return cached response if within TTL
    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    import time as _time
    _t0 = _time.monotonic()

    identity_id = g.identity_id
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    _t_own = _time.monotonic()
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    try:
        ms = get_rigging_task(job_id)
    except MeshyTaskNotFoundError:
        print(f"[MESHY] Task expired: rig job_id={job_id}")
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({
            "status": "failed",
            "error": "TASK_EXPIRED",
            "message": "This rigging task has expired on the provider.",
            "job_id": job_id,
        }), 200
    except Exception as e:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} error={e}")
        return jsonify({
            "error": "MODEL_GENERATION_FAILED",
            "message": "Failed to fetch rigging status. Please try again.",
        }), 502

    _t_meshy = _time.monotonic()
    out = normalize_rigging_response(ms)

    # Compact timing log with Meshy's raw status and queue position
    _elapsed_own = int((_t_own - _t0) * 1000)
    _elapsed_meshy = int((_t_meshy - _t_own) * 1000)
    _queue = out.get("preceding_tasks")
    _queue_str = f" queue={_queue}" if _queue is not None else ""
    print(
        f"[rig/status] job={job_id} status={out['status']}({out.get('meshy_status','?')}) "
        f"pct={out['pct']}{_queue_str} "
        f"own={_elapsed_own}ms meshy={_elapsed_meshy}ms src={ownership.get('source','?')}"
    )

    # For non-final states, cache and return immediately — no S3 work, no heavy logging
    if out["status"] not in ("done", "failed"):
        cache_status(job_id, out, is_terminal=False)
        return jsonify(out)

    # ── Async credit handling (same pattern as retexture/remesh) ──
    if out["status"] == "failed":
        try:
            from backend.services.credits_helper import refund_failed_job
            refund_failed_job(job_id)
        except Exception as e:
            print(f"[rig/status] auto-refund failed: {e}")

    if out["status"] == "done":
        try:
            store_for_credits = load_store()
            meta_for_credits = get_job_metadata(job_id, store_for_credits)
            res_id = meta_for_credits.get("reservation_id")
            int_job = meta_for_credits.get("internal_job_id") or job_id
            cred_identity = meta_for_credits.get("identity_id") or identity_id
            if res_id:
                from backend.services.credits_helper import finalize_job_credits
                finalize_job_credits(res_id, int_job, cred_identity)
        except Exception as e:
            print(f"[rig/status] credit finalize on done failed: {e}")

    # ── Final state: persist rigging outputs ──
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
                    print(
                        f"[ASSET_SAVE] FAIL rig job={job_id} "
                        f"model_id={s3_result.get('model_id')} "
                        f"db_errors={s3_result.get('db_errors')}"
                    )
                else:
                    print(
                        f"[RIG_DONE] job={job_id} "
                        f"model_id={s3_result.get('model_id')} "
                        f"glb={'yes' if s3_result.get('glb_url') else 'no'} "
                        f"thumb={'yes' if s3_result.get('thumbnail_url') else 'no'} "
                        f"db=ok"
                    )
            else:
                print(f"[ASSET_SAVE] FAIL rig job={job_id} save_returned={s3_result}")
        except Exception as e:
            print(f"[ASSET_SAVE] ERROR rig job={job_id} error={e}")

    cache_status(job_id, out, is_terminal=True)
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
    print(f"[ANIM_SUBMIT] identity={identity_id} rig_task_id={body.get('rig_task_id')} action_id={body.get('action_id')}")

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

    # Accept optional prompt/title from frontend for title persistence
    anim_prompt = (body.get("prompt") or body.get("title") or "").strip()

    job_meta = {
        "stage": "animate",
        "rig_task_id": rig_task_id,
        "action_id": action_id,
        "source_task_id": rig_task_id,  # parent lookup uses this to inherit title
        "prompt": anim_prompt or None,
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

    # Credits: do NOT finalize now — animation is async.
    # Finalize on terminal success in status endpoint; release on failure.
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

    store = load_store()
    store[meshy_task_id] = {
        "stage": "animate",
        "created_at": now_s() * 1000,
        "rig_task_id": rig_task_id,
        "action_id": action_id,
        "source_task_id": rig_task_id,
        "prompt": anim_prompt or None,
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
@with_session_readonly
def rig_animate_status(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Short-circuit: return cached response if within TTL
    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    import time as _time
    _t0 = _time.monotonic()

    identity_id = g.identity_id
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    _t_own = _time.monotonic()
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    try:
        ms = get_animation_task(job_id)
    except MeshyTaskNotFoundError:
        print(f"[MESHY] Task expired: anim job_id={job_id}")
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({
            "status": "failed",
            "error": "TASK_EXPIRED",
            "message": "This animation task has expired on the provider.",
            "job_id": job_id,
        }), 200
    except Exception as e:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} error={e}")
        return jsonify({
            "error": "MODEL_GENERATION_FAILED",
            "message": "Failed to fetch animation status. Please try again.",
        }), 502

    _t_meshy = _time.monotonic()
    out = normalize_animation_response(ms)

    _elapsed_own = int((_t_own - _t0) * 1000)
    _elapsed_meshy = int((_t_meshy - _t_own) * 1000)
    _queue = out.get("preceding_tasks")
    _queue_str = f" queue={_queue}" if _queue is not None else ""
    print(
        f"[anim/status] job={job_id} status={out['status']}({out.get('meshy_status','?')}) "
        f"pct={out['pct']}{_queue_str} "
        f"own={_elapsed_own}ms meshy={_elapsed_meshy}ms src={ownership.get('source','?')}"
    )

    # For non-final states, cache and return immediately
    if out["status"] not in ("done", "failed"):
        cache_status(job_id, out, is_terminal=False)
        return jsonify(out)

    # ── Async credit handling ──
    if out["status"] == "failed":
        try:
            from backend.services.credits_helper import refund_failed_job
            refund_failed_job(job_id)
        except Exception as e:
            print(f"[anim/status] auto-refund failed: {e}")

    if out["status"] == "done":
        try:
            store_for_credits = load_store()
            meta_for_credits = get_job_metadata(job_id, store_for_credits)
            res_id = meta_for_credits.get("reservation_id")
            int_job = meta_for_credits.get("internal_job_id") or job_id
            cred_identity = meta_for_credits.get("identity_id") or identity_id
            if res_id:
                from backend.services.credits_helper import finalize_job_credits
                finalize_job_credits(res_id, int_job, cred_identity)
        except Exception as e:
            print(f"[anim/status] credit finalize on done failed: {e}")

    # ── Final state: persist animation outputs ──
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
                    print(
                        f"[ASSET_SAVE] FAIL anim job={job_id} "
                        f"model_id={s3_result.get('model_id')} "
                        f"db_errors={s3_result.get('db_errors')}"
                    )
                else:
                    print(
                        f"[ANIM_DONE] job={job_id} "
                        f"model_id={s3_result.get('model_id')} "
                        f"glb={'yes' if s3_result.get('glb_url') else 'no'} "
                        f"thumb={'yes' if s3_result.get('thumbnail_url') else 'no'} "
                        f"db=ok"
                    )
            else:
                print(f"[ASSET_SAVE] FAIL anim job={job_id} save_returned={s3_result}")
        except Exception as e:
            print(f"[ASSET_SAVE] ERROR anim job={job_id} error={e}")

    cache_status(job_id, out, is_terminal=True)
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

# ─── PATCH /rig/thumbnail/<job_id> — persist captured thumbnail to S3 + DB ──

@bp.route("/rig/thumbnail/<job_id>", methods=["PATCH", "OPTIONS"])
@with_session
def rig_thumbnail(job_id: str):
    """
    Persist a frontend-captured thumbnail for a rig/animate asset.

    The viewer captures a data URL after model loads. This endpoint uploads it
    to S3 and updates models + history_items so thumbnails survive page reload.
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    thumbnail_data_url = (body.get("thumbnail_url") or "").strip()
    if not thumbnail_data_url:
        return jsonify({"ok": False, "error": "thumbnail_url required"}), 400
    if not thumbnail_data_url.startswith("data:"):
        return jsonify({"ok": False, "error": "Only data: URLs accepted (base64 thumbnail)"}), 400

    # Verify ownership
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"ok": False, "error": "Job not found"}), 404
    if not ownership["authorized"]:
        return jsonify({"ok": False, "error": "Access denied"}), 403

    # Upload data URL to S3
    try:
        from backend.services.s3_service import ensure_s3_url_for_data_uri
        s3_thumbnail_url = ensure_s3_url_for_data_uri(
            thumbnail_data_url,
            "thumbnails",
            f"thumbnails/{identity_id}/{job_id}",
            user_id=identity_id,
            name="thumbnail",
            provider="meshy",
        )
        if not s3_thumbnail_url:
            print(f"[RIG_THUMB] S3 upload returned None for job={job_id}")
            return jsonify({"ok": False, "error": "Thumbnail upload failed"}), 500
    except Exception as e:
        print(f"[RIG_THUMB] S3 upload error for job={job_id}: {e}")
        return jsonify({"ok": False, "error": "Thumbnail upload failed"}), 500

    # Update models + history_items in DB
    updated_tables = []
    if USE_DB:
        try:
            from backend.db import transaction, Tables
            from backend.services.s3_service import parse_s3_key
            s3_key = parse_s3_key(s3_thumbnail_url) if s3_thumbnail_url else None
            with transaction("rig_thumbnail_update") as cur:
                # Update models table
                cur.execute(
                    f"""
                    UPDATE {Tables.MODELS}
                    SET thumbnail_url = %s,
                        thumbnail_s3_key = COALESCE(%s, thumbnail_s3_key),
                        updated_at = NOW()
                    WHERE upstream_job_id = %s AND identity_id = %s
                    RETURNING id
                    """,
                    (s3_thumbnail_url, s3_key, job_id, identity_id),
                )
                model_row = cur.fetchone()
                if model_row:
                    updated_tables.append("models")

                # Update history_items table
                cur.execute(
                    f"""
                    UPDATE {Tables.HISTORY_ITEMS}
                    SET thumbnail_url = %s, updated_at = NOW()
                    WHERE identity_id = %s
                      AND (payload->>'original_job_id' = %s
                           OR id::text = %s)
                    RETURNING id
                    """,
                    (s3_thumbnail_url, identity_id, job_id, job_id),
                )
                hist_row = cur.fetchone()
                if hist_row:
                    updated_tables.append("history_items")
            # transaction() auto-commits on success.
        except Exception as e:
            print(f"[RIG_THUMB] DB update error for job={job_id}: {e}")

    print(
        f"[RIG_THUMB] job={job_id} "
        f"s3_url={s3_thumbnail_url[:60]}... "
        f"updated={','.join(updated_tables) or 'none'}"
    )
    return jsonify({
        "ok": True,
        "thumbnail_url": s3_thumbnail_url,
        "updated": updated_tables,
    })


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
