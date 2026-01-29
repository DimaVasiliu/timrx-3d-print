"""
Text-to-3D Routes Blueprint (Modular)
------------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

import uuid

from flask import Blueprint, jsonify, request, g

from backend.config import ACTION_KEYS, DEFAULT_MODEL_TITLE, MESHY_API_KEY
from backend.db import USE_DB
from backend.middleware import with_session
from backend.services.async_dispatch import (
    _dispatch_meshy_refine_async,
    _dispatch_meshy_text_to_3d_async,
    get_executor,
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
from backend.services.meshy_service import mesh_get, mesh_post, normalize_status
from backend.services.s3_service import save_finished_job_to_normalized_db
from backend.services.history_service import get_canonical_model_row
from backend.utils.helpers import clamp_int, log_event, log_status_summary, normalize_license, now_s

bp = Blueprint("text_to_3d", __name__)


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
    payload = {
        "mode": "preview",
        "prompt": prompt,
        "ai_model": body.get("model") or "latest",
    }

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

    job_meta = {
        "prompt": prompt,
        "root_prompt": prompt,
        "title": prompt[:50] if prompt else None,
        "stage": "preview",
        "art_style": art_style or "realistic",
        "model": payload["ai_model"],
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "is_a_t_pose": bool(body.get("is_a_t_pose")),
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
        "title": prompt[:50] if prompt else DEFAULT_MODEL_TITLE,
        "root_prompt": prompt,
        "art_style": art_style or "realistic",
        "model": payload["ai_model"],
        "created_at": now_s() * 1000,
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "is_a_t_pose": bool(body.get("is_a_t_pose")),
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
    print(f"[Refine][mod] Resolved preview_task_id: {preview_task_id_input} -> {preview_task_id}")

    store = load_store()
    preview_meta = get_job_metadata(preview_task_id_input, store)
    if not preview_meta.get("prompt"):
        preview_meta = get_job_metadata(preview_task_id, store)
    original_prompt = preview_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = preview_meta.get("root_prompt") or original_prompt
    texture_prompt = body.get("texture_prompt")
    title = f"(refine) {original_prompt[:40]}" if original_prompt else body.get("title", DEFAULT_MODEL_TITLE)

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["text-to-3d-refine"]
    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "refine",
        "preview_task_id": preview_task_id,
    }
    if texture_prompt:
        job_meta["texture_prompt"] = texture_prompt

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    payload = {
        "mode": "refine",
        "preview_task_id": preview_task_id,
        "enable_pbr": bool(body.get("enable_pbr", True)),
    }
    if texture_prompt:
        payload["texture_prompt"] = texture_prompt

    store_meta = {
        "stage": "refine",
        "preview_task_id": preview_task_id,
        "created_at": now_s() * 1000,
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "art_style": preview_meta.get("art_style"),
        "texture_prompt": texture_prompt,
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
    payload = {
        "mode": "preview",
        "prompt": prompt,
        "ai_model": body.get("model") or "latest",
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

    job_meta = {
        "prompt": prompt,
        "root_prompt": prompt,
        "title": prompt[:50] if prompt else None,
        "stage": "preview",
        "art_style": payload.get("art_style"),
        "model": payload.get("ai_model"),
        "license": license_choice,
        "symmetry_mode": payload.get("symmetry_mode", "auto"),
        "is_a_t_pose": bool(body.get("is_a_t_pose")),
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
            return jsonify({"ok": False, "error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        return jsonify({"ok": False, "error": str(e)}), 502

    finalize_job_credits(reservation_id, meshy_task_id)

    store = load_store()
    store[meshy_task_id] = {
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


@bp.route("/text-to-3d/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def text_to_3d_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("text-to-3d/status:incoming[mod]", {"job_id": job_id})
    if not job_id:
        return jsonify({"error": "job_id required"}), 400
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

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
                        SELECT id, status, upstream_job_id, error_message, meta
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
    except Exception as e:
        return jsonify({"error": str(e)}), 404

    out = normalize_status(ms)
    log_status_summary("text-to-3d[mod]", job_id, out)

    store = load_store()
    meta = (store.get(job_id) or store.get(meshy_job_id)
            or get_job_metadata(meshy_job_id, store) or get_job_metadata(job_id, store) or {})
    if identity_id and not meta.get("identity_id"):
        meta["identity_id"] = identity_id
        meta["user_id"] = identity_id

    for key in ("batch_count", "batch_slot", "batch_group_id", "license", "symmetry_mode", "is_a_t_pose"):
        if key in meta and key not in out:
            out[key] = meta.get(key)
    meta.update({"last_status": out["status"], "last_pct": out["pct"], "stage": out["stage"]})
    if out.get("glb_url"):
        meta["glb_url"] = out["glb_url"]
    if out.get("thumbnail_url"):
        meta["thumbnail_url"] = out["thumbnail_url"]
    store[job_id] = meta
    save_store(store)

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

            reservation_id = meta.get("reservation_id")
            internal_job_id = meta.get("internal_job_id")
            if reservation_id:
                finalize_job_credits(reservation_id, job_id)

            if internal_job_id:
                _update_job_status_ready(
                    internal_job_id,
                    upstream_job_id=job_id,
                    model_id=s3_result.get("model_id"),
                    glb_url=s3_result.get("glb_url"),
                )

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

    return jsonify(out)


@bp.route("/text-to-3d/list", methods=["GET", "OPTIONS"])
def text_to_3d_list_mod():
    if request.method == "OPTIONS":
        return ("", 204)
    store = load_store()
    items = [{"job_id": jid, **meta} for jid, meta in store.items()]
    return jsonify([x["job_id"] for x in items if "job_id" in x] or list(store.keys()))
