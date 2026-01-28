"""
Image-to-3D Routes Blueprint (Modular)
-------------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

import uuid

from flask import Blueprint, jsonify, request, g

from backend.config import ACTION_KEYS, DEFAULT_MODEL_TITLE, MESHY_API_KEY
from backend.db import USE_DB, get_conn
from backend.middleware import with_session
from backend.services.async_dispatch import _dispatch_meshy_image_to_3d_async, get_executor
from backend.services.credits_helper import finalize_job_credits, get_current_balance, release_job_credits, start_paid_job
from backend.services.identity_service import require_identity
from backend.services.job_service import (
    _update_job_status_failed,
    _update_job_status_ready,
    create_internal_job_row,
    get_job_metadata,
    load_store,
    save_store,
    verify_job_ownership,
)
from backend.services.meshy_service import mesh_get, normalize_meshy_task
from backend.services.s3_service import save_finished_job_to_normalized_db
from backend.utils.helpers import log_event, log_status_summary, now_s

bp = Blueprint("image_to_3d", __name__)


@bp.route("/image-to-3d/start", methods=["POST", "OPTIONS"])
@with_session
def image_to_3d_start_mod():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("image-to-3d/start:incoming[mod]", body)
    image_url = (body.get("image_url") or "").strip()
    if not image_url:
        return jsonify({"error": "image_url required"}), 400

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["image-to-3d"]
    prompt = (body.get("prompt") or "").strip()
    job_meta = {
        "prompt": prompt,
        "root_prompt": prompt,
        "title": f"(image2-3d) {prompt[:40]}" if prompt else f"(image2-3d) {DEFAULT_MODEL_TITLE}",
        "stage": "image3d",
    }
    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    payload = {
        "image_url": image_url,
        "prompt": prompt,
        "ai_model": body.get("model") or "latest",
        "enable_pbr": True,
    }

    store_meta = {
        "stage": "image3d",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "root_prompt": prompt,
        "title": f"(image2-3d) {prompt[:40]}" if prompt else f"(image2-3d) {DEFAULT_MODEL_TITLE}",
        "original_image_url": image_url,
        "ai_model": payload.get("ai_model"),
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
        _dispatch_meshy_image_to_3d_async,
        internal_job_id,
        identity_id,
        reservation_id,
        payload,
        store_meta,
    )

    log_event("image-to-3d/start:dispatched[mod]", {"internal_job_id": internal_job_id})

    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "source": "modular",
    })


@bp.route("/image-to-3d/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def image_to_3d_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("image-to-3d/status:incoming[mod]", {"job_id": job_id})
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
                        SELECT id, status, upstream_job_id, error_message
                        FROM timrx_billing.jobs
                        WHERE id::text = %s AND identity_id = %s
                        LIMIT 1
                        """,
                        (job_id, identity_id),
                    )
                    internal_job = cur.fetchone()

            if internal_job:
                ownership_verified = True  # Found in jobs table with matching identity_id
                if internal_job["status"] == "queued":
                    return jsonify({
                        "status": "queued",
                        "pct": 0,
                        "stage": "image3d",
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
                        "stage": "image3d",
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
            stage_hint = store_meta.get("stage") or "image3d"
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
        ms = mesh_get(f"/openapi/v1/image-to-3d/{meshy_job_id}")
        log_event("image-to-3d/status:meshy-resp[mod]", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    out = normalize_meshy_task(ms, stage="image3d")
    log_status_summary("image-to-3d[mod]", meshy_job_id, out)

    store = load_store()
    meta = store.get(meshy_job_id) or store.get(job_id) or get_job_metadata(meshy_job_id, store) or {}
    if identity_id and not meta.get("identity_id"):
        meta["identity_id"] = identity_id
        meta["user_id"] = identity_id

    if out["status"] == "done" and (out.get("glb_url") or out.get("thumbnail_url")):
        if not meta.get("prompt"):
            meta["prompt"] = out.get("prompt") or ""
        if not meta.get("root_prompt"):
            meta["root_prompt"] = meta.get("prompt")

        if not meta.get("title"):
            prompt_for_title = meta.get("prompt") or ""
            meta["title"] = f"(image-to-3d) {prompt_for_title[:40]}" if prompt_for_title else f"(image-to-3d) {DEFAULT_MODEL_TITLE}"

        user_id = meta.get("identity_id") or meta.get("user_id") or getattr(g, 'identity_id', None)
        s3_result = save_finished_job_to_normalized_db(meshy_job_id, out, meta, job_type="image-to-3d", user_id=user_id)

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

            reservation_id = meta.get("reservation_id")
            internal_job_id = meta.get("internal_job_id")
            if reservation_id:
                finalize_job_credits(reservation_id, meshy_job_id)

            if internal_job_id:
                _update_job_status_ready(
                    internal_job_id,
                    upstream_job_id=meshy_job_id,
                    model_id=s3_result.get("model_id"),
                    glb_url=s3_result.get("glb_url"),
                )

    if out["status"] == "failed":
        reservation_id = meta.get("reservation_id")
        internal_job_id = meta.get("internal_job_id")
        error_msg = out.get("message") or out.get("error") or "Provider job failed"

        if reservation_id:
            release_job_credits(reservation_id, "provider_job_failed", meshy_job_id)

        if internal_job_id:
            _update_job_status_failed(internal_job_id, error_msg)

    return jsonify(out)
