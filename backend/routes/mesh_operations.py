"""
Mesh Operations Routes Blueprint (Modular)
-----------------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

import uuid

from flask import Blueprint, jsonify, request, g

from backend.config import ACTION_KEYS, DEFAULT_MODEL_TITLE, MESHY_API_KEY
from backend.middleware import with_session
from backend.services.credits_helper import finalize_job_credits, get_current_balance, release_job_credits, start_paid_job
from backend.services.identity_service import require_identity
from backend.services.job_service import get_job_metadata, load_store, save_store, verify_job_ownership
from backend.services.meshy_service import build_source_payload, mesh_get, mesh_post, normalize_meshy_task
from backend.services.s3_service import save_finished_job_to_normalized_db
from backend.utils.helpers import log_event, log_status_summary, now_s

bp = Blueprint("mesh_operations", __name__)


@bp.route("/mesh/remesh", methods=["POST", "OPTIONS"])
@with_session
def mesh_remesh_mod():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("mesh/remesh:incoming[mod]", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["remesh"]
    payload = {**source, "target_formats": body.get("target_formats") or ["glb"]}

    topology = (body.get("topology") or "").strip().lower()
    if topology in {"triangle", "quad"}:
        payload["topology"] = topology
    try:
        tp = int(body.get("target_polycount"))
        if tp > 0:
            payload["target_polycount"] = tp
    except Exception:
        pass

    try:
        rh = float(body.get("resize_height"))
        if rh > 0:
            payload["resize_height"] = rh
    except Exception:
        pass

    origin_at = (body.get("origin_at") or "").strip().lower()
    if origin_at in {"bottom", "center"}:
        payload["origin_at"] = origin_at

    if body.get("convert_format_only") is not None:
        payload["convert_format_only"] = bool(body.get("convert_format_only"))

    source_task_id = body.get("source_task_id") or body.get("model_task_id")
    store = load_store()
    source_meta = get_job_metadata(source_task_id, store) if source_task_id else {}
    original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt
    title = f"(remesh) {original_prompt[:40]}" if original_prompt else body.get("title", DEFAULT_MODEL_TITLE)

    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "remesh",
        "source_task_id": source_task_id,
        "topology": topology,
        "target_polycount": payload.get("target_polycount"),
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    try:
        resp = mesh_post("/openapi/v1/remesh", payload)
        log_event("mesh/remesh:meshy-resp[mod]", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            return jsonify({"ok": False, "error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        return jsonify({"ok": False, "error": str(e)}), 502

    finalize_job_credits(reservation_id, meshy_task_id)

    store[meshy_task_id] = {
        "stage": "remesh",
        "source_task_id": source_task_id,
        "created_at": now_s() * 1000,
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "topology": topology,
        "target_polycount": payload.get("target_polycount"),
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


@bp.route("/mesh/remesh/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def mesh_remesh_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("mesh/remesh/status:incoming[mod]", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id = g.identity_id
    if not verify_job_ownership(job_id, identity_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v1/remesh/{job_id}")
        log_event("mesh/remesh/status:meshy-resp[mod]", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    out = normalize_meshy_task(ms, stage="remesh")
    log_status_summary("mesh/remesh[mod]", job_id, out)

    if out["status"] == "done" and (out.get("glb_url") or out.get("thumbnail_url")):
        store = load_store()
        meta = get_job_metadata(job_id, store)
        if identity_id and not meta.get("identity_id"):
            meta["identity_id"] = identity_id
            meta["user_id"] = identity_id

        source_id = meta.get("source_task_id") or out.get("source_task_id")
        if source_id and (not meta.get("prompt") or not meta.get("title")):
            source_meta = get_job_metadata(source_id, store)
            if not meta.get("prompt"):
                meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
            if not meta.get("root_prompt"):
                meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        if not meta.get("title"):
            prompt_for_title = meta.get("prompt") or meta.get("root_prompt") or ""
            meta["title"] = f"(remesh) {prompt_for_title[:40]}" if prompt_for_title else f"(remesh) {DEFAULT_MODEL_TITLE}"

        user_id = meta.get("identity_id") or meta.get("user_id") or getattr(g, 'identity_id', None)
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type="remesh", user_id=user_id)

        if s3_result and s3_result.get("success"):
            if s3_result.get("glb_url"):
                out["glb_url"] = s3_result["glb_url"]
            if s3_result.get("thumbnail_url"):
                out["thumbnail_url"] = s3_result["thumbnail_url"]
            if s3_result.get("model_urls"):
                out["model_urls"] = s3_result["model_urls"]
            if s3_result.get("db_ok") is False:
                out["db_ok"] = False
                out["db_errors"] = s3_result.get("db_errors")

    return jsonify(out)


@bp.route("/mesh/retexture", methods=["POST", "OPTIONS"])
@with_session
def mesh_retexture_mod():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("mesh/retexture:incoming[mod]", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"error": err}), 400

    prompt = (body.get("text_style_prompt") or "").strip()
    style_img = (body.get("image_style_url") or "").strip()
    if not prompt and not style_img:
        return jsonify({"error": "text_style_prompt or image_style_url required"}), 400

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["retexture"]

    source_task_id = body.get("source_task_id") or body.get("model_task_id")
    store = load_store()
    source_meta = get_job_metadata(source_task_id, store) if source_task_id else {}
    original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt
    title = f"(texture) {original_prompt[:40]}" if original_prompt else body.get("title", DEFAULT_MODEL_TITLE)

    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "texture",
        "source_task_id": source_task_id,
        "texture_prompt": prompt or None,
        "enable_pbr": bool(body.get("enable_pbr", False)),
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    payload = {
        **source,
        "enable_original_uv": bool(body.get("enable_original_uv", True)),
        "enable_pbr": bool(body.get("enable_pbr", False)),
    }
    if prompt:
        payload["text_style_prompt"] = prompt
    if style_img:
        payload["image_style_url"] = style_img
    ai_model = (body.get("ai_model") or "").strip()
    if ai_model:
        payload["ai_model"] = ai_model

    try:
        resp = mesh_post("/openapi/v1/retexture", payload)
        log_event("mesh/retexture:meshy-resp[mod]", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            return jsonify({"error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        return jsonify({"error": str(e)}), 502

    finalize_job_credits(reservation_id, meshy_task_id)

    store[meshy_task_id] = {
        "stage": "texture",
        "source_task_id": source_task_id,
        "created_at": now_s() * 1000,
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "texture_prompt": prompt,
        "enable_pbr": bool(body.get("enable_pbr", False)),
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


@bp.route("/mesh/retexture/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def mesh_retexture_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("mesh/retexture/status:incoming[mod]", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id = g.identity_id
    if not verify_job_ownership(job_id, identity_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v1/retexture/{job_id}")
        log_event("mesh/retexture/status:meshy-resp[mod]", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    out = normalize_meshy_task(ms, stage="texture")
    log_status_summary("mesh/retexture[mod]", job_id, out)

    if out["status"] == "done" and (out.get("glb_url") or out.get("textured_glb_url") or out.get("thumbnail_url")):
        store = load_store()
        meta = get_job_metadata(job_id, store)
        if identity_id and not meta.get("identity_id"):
            meta["identity_id"] = identity_id
            meta["user_id"] = identity_id

        source_id = meta.get("source_task_id") or out.get("source_task_id")
        if source_id and (not meta.get("prompt") or not meta.get("title")):
            source_meta = get_job_metadata(source_id, store)
            if not meta.get("prompt"):
                meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
            if not meta.get("root_prompt"):
                meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        if not meta.get("title"):
            prompt_for_title = meta.get("prompt") or meta.get("root_prompt") or ""
            meta["title"] = f"(texture) {prompt_for_title[:40]}" if prompt_for_title else f"(texture) {DEFAULT_MODEL_TITLE}"

        user_id = meta.get("identity_id") or meta.get("user_id") or getattr(g, 'identity_id', None)
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type="texture", user_id=user_id)

        if s3_result and s3_result.get("success"):
            if s3_result.get("glb_url"):
                out["glb_url"] = s3_result["glb_url"]
            if s3_result.get("thumbnail_url"):
                out["thumbnail_url"] = s3_result["thumbnail_url"]
            if s3_result.get("textured_glb_url"):
                out["textured_glb_url"] = s3_result["textured_glb_url"]
            if s3_result.get("texture_urls"):
                out["texture_urls"] = s3_result["texture_urls"]
            if s3_result.get("model_urls"):
                out["model_urls"] = s3_result["model_urls"]
            if s3_result.get("db_ok") is False:
                out["db_ok"] = False
                out["db_errors"] = s3_result.get("db_errors")

    return jsonify(out)


@bp.route("/mesh/rigging", methods=["POST", "OPTIONS"])
@with_session
def mesh_rigging_mod():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("mesh/rigging:incoming[mod]", body)
    source, err = build_source_payload(body)
    if err:
        return jsonify({"error": err}), 400

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["rigging"]
    payload = {**source}
    try:
        h = float(body.get("height_meters"))
        if h > 0:
            payload["height_meters"] = h
    except Exception:
        pass
    tex_img = (body.get("texture_image_url") or "").strip()
    if tex_img:
        payload["texture_image_url"] = tex_img

    source_task_id = body.get("source_task_id") or body.get("model_task_id")
    store = load_store()
    source_meta = get_job_metadata(source_task_id, store) if source_task_id else {}
    original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt
    title = f"(rigged) {original_prompt[:40]}" if original_prompt else body.get("title", DEFAULT_MODEL_TITLE)

    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "rigging",
        "source_task_id": source_task_id,
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    try:
        resp = mesh_post("/openapi/v1/rigging", payload)
        log_event("mesh/rigging:meshy-resp[mod]", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            return jsonify({"error": "No job id in response", "raw": resp}), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        return jsonify({"error": str(e)}), 502

    finalize_job_credits(reservation_id, meshy_task_id)

    store[meshy_task_id] = {
        "stage": "rigging",
        "source_task_id": source_task_id,
        "created_at": now_s() * 1000,
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
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


@bp.route("/mesh/rigging/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def mesh_rigging_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("mesh/rigging/status:incoming[mod]", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    identity_id = g.identity_id
    if not verify_job_ownership(job_id, identity_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v1/rigging/{job_id}")
        log_event("mesh/rigging/status:meshy-resp[mod]", ms)
    except Exception as e:
        return jsonify({"error": str(e)}), 404
    out = normalize_meshy_task(ms, stage="rig")
    log_status_summary("mesh/rigging[mod]", job_id, out)

    if out["status"] == "done" and (out.get("rigged_character_glb_url") or out.get("thumbnail_url")):
        store = load_store()
        meta = get_job_metadata(job_id, store)
        if identity_id and not meta.get("identity_id"):
            meta["identity_id"] = identity_id
            meta["user_id"] = identity_id

        source_id = meta.get("source_task_id") or out.get("source_task_id")
        if source_id and (not meta.get("prompt") or not meta.get("title")):
            source_meta = get_job_metadata(source_id, store)
            if not meta.get("prompt"):
                meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
            if not meta.get("root_prompt"):
                meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        if not meta.get("title"):
            prompt_for_title = meta.get("prompt") or meta.get("root_prompt") or ""
            meta["title"] = f"(rigged) {prompt_for_title[:40]}" if prompt_for_title else f"(rigged) {DEFAULT_MODEL_TITLE}"

        user_id = meta.get("identity_id") or meta.get("user_id") or getattr(g, 'identity_id', None)
        s3_result = save_finished_job_to_normalized_db(job_id, out, meta, job_type="rig", user_id=user_id)

        if s3_result and s3_result.get("success"):
            if s3_result.get("glb_url"):
                out["glb_url"] = s3_result["glb_url"]
            if s3_result.get("thumbnail_url"):
                out["thumbnail_url"] = s3_result["thumbnail_url"]
            if s3_result.get("rigged_character_glb_url"):
                out["rigged_character_glb_url"] = s3_result["rigged_character_glb_url"]
            if s3_result.get("rigged_character_fbx_url"):
                out["rigged_character_fbx_url"] = s3_result["rigged_character_fbx_url"]
            if s3_result.get("model_urls"):
                out["model_urls"] = s3_result["model_urls"]
            if s3_result.get("db_ok") is False:
                out["db_ok"] = False
                out["db_errors"] = s3_result.get("db_errors")

    return jsonify(out)
