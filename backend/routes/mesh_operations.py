"""
Mesh Operations Routes Blueprint (Modular)
-----------------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

import uuid

from flask import Blueprint, jsonify, request, g

from backend.config import ACTION_KEYS, MESHY_API_KEY
from backend.db import USE_DB
from backend.middleware import with_session
from backend.utils import derive_display_title, is_generic_title
from backend.services.async_dispatch import update_job_with_upstream_id
from backend.services.credits_helper import finalize_job_credits, get_current_balance, release_job_credits, start_paid_job
from backend.services.identity_service import require_identity
from backend.services.history_service import get_canonical_model_row
from backend.services.job_service import create_internal_job_row, get_job_metadata, load_store, resolve_meshy_job_id, save_store, verify_job_ownership_detailed
from backend.services.meshy_service import build_source_payload, mesh_get, mesh_post, normalize_meshy_task
from backend.services.s3_service import save_finished_job_to_normalized_db
from backend.utils.helpers import log_event, log_status_summary, now_s

bp = Blueprint("mesh_operations", __name__)


def _resolve_and_validate_source_task(source_task_id_input: str, store: dict) -> tuple[str | None, dict | None]:
    """
    Resolve a source task ID to the actual Meshy task ID.
    Returns (resolved_task_id, error_response) - if error_response is not None, return it.
    """
    import re

    if not source_task_id_input:
        return None, (jsonify({
            "ok": False,
            "error": "source_task_id or model_task_id required",
            "code": "SOURCE_TASK_REQUIRED",
        }), 400)

    resolved_id = resolve_meshy_job_id(source_task_id_input)
    print(f"[MeshOps] Resolved source_task_id: {source_task_id_input} -> {resolved_id}")

    # Check if input looks like a UUID that wasn't resolved
    uuid_pattern = r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    if re.match(uuid_pattern, source_task_id_input) and resolved_id == source_task_id_input:
        # Check if this might be a valid Meshy task ID directly
        store_entry = store.get(source_task_id_input) or {}
        if not store_entry.get("glb_url") and store_entry.get("status") != "done":
            print(f"[MeshOps] ERROR: Could not resolve source_task_id {source_task_id_input} to Meshy task ID")
            return None, (jsonify({
                "ok": False,
                "error": "Source task ID not found or not yet ready. Ensure the source task completed successfully.",
                "code": "SOURCE_TASK_NOT_FOUND",
            }), 400)

    return resolved_id, None


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
    source, err = build_source_payload(body, identity_id=identity_id)
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

    # Check for source task ID in various field names (frontend sends input_task_id)
    source_task_id_input = body.get("source_task_id") or body.get("model_task_id") or body.get("input_task_id")
    store = load_store()

    # Only validate source_task_id if not using model_url (model_url is the source itself)
    source_task_id = None
    if source.get("model_url"):
        # When using model_url, we don't need a source_task_id for Meshy
        source_task_id = source_task_id_input  # May be None, that's OK
    else:
        # Resolve and validate source task ID before reserving credits
        source_task_id, validation_error = _resolve_and_validate_source_task(source_task_id_input, store)
        if validation_error:
            return validation_error

    source_meta = get_job_metadata(source_task_id_input, store) or get_job_metadata(source_task_id, store) or {}
    original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt
    # Derive title - derive_display_title handles generic titles automatically
    explicit_title = body.get("title") or source_meta.get("title")
    title = derive_display_title(original_prompt, explicit_title, root_prompt=root_prompt)

    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "remesh",
        "source_task_id": source_task_id,  # Use resolved ID
        "topology": topology,
        "target_polycount": payload.get("target_polycount"),
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    # Persist job row so status polling/ownership checks work across workers
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=original_prompt,
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
    )

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

    # Use internal_job_id (not meshy_task_id) for credit finalization tracking
    finalize_job_credits(reservation_id, internal_job_id, identity_id)

    # Update internal job with upstream id for ownership/status
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

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
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

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

        # Use original_job_id from Meshy response (parent reference) or fallback to source_task_id
        parent_job_id = out.get("original_job_id") or meta.get("source_task_id") or meta.get("preview_task_id")
        source_meta = None  # Initialize before conditional to prevent undefined access
        # Always fetch source_meta when title is generic (e.g., "Untitled") to enable inheritance
        if parent_job_id and (not meta.get("prompt") or not meta.get("title") or is_generic_title(meta.get("title"))):
            source_meta = get_job_metadata(parent_job_id, store)
            if source_meta:
                if not meta.get("prompt"):
                    meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
                if not meta.get("root_prompt"):
                    meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        if not meta.get("title") or is_generic_title(meta.get("title")):
            meta["title"] = derive_display_title(
                meta.get("prompt"),
                source_meta.get("title") if source_meta else None,
                root_prompt=meta.get("root_prompt"),
            )

        # Persist original_job_id for lineage tracking
        if parent_job_id:
            meta["original_job_id"] = parent_job_id

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

    # If DB has the finalized model, prefer S3 URLs for frontend rendering.
    if USE_DB and identity_id:
        try:
            canonical = get_canonical_model_row(identity_id, upstream_job_id=job_id)
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
            print(f"[mesh/remesh][mod] DB lookup for finalized model failed: {e}")

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
    source, err = build_source_payload(body, identity_id=identity_id)
    if err:
        return jsonify({"error": err}), 400

    prompt = (body.get("text_style_prompt") or "").strip()
    style_img = (body.get("image_style_url") or "").strip()
    if not prompt and not style_img:
        return jsonify({"error": "text_style_prompt or image_style_url required"}), 400

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["retexture"]

    # Check for source task ID in various field names (frontend sends input_task_id)
    source_task_id_input = body.get("source_task_id") or body.get("model_task_id") or body.get("input_task_id")
    store = load_store()

    # Only validate source_task_id if not using model_url (model_url is the source itself)
    source_task_id = None
    if source.get("model_url"):
        # When using model_url, we don't need a source_task_id for Meshy
        source_task_id = source_task_id_input  # May be None, that's OK
    else:
        # Resolve and validate source task ID before reserving credits
        source_task_id, validation_error = _resolve_and_validate_source_task(source_task_id_input, store)
        if validation_error:
            return validation_error

    source_meta = get_job_metadata(source_task_id_input, store) or get_job_metadata(source_task_id, store) or {}
    # Use texture_prompt as fallback for prompt/root_prompt (often contains original description)
    texture_prompt_text = prompt or ""  # texture prompt from body
    original_prompt = source_meta.get("prompt") or body.get("prompt") or texture_prompt_text or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt or texture_prompt_text or ""
    # Use explicit title, or derive from prompt/root_prompt
    # derive_display_title handles generic titles (like "Textured Model") automatically
    explicit_title = body.get("title") or source_meta.get("title")
    title = derive_display_title(original_prompt or texture_prompt_text, explicit_title, root_prompt=root_prompt or texture_prompt_text)

    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "texture",
        "source_task_id": source_task_id,  # Use resolved ID
        "texture_prompt": prompt or None,
        "enable_pbr": bool(body.get("enable_pbr", False)),
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    # Persist job row so status polling/ownership checks work across workers
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=original_prompt,
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
    )

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

    # Use internal_job_id (not meshy_task_id) for credit finalization tracking
    finalize_job_credits(reservation_id, internal_job_id, identity_id)

    # Update internal job with upstream id for ownership/status
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

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
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

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

        # Use original_job_id from Meshy response (parent reference) or fallback to source_task_id
        parent_job_id = out.get("original_job_id") or meta.get("source_task_id") or meta.get("preview_task_id")
        source_meta = None  # Initialize before conditional
        # Always fetch source_meta when title is generic (e.g., "Untitled") to enable inheritance
        if parent_job_id and (not meta.get("prompt") or not meta.get("title") or is_generic_title(meta.get("title"))):
            source_meta = get_job_metadata(parent_job_id, store)
            if source_meta:
                if not meta.get("prompt"):
                    meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
                if not meta.get("root_prompt"):
                    meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        # Fallback: use texture_prompt for title derivation when no prompt/root_prompt available
        texture_prompt_fallback = meta.get("texture_prompt") or ""
        if not meta.get("prompt") and texture_prompt_fallback:
            meta["prompt"] = texture_prompt_fallback
        if not meta.get("root_prompt") and texture_prompt_fallback:
            meta["root_prompt"] = texture_prompt_fallback

        if not meta.get("title") or is_generic_title(meta.get("title")):
            # derive_display_title handles generic titles automatically
            prompt_for_title = meta.get("prompt") or texture_prompt_fallback
            meta["title"] = derive_display_title(
                prompt_for_title,
                source_meta.get("title") if source_meta else None,
                root_prompt=meta.get("root_prompt") or texture_prompt_fallback,
            )

        # Persist original_job_id for lineage tracking
        if parent_job_id:
            meta["original_job_id"] = parent_job_id

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

    # If DB has the finalized model, prefer S3 URLs for frontend rendering.
    if USE_DB and identity_id:
        try:
            canonical = get_canonical_model_row(identity_id, upstream_job_id=job_id)
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
            print(f"[mesh/retexture][mod] DB lookup for finalized model failed: {e}")

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
    source, err = build_source_payload(body, identity_id=identity_id)
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

    # Check for source task ID in various field names (frontend sends input_task_id)
    source_task_id_input = body.get("source_task_id") or body.get("model_task_id") or body.get("input_task_id")
    store = load_store()

    # Only validate source_task_id if not using model_url (model_url is the source itself)
    source_task_id = None
    if source.get("model_url"):
        # When using model_url, we don't need a source_task_id for Meshy
        source_task_id = source_task_id_input  # May be None, that's OK
    else:
        # Resolve and validate source task ID before reserving credits
        source_task_id, validation_error = _resolve_and_validate_source_task(source_task_id_input, store)
        if validation_error:
            return validation_error

    source_meta = get_job_metadata(source_task_id_input, store) or get_job_metadata(source_task_id, store) or {}
    original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt
    # Derive title - derive_display_title handles generic titles automatically
    explicit_title = body.get("title") or source_meta.get("title")
    title = derive_display_title(original_prompt, explicit_title, root_prompt=root_prompt)

    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "rigging",
        "source_task_id": source_task_id,  # Use resolved ID
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    # Persist job row so status polling/ownership checks work across workers
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=original_prompt,
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
    )

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

    # Use internal_job_id (not meshy_task_id) for credit finalization tracking
    finalize_job_credits(reservation_id, internal_job_id, identity_id)

    # Update internal job with upstream id for ownership/status
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

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
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

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

        # Use original_job_id from Meshy response (parent reference) or fallback to source_task_id
        parent_job_id = out.get("original_job_id") or meta.get("source_task_id") or meta.get("preview_task_id")
        source_meta = None  # Initialize before conditional to prevent undefined access
        # Always fetch source_meta when title is generic (e.g., "Untitled") to enable inheritance
        if parent_job_id and (not meta.get("prompt") or not meta.get("title") or is_generic_title(meta.get("title"))):
            source_meta = get_job_metadata(parent_job_id, store)
            if source_meta:
                if not meta.get("prompt"):
                    meta["prompt"] = source_meta.get("prompt") or source_meta.get("root_prompt") or out.get("prompt") or ""
                if not meta.get("root_prompt"):
                    meta["root_prompt"] = source_meta.get("root_prompt") or meta.get("prompt")

        if not meta.get("title") or is_generic_title(meta.get("title")):
            meta["title"] = derive_display_title(
                meta.get("prompt"),
                source_meta.get("title") if source_meta else None,
                root_prompt=meta.get("root_prompt"),
            )

        # Persist original_job_id for lineage tracking
        if parent_job_id:
            meta["original_job_id"] = parent_job_id

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

    # If DB has the finalized model, prefer S3 URLs for frontend rendering.
    if USE_DB and identity_id:
        try:
            canonical = get_canonical_model_row(identity_id, upstream_job_id=job_id)
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
            print(f"[mesh/rigging][mod] DB lookup for finalized model failed: {e}")

    return jsonify(out)
