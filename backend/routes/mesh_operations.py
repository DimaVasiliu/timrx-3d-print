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
from backend.middleware import with_session, with_session_readonly
from backend.utils import derive_display_title, is_generic_title
from backend.services.async_dispatch import update_job_with_upstream_id
from backend.services.credits_helper import finalize_job_credits, get_current_balance, release_job_credits, start_paid_job
from backend.services.identity_service import require_identity
from backend.services.history_service import get_canonical_model_row
from backend.services.job_service import create_internal_job_row, get_job_metadata, load_store, resolve_meshy_job_id, save_store, verify_job_ownership_detailed, _update_job_status_ready
from backend.services.meshy_service import build_source_payload, mesh_get, mesh_post, normalize_meshy_task, MeshyTaskNotFoundError, terminalize_expired_meshy_job
from backend.services.s3_service import save_finished_job_to_normalized_db
from backend.services.status_cache import get_cached_status, cache_status
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
    source, err = build_source_payload(body, identity_id=identity_id, prefer="model_url")
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
            print(f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} error=no_task_id_in_response raw={resp}")
            return jsonify({"ok": False, "error": "MODEL_GENERATION_FAILED", "message": "3D model generation failed. Please try again."}), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_provider_error, MODEL_GENERATION_FAILED
        return jsonify(sanitize_provider_error(
            provider="meshy", error=e, job_id=internal_job_id,
            code=MODEL_GENERATION_FAILED,
        )), 502

    # Credits: do NOT finalize now — remesh is async.
    # Finalize on terminal success in status endpoint; release on failure.

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
@with_session_readonly
def mesh_remesh_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("mesh/remesh/status:incoming[mod]", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Short-circuit: return cached response if within TTL
    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    identity_id = g.identity_id
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    try:
        ms = mesh_get(f"/openapi/v1/remesh/{job_id}")
        log_event("mesh/remesh/status:meshy-resp[mod]", ms)
    except MeshyTaskNotFoundError:
        print(f"[MESHY] Task expired: remesh job_id={job_id}")
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({"status": "failed", "error": "TASK_EXPIRED", "message": "This generation has expired on the provider."}), 200
    except Exception as e:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} error={e}")
        return jsonify({"error": "MODEL_GENERATION_FAILED", "message": "Failed to fetch job status. Please try again."}), 502
    out = normalize_meshy_task(ms, stage="remesh")
    log_status_summary("mesh/remesh[mod]", job_id, out)

    # ── Async credit handling (same pattern as retexture) ──────────────
    if out["status"] == "failed":
        try:
            from backend.services.credits_helper import refund_failed_job
            refund_failed_job(job_id)
        except Exception as e:
            print(f"[mesh/remesh] auto-refund failed: {e}")

    if out["status"] == "done":
        # Finalize (capture) credits now that Meshy confirmed success
        try:
            store_for_credits = load_store()
            meta_for_credits = get_job_metadata(job_id, store_for_credits)
            res_id = meta_for_credits.get("reservation_id")
            int_job = meta_for_credits.get("internal_job_id") or job_id
            cred_identity = meta_for_credits.get("identity_id") or identity_id
            if res_id:
                finalize_job_credits(res_id, int_job, cred_identity)
        except Exception as e:
            print(f"[mesh/remesh] credit finalize on done failed: {e}")

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

            # Transition jobs.status → 'ready' so completed remesh jobs
            # are excluded from /api/jobs/active on next reload.
            # Resolve internal_job_id from store or DB (store may be
            # empty after worker restart).
            try:
                int_job = meta.get("internal_job_id")
                if not int_job and USE_DB:
                    from backend.db import query_one, Tables as _T
                    _row = query_one(f"SELECT id::text AS jid FROM {_T.JOBS} WHERE upstream_job_id = %s LIMIT 1", (job_id,))
                    int_job = _row["jid"] if _row else None
                if int_job:
                    _update_job_status_ready(
                        int_job,
                        upstream_job_id=job_id,
                        model_id=s3_result.get("model_id"),
                        glb_url=s3_result.get("glb_url"),
                    )
            except Exception as e:
                print(f"[mesh/remesh] job status→ready failed: {e}")

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

    cache_status(job_id, out, is_terminal=(out["status"] in ("done", "failed")))
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

    # ── Source resolution ────────────────────────────────────────────────
    source, err = build_source_payload(body, identity_id=identity_id, prefer="input_task_id")
    if err:
        return jsonify({"error": err}), 400

    source_mode = "task" if source.get("input_task_id") else "model_url"

    # ── Upstream preflight: validate the Meshy task before submitting ────
    # Meshy retexture requires a SUCCEEDED task of type:
    # text_to_3d, text_to_3d_refine, image_to_3d, or remesh.
    # If the upstream task is not valid, fall back to model_url.
    upstream_info = None
    force_no_original_uv = False
    if source.get("input_task_id"):
        upstream_info = _preflight_retexture_upstream(source["input_task_id"])
        if upstream_info and not upstream_info["usable"]:
            # Upstream task is not valid — fall back to model_url if available
            if body.get("model_url"):
                print(f"[RETEXTURE:PREFLIGHT] Upstream not usable ({upstream_info['reason']}), falling back to model_url")
                fallback_source, fallback_err = build_source_payload(
                    {"model_url": body["model_url"]},
                    identity_id=identity_id,
                    prefer="model_url",
                )
                if not fallback_err:
                    source = fallback_source
                    source_mode = "model_url"
                    force_no_original_uv = True  # imported models: let Meshy re-UV
            elif not upstream_info["usable"]:
                return jsonify({
                    "ok": False,
                    "error": "RETEXTURE_SOURCE_INVALID",
                    "message": f"Cannot retexture: {upstream_info['reason']}. Try generating a new model first.",
                }), 400

        # Preview models often have auto-generated UVs that break retexture
        if upstream_info and upstream_info.get("task_type") in ("text_to_3d",):
            force_no_original_uv = True

    if source_mode == "model_url":
        force_no_original_uv = True  # external/S3 models: let Meshy create fresh UVs

    # ── Prompt handling ──────────────────────────────────────────────────
    prompt = (body.get("text_style_prompt") or "").strip()
    style_img = (body.get("image_style_url") or "").strip()
    if not prompt and not style_img:
        return jsonify({"error": "text_style_prompt or image_style_url required"}), 400

    # Sanitize: ensure prompt describes texture style, not model geometry
    original_prompt_len = len(prompt)
    prompt = _sanitize_texture_prompt(prompt)
    prompt_mode = "sanitized" if len(prompt) != original_prompt_len else "raw"

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS["retexture"]

    source_task_id_input = body.get("source_task_id") or body.get("model_task_id") or body.get("input_task_id")
    store = load_store()

    source_task_id = None
    if source.get("model_url"):
        source_task_id = source_task_id_input
    else:
        source_task_id, validation_error = _resolve_and_validate_source_task(source_task_id_input, store)
        if validation_error:
            return validation_error

    source_meta = get_job_metadata(source_task_id_input, store) or get_job_metadata(source_task_id, store) or {}
    texture_prompt_text = prompt or ""
    original_prompt_text = source_meta.get("prompt") or body.get("prompt") or texture_prompt_text or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt_text or texture_prompt_text or ""
    explicit_title = body.get("title") or source_meta.get("title")
    title = derive_display_title(original_prompt_text or texture_prompt_text, explicit_title, root_prompt=root_prompt or texture_prompt_text)

    # Decide enable_original_uv: respect frontend choice unless preflight overrides
    enable_original_uv = bool(body.get("enable_original_uv", True))
    if force_no_original_uv:
        enable_original_uv = False

    enable_pbr = bool(body.get("enable_pbr", False))

    # ── Final source validation BEFORE reserving credits ─────────────────
    # If after all fallbacks we still have no usable source, fail early
    # without touching credits.
    if not source.get("input_task_id") and not source.get("model_url"):
        return jsonify({
            "ok": False,
            "error": "RETEXTURE_SOURCE_INVALID",
            "message": "No valid source model available. The original model may have expired. Please generate a new model.",
        }), 400

    job_meta = {
        "prompt": original_prompt_text,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "texture",
        "source_task_id": source_task_id,
        "texture_prompt": prompt or None,
        "enable_pbr": enable_pbr,
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=original_prompt_text,
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    # ── Build Meshy payload — NEVER include both input_task_id and model_url ─
    payload = {
        **source,
        "enable_original_uv": enable_original_uv,
        "enable_pbr": enable_pbr,
    }
    if prompt:
        payload["text_style_prompt"] = prompt
    if style_img:
        payload["image_style_url"] = style_img
    ai_model = (body.get("ai_model") or "").strip()
    if ai_model:
        payload["ai_model"] = ai_model

    # ── Structured diagnostics ───────────────────────────────────────────
    _log_retexture_diagnostics(
        source=source, body=body, source_mode=source_mode,
        upstream_info=upstream_info, enable_original_uv=enable_original_uv,
        enable_pbr=enable_pbr, prompt=prompt, prompt_mode=prompt_mode,
        original_prompt_len=original_prompt_len, ai_model=ai_model or "default",
    )

    log_payload = {k: (v[:80] + "..." if isinstance(v, str) and len(v) > 80 else v) for k, v in payload.items()}
    print(f"[RETEXTURE] Sending to Meshy: identity={identity_id} job={internal_job_id} source_mode={source_mode} payload={log_payload}")

    # ── Dispatch to Meshy with fallback chain ────────────────────────────
    # Attempt 1: primary source + current UV setting
    # Attempt 2: same source, enable_original_uv=false (if was true)
    # Attempt 3: model_url fallback (if input_task_id was used and model_url is available)
    resp = None
    used_fallback = False
    try:
        resp = mesh_post("/openapi/v1/retexture", payload)
    except Exception as e:
        err_str = str(e).lower()
        has_model_url_fallback = bool(body.get("model_url")) and source.get("input_task_id")
        task_id_rejected = (
            "task not found" in err_str
            or "input task" in err_str
            or ("400" in err_str and "input_task_id" in err_str)
        )

        if task_id_rejected and has_model_url_fallback:
            print(f"[RETEXTURE:FALLBACK] input_task_id rejected, retrying with model_url job={internal_job_id}")
            fallback_source, fallback_err = build_source_payload(
                {"model_url": body["model_url"]},
                identity_id=identity_id,
                prefer="model_url",
            )
            if not fallback_err:
                fallback_payload = {k: v for k, v in payload.items() if k != "input_task_id"}
                fallback_payload["model_url"] = fallback_source["model_url"]
                fallback_payload["enable_original_uv"] = False  # safe mode for fallback
                try:
                    resp = mesh_post("/openapi/v1/retexture", fallback_payload)
                    used_fallback = True
                    print(f"[RETEXTURE:FALLBACK] model_url fallback succeeded job={internal_job_id}")
                except Exception as e2:
                    release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
                    from backend.services.error_sanitizer import sanitize_provider_error, MODEL_GENERATION_FAILED
                    print(f"[RETEXTURE:FALLBACK] model_url fallback also failed job={internal_job_id}: {e2}")
                    return jsonify(sanitize_provider_error(
                        provider="meshy", error=e2, job_id=internal_job_id,
                        code=MODEL_GENERATION_FAILED,
                    )), 502

        if resp is None:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
            from backend.services.error_sanitizer import sanitize_provider_error, MODEL_GENERATION_FAILED
            return jsonify(sanitize_provider_error(
                provider="meshy", error=e, job_id=internal_job_id,
                code=MODEL_GENERATION_FAILED,
            )), 502

    log_event("mesh/retexture:meshy-resp[mod]", resp)
    meshy_task_id = resp.get("result") or resp.get("id")
    if not meshy_task_id:
        release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
        print(f"[PROVIDER_ERROR] provider=meshy job_id={internal_job_id} error=no_task_id_in_response raw={resp}")
        return jsonify({"ok": False, "error": "MODEL_GENERATION_FAILED", "message": "3D model generation failed. Please try again."}), 502

    # Credits: do NOT finalize now — retexture is async.
    # Finalize on terminal success in status endpoint; release on failure.

    update_job_with_upstream_id(internal_job_id, meshy_task_id)

    store[meshy_task_id] = {
        "stage": "texture",
        "source_task_id": source_task_id,
        "created_at": now_s() * 1000,
        "prompt": original_prompt_text,
        "root_prompt": root_prompt,
        "title": title,
        "texture_prompt": prompt,
        "enable_pbr": enable_pbr,
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
        "source_mode": "model_url" if used_fallback else source_mode,
    })


# ── Retexture helpers ────────────────────────────────────────────────────────

def _preflight_retexture_upstream(task_id: str) -> dict | None:
    """GET the upstream Meshy task and validate it for retexture.

    Returns dict with keys: usable, status, task_type, reason.
    Returns None if the preflight check itself fails (network error etc).
    """
    SUPPORTED_TYPES = {"text_to_3d", "text_to_3d_refine", "image_to_3d", "remesh"}
    try:
        # Try the most common endpoints to identify the task
        for endpoint_type, api_path in [
            ("text_to_3d", f"/openapi/v2/text-to-3d/{task_id}"),
            ("image_to_3d", f"/openapi/v1/image-to-3d/{task_id}"),
            ("remesh", f"/openapi/v1/remesh/{task_id}"),
        ]:
            try:
                ms = mesh_get(api_path)
                raw_status = (ms.get("status") or "").upper()
                task_type = endpoint_type
                # For text-to-3d v2, check if it's a refine
                if endpoint_type == "text_to_3d" and ms.get("mode") == "refine":
                    task_type = "text_to_3d_refine"

                usable = raw_status == "SUCCEEDED" and task_type in SUPPORTED_TYPES
                reason = None
                if raw_status != "SUCCEEDED":
                    reason = f"upstream task status is {raw_status}, expected SUCCEEDED"
                elif task_type not in SUPPORTED_TYPES:
                    reason = f"upstream task type '{task_type}' not supported for retexture"

                result = {
                    "usable": usable,
                    "status": raw_status,
                    "task_type": task_type,
                    "reason": reason,
                    "task_id": task_id,
                }
                print(
                    f"[RETEXTURE:UPSTREAM] task_id={task_id} type={task_type}"
                    f" status={raw_status} supported={'yes' if usable else 'no'}"
                    f"{' reason=' + reason if reason else ''}"
                )
                return result
            except RuntimeError as e:
                if "404" in str(e):
                    continue  # try next endpoint type
                raise  # re-raise non-404 errors

        # No endpoint matched — task ID may be invalid or from an unsupported type
        print(f"[RETEXTURE:UPSTREAM] task_id={task_id} type=unknown status=not_found supported=no")
        return {"usable": False, "status": "not_found", "task_type": "unknown",
                "reason": "upstream task not found on any known Meshy endpoint", "task_id": task_id}

    except Exception as e:
        # Preflight failed (network error) — don't block, let Meshy decide
        print(f"[RETEXTURE:UPSTREAM] preflight failed for {task_id}: {e} — proceeding without validation")
        return None


def _sanitize_texture_prompt(prompt: str) -> str:
    """Ensure text_style_prompt describes texture/material style, not model geometry.

    Meshy retexture's text_style_prompt should describe surface appearance:
      "matte black suit with metallic accents"
    NOT 3D model structure:
      "Full body humanoid character, front-facing, neutral T-pose..."

    Also enforces the 600-char limit.
    """
    if not prompt:
        return prompt

    # Detect generation-style prompt patterns (describes geometry, not texture)
    _GENERATION_MARKERS = (
        "full body", "front-facing", "t-pose", "a-pose", "arms extended",
        "legs apart", "neutral pose", "character reference", "reference sheet",
        "3d model of", "3d render of", "humanoid character",
    )
    lower = prompt.lower()
    is_generation_prompt = sum(1 for m in _GENERATION_MARKERS if m in lower) >= 2

    if is_generation_prompt:
        # Extract just the descriptive adjectives / material hints if possible
        # Fall back to a safe generic prompt
        print(f"[RETEXTURE:PROMPT] Detected generation-style prompt ({len(prompt)} chars), sanitizing")
        # Try to salvage material/style words from the prompt
        salvaged = _extract_texture_hints(prompt)
        prompt = salvaged if salvaged else "High quality realistic PBR texture with detailed surface materials"

    # Enforce Meshy's 600-char limit
    if len(prompt) > 600:
        print(f"[RETEXTURE:PROMPT] Truncating from {len(prompt)} to 600 chars")
        prompt = prompt[:597] + "..."

    return prompt


def _extract_texture_hints(prompt: str) -> str:
    """Try to extract texture/material/style descriptors from a generation prompt."""
    import re
    # Look for material/texture descriptive phrases
    texture_words = []
    # Common texture/material adjectives and nouns
    _TEX_PATTERNS = [
        r'\b(metallic|matte|glossy|shiny|rough|smooth|worn|weathered|polished|rustic)\b',
        r'\b(leather|fabric|metal|wood|stone|ceramic|glass|rubber|chrome|bronze|gold|silver)\b',
        r'\b(armor|suit|clothing|outfit|costume|gear|uniform)\b',
        r'\b(dark|black|white|red|blue|green|grey|gray|brown|tan|crimson|scarlet)\b',
        r'\b(detailed|realistic|stylized|cel[- ]shad|cartoon|comic|anime)\b',
        r'\b(texture|material|surface|finish|coating|paint|skin)\b',
    ]
    for pat in _TEX_PATTERNS:
        texture_words.extend(re.findall(pat, prompt, re.IGNORECASE))

    if len(texture_words) >= 3:
        # Build a texture-style prompt from extracted words
        unique = list(dict.fromkeys(w.lower() for w in texture_words))
        return f"Realistic texture with {', '.join(unique[:12])} details"
    return ""


def _log_retexture_diagnostics(
    *, source, body, source_mode, upstream_info, enable_original_uv,
    enable_pbr, prompt, prompt_mode, original_prompt_len, ai_model,
):
    """Structured diagnostic log block for retexture request."""
    input_id = body.get("input_task_id") or body.get("source_task_id") or ""
    resolved_id = source.get("input_task_id") or ""
    model_url = body.get("model_url") or ""

    # Source log
    print(
        f"[RETEXTURE:SRC] source_mode={source_mode}"
        f" input_task_id={input_id[:40] if input_id else 'none'}"
        f" resolved_task_id={resolved_id[:40] if resolved_id else 'none'}"
        f" model_url={'yes_s3' if 's3' in model_url.lower() else ('yes_other' if model_url else 'none')}"
    )

    # Params log
    print(
        f"[RETEXTURE:PARAMS] source_mode={source_mode}"
        f" enable_original_uv={enable_original_uv}"
        f" enable_pbr={enable_pbr}"
        f" ai_model={ai_model}"
    )

    # Prompt log
    print(
        f"[RETEXTURE:PROMPT] original_len={original_prompt_len}"
        f" sanitized_len={len(prompt)}"
        f" mode={prompt_mode}"
        f" preview={prompt[:60]}..."
    )


@bp.route("/mesh/retexture/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def mesh_retexture_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("mesh/retexture/status:incoming[mod]", {"job_id": job_id})
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # Short-circuit: return cached response if within TTL
    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    identity_id = g.identity_id
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    try:
        ms = mesh_get(f"/openapi/v1/retexture/{job_id}")
        log_event("mesh/retexture/status:meshy-resp[mod]", ms)
    except MeshyTaskNotFoundError:
        print(f"[MESHY] Task expired: retexture job_id={job_id}")
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({"status": "failed", "error": "TASK_EXPIRED", "message": "This generation has expired on the provider."}), 200
    except Exception as e:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} error={e}")
        return jsonify({"error": "MODEL_GENERATION_FAILED", "message": "Failed to fetch job status. Please try again."}), 502
    out = normalize_meshy_task(ms, stage="texture")
    log_status_summary("mesh/retexture[mod]", job_id, out)

    # ── Async credit handling ────────────────────────────────────────────
    # Credits are reserved at POST time but NOT finalized until terminal
    # status.  This ensures failed async jobs refund properly.
    if out["status"] == "failed":
        try:
            from backend.services.credits_helper import refund_failed_job
            refund_failed_job(job_id)
        except Exception as e:
            print(f"[mesh/retexture] auto-refund failed: {e}")

    if out["status"] == "done":
        # Finalize (capture) credits now that Meshy confirmed success
        try:
            store_for_credits = load_store()
            meta_for_credits = get_job_metadata(job_id, store_for_credits)
            res_id = meta_for_credits.get("reservation_id")
            int_job = meta_for_credits.get("internal_job_id") or job_id
            cred_identity = meta_for_credits.get("identity_id") or identity_id
            if res_id:
                finalize_job_credits(res_id, int_job, cred_identity)
        except Exception as e:
            print(f"[mesh/retexture] credit finalize on done failed: {e}")

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

            # Transition jobs.status → 'ready' so completed retexture jobs
            # are excluded from /api/jobs/active on next reload.
            # Resolve internal_job_id from store or DB (store may be
            # empty after worker restart).
            try:
                int_job = meta.get("internal_job_id")
                if not int_job and USE_DB:
                    from backend.db import query_one, Tables as _T
                    _row = query_one(f"SELECT id::text AS jid FROM {_T.JOBS} WHERE upstream_job_id = %s LIMIT 1", (job_id,))
                    int_job = _row["jid"] if _row else None
                if int_job:
                    _update_job_status_ready(
                        int_job,
                        upstream_job_id=job_id,
                        model_id=s3_result.get("model_id"),
                        glb_url=s3_result.get("glb_url"),
                    )
            except Exception as e:
                print(f"[mesh/retexture] job status→ready failed: {e}")

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

    cache_status(job_id, out, is_terminal=(out["status"] in ("done", "failed")))
    return jsonify(out)
