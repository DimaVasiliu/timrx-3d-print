"""
Meshy Printability Routes Blueprint (Modular)
---------------------------------------------
Registered under /api/_mod.

Wraps Meshy's two printability APIs, which sit alongside — not instead of —
TimrX's local trimesh print check (`/api/_mod/print-check/<job_id>`):

    POST /api/_mod/mesh/print/analyze              -> POST /openapi/v1/print/analyze
    GET  /api/_mod/mesh/print/analyze/status/<id>  -> GET  /openapi/v1/print/analyze/:id
    POST /api/_mod/mesh/print/repair               -> POST /openapi/v1/print/repair
    GET  /api/_mod/mesh/print/repair/status/<id>   -> GET  /openapi/v1/print/repair/:id

Analyze is free (0 credits) and returns metrics only. Repair costs 10 credits
and returns a repaired model, which is saved to history like any other
derived model. Both accept `input_task_id` or `model_url`.

Docs checked 2026-08-15:
  https://docs.meshy.ai/en/api/analyze-printability
  https://docs.meshy.ai/en/api/repair-printability
"""

from __future__ import annotations

import uuid

from flask import Blueprint, g, jsonify, request

from backend.config import ACTION_KEYS, MESHY_API_KEY
from backend.db import USE_DB
from backend.middleware import with_session, with_session_readonly
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
    get_job_by_idempotency_key,
    get_job_metadata,
    load_store,
    save_store,
    verify_job_ownership,
)
from backend.services.meshy_service import (
    MeshyTaskNotFoundError,
    build_source_payload,
    mesh_get,
    mesh_post,
    meshy_alpha_thumbnail,
    normalize_meshy_task,
    terminalize_expired_meshy_job,
)
from backend.services.s3_service import save_finished_job_to_normalized_db
from backend.services.status_cache import cache_status, get_cached_status
from backend.utils import derive_display_title
from backend.utils.helpers import log_event, log_status_summary, now_s

bp = Blueprint("meshy_printability", __name__)

# stage -> (provider path segment, action key, human label)
_PRINTABILITY_OPS = {
    "print_analyze": ("print/analyze", ACTION_KEYS["print-analyze"], "Printability analysis"),
    "print_repair": ("print/repair", ACTION_KEYS["print-repair"], "Printability repair"),
}


def _as_int(value):
    try:
        if value is None or isinstance(value, bool):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value):
    try:
        if value is None or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_printability(ms: dict) -> dict | None:
    """
    Flatten Meshy's printability payload into one stable shape.

    Meshy nests the metrics under `printability` (and has used `result` as a
    wrapper on some task responses), so accept either and always return the
    same keys. Returns None when the task carries no metrics yet.
    """
    if not isinstance(ms, dict):
        return None

    candidates = []
    for container in (ms, ms.get("result"), ms.get("data")):
        if isinstance(container, dict):
            candidates.append(container)
            nested = container.get("printability")
            if isinstance(nested, dict):
                candidates.insert(0, nested)

    source = None
    for candidate in candidates:
        if any(
            key in candidate
            for key in ("is_watertight", "non_manifold_edges", "degenerate_faces", "holes", "volume")
        ):
            source = candidate
            break
    if source is None:
        return None

    is_watertight = source.get("is_watertight")
    holes = _as_int(source.get("holes"))
    non_manifold_edges = _as_int(source.get("non_manifold_edges"))
    degenerate_faces = _as_int(source.get("degenerate_faces"))
    error_count = _as_int(source.get("error_count"))
    warning_count = _as_int(source.get("warning_count"))

    status = (source.get("status") or "").strip().lower() or "unknown"
    if status not in {"healthy", "warning", "error", "unknown"}:
        status = "unknown"

    # Anything that would stop a slicer counts as repairable.
    needs_repair = bool(
        (is_watertight is False)
        or (holes or 0) > 0
        or (non_manifold_edges or 0) > 0
        or (degenerate_faces or 0) > 0
        or (error_count or 0) > 0
        or status == "error"
    )

    return {
        "status": status,
        "is_watertight": bool(is_watertight) if is_watertight is not None else None,
        "volume": _as_float(source.get("volume")),
        "holes": holes,
        "non_manifold_edges": non_manifold_edges,
        "degenerate_faces": degenerate_faces,
        "error_count": error_count,
        "warning_count": warning_count,
        "evaluated_at": source.get("evaluated_at"),
        "needs_repair": needs_repair,
    }


def _start_printability_task(*, stage: str, body: dict, identity_id: str, payload: dict):
    """Reserve credits, create the job row, and dispatch to Meshy."""
    provider_segment, action_key, label = _PRINTABILITY_OPS[stage]

    # Repair costs 10 credits, so a double submit must not start two jobs.
    idempotency_key = (
        request.headers.get("Idempotency-Key") or body.get("idempotency_key") or ""
    ).strip() or None
    if idempotency_key:
        existing_job = get_job_by_idempotency_key(identity_id, idempotency_key)
        if existing_job and existing_job.get("upstream_job_id"):
            balance_info = get_current_balance(identity_id)
            return jsonify({
                "ok": True,
                "job_id": existing_job["upstream_job_id"],
                "reservation_id": existing_job.get("reservation_id"),
                "new_balance": balance_info["available"] if balance_info else None,
                "stage": stage,
                "source": "modular",
                "was_existing": True,
            })
        if existing_job:
            return jsonify({
                "ok": False,
                "error": "JOB_ALREADY_STARTING",
                "message": f"{label} is already being started. Please wait a moment.",
            }), 409

    internal_job_id = str(uuid.uuid4())
    store = load_store()
    source_task_id = payload.get("input_task_id") or body.get("source_task_id")
    source_meta = get_job_metadata(source_task_id, store) or {}
    original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt
    title = derive_display_title(
        original_prompt,
        body.get("title") or source_meta.get("title"),
        root_prompt=root_prompt,
    )

    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": stage,
        "source_task_id": source_task_id,
    }
    if "alpha_thumbnail" in payload:
        job_meta["alpha_thumbnail"] = payload["alpha_thumbnail"]

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=original_prompt,
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
        idempotency_key=idempotency_key,
    )

    try:
        resp = mesh_post(f"/openapi/v1/{provider_segment}", payload)
        log_event(f"mesh/{stage}:meshy-resp[mod]", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            _update_job_status_failed(internal_job_id, f"{label} failed to start")
            return jsonify({
                "ok": False,
                "error": "PRINTABILITY_FAILED",
                "message": f"{label} could not be started. Please try again.",
            }), 502
    except Exception as exc:
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        _update_job_status_failed(internal_job_id, str(exc))
        from backend.services.error_sanitizer import MODEL_GENERATION_FAILED, sanitize_provider_error

        return jsonify(sanitize_provider_error(
            provider="meshy", error=exc, job_id=internal_job_id, code=MODEL_GENERATION_FAILED,
        )), 502

    from backend.services.async_dispatch import update_job_with_upstream_id

    update_job_with_upstream_id(internal_job_id, meshy_task_id)
    store[meshy_task_id] = {
        **job_meta,
        "created_at": now_s() * 1000,
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
        "stage": stage,
        "source": "modular",
    })


def _printability_status(stage: str, job_id: str):
    """Shared status handler for both printability tasks."""
    provider_segment, _action_key, label = _PRINTABILITY_OPS[stage]

    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    identity_id = g.identity_id
    if not verify_job_ownership(job_id, identity_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/v1/{provider_segment}/{job_id}")
        log_event(f"mesh/{stage}/status:meshy-resp[mod]", ms)
    except MeshyTaskNotFoundError:
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({
            "status": "failed",
            "error": "TASK_EXPIRED",
            "message": "This printability task has expired on the provider.",
        }), 200
    except Exception as exc:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} stage={stage} error={exc}")
        return jsonify({
            "error": "PRINTABILITY_STATUS_FAILED",
            "message": "Failed to fetch printability status. Please try again.",
        }), 502

    out = normalize_meshy_task(ms, stage=stage)
    printability = normalize_printability(ms)
    if printability:
        out["printability"] = printability
    log_status_summary(f"mesh/{stage}[mod]", job_id, out)

    store = load_store()
    meta = get_job_metadata(job_id, store) or {}
    reservation_id = meta.get("reservation_id")
    internal_job_id = meta.get("internal_job_id")

    if out["status"] == "failed":
        error_msg = out.get("message") or out.get("error") or f"{label} failed"
        if reservation_id:
            release_job_credits(reservation_id, "provider_job_failed", internal_job_id or job_id)
        if internal_job_id:
            _update_job_status_failed(internal_job_id, error_msg)

    elif out["status"] == "done":
        if reservation_id:
            finalize_job_credits(reservation_id, internal_job_id or job_id, meta.get("identity_id") or identity_id)

        # Analyze produces metrics only; repair produces a model worth keeping.
        if stage == "print_repair" and (out.get("glb_url") or out.get("model_urls")):
            if identity_id and not meta.get("identity_id"):
                meta["identity_id"] = identity_id
                meta["user_id"] = identity_id
            user_id = meta.get("identity_id") or meta.get("user_id") or identity_id
            try:
                s3_result = save_finished_job_to_normalized_db(
                    job_id, out, meta, job_type="print_repair", user_id=user_id
                )
                if s3_result and s3_result.get("success"):
                    if s3_result.get("glb_url"):
                        out["glb_url"] = s3_result["glb_url"]
                    if s3_result.get("thumbnail_url"):
                        out["thumbnail_url"] = s3_result["thumbnail_url"]
                    if s3_result.get("model_urls"):
                        out["model_urls"] = s3_result["model_urls"]
                    if internal_job_id:
                        _update_job_status_ready(
                            internal_job_id,
                            upstream_job_id=job_id,
                            model_id=s3_result.get("model_id"),
                            glb_url=s3_result.get("glb_url"),
                        )
            except Exception as exc:
                print(f"[mesh/{stage}] saving repaired model failed: {exc}")
        elif internal_job_id and USE_DB:
            try:
                _update_job_status_ready(internal_job_id, upstream_job_id=job_id)
            except Exception as exc:
                print(f"[mesh/{stage}] job status→ready failed: {exc}")

    cache_status(job_id, out, is_terminal=(out["status"] in ("done", "failed")))
    return jsonify(out)


# ─────────────────────────────────────────────────────────────
# Analyze Printability — free, metrics only
# ─────────────────────────────────────────────────────────────

@bp.route("/mesh/print/analyze", methods=["POST", "OPTIONS"])
@with_session
def mesh_print_analyze_mod():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("mesh/print/analyze:incoming[mod]", body)
    source, err = build_source_payload(body, identity_id=identity_id, prefer="input_task_id")
    if err:
        return jsonify({"ok": False, "error": err}), 400

    return _start_printability_task(
        stage="print_analyze", body=body, identity_id=identity_id, payload={**source},
    )


@bp.route("/mesh/print/analyze/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def mesh_print_analyze_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503
    return _printability_status("print_analyze", job_id)


# ─────────────────────────────────────────────────────────────
# Repair Printability — 10 credits, returns a repaired model
# ─────────────────────────────────────────────────────────────

@bp.route("/mesh/print/repair", methods=["POST", "OPTIONS"])
@with_session
def mesh_print_repair_mod():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("mesh/print/repair:incoming[mod]", body)
    source, err = build_source_payload(body, identity_id=identity_id, prefer="input_task_id")
    if err:
        return jsonify({"ok": False, "error": err}), 400

    payload = {**source}
    # alpha_thumbnail is the only optional parameter Meshy documents here.
    alpha_thumbnail = meshy_alpha_thumbnail(body)
    if alpha_thumbnail is not None:
        payload["alpha_thumbnail"] = alpha_thumbnail

    return _start_printability_task(
        stage="print_repair", body=body, identity_id=identity_id, payload=payload,
    )


@bp.route("/mesh/print/repair/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def mesh_print_repair_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503
    return _printability_status("print_repair", job_id)
