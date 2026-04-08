"""
Multi-Color 3D Print Route
---------------------------
Converts textured 3D models into slicer-ready 3MF files with
configurable color palettes (1–16 colors).

POST /api/_mod/print/multi-color   — Start a multi-color print job
GET  /api/_mod/print/multi-color/<job_id> — Poll job status

Uses Meshy API:  POST /openapi/v1/print/multi-color
                 GET  /openapi/v1/print/multi-color/<id>
"""

from __future__ import annotations

import time as _time
import uuid

from flask import Blueprint, jsonify, request, g

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
    _update_job_status_ready,
)
from backend.services.meshy_service import (
    build_source_payload,
    mesh_get,
    mesh_post,
    MeshyTaskNotFoundError,
    terminalize_expired_meshy_job,
)
from backend.services.status_cache import get_cached_status, cache_status
from backend.utils.helpers import log_event, log_status_summary, now_s

bp = Blueprint("multi_color_print", __name__)

# ── Finalization dedup (same pattern as text_to_3d / remesh) ──
_finalized_jobs: dict = {}  # job_id -> monotonic timestamp
_FINALIZED_TTL = 1800  # 30 min


def _already_finalized(job_id: str) -> bool:
    ts = _finalized_jobs.get(job_id)
    if ts and (_time.monotonic() - ts) < _FINALIZED_TTL:
        return True
    return False


def _mark_finalized(job_id: str):
    _finalized_jobs[job_id] = _time.monotonic()
    # Lazy cleanup of expired entries
    if len(_finalized_jobs) > 200:
        cutoff = _time.monotonic() - _FINALIZED_TTL
        expired = [k for k, v in _finalized_jobs.items() if v < cutoff]
        for k in expired:
            del _finalized_jobs[k]


def _normalize_multi_color_task(ms: dict) -> dict:
    """
    Normalize Meshy multi-color print response to standard frontend shape.
    The multi-color endpoint returns a different structure than model tasks.
    """
    status_raw = (ms.get("status") or "PENDING").upper()
    status_map = {
        "PENDING": "pending",
        "IN_PROGRESS": "running",
        "SUCCEEDED": "done",
        "COMPLETED": "done",
        "FINISHED": "done",
        "FAILED": "failed",
        "CANCELLED": "failed",
        "TIMEOUT": "failed",
    }
    status = status_map.get(status_raw, "pending")
    pct = ms.get("progress") or 0
    if status == "done":
        pct = 100

    result = ms.get("result") or ms

    out = {
        "id": ms.get("id") or result.get("id"),
        "status": status,
        "pct": pct,
        "stage": "multi_color_print",
        "three_mf_url": result.get("three_mf_url") or ms.get("three_mf_url"),
        "thumbnail_url": result.get("thumbnail_url") or ms.get("thumbnail_url"),
        "created_at": ms.get("created_at"),
    }

    if status == "failed":
        err = ms.get("task_error") or {}
        out["message"] = err.get("message") or ms.get("message") or "Multi-color print failed"

    return out


@bp.route("/print/multi-color", methods=["POST", "OPTIONS"])
@with_session
def multi_color_start():
    """
    Start a multi-color 3D print job.

    Body:
        input_task_id: str  — Task ID from a prior 3D generation
                              (text-to-3d, image-to-3d, remesh, retexture)
        max_colors: int     — Color palette size, 1–16 (default 4)
        max_depth: int      — Quadtree depth for color precision, 3–6 (default 4)
    """
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("print/multi-color:incoming", body)

    # ── Source resolution ────────────────────────────────────────────
    # Multi-color print requires input_task_id — a completed Meshy task.
    source, err = build_source_payload(body, identity_id=identity_id, prefer="input_task_id")
    if err:
        return jsonify({"ok": False, "error": err}), 400

    input_task_id = source.get("input_task_id")
    if not input_task_id:
        return jsonify({
            "ok": False,
            "error": "input_task_id required — select a completed 3D model first.",
            "code": "SOURCE_TASK_REQUIRED",
        }), 400

    # ── Validate parameters ──────────────────────────────────────────
    try:
        max_colors = int(body.get("max_colors", 4))
    except (TypeError, ValueError):
        max_colors = 4
    max_colors = max(1, min(16, max_colors))

    try:
        max_depth = int(body.get("max_depth", 4))
    except (TypeError, ValueError):
        max_depth = 4
    max_depth = max(3, min(6, max_depth))

    # ── Build Meshy payload ──────────────────────────────────────────
    payload = {
        "input_task_id": input_task_id,
        "max_colors": max_colors,
        "max_depth": max_depth,
    }

    # ── Resolve source metadata for history lineage ──────────────────
    store = load_store()
    source_meta = get_job_metadata(input_task_id, store) or {}
    original_prompt = source_meta.get("prompt") or body.get("prompt") or ""
    root_prompt = source_meta.get("root_prompt") or original_prompt
    title = source_meta.get("title") or original_prompt[:60] or "Multi-Color Print"

    internal_job_id = str(uuid.uuid4())
    action_key = ACTION_KEYS.get("multi-color-print", "multi_color_print")

    job_meta = {
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "stage": "multi_color_print",
        "source_task_id": input_task_id,
        "max_colors": max_colors,
        "max_depth": max_depth,
    }

    # ── Reserve credits BEFORE calling upstream ──────────────────────
    reservation_id, credit_error = start_paid_job(
        identity_id, action_key, internal_job_id, job_meta
    )
    if credit_error:
        return credit_error

    # ── Persist job row for ownership/status tracking ────────────────
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

    # ── Dispatch to Meshy ────────────────────────────────────────────
    try:
        resp = mesh_post("/openapi/v1/print/multi-color", payload)
        log_event("print/multi-color:meshy-resp", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            print(f"[MULTI_COLOR] provider=meshy job_id={internal_job_id} error=no_task_id raw={resp}")
            return jsonify({
                "ok": False,
                "error": "PRINT_JOB_FAILED",
                "message": "Multi-color print job could not be started. Please try again.",
            }), 502
    except Exception as e:
        release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        from backend.services.error_sanitizer import sanitize_provider_error, MODEL_GENERATION_FAILED
        return jsonify(sanitize_provider_error(
            provider="meshy", error=e, job_id=internal_job_id,
            code=MODEL_GENERATION_FAILED,
        )), 502

    # ── Link internal job to upstream Meshy task ─────────────────────
    update_job_with_upstream_id(internal_job_id, meshy_task_id)

    # ── Persist to in-memory store for metadata retrieval on poll ─────
    store[meshy_task_id] = {
        "stage": "multi_color_print",
        "source_task_id": input_task_id,
        "created_at": now_s() * 1000,
        "prompt": original_prompt,
        "root_prompt": root_prompt,
        "title": title,
        "max_colors": max_colors,
        "max_depth": max_depth,
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


@bp.route("/print/multi-color/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def multi_color_status(job_id: str):
    """Poll status of a multi-color print job."""
    if request.method == "OPTIONS":
        return ("", 204)
    log_event("print/multi-color/status:incoming", {"job_id": job_id})

    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    # ── Short-circuit: cached response ───────────────────────────────
    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    identity_id = g.identity_id

    # ── Ownership check ──────────────────────────────────────────────
    ownership = verify_job_ownership_detailed(job_id, identity_id)
    if not ownership["found"]:
        return jsonify({"error": "Job not found", "code": "JOB_NOT_FOUND"}), 404
    if not ownership["authorized"]:
        return jsonify({"error": "Access denied", "code": "FORBIDDEN"}), 403

    # ── Poll Meshy ───────────────────────────────────────────────────
    try:
        ms = mesh_get(f"/openapi/v1/print/multi-color/{job_id}")
        log_event("print/multi-color/status:meshy-resp", ms)
    except MeshyTaskNotFoundError:
        print(f"[MULTI_COLOR] Task expired: job_id={job_id}")
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({
            "status": "failed",
            "error": "TASK_EXPIRED",
            "message": "This job has expired on the provider.",
        }), 200
    except Exception as e:
        print(f"[MULTI_COLOR] provider=meshy job_id={job_id} error={e}")
        return jsonify({
            "error": "PRINT_JOB_FAILED",
            "message": "Failed to fetch job status. Please try again.",
        }), 502

    out = _normalize_multi_color_task(ms)
    log_status_summary("print/multi-color", job_id, out)

    # ── Handle failure → refund ──────────────────────────────────────
    if out["status"] == "failed":
        try:
            from backend.services.credits_helper import refund_failed_job
            refund_failed_job(job_id)
        except Exception as e:
            print(f"[MULTI_COLOR] auto-refund failed: {e}")

    # ── Handle success → finalize credits + persist ──────────────────
    if out["status"] == "done" and not _already_finalized(job_id):
        store = load_store()
        meta = get_job_metadata(job_id, store) or {}

        # Finalize credits
        try:
            res_id = meta.get("reservation_id")
            int_job = meta.get("internal_job_id") or job_id
            cred_identity = meta.get("identity_id") or identity_id
            if res_id:
                finalize_job_credits(res_id, int_job, cred_identity)
                _mark_finalized(job_id)
        except Exception as e:
            print(f"[MULTI_COLOR] credit finalize failed: {e}")

        # Persist 3MF result to history
        if USE_DB and identity_id and out.get("three_mf_url"):
            try:
                _save_multi_color_to_history(
                    job_id=job_id,
                    identity_id=meta.get("identity_id") or identity_id,
                    meta=meta,
                    three_mf_url=out["three_mf_url"],
                    thumbnail_url=out.get("thumbnail_url"),
                )
            except Exception as e:
                print(f"[MULTI_COLOR] history save failed: {e}")

        # Update internal job status
        try:
            int_job = meta.get("internal_job_id")
            if not int_job and USE_DB:
                from backend.db import query_one, Tables as _T
                _row = query_one(
                    f"SELECT id::text AS jid FROM {_T.JOBS} WHERE upstream_job_id = %s LIMIT 1",
                    (job_id,),
                )
                int_job = _row["jid"] if _row else None
            if int_job:
                _update_job_status_ready(
                    int_job,
                    upstream_job_id=job_id,
                    glb_url=out.get("three_mf_url"),
                )
        except Exception as e:
            print(f"[MULTI_COLOR] job status→ready failed: {e}")

    cache_status(job_id, out, is_terminal=(out["status"] in ("done", "failed")))
    return jsonify(out)


def _save_multi_color_to_history(
    job_id: str,
    identity_id: str,
    meta: dict,
    three_mf_url: str,
    thumbnail_url: str | None,
):
    """
    Persist the multi-color print result as a history item so it appears
    in the user's generation history and can be downloaded later.
    """
    from backend.db import get_conn, Tables
    import json

    payload = {
        "stage": "multi_color_print",
        "source_task_id": meta.get("source_task_id"),
        "three_mf_url": three_mf_url,
        "max_colors": meta.get("max_colors", 4),
        "max_depth": meta.get("max_depth", 4),
        "prompt": meta.get("prompt"),
        "title": meta.get("title"),
    }

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {Tables.HISTORY_ITEMS}
                        (id, identity_id, item_type, stage, title, prompt, glb_url, thumbnail_url, payload)
                    VALUES (%s, %s, 'model', 'multi_color_print', %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        glb_url = EXCLUDED.glb_url,
                        thumbnail_url = EXCLUDED.thumbnail_url,
                        payload = EXCLUDED.payload,
                        updated_at = NOW()
                    """,
                    (
                        job_id,
                        identity_id,
                        meta.get("title") or "Multi-Color Print",
                        meta.get("prompt") or "",
                        three_mf_url,
                        thumbnail_url,
                        json.dumps(payload),
                    ),
                )
            conn.commit()
        print(f"[MULTI_COLOR] Saved to history: job_id={job_id}")
    except Exception as e:
        print(f"[MULTI_COLOR] History save SQL failed: {e}")
        raise
