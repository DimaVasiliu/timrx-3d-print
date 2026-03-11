"""
Webhook routes for external service callbacks.

Two webhook endpoints:

1. POST /api/webhooks/piapi        — PiAPI account-level notifications (quota, suspension)
2. POST /api/webhooks/piapi/task   — PiAPI task-level completion callbacks (video jobs)

The task webhook is the real completion path for Seedance video jobs.
Polling in job_worker.py remains as a fallback safety net.

Render env vars:
    PIAPI_WEBHOOK_ENABLED   — "true" to accept webhooks (default: false)
    PIAPI_WEBHOOK_SECRET    — shared secret for verification
    PIAPI_WEBHOOK_LOG_BODY  — "true" to log full payload (default: false)

Webhook URL to register in PiAPI dashboard:
    https://3d.timrx.live/api/webhooks/piapi       (account)
    https://3d.timrx.live/api/webhooks/piapi/task   (task — set via webhook_config per task)
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional, Tuple

from flask import Blueprint, request, jsonify

from backend.config import config

bp = Blueprint("webhooks", __name__)


# ── Status mapping (same as seedance_service._STATUS_MAP) ────
_STATUS_MAP = {
    "Completed": "done",
    "completed": "done",
    "Processing": "processing",
    "processing": "processing",
    "Pending": "pending",
    "pending": "pending",
    "Staged": "pending",
    "staged": "pending",
    "Failed": "failed",
    "failed": "failed",
}

# States that must never be overwritten by a webhook.
_TERMINAL_AND_FINALIZING = frozenset({
    "succeeded", "failed", "refunded", "ready", "ready_unbilled",
    "abandoned_legacy", "recovery_blocked", "finalizing",
})


# ── Helpers ──────────────────────────────────────────────────

def _safe_get_json() -> Tuple[Optional[Dict], Optional[str]]:
    """Parse JSON body, return (dict, None) or (None, error_string)."""
    try:
        data = request.get_json(silent=True)
        if data is None:
            return None, "missing or invalid JSON body"
        if not isinstance(data, dict):
            return None, "body must be a JSON object"
        return data, None
    except Exception as e:
        return None, f"JSON parse error: {e}"


def _webhook_secret_is_valid() -> bool:
    """
    Check webhook secret if configured.

    Accepts either:
      - Header: X-Webhook-Secret: <secret>
      - Query param: ?secret=<secret>

    Returns True if secret matches or if no secret is configured.
    """
    secret = config.PIAPI_WEBHOOK_SECRET
    if not secret:
        return True

    header_val = request.headers.get("X-Webhook-Secret", "")
    query_val = request.args.get("secret", "")
    return header_val == secret or query_val == secret


def _extract_task_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract the task payload from a PiAPI webhook body.

    PiAPI may send the task object at top-level, nested under "data",
    or nested under "task".  This handles all three.
    """
    # Prefer nested data/task objects if they contain a task_id
    for key in ("data", "task"):
        nested = data.get(key)
        if isinstance(nested, dict) and nested.get("task_id"):
            return nested
    # Fall through to top-level
    return data


def _extract_video_url(task_data: Dict[str, Any]) -> Optional[str]:
    """Extract video URL from a completed task payload."""
    output = task_data.get("output") or {}
    url = (
        output.get("video")
        or output.get("video_url")
        or output.get("video_urls", [None])[0]
    )
    if not url:
        url = task_data.get("video_url") or task_data.get("video")
    return url


def _extract_error_message(task_data: Dict[str, Any]) -> str:
    """Extract a human-readable error message from a failed task payload."""
    error = task_data.get("error", {})
    if isinstance(error, dict):
        return error.get("message", "") or error.get("code", "") or "Provider generation failed"
    return str(error) or "Provider generation failed"


# ── DB helpers ───────────────────────────────────────────────

def _find_and_claim_job(upstream_id: str, new_status: str) -> Optional[Dict[str, Any]]:
    """
    Atomically find a video job by upstream_job_id and transition it.

    Returns the job row dict if the transition succeeded, or None if:
      - no job found for this upstream_id
      - job is already in a terminal or finalizing state (idempotent no-op)

    Uses UPDATE ... WHERE status NOT IN (...) RETURNING to guarantee
    only one actor (webhook or poll worker) can claim the transition.
    """
    from backend.db import USE_DB, get_conn, Tables

    if not USE_DB:
        return None

    excluded = ", ".join(f"'{s}'" for s in _TERMINAL_AND_FINALIZING)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = %s,
                        updated_at = NOW(),
                        meta = COALESCE(meta, '{{}}'::jsonb)
                               || %s::jsonb
                    WHERE upstream_job_id = %s
                      AND stage = 'video'
                      AND status NOT IN ({excluded})
                    RETURNING id, identity_id, provider, reservation_id,
                              upstream_job_id, prompt, meta, action_code,
                              status, stage
                    """,
                    (
                        new_status,
                        json.dumps({
                            "webhook_claimed_at": time.time(),
                            "webhook_status": new_status,
                        }),
                        upstream_id,
                    ),
                )
                row = cur.fetchone()
            conn.commit()
            return row
    except Exception as e:
        print(f"[WEBHOOK] DB error finding job for upstream_id={upstream_id}: {e}")
        return None


def _transition_job_status(
    job_id: str,
    new_status: str,
    field_updates: Optional[Dict[str, Any]] = None,
    meta_patch: Optional[Dict[str, Any]] = None,
) -> None:
    """Lightweight status transition for webhook use (mirrors job_worker._transition_job)."""
    from backend.db import USE_DB, get_conn, Tables

    if not USE_DB:
        return

    try:
        set_clauses = ["status = %s", "updated_at = NOW()"]
        params: list = [new_status]

        if field_updates:
            for col, val in field_updates.items():
                if col == "meta":
                    continue
                if isinstance(val, str) and any(kw in val.upper() for kw in ("NOW()", "INTERVAL")):
                    set_clauses.append(f"{col} = {val}")
                else:
                    set_clauses.append(f"{col} = %s")
                    params.append(val)

        if meta_patch:
            set_clauses.append("meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb")
            params.append(json.dumps(meta_patch, default=str))

        params.append(job_id)

        sql = f"""
            UPDATE {Tables.JOBS}
            SET {', '.join(set_clauses)}
            WHERE id::text = %s
        """

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
            conn.commit()
    except Exception as e:
        print(f"[WEBHOOK] transition error job={job_id} -> {new_status}: {e}")


# ── Webhook finalization (runs in background thread) ─────────

def _webhook_finalize_success(
    job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    video_url: str,
    meta: Dict[str, Any],
    provider_name: str,
) -> None:
    """
    Finalize a successful video job triggered by webhook.

    Reuses the same finalization path as job_worker:
      1. Download video from provider
      2. Upload to S3
      3. Extract thumbnail
      4. Finalize credits (capture)
      5. Save to normalized DB tables
      6. Transition to 'ready'

    If finalization fails, transitions to 'stalled' so the
    poll worker can recover.
    """
    try:
        from backend.services.async_dispatch import _finalize_video_success
        from backend.services.expense_guard import ExpenseGuard

        store_meta = {
            "status": "processing",
            "provider": provider_name,
            "upstream_id": meta.get("upstream_id", ""),
            "operation_name": meta.get("upstream_id", ""),
            "prompt": meta.get("prompt", ""),
            "identity_id": identity_id,
            "reservation_id": reservation_id,
            "duration_seconds": meta.get("duration_seconds"),
            "aspect_ratio": meta.get("aspect_ratio"),
            "resolution": meta.get("resolution"),
            "task_type": meta.get("task_type") or meta.get("seedance_variant") or "seedance-2-fast-preview",
            "seedance_variant": meta.get("seedance_variant") or meta.get("task_type") or "seedance-2-fast-preview",
            "seedance_tier": meta.get("seedance_tier", "fast"),
            "stage": "video",
            "task": meta.get("task", "text2video"),
            "internal_job_id": job_id,
        }

        _finalize_video_success(
            internal_job_id=job_id,
            identity_id=identity_id,
            reservation_id=reservation_id,
            video_url=video_url,
            store_meta=store_meta,
            provider_name=provider_name,
        )

        _transition_job_status(job_id, "ready", {
            "result_url": video_url,
            "completed_at": "NOW()",
            "claimed_by": None,
            "claimed_at": None,
        }, meta_patch={
            "webhook_finalized": True,
            "webhook_finalized_at": time.time(),
        })

        ExpenseGuard.unregister_active_job(job_id)

        print(f"[WEBHOOK] finalized job={job_id} provider={provider_name}")

    except Exception as e:
        print(f"[WEBHOOK] finalization FAILED job={job_id}: {e}")
        import traceback
        traceback.print_exc()

        # Mark as stalled so the poll worker can recover
        _transition_job_status(job_id, "stalled", meta_patch={
            "webhook_finalize_error": str(e)[:300],
            "webhook_finalize_failed_at": time.time(),
        })
        print(f"[WEBHOOK] job={job_id} marked stalled for worker recovery")


def _webhook_mark_failed(
    job_id: str,
    meta: Dict[str, Any],
    error_msg: str,
    error_code: str,
    provider_name: str,
) -> None:
    """
    Mark a job as failed via webhook. Releases credits for terminal errors.
    """
    from backend.services.credits_helper import release_job_credits

    print(f"[WEBHOOK] FAIL job={job_id} code={error_code} provider={provider_name}")

    reservation_id = meta.get("reservation_id")
    if reservation_id:
        try:
            release_job_credits(str(reservation_id), error_code, job_id)
            print(f"[WEBHOOK] credits RELEASED job={job_id} reason={error_code}")
        except Exception as e:
            print(f"[WEBHOOK] credit release error job={job_id}: {e}")

    _transition_job_status(job_id, "failed", {
        "completed_at": "NOW()",
        "claimed_by": None,
    }, meta_patch={
        "error_code": error_code,
        "error_message": error_msg[:500],
        "failure_provider": provider_name,
        "webhook_failed": True,
        "webhook_failed_at": time.time(),
    })


# ── PiAPI Account Webhook (existing) ────────────────────────

@bp.route("/webhooks/piapi", methods=["POST"])
def piapi_webhook():
    """
    Receive PiAPI account-level notifications.

    Handles quota alerts, suspension notices, and general account events.
    Does NOT interact with job polling or video generation logic.
    """
    if not config.PIAPI_WEBHOOK_ENABLED:
        return jsonify({"error": "webhook_disabled"}), 403

    if not _webhook_secret_is_valid():
        print("[PIAPI_WEBHOOK] unauthorized")
        return jsonify({"error": "unauthorized"}), 401

    if not config.PIAPI_WEBHOOK_SECRET:
        print("[PIAPI_WEBHOOK] verification disabled (no PIAPI_WEBHOOK_SECRET set)")

    data, err = _safe_get_json()
    if err:
        print(f"[PIAPI_WEBHOOK] bad request: {err}")
        return jsonify({"error": "bad_request", "message": err}), 400

    try:
        event = data.get("event") or data.get("type") or data.get("action") or "unknown"
        event_type = data.get("type") or data.get("event_type") or ""
        top_keys = sorted(data.keys())

        print(
            f"[PIAPI_WEBHOOK] received event={event} type={event_type} "
            f"keys={top_keys}"
        )

        if config.PIAPI_WEBHOOK_LOG_BODY:
            print(f"[PIAPI_WEBHOOK] body={json.dumps(data, default=str)}")

        return jsonify({"ok": True}), 200

    except Exception as e:
        print(f"[PIAPI_WEBHOOK][ERROR] {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "internal_error"}), 500


# ── PiAPI Task Webhook (NEW — real completion handler) ───────

@bp.route("/webhooks/piapi/task", methods=["POST"])
def piapi_task_webhook():
    """
    Receive PiAPI task-level completion callbacks for video jobs.

    This is the fast completion path. When a Seedance video job finishes
    (success or failure), PiAPI posts the task result here. The handler:

    1. Verifies the shared secret
    2. Extracts the task_id and status from the payload
    3. Finds the local job by upstream_job_id
    4. Atomically transitions the job (prevents duplicate handling)
    5. Finalizes success or marks failure
    6. Returns 200 so PiAPI does not retry

    Idempotency: duplicate deliveries are safe — the atomic transition
    ensures only the first delivery triggers state changes.
    """
    # Gate
    if not config.PIAPI_WEBHOOK_ENABLED:
        return jsonify({"error": "webhook_disabled"}), 403

    # Auth
    if not _webhook_secret_is_valid():
        print("[WEBHOOK] rejected: invalid secret")
        return jsonify({"error": "unauthorized"}), 401

    # Parse
    data, err = _safe_get_json()
    if err:
        print(f"[WEBHOOK] bad request: {err}")
        return jsonify({"error": "bad_request", "message": err}), 400

    if config.PIAPI_WEBHOOK_LOG_BODY:
        print(f"[WEBHOOK] raw body={json.dumps(data, default=str)}")

    # Extract task data from potentially nested payload
    task_data = _extract_task_data(data)
    task_id = task_data.get("task_id") or data.get("task_id")

    if not task_id:
        print(f"[WEBHOOK] ignored: no task_id in payload keys={sorted(data.keys())}")
        return jsonify({"ok": True, "action": "ignored_no_task_id"}), 200

    # Map provider status to internal status
    raw_status = task_data.get("status", "unknown")
    internal_status = _STATUS_MAP.get(raw_status, "unknown")

    print(
        f"[WEBHOOK] received task_id={task_id} status={raw_status} "
        f"internal={internal_status} progress={task_data.get('progress', '?')}"
    )

    # ── Success ──────────────────────────────────────────────
    if internal_status == "done":
        video_url = _extract_video_url(task_data)
        if not video_url:
            print(f"[WEBHOOK] task_id={task_id} status=done but no video_url, ignoring (poll will retry)")
            return jsonify({"ok": True, "action": "ignored_no_video_url"}), 200

        # Atomically claim the job for finalization
        job = _find_and_claim_job(task_id, "finalizing")
        if not job:
            print(f"[WEBHOOK] task_id={task_id} no claimable job (unknown or already handled)")
            return jsonify({"ok": True, "action": "no_op"}), 200

        job_id = str(job["id"])
        identity_id = str(job.get("identity_id") or "")
        reservation_id = str(job.get("reservation_id") or "") or None
        provider_name = job.get("provider") or "seedance"
        meta = job.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except (json.JSONDecodeError, TypeError):
                meta = {}

        print(f"[WEBHOOK] finalizing job={job_id} task_id={task_id} provider={provider_name}")

        # Dispatch finalization to background thread so we return 200 fast.
        # If finalization fails, the job is marked 'stalled' for worker recovery.
        from backend.services.async_dispatch import get_executor
        executor = get_executor()
        executor.submit(
            _webhook_finalize_success,
            job_id, identity_id, reservation_id,
            video_url, meta, provider_name,
        )

        return jsonify({"ok": True, "action": "finalizing", "job_id": job_id}), 200

    # ── Failure ──────────────────────────────────────────────
    if internal_status == "failed":
        error_msg = _extract_error_message(task_data)
        error_code = "seedance_generation_failed"

        job = _find_and_claim_job(task_id, "failed")
        if not job:
            print(f"[WEBHOOK] task_id={task_id} failed but no claimable job (unknown or already handled)")
            return jsonify({"ok": True, "action": "no_op"}), 200

        job_id = str(job["id"])
        provider_name = job.get("provider") or "seedance"
        meta = job.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except (json.JSONDecodeError, TypeError):
                meta = {}

        print(f"[WEBHOOK] failing job={job_id} task_id={task_id} error={error_msg[:100]}")

        _webhook_mark_failed(job_id, meta, error_msg, error_code, provider_name)

        return jsonify({"ok": True, "action": "failed", "job_id": job_id}), 200

    # ── In-progress updates (pending/processing) ────────────
    # These are informational only. The poll worker handles progress
    # tracking. We just log and acknowledge.
    if internal_status in ("processing", "pending"):
        print(f"[WEBHOOK] progress task_id={task_id} status={internal_status}")
        return jsonify({"ok": True, "action": "progress_noted"}), 200

    # ── Unknown status ───────────────────────────────────────
    print(f"[WEBHOOK] unknown status={raw_status} task_id={task_id}")
    return jsonify({"ok": True, "action": "ignored_unknown_status"}), 200
