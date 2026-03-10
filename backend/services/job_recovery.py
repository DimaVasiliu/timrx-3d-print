"""
Job Recovery Service.

Recovers video generation jobs that were in-progress when the server restarted.
Instead of blindly marking stale jobs as failed, checks PiAPI upstream status
first and takes the correct action:
  - PiAPI done   -> finalize (download, S3, credits, history)
  - PiAPI failed -> mark failed, release credits
  - PiAPI still running -> resume polling thread
  - PiAPI unreachable   -> skip, retry on next restart
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


def recover_stale_jobs(app) -> Dict[str, int]:
    """
    Startup recovery: find stale video jobs and reconcile with PiAPI.

    Called once from create_app(). Runs synchronously — PiAPI status checks
    are fast (~200ms each). Resume-polling is dispatched to the background
    ThreadPoolExecutor.

    Returns dict with counts: {recovered, resumed, failed, skipped}.
    """
    from backend.db import USE_DB, get_conn, Tables

    if not USE_DB:
        return {"recovered": 0, "resumed": 0, "failed": 0, "skipped": 0}

    counts = {"recovered": 0, "resumed": 0, "failed": 0, "skipped": 0}

    # Step 1: Claim stale jobs inside a transaction with FOR UPDATE SKIP LOCKED
    stale_rows = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, user_id, upstream_job_id, meta, status, created_at, updated_at
                    FROM {Tables.JOBS}
                    WHERE status IN ('pending', 'processing', 'provider_pending', 'provider_processing', 'recovering')
                      AND meta->>'stage' = 'video'
                      AND updated_at < NOW() - INTERVAL '5 minutes'
                    FOR UPDATE SKIP LOCKED
                    """,
                )
                stale_rows = cur.fetchall()

                if stale_rows:
                    job_ids = [str(r["id"]) for r in stale_rows]
                    cur.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'recovering', updated_at = NOW()
                        WHERE id::text = ANY(%s)
                        """,
                        (job_ids,),
                    )
            conn.commit()
    except Exception as e:
        print(f"[RECOVERY] Error querying stale jobs: {e}")
        return counts

    if not stale_rows:
        return counts

    print(f"[RECOVERY] Found {len(stale_rows)} stale video jobs")

    # Step 2: Process each job outside the transaction
    for row in stale_rows:
        job_id = str(row["id"])
        original_status = row["status"]
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except (json.JSONDecodeError, TypeError):
                meta = {}

        upstream_id = row.get("upstream_job_id") or meta.get("upstream_id")

        try:
            if not upstream_id:
                # Job never reached PiAPI — mark failed, release credits
                print(f"[RECOVERY] Job {job_id}: No upstream_job_id, marking failed")
                _mark_failed_and_release(job_id, meta, "Job never reached provider")
                counts["failed"] += 1
                continue

            # Check PiAPI status
            piapi_result = _check_upstream(upstream_id)

            if piapi_result["status"] == "done":
                video_url = piapi_result.get("video_url")
                if not video_url:
                    print(f"[RECOVERY] Job {job_id}: PiAPI done but no video_url, marking failed")
                    _mark_failed_and_release(job_id, meta, "Provider completed but no video URL")
                    counts["failed"] += 1
                    continue

                print(f"[RECOVERY] Job {job_id}: PiAPI status=done, finalizing...")
                try:
                    _finalize_recovered_job(job_id, row, meta, video_url)
                    print(f"[RECOVERY] Job {job_id}: Finalized successfully")
                    counts["recovered"] += 1
                except Exception as fin_err:
                    print(f"[RECOVERY] Job {job_id}: Finalization failed: {fin_err}")
                    _mark_failed_and_release(job_id, meta, f"Recovery finalization failed: {fin_err}")
                    counts["failed"] += 1

            elif piapi_result["status"] == "failed":
                error_msg = piapi_result.get("message", "Provider reported failure")
                print(f"[RECOVERY] Job {job_id}: PiAPI status=failed ({error_msg})")
                _mark_failed_and_release(job_id, meta, error_msg)
                counts["failed"] += 1

            elif piapi_result["status"] in ("processing", "pending"):
                progress = piapi_result.get("progress", 0)
                print(f"[RECOVERY] Job {job_id}: PiAPI status={piapi_result['status']} ({progress}%), resuming poll")
                try:
                    resumed = resume_polling(job_id, row, meta, upstream_id, app)
                    if resumed:
                        counts["resumed"] += 1
                    else:
                        # Could not resume — mark failed to avoid limbo
                        _mark_failed_and_release(job_id, meta, "Failed to resume polling thread")
                        counts["failed"] += 1
                except Exception as res_err:
                    print(f"[RECOVERY] Job {job_id}: Resume failed: {res_err}")
                    _mark_failed_and_release(job_id, meta, f"Resume failed: {res_err}")
                    counts["failed"] += 1

            elif piapi_result["status"] == "error":
                # Network error checking PiAPI — skip, don't mark failed
                print(f"[RECOVERY] Job {job_id}: PiAPI check failed (network), skipping")
                _restore_status(job_id, original_status)
                counts["skipped"] += 1

            else:
                print(f"[RECOVERY] Job {job_id}: Unexpected PiAPI status '{piapi_result['status']}', skipping")
                _restore_status(job_id, original_status)
                counts["skipped"] += 1

        except Exception as e:
            print(f"[RECOVERY] Job {job_id}: Unexpected error: {e}")
            try:
                _mark_failed_and_release(job_id, meta, f"Recovery error: {e}")
            except Exception:
                pass
            counts["failed"] += 1

    print(f"[RECOVERY] Complete: {counts}")
    return counts


def resume_polling(
    job_id: str,
    row: Dict[str, Any],
    meta: Dict[str, Any],
    upstream_id: str,
    app,
) -> bool:
    """
    Re-attach a background polling thread for a job still running on PiAPI.

    Returns True if polling thread was successfully submitted.
    """
    from backend.services.async_dispatch import (
        _poll_seedance_with_state_awareness,
        get_executor,
    )
    from backend.services.expense_guard import ExpenseGuard
    from backend.services.job_service import load_store, save_store
    from backend.services.video_router import resolve_video_provider

    # Build store_meta from DB meta
    store_meta = _build_store_meta(job_id, row, meta, upstream_id)

    # Write to in-memory store so frontend status polling works
    store = load_store()
    store[job_id] = store_meta
    save_store(store)

    # Update job status back to provider_processing
    _update_status(job_id, "provider_processing")

    # Resolve provider
    provider = resolve_video_provider("seedance")
    if not provider:
        print(f"[RECOVERY] Job {job_id}: Could not resolve seedance provider")
        return False

    # Register with ExpenseGuard for concurrency tracking
    ExpenseGuard.register_active_job(job_id)

    # Extract params for polling function
    identity_id = meta.get("identity_id") or str(row.get("user_id") or "")
    reservation_id = meta.get("reservation_id")

    # Submit to thread pool
    executor = get_executor()
    executor.submit(
        _poll_seedance_with_state_awareness,
        job_id,
        identity_id,
        reservation_id,
        upstream_id,
        provider,
        store_meta,
    )

    return True


# ── Internal helpers ─────────────────────────────────────────


def _check_upstream(upstream_id: str) -> Dict[str, Any]:
    """Check PiAPI status for an upstream task."""
    from backend.services.seedance_service import check_seedance_status

    return check_seedance_status(upstream_id)


def _build_store_meta(
    job_id: str,
    row: Dict[str, Any],
    meta: Dict[str, Any],
    upstream_id: str,
) -> dict:
    """Reconstruct store_meta dict from jobs table meta jsonb."""
    identity_id = meta.get("identity_id") or str(row.get("user_id") or "")

    return {
        "status": "processing",
        "provider": meta.get("provider", "seedance"),
        "upstream_id": upstream_id,
        "operation_name": upstream_id,
        "prompt": meta.get("prompt", ""),
        "identity_id": identity_id,
        "reservation_id": meta.get("reservation_id"),
        "duration_seconds": meta.get("duration_seconds"),
        "aspect_ratio": meta.get("aspect_ratio"),
        "resolution": meta.get("resolution"),
        "task_type": meta.get("task_type") or meta.get("seedance_variant") or "seedance-2-fast-preview",
        "seedance_variant": meta.get("seedance_variant") or meta.get("task_type") or "seedance-2-fast-preview",
        "seedance_tier": meta.get("seedance_tier", "fast"),
        "stage": "video",
        "task": meta.get("task", "text2video"),
        "internal_job_id": job_id,
        "recovered": True,
    }


def _mark_failed_and_release(job_id: str, meta: Dict[str, Any], error_message: str):
    """Mark a job as failed and release its credit reservation."""
    from backend.services.async_dispatch import update_job_status_failed
    from backend.services.credits_helper import release_job_credits

    update_job_status_failed(job_id, f"recovery: {error_message}")

    reservation_id = meta.get("reservation_id")
    if reservation_id:
        try:
            release_job_credits(reservation_id, "recovery_release", job_id)
        except Exception as e:
            print(f"[RECOVERY] Warning: Failed to release credits for job {job_id}: {e}")


def _finalize_recovered_job(
    job_id: str,
    row: Dict[str, Any],
    meta: Dict[str, Any],
    video_url: str,
):
    """Finalize a job that PiAPI reports as done."""
    from backend.services.async_dispatch import _finalize_video_success

    identity_id = meta.get("identity_id") or str(row.get("user_id") or "")
    reservation_id = meta.get("reservation_id")
    provider_name = meta.get("provider", "seedance")
    upstream_id = row.get("upstream_job_id") or meta.get("upstream_id", "")

    store_meta = _build_store_meta(job_id, row, meta, upstream_id)

    _finalize_video_success(
        internal_job_id=job_id,
        identity_id=identity_id,
        reservation_id=reservation_id,
        video_url=video_url,
        store_meta=store_meta,
        provider_name=provider_name,
    )


def _update_status(job_id: str, status: str):
    """Update job status in DB."""
    from backend.db import USE_DB, get_conn, Tables

    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = %s, updated_at = NOW()
                    WHERE id::text = %s
                    """,
                    (status, job_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[RECOVERY] Error updating status for job {job_id}: {e}")


def _restore_status(job_id: str, original_status: str):
    """Revert a job from 'recovering' back to its original status."""
    _update_status(job_id, original_status)
