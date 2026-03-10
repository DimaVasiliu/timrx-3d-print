"""
Durable DB-driven Job Worker.

Replaces fragile background-thread polling with a persistent worker loop
that claims jobs from the DB, polls providers, and updates state durably.

All job state lives in PostgreSQL. Workers are stateless and restart-safe.
If a worker dies, another worker (or the same worker after restart) reclaims
the job via heartbeat expiry.

Lifecycle states:
  created -> queued -> dispatched -> provider_pending -> provider_processing
    -> provider_succeeded -> finalizing -> succeeded
    -> provider_failed -> failed
    -> stalled (reclaimable)
    -> refunded

Terminal states: succeeded, failed, refunded
"""

from __future__ import annotations

import json
import os
import time
import threading
import traceback
import uuid
from typing import Any, Dict, List, Optional

from backend.db import USE_DB, get_conn, Tables
from backend.config import AWS_BUCKET_MODELS


# Worker identity: unique per process
WORKER_ID = f"worker-{os.getpid()}-{uuid.uuid4().hex[:8]}"

# Timing constants
HEARTBEAT_INTERVAL = 30          # seconds between heartbeat updates
HEARTBEAT_TIMEOUT = 90           # seconds before a claim is considered expired
STALL_TIMEOUT = 120              # seconds before marking a job as stalled
POLL_SLEEP_PENDING = 15          # seconds between provider polls (pending)
POLL_SLEEP_PROCESSING = 10       # seconds between provider polls (processing)
WORKER_LOOP_SLEEP = 2            # seconds between claim attempts when idle
MAX_ATTEMPTS = 5                 # max retry attempts before permanent failure

# Provider status -> internal status mapping
_PROVIDER_PENDING_STATUSES = {"pending", "queued", "staged"}
_PROVIDER_PROCESSING_STATUSES = {"processing", "running"}

# Seedance timeouts (pend_soft, pend_hard, proc_soft, proc_hard)
_SEEDANCE_TIMEOUTS = {
    "seedance-2-fast-preview": (5 * 60, 15 * 60, 10 * 60, 20 * 60),
    "seedance-2-preview":      (15 * 60, 30 * 60, 15 * 60, 30 * 60),
}
_SEEDANCE_DEFAULT_TIMEOUTS = (5 * 60, 15 * 60, 10 * 60, 20 * 60)

_FAILURE_REASON_MAP = {
    "seedance_pending_timeout": "Provider queue timed out -- Seedance did not start this job in time",
    "seedance_processing_timeout": "Render timed out -- Seedance started but did not finish in time",
    "seedance_poll_error": "Lost connection to provider during generation",
    "seedance_generation_failed": "Seedance rejected this generation",
    "seedance_no_video_url": "Generation completed but no video was returned",
    "seedance_auth_error": "Provider authentication failed",
}

# Terminal states -- worker must never touch these
TERMINAL_STATES = {"succeeded", "failed", "refunded", "ready", "ready_unbilled"}


# ── Worker Thread Management ────────────────────────────────

_worker_thread: Optional[threading.Thread] = None
_worker_stop = threading.Event()


def start_worker():
    """Start the durable job worker in a background daemon thread."""
    global _worker_thread
    if _worker_thread and _worker_thread.is_alive():
        print(f"[JOB] Worker already running: {WORKER_ID}")
        return

    _worker_stop.clear()
    _worker_thread = threading.Thread(
        target=_worker_loop,
        name=f"job-worker-{WORKER_ID}",
        daemon=True,
    )
    _worker_thread.start()
    print(f"[JOB] Worker started: {WORKER_ID}")


def stop_worker():
    """Signal the worker to stop gracefully."""
    _worker_stop.set()
    if _worker_thread:
        _worker_thread.join(timeout=10)
    print(f"[JOB] Worker stopped: {WORKER_ID}")


# ── Core Worker Loop ────────────────────────────────────────

def _worker_loop():
    """
    Persistent worker loop. Claims jobs, processes them, releases claims.

    Never crashes -- all errors are caught per-job. The loop itself only
    exits when _worker_stop is set.
    """
    print(f"[JOB] Worker loop started: {WORKER_ID}")

    while not _worker_stop.is_set():
        try:
            job = _claim_next_job()

            if job is None:
                # No work available -- sleep briefly and retry
                _worker_stop.wait(timeout=WORKER_LOOP_SLEEP)
                continue

            job_id = str(job["id"])
            print(f"[JOB] claimed job={job_id} status={job['status']} attempt={job.get('attempt_count', 0)}")

            try:
                _process_job(job)
            except Exception as e:
                print(f"[JOB] ERROR processing job={job_id}: {e}")
                traceback.print_exc()
                _handle_job_error(job, str(e))
            finally:
                _release_claim(job_id)

        except Exception as e:
            # Claim-level error -- should be rare
            print(f"[JOB] Worker claim error: {e}")
            traceback.print_exc()
            _worker_stop.wait(timeout=5)

    print(f"[JOB] Worker loop exiting: {WORKER_ID}")


# ── Job Claim ───────────────────────────────────────────────

def _claim_next_job() -> Optional[Dict[str, Any]]:
    """
    Claim the next available job using SELECT ... FOR UPDATE SKIP LOCKED.

    Returns the job row dict, or None if no work available.
    """
    if not USE_DB:
        return None

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Find and lock one claimable job
                cur.execute(
                    f"""
                    SELECT id, identity_id, provider, action_code, status,
                           upstream_job_id, prompt, meta, error_message,
                           reservation_id, job_type, stage, progress,
                           attempt_count, claimed_by, heartbeat_at,
                           next_poll_at, last_provider_status,
                           result_url, thumbnail_url,
                           created_at, updated_at
                    FROM {Tables.JOBS}
                    WHERE status IN ('queued', 'dispatched', 'provider_pending', 'provider_processing', 'stalled')
                      AND (claimed_by IS NULL OR heartbeat_at < NOW() - INTERVAL '{HEARTBEAT_TIMEOUT} seconds')
                      AND (next_poll_at IS NULL OR next_poll_at <= NOW())
                    ORDER BY
                        CASE WHEN status = 'stalled' THEN 0 ELSE 1 END,
                        created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """,
                )
                row = cur.fetchone()

                if not row:
                    return None

                job_id = str(row["id"])
                attempt = (row.get("attempt_count") or 0)

                # Claim it
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET claimed_by = %s,
                        claimed_at = NOW(),
                        heartbeat_at = NOW(),
                        attempt_count = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (WORKER_ID, attempt + 1, row["id"]),
                )
                claimed = cur.fetchone()
            conn.commit()
            return claimed

    except Exception as e:
        print(f"[JOB] Claim error: {e}")
        return None


def _release_claim(job_id: str):
    """Release worker claim on a job (clear claimed_by)."""
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET claimed_by = NULL,
                        claimed_at = NULL,
                        updated_at = NOW()
                    WHERE id::text = %s
                      AND claimed_by = %s
                    """,
                    (job_id, WORKER_ID),
                )
            conn.commit()
    except Exception as e:
        print(f"[JOB] Release claim error for {job_id}: {e}")


def _update_heartbeat(job_id: str):
    """Update heartbeat timestamp to signal worker is alive."""
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET heartbeat_at = NOW(), updated_at = NOW()
                    WHERE id::text = %s AND claimed_by = %s
                    """,
                    (job_id, WORKER_ID),
                )
            conn.commit()
    except Exception:
        pass  # Non-critical -- next heartbeat will catch up


# ── Job Processing ──────────────────────────────────────────

def _process_job(job: Dict[str, Any]):
    """
    Route job to appropriate handler based on current status.

    Status routing:
      queued/stalled           -> dispatch to provider
      dispatched               -> begin polling
      provider_pending         -> poll provider
      provider_processing      -> poll provider
    """
    status = job["status"]
    job_id = str(job["id"])
    meta = _parse_meta(job.get("meta"))

    if status in ("queued", "stalled"):
        # Job needs to be dispatched (or re-dispatched after stall)
        if job.get("upstream_job_id"):
            # Already dispatched but stalled -- resume polling
            _poll_provider(job, meta)
        else:
            _dispatch_to_provider(job, meta)

    elif status == "dispatched":
        _poll_provider(job, meta)

    elif status in ("provider_pending", "provider_processing"):
        _poll_provider(job, meta)

    else:
        print(f"[JOB] Unexpected status={status} for job={job_id}, skipping")


def _dispatch_to_provider(job: Dict[str, Any], meta: Dict[str, Any]):
    """
    Step 1: Dispatch job to upstream provider API.

    For video jobs, calls the appropriate provider via VideoRouter.
    Updates job with upstream_job_id and transitions to provider_pending.
    """
    job_id = str(job["id"])
    stage = job.get("stage") or meta.get("stage", "")
    provider_name = job.get("provider") or meta.get("provider", "seedance")
    identity_id = str(job.get("identity_id") or meta.get("identity_id", ""))
    reservation_id = str(job.get("reservation_id") or meta.get("reservation_id", "")) or None

    # Only handle video jobs in the durable worker for now
    if stage != "video":
        print(f"[JOB] Non-video job {job_id} stage={stage}, skipping durable dispatch")
        return

    print(f"[JOB] Dispatching job={job_id} provider={provider_name} stage={stage}")

    try:
        from backend.services.video_router import resolve_video_provider, ProviderUnavailableError

        provider = resolve_video_provider(provider_name)
        if not provider:
            raise ProviderUnavailableError(f"Provider {provider_name} not available")

        configured, err = provider.is_configured()
        if not configured:
            raise ProviderUnavailableError(f"Provider {provider_name} not configured: {err}")

        # Build dispatch params from meta
        task = meta.get("task", "text2video")
        prompt = meta.get("prompt", "")
        aspect_ratio = meta.get("aspect_ratio", "16:9")
        resolution = meta.get("resolution", "720p")
        duration_seconds = meta.get("duration_seconds", 5)
        seedance_variant = meta.get("seedance_variant") or meta.get("task_type") or "seedance-2-fast-preview"

        try:
            duration_seconds = int(duration_seconds)
        except (ValueError, TypeError):
            duration_seconds = 5

        route_params = dict(
            aspect_ratio=aspect_ratio,
            resolution=resolution,
            duration_seconds=duration_seconds,
            negative_prompt=meta.get("negative_prompt", ""),
            seed=meta.get("seed"),
            task_type=seedance_variant,
        )

        if task == "image2video":
            prompt = meta.get("motion") or prompt
            resp = provider.start_image_to_video(
                image_data=meta.get("image_data", ""),
                prompt=prompt,
                **route_params,
            )
        else:
            resp = provider.start_text_to_video(prompt=prompt, **route_params)

        upstream_id = resp.get("operation_name") or resp.get("task_id")
        if not upstream_id:
            raise RuntimeError("Provider returned no task ID")

        # Update job with upstream ID and transition to dispatched
        _transition_job(job_id, "dispatched", {
            "upstream_job_id": upstream_id,
            "last_provider_status": "pending",
            "next_poll_at": "NOW() + INTERVAL '5 seconds'",
        }, meta_patch={
            "upstream_id": upstream_id,
            "provider": provider_name,
            "dispatched_by": WORKER_ID,
        })

        print(f"[JOB] Dispatched job={job_id} upstream={upstream_id}")

        # Update in-memory store for frontend status polling
        _update_store(job_id, meta, upstream_id, "processing")

        # Now poll immediately in the same worker pass
        job["upstream_job_id"] = upstream_id
        job["status"] = "dispatched"
        meta["upstream_id"] = upstream_id
        _poll_provider(job, meta)

    except Exception as e:
        print(f"[JOB] Dispatch failed for job={job_id}: {e}")
        _fail_job(job_id, meta, f"dispatch_failed: {e}", "dispatch_failed")


def _poll_provider(job: Dict[str, Any], meta: Dict[str, Any]):
    """
    Step 2: Poll provider for job status until terminal state.

    Runs a polling loop with heartbeat updates. When the provider reports
    a terminal state, transitions the job accordingly.
    """
    job_id = str(job["id"])
    upstream_id = job.get("upstream_job_id") or meta.get("upstream_id")
    provider_name = job.get("provider") or meta.get("provider", "seedance")
    identity_id = str(job.get("identity_id") or meta.get("identity_id", ""))
    reservation_id = str(job.get("reservation_id") or meta.get("reservation_id", "")) or None
    task_type = meta.get("task_type") or meta.get("seedance_variant") or "seedance-2-fast-preview"

    if not upstream_id:
        print(f"[JOB] No upstream_id for job={job_id}, cannot poll")
        _fail_job(job_id, meta, "No upstream job ID", "no_upstream_id")
        return

    from backend.services.video_router import resolve_video_provider
    provider = resolve_video_provider(provider_name)

    # Timeout tracking
    pend_soft, pend_hard, proc_soft, proc_hard = _SEEDANCE_TIMEOUTS.get(
        task_type, _SEEDANCE_DEFAULT_TIMEOUTS,
    )
    pending_elapsed = 0.0
    processing_elapsed = 0.0
    last_phase = "pending"
    consecutive_errors = 0
    poll_num = 0
    last_heartbeat = time.time()

    while not _worker_stop.is_set():
        # Determine poll interval
        if last_phase == "pending":
            phase_elapsed = pending_elapsed
            past_soft = phase_elapsed >= pend_soft
        else:
            phase_elapsed = processing_elapsed
            past_soft = phase_elapsed >= proc_soft

        sleep_sec = 15 if past_soft else (POLL_SLEEP_PENDING if last_phase == "pending" else POLL_SLEEP_PROCESSING)

        # Sleep in small increments so we can check stop signal
        sleep_start = time.time()
        while time.time() - sleep_start < sleep_sec:
            if _worker_stop.is_set():
                # Graceful shutdown -- leave job in current state for reclaim
                print(f"[JOB] Worker stopping, releasing job={job_id} for reclaim")
                return
            time.sleep(min(2, sleep_sec - (time.time() - sleep_start)))

        poll_num += 1

        # Update heartbeat periodically
        if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
            _update_heartbeat(job_id)
            last_heartbeat = time.time()

        # Hard timeout checks
        if last_phase == "pending" and pending_elapsed >= pend_hard:
            # Attempt seedance fallback for preview -> fast tier
            if task_type == "seedance-2-preview":
                fallback = _attempt_seedance_fallback(job_id, meta, provider)
                if fallback:
                    upstream_id = fallback
                    task_type = "seedance-2-fast-preview"
                    pend_soft, pend_hard, proc_soft, proc_hard = _SEEDANCE_TIMEOUTS.get(
                        task_type, _SEEDANCE_DEFAULT_TIMEOUTS,
                    )
                    pending_elapsed = 0.0
                    processing_elapsed = 0.0
                    last_phase = "pending"
                    poll_num = 0
                    continue

            _fail_job(job_id, meta, f"Provider never started after {int(pending_elapsed)}s", "seedance_pending_timeout")
            return

        if last_phase == "processing" and processing_elapsed >= proc_hard:
            _fail_job(job_id, meta, f"Provider did not finish after {int(processing_elapsed)}s", "seedance_processing_timeout")
            return

        # Poll provider
        try:
            if provider:
                status_resp = provider.check_status(upstream_id)
            else:
                from backend.services.seedance_service import check_seedance_status
                status_resp = check_seedance_status(upstream_id)
        except Exception as e:
            consecutive_errors += 1
            print(f"[JOB] Poll error #{consecutive_errors} for job={job_id}: {e}")
            if consecutive_errors >= 3:
                _fail_job(job_id, meta, f"Poll error: {e}", "seedance_poll_error")
                return
            pending_elapsed += sleep_sec
            processing_elapsed += sleep_sec
            continue

        consecutive_errors = 0
        status = status_resp.get("status", "pending")
        provider_status = status_resp.get("provider_status", status)
        progress = status_resp.get("progress", 0)

        # Log periodically
        if poll_num <= 3 or poll_num % 10 == 0 or status != last_phase:
            print(
                f"[JOB] provider status={provider_status} internal={status} "
                f"job={job_id} poll={poll_num} "
                f"pending={int(pending_elapsed)}s processing={int(processing_elapsed)}s"
            )

        # Route by status
        if status == "done":
            video_url = status_resp.get("video_url")
            if video_url:
                _finalize_success(job_id, identity_id, reservation_id, video_url, meta, provider_name)
            else:
                _fail_job(job_id, meta, "Completed but no video URL", "seedance_no_video_url")
            return

        if status == "failed":
            error_code = status_resp.get("error", "seedance_generation_failed")
            error_msg = status_resp.get("message", "Provider generation failed")
            _fail_job(job_id, meta, f"{error_code}: {error_msg}", error_code)
            return

        if status == "error":
            # Network error -- increment counters but don't fail yet
            consecutive_errors += 1
            if consecutive_errors >= 3:
                _fail_job(job_id, meta, "Repeated network errors polling provider", "seedance_poll_error")
                return
            pending_elapsed += sleep_sec
            processing_elapsed += sleep_sec
            continue

        # Pending
        if status == "pending":
            pending_elapsed += sleep_sec
            last_phase = "pending"
            _update_job_state(job_id, "provider_pending", provider_status, progress, {
                "pending_seconds": int(pending_elapsed),
            })

        # Processing
        elif status == "processing":
            processing_elapsed += sleep_sec
            if last_phase == "pending":
                print(f"[JOB] job={job_id} transitioned pending->processing after {int(pending_elapsed)}s")
            last_phase = "processing"
            _update_job_state(job_id, "provider_processing", provider_status, progress, {
                "started_at": status_resp.get("started_at"),
            })

        else:
            # Unknown -- treat as pending
            pending_elapsed += sleep_sec

        # Update in-memory store for frontend
        db_status = "provider_pending" if status == "pending" else "processing"
        _update_store(job_id, meta, upstream_id, db_status, progress=progress)


# ── Finalization ────────────────────────────────────────────

def _finalize_success(
    job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    video_url: str,
    meta: Dict[str, Any],
    provider_name: str,
):
    """
    Finalize a successful video generation.

    1. Download video from provider
    2. Upload to S3
    3. Transition to 'finalizing'
    4. Finalize credits (idempotent)
    5. Save to history
    6. Transition to 'succeeded'
    """
    print(f"[JOB] Finalizing job={job_id} video_url={video_url[:80]}...")

    # Transition to finalizing (prevents double finalization)
    _transition_job(job_id, "finalizing", {
        "last_provider_status": "done",
    })

    # Delegate to existing finalization logic which handles S3, credits, history
    from backend.services.async_dispatch import _finalize_video_success

    # Build store_meta compatible with existing function
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

    # _finalize_video_success already sets status='ready', handles credits + history.
    # Record completion metadata on the durable worker columns.
    _transition_job(job_id, "ready", {
        "result_url": video_url,
        "completed_at": "NOW()",
        "claimed_by": None,
        "claimed_at": None,
    })

    print(f"[JOB] credits finalized job={job_id}")
    print(f"[JOB] uploaded result job={job_id}")
    print(f"[JOB] job={job_id} succeeded")


# ── Failure Handling ────────────────────────────────────────

def _fail_job(job_id: str, meta: Dict[str, Any], error_msg: str, error_code: str):
    """Mark job as failed, release credits, update store."""
    print(f"[JOB] FAIL job={job_id} code={error_code} msg={error_msg}")

    reservation_id = meta.get("reservation_id")
    if reservation_id:
        from backend.services.credits_helper import release_job_credits
        try:
            release_job_credits(reservation_id, error_code, job_id)
            print(f"[JOB] credits refunded job={job_id}")
        except Exception as e:
            print(f"[JOB] WARNING: credit release failed for job={job_id}: {e}")

    # Determine if this is a stall (timeout) vs hard failure
    is_stall = error_code in (
        "seedance_pending_timeout",
        "seedance_processing_timeout",
        "seedance_poll_error",
    )

    final_status = "failed"

    user_message = _FAILURE_REASON_MAP.get(error_code, error_msg)

    _transition_job(job_id, final_status, {
        "last_error_code": error_code,
        "last_error_message": error_msg[:500],
        "completed_at": "NOW()",
    }, meta_patch={
        "error_code": error_code,
        "error_message": error_msg,
        "failure_reason": user_message,
    })

    # Update in-memory store for frontend
    from backend.services.job_service import load_store, save_store
    store = load_store()
    sm = store.get(job_id)
    if sm:
        sm["status"] = final_status
        sm["error_code"] = error_code
        sm["error"] = user_message
        store[job_id] = sm
        save_store(store)

    # Unregister from ExpenseGuard
    from backend.services.expense_guard import ExpenseGuard
    ExpenseGuard.unregister_active_job(job_id)


def _handle_job_error(job: Dict[str, Any], error_msg: str):
    """Handle unexpected errors during job processing."""
    job_id = str(job["id"])
    meta = _parse_meta(job.get("meta"))
    attempt = job.get("attempt_count", 0)

    if attempt >= MAX_ATTEMPTS:
        print(f"[JOB] job={job_id} exceeded max attempts ({MAX_ATTEMPTS}), marking failed")
        _fail_job(job_id, meta, f"Exceeded max attempts: {error_msg}", "max_attempts_exceeded")
    else:
        # Mark as stalled for retry
        _transition_job(job_id, "stalled", {
            "last_error_code": "worker_error",
            "last_error_message": error_msg[:500],
            "next_poll_at": "NOW() + INTERVAL '30 seconds'",
        })
        print(f"[JOB] job={job_id} stalled for retry (attempt {attempt}/{MAX_ATTEMPTS})")


# ── Seedance Fallback ───────────────────────────────────────

def _attempt_seedance_fallback(job_id: str, meta: Dict[str, Any], provider) -> Optional[str]:
    """
    Attempt to retry a Seedance preview job with fast tier.
    Returns new upstream_id on success, None on failure.
    """
    try:
        print(f"[JOB] Attempting preview->fast fallback for job={job_id}")

        from backend.services.seedance_service import create_seedance_task

        prompt = meta.get("prompt", "")
        duration = meta.get("duration_seconds", 5)
        aspect = meta.get("aspect_ratio", "16:9")
        image_urls = None
        if meta.get("task") == "image2video":
            img = meta.get("image_data") or ""
            if img:
                image_urls = [img]

        resp = create_seedance_task(
            prompt=prompt,
            duration=int(duration) if duration else 5,
            aspect_ratio=aspect,
            image_urls=image_urls,
            task_type="seedance-2-fast-preview",
        )

        new_upstream = resp.get("task_id")
        if not new_upstream:
            return None

        # Update DB
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET upstream_job_id = %s,
                        status = 'dispatched',
                        meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE id::text = %s
                    """,
                    (
                        new_upstream,
                        json.dumps({
                            "upstream_id": new_upstream,
                            "task_type": "seedance-2-fast-preview",
                            "fallback_from": "seedance-2-preview",
                            "fallback_reason": "pending_timeout",
                        }),
                        job_id,
                    ),
                )
            conn.commit()

        print(f"[JOB] Fallback succeeded for job={job_id}, new upstream={new_upstream}")
        return new_upstream

    except Exception as e:
        print(f"[JOB] Fallback failed for job={job_id}: {e}")
        return None


# ── DB State Transitions ────────────────────────────────────

def _transition_job(
    job_id: str,
    new_status: str,
    field_updates: Optional[Dict[str, str]] = None,
    meta_patch: Optional[Dict[str, Any]] = None,
):
    """
    Atomically transition a job to a new status with optional field updates.

    field_updates: dict of column_name -> SQL value (use %s for parameterized,
                   or raw SQL like 'NOW()' for server-side expressions)
    meta_patch: dict to merge into the meta JSONB column
    """
    if not USE_DB:
        return

    try:
        set_clauses = ["status = %s", "updated_at = NOW()"]
        params: list = [new_status]

        if field_updates:
            for col, val in field_updates.items():
                if col == "meta":
                    continue  # handled separately
                if isinstance(val, str) and any(kw in val.upper() for kw in ("NOW()", "INTERVAL")):
                    # Raw SQL expression
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
        print(f"[JOB] Transition error job={job_id} -> {new_status}: {e}")


def _update_job_state(
    job_id: str,
    status: str,
    provider_status: str,
    progress: int,
    extra_meta: Optional[Dict[str, Any]] = None,
):
    """Update job with current polling state (non-terminal)."""
    if not USE_DB:
        return

    next_poll_interval = POLL_SLEEP_PENDING if status == "provider_pending" else POLL_SLEEP_PROCESSING

    try:
        meta_patch = {"progress": progress, "provider_status": provider_status}
        if extra_meta:
            meta_patch.update(extra_meta)

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = %s,
                        last_provider_status = %s,
                        progress = %s,
                        next_poll_at = NOW() + INTERVAL '%s seconds',
                        heartbeat_at = NOW(),
                        meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE id::text = %s
                    """,
                    (status, provider_status, progress, next_poll_interval,
                     json.dumps(meta_patch, default=str), job_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[JOB] State update error job={job_id}: {e}")


# ── Store Compat (in-memory store for frontend polling) ─────

def _update_store(
    job_id: str,
    meta: Dict[str, Any],
    upstream_id: str,
    status: str,
    progress: int = 0,
):
    """Update the in-memory job store so frontend status polling works."""
    try:
        from backend.services.job_service import load_store, save_store

        store = load_store()
        store_entry = store.get(job_id, {})
        store_entry.update({
            "status": status,
            "provider": meta.get("provider", "seedance"),
            "upstream_id": upstream_id,
            "operation_name": upstream_id,
            "prompt": meta.get("prompt", ""),
            "identity_id": meta.get("identity_id", ""),
            "reservation_id": meta.get("reservation_id"),
            "stage": "video",
            "internal_job_id": job_id,
            "progress": progress,
        })
        store[job_id] = store_entry
        save_store(store)
    except Exception:
        pass  # Non-critical


# ── Stall Detection ─────────────────────────────────────────

def detect_stalled_jobs():
    """
    Find jobs with expired heartbeats and mark them as stalled.

    Called periodically (e.g., every 60s) or at startup.
    """
    if not USE_DB:
        return 0

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'stalled',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        updated_at = NOW()
                    WHERE status IN ('dispatched', 'provider_pending', 'provider_processing')
                      AND claimed_by IS NOT NULL
                      AND heartbeat_at < NOW() - INTERVAL '{STALL_TIMEOUT} seconds'
                    RETURNING id
                    """,
                )
                stalled = cur.fetchall()
            conn.commit()

        if stalled:
            ids = [str(r["id"]) for r in stalled]
            print(f"[JOB] Detected {len(stalled)} stalled jobs: {ids}")

        return len(stalled) if stalled else 0

    except Exception as e:
        print(f"[JOB] Stall detection error: {e}")
        return 0


# ── Startup Recovery ────────────────────────────────────────

def recover_stale_jobs():
    """
    Startup recovery: find all non-terminal jobs with expired claims
    and mark them as stalled so the worker loop picks them up.

    This replaces the old thread-based recovery. The worker loop itself
    handles re-dispatching and re-polling.
    """
    if not USE_DB:
        return {"recovered": 0}

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Mark all orphaned in-progress jobs as stalled
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'stalled',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        updated_at = NOW()
                    WHERE status IN (
                        'queued', 'dispatched', 'pending', 'processing',
                        'provider_pending', 'provider_processing', 'recovering'
                    )
                    AND status NOT IN ({', '.join(f"'{s}'" for s in TERMINAL_STATES)})
                    RETURNING id, status
                    """,
                )
                recovered = cur.fetchall()
            conn.commit()

        count = len(recovered) if recovered else 0
        if count > 0:
            print(f"[JOB] Startup recovery: marked {count} orphaned jobs as stalled for worker pickup")
            for r in recovered:
                print(f"[JOB] reclaimed job={r['id']} was_status={r['status']}")
        else:
            print(f"[JOB] Startup recovery: no orphaned jobs found")

        return {"recovered": count}

    except Exception as e:
        print(f"[JOB] Startup recovery error: {e}")
        return {"recovered": 0, "error": str(e)}


# ── Stall Detection Thread ──────────────────────────────────

_stall_thread: Optional[threading.Thread] = None


def start_stall_detector(interval: int = 60):
    """Start a background thread that periodically detects stalled jobs."""
    global _stall_thread

    def _loop():
        while not _worker_stop.is_set():
            try:
                detect_stalled_jobs()
            except Exception as e:
                print(f"[JOB] Stall detector error: {e}")
            _worker_stop.wait(timeout=interval)

    _stall_thread = threading.Thread(
        target=_loop,
        name="job-stall-detector",
        daemon=True,
    )
    _stall_thread.start()
    print(f"[JOB] Stall detector started (interval={interval}s)")


# ── Helpers ─────────────────────────────────────────────────

def _parse_meta(meta) -> Dict[str, Any]:
    """Parse meta JSONB column, handling string or dict."""
    if meta is None:
        return {}
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, str):
        try:
            return json.loads(meta)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}
