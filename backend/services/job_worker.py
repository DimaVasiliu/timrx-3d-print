"""
Durable DB-driven Job Worker.

Replaces fragile background-thread polling with a persistent worker loop
that claims jobs from the DB, polls providers, and updates state durably.

All job state lives in PostgreSQL. Workers are stateless and restart-safe.
If a worker dies, another worker (or the same worker after restart) reclaims
the job via heartbeat expiry.

Architecture (v2 — single-poll-per-claim):
  Each claim cycle does ONE provider poll, then either finalizes, fails, or
  schedules next_poll_at and releases the claim. The worker loop picks the
  job back up when next_poll_at arrives. This prevents rapid-fire polling
  on errors and gives natural backoff via DB scheduling.

Single-worker guarantee:
  Uses pg_try_advisory_lock(LEADER_LOCK_ID) on a dedicated connection.
  Only one process across all Gunicorn workers acquires the lock. Others
  log a standby message and exit their worker thread immediately.

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
from typing import Any, Dict, Optional

from backend.db import USE_DB, get_conn, Tables

try:
    from backend.db import _create_connection
except ImportError:
    _create_connection = None


# Worker identity: unique per process
WORKER_ID = f"worker-{os.getpid()}-{uuid.uuid4().hex[:8]}"

# Advisory lock ID for single-worker guarantee (arbitrary fixed integer)
LEADER_LOCK_ID = 737483

# Timing constants
HEARTBEAT_INTERVAL = 30          # seconds between heartbeat updates
HEARTBEAT_TIMEOUT = 90           # seconds before a claim is considered expired
STALL_TIMEOUT = 120              # seconds before marking a job as stalled
POLL_SLEEP_PENDING = 15          # seconds between provider polls (pending)
POLL_SLEEP_PROCESSING = 10       # seconds between provider polls (processing)
WORKER_LOOP_SLEEP = 2            # seconds between claim attempts when idle
MAX_ATTEMPTS = 5                 # max retry attempts before permanent failure

# Stepped backoff for provider errors (indexed by consecutive_errors count)
BACKOFF_STEPS = [30, 60, 120, 300]  # seconds

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

# Error codes that warrant credit release (terminal, confirmed failures).
# Timeouts and poll errors do NOT release credits — the provider may still
# complete the job, and the rescue service can recover it later.
_TERMINAL_ERROR_CODES = {
    "seedance_generation_failed",   # Provider confirmed failure
    "seedance_no_video_url",        # Provider said done but no URL
    "seedance_auth_error",          # Auth failure — won't recover
    "max_attempts_exceeded",        # Exhausted all retries
    "dispatch_failed",              # Could not dispatch to provider
    "no_upstream_id",               # Missing upstream ID — cannot poll
}


# ── Worker Thread Management ────────────────────────────────

_worker_thread: Optional[threading.Thread] = None
_worker_stop = threading.Event()
_leader_conn = None  # Dedicated connection holding the advisory lock


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
    print(f"[JOB] Worker thread launched: {WORKER_ID}")


def stop_worker():
    """Signal the worker to stop gracefully."""
    global _leader_conn
    _worker_stop.set()
    if _worker_thread:
        _worker_thread.join(timeout=10)

    # Release the advisory lock
    if _leader_conn:
        try:
            _leader_conn.close()
        except Exception:
            pass
        _leader_conn = None

    print(f"[JOB] Worker stopped: {WORKER_ID}")


# ── Leader Election (Advisory Lock) ─────────────────────────

def _acquire_leader_lock() -> bool:
    """
    Try to acquire a PostgreSQL advisory lock for single-worker guarantee.

    Uses a dedicated connection that stays open for the worker's lifetime.
    The lock is automatically released when the connection closes (on stop
    or process death).

    Returns True if this process is the leader, False otherwise.
    """
    global _leader_conn

    if not USE_DB:
        return True  # No DB = no contention, just run

    try:
        if _create_connection:
            _leader_conn = _create_connection()
        else:
            # Fallback: open a raw connection
            import psycopg
            from psycopg.rows import dict_row
            db_url = os.getenv("DATABASE_URL", "")
            _leader_conn = psycopg.connect(db_url, row_factory=dict_row)

        with _leader_conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s) AS acquired", (LEADER_LOCK_ID,))
            row = cur.fetchone()
        _leader_conn.commit()

        acquired = row and row.get("acquired", False)

        if acquired:
            print(f"[JOB] Leader lock acquired by {WORKER_ID} (lock_id={LEADER_LOCK_ID})")
        else:
            print(f"[JOB] Leader lock NOT acquired — another worker is active. {WORKER_ID} standing by.")
            _leader_conn.close()
            _leader_conn = None

        return acquired

    except Exception as e:
        print(f"[JOB] Leader lock error: {e} — proceeding without lock (single-instance assumed)")
        if _leader_conn:
            try:
                _leader_conn.close()
            except Exception:
                pass
            _leader_conn = None
        return True  # Fail-open: if we can't check, assume single instance


def _release_leader_lock():
    """Release the advisory lock by closing the dedicated connection."""
    global _leader_conn
    if _leader_conn:
        try:
            _leader_conn.close()
        except Exception:
            pass
        _leader_conn = None


# ── Core Worker Loop ───────────────���────────────────────────

def _worker_loop():
    """
    Persistent worker loop. Claims jobs, does one poll per claim, releases.

    Never crashes — all errors are caught per-job. The loop itself only
    exits when _worker_stop is set or leader lock is not acquired.
    """
    # Step 1: Acquire leader lock — only one worker per deployment
    if not _acquire_leader_lock():
        print(f"[JOB] {WORKER_ID} exiting — not the leader")
        return

    print(f"[JOB] Worker loop started: {WORKER_ID} (leader)")

    try:
        while not _worker_stop.is_set():
            try:
                job = _claim_next_job()

                if job is None:
                    _worker_stop.wait(timeout=WORKER_LOOP_SLEEP)
                    continue

                job_id = str(job["id"])
                provider = job.get("provider") or "unknown"
                upstream = job.get("upstream_job_id") or "none"
                attempt = job.get("attempt_count", 0)
                print(
                    f"[JOB] claimed job={job_id} status={job['status']} "
                    f"provider={provider} upstream={upstream} attempt={attempt}"
                )

                try:
                    _process_job(job)
                except Exception as e:
                    print(f"[JOB] ERROR processing job={job_id} provider={provider}: {e}")
                    traceback.print_exc()
                    _handle_job_error(job, str(e))
                finally:
                    _release_claim(job_id)

            except Exception as e:
                print(f"[JOB] Worker claim error: {e}")
                traceback.print_exc()
                _worker_stop.wait(timeout=5)

    finally:
        _release_leader_lock()
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
        pass  # Non-critical


# ── Job Processing ──────────────────────────────────────────

def _process_job(job: Dict[str, Any]):
    """
    Route job to appropriate handler based on current status.

    Validates required fields before dispatching or polling.
    """
    status = job["status"]
    job_id = str(job["id"])
    meta = _parse_meta(job.get("meta"))
    provider_name = job.get("provider") or meta.get("provider", "")
    upstream_id = job.get("upstream_job_id") or meta.get("upstream_id", "")

    if status in ("queued", "stalled"):
        if upstream_id:
            # Already dispatched but stalled — validate before resuming poll
            if not _validate_poll_fields(job, meta):
                return
            _poll_provider_once(job, meta)
        else:
            # Needs dispatch — validate dispatch fields
            if not _validate_dispatch_fields(job, meta):
                return
            _dispatch_to_provider(job, meta)

    elif status in ("dispatched", "provider_pending", "provider_processing"):
        if not _validate_poll_fields(job, meta):
            return
        _poll_provider_once(job, meta)

    else:
        print(f"[JOB] skip job={job_id} reason=unexpected_status status={status}")


def _validate_dispatch_fields(job: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    """Validate required fields before dispatching. Returns True if valid."""
    job_id = str(job["id"])
    stage = job.get("stage") or meta.get("stage", "")
    provider_name = job.get("provider") or meta.get("provider", "")
    identity_id = str(job.get("identity_id") or meta.get("identity_id", ""))

    missing = []
    if not provider_name:
        missing.append("provider")
    if not identity_id:
        missing.append("identity_id")
    if not stage:
        missing.append("stage")

    if missing:
        msg = f"Missing required dispatch fields: {', '.join(missing)}"
        print(f"[JOB] skip job={job_id} reason=missing_fields fields={','.join(missing)}")
        _fail_job(job_id, meta, msg, "missing_fields")
        return False

    return True


def _validate_poll_fields(job: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    """Validate required fields before polling. Returns True if valid."""
    job_id = str(job["id"])
    upstream_id = job.get("upstream_job_id") or meta.get("upstream_id", "")
    provider_name = job.get("provider") or meta.get("provider", "")

    missing = []
    if not upstream_id:
        missing.append("upstream_job_id")
    if not provider_name:
        missing.append("provider")

    if missing:
        msg = f"Missing required poll fields: {', '.join(missing)}"
        print(f"[JOB] skip job={job_id} reason=missing_fields fields={','.join(missing)}")
        _fail_job(job_id, meta, msg, "missing_fields")
        return False

    return True


def _dispatch_to_provider(job: Dict[str, Any], meta: Dict[str, Any]):
    """
    Dispatch job to upstream provider API.

    For video jobs, calls the appropriate provider via VideoRouter.
    Updates job with upstream_job_id and transitions to dispatched.
    """
    job_id = str(job["id"])
    stage = job.get("stage") or meta.get("stage", "")
    provider_name = job.get("provider") or meta.get("provider", "seedance")
    identity_id = str(job.get("identity_id") or meta.get("identity_id", ""))
    reservation_id = str(job.get("reservation_id") or meta.get("reservation_id", "")) or None

    if stage != "video":
        print(f"[JOB] skip job={job_id} reason=non_video_stage stage={stage}")
        return

    print(f"[JOB] dispatching job={job_id} provider={provider_name} stage={stage}")

    try:
        from backend.services.video_router import resolve_video_provider, ProviderUnavailableError

        provider = resolve_video_provider(provider_name)
        if not provider:
            raise ProviderUnavailableError(f"Provider {provider_name} not available")

        configured, err = provider.is_configured()
        if not configured:
            raise ProviderUnavailableError(f"Provider {provider_name} not configured: {err}")

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

        _transition_job(job_id, "dispatched", {
            "upstream_job_id": upstream_id,
            "last_provider_status": "pending",
            "next_poll_at": "NOW() + INTERVAL '5 seconds'",
        }, meta_patch={
            "upstream_id": upstream_id,
            "provider": provider_name,
            "dispatched_by": WORKER_ID,
            "dispatched_at": time.time(),
            "consecutive_errors": 0,
        })

        print(f"[JOB] dispatched job={job_id} upstream={upstream_id} provider={provider_name}")

        # Update in-memory store for frontend status polling
        _update_store(job_id, meta, upstream_id, "processing")

    except Exception as e:
        print(f"[JOB] dispatch FAILED job={job_id} provider={provider_name}: {e}")
        _fail_job(job_id, meta, f"dispatch_failed: {e}", "dispatch_failed")


def _poll_provider_once(job: Dict[str, Any], meta: Dict[str, Any]):
    """
    Single-poll-per-claim: do ONE provider status check, then either
    finalize, fail, or schedule next_poll_at and return.

    This replaces the old inner while-loop that could burn through
    multiple polls in a single claim cycle.
    """
    job_id = str(job["id"])
    upstream_id = job.get("upstream_job_id") or meta.get("upstream_id")
    provider_name = job.get("provider") or meta.get("provider", "seedance")
    identity_id = str(job.get("identity_id") or meta.get("identity_id", ""))
    reservation_id = str(job.get("reservation_id") or meta.get("reservation_id", "")) or None
    task_type = meta.get("task_type") or meta.get("seedance_variant") or "seedance-2-fast-preview"
    consecutive_errors = meta.get("consecutive_errors", 0)

    if not upstream_id:
        print(f"[JOB] FAIL job={job_id} reason=no_upstream_id provider={provider_name}")
        _fail_job(job_id, meta, "No upstream job ID", "no_upstream_id")
        return

    # Compute elapsed times from timestamps
    dispatched_at = meta.get("dispatched_at") or _ts(job.get("created_at"))
    processing_started_at = meta.get("processing_started_at")
    now = time.time()
    pending_elapsed = now - dispatched_at if dispatched_at else 0
    processing_elapsed = (now - processing_started_at) if processing_started_at else 0

    # Timeout thresholds
    pend_soft, pend_hard, proc_soft, proc_hard = _SEEDANCE_TIMEOUTS.get(
        task_type, _SEEDANCE_DEFAULT_TIMEOUTS,
    )

    # Hard timeout checks BEFORE polling
    last_status = job.get("last_provider_status") or meta.get("provider_status", "pending")

    if last_status in ("pending", "queued", "staged") and pending_elapsed >= pend_hard:
        # Attempt fallback for preview tier
        if task_type == "seedance-2-preview":
            from backend.services.video_router import resolve_video_provider
            provider = resolve_video_provider(provider_name)
            fallback = _attempt_seedance_fallback(job_id, meta, provider)
            if fallback:
                print(f"[JOB] fallback job={job_id} new_upstream={fallback} reason=pending_timeout")
                return

        print(f"[JOB] FAIL job={job_id} reason=pending_timeout elapsed={int(pending_elapsed)}s hard={pend_hard}s")
        _fail_job(job_id, meta, f"Provider never started after {int(pending_elapsed)}s", "seedance_pending_timeout")
        return

    if processing_started_at and processing_elapsed >= proc_hard:
        print(f"[JOB] FAIL job={job_id} reason=processing_timeout elapsed={int(processing_elapsed)}s hard={proc_hard}s")
        _fail_job(job_id, meta, f"Provider did not finish after {int(processing_elapsed)}s", "seedance_processing_timeout")
        return

    # Update heartbeat
    _update_heartbeat(job_id)

    # Do ONE provider status check
    from backend.services.video_router import resolve_video_provider
    provider = resolve_video_provider(provider_name)

    try:
        if provider:
            status_resp = provider.check_status(upstream_id)
        else:
            from backend.services.seedance_service import check_seedance_status
            status_resp = check_seedance_status(upstream_id)
    except Exception as e:
        # Network/provider error — schedule retry with backoff
        consecutive_errors += 1
        backoff = _get_backoff(consecutive_errors)

        print(
            f"[JOB] poll ERROR job={job_id} upstream={upstream_id} provider={provider_name} "
            f"consecutive_errors={consecutive_errors} backoff={backoff}s error={e}"
        )

        if consecutive_errors >= MAX_ATTEMPTS:
            print(f"[JOB] FAIL job={job_id} reason=max_poll_errors consecutive_errors={consecutive_errors}")
            _fail_job(job_id, meta, f"Poll error after {consecutive_errors} attempts: {e}", "seedance_poll_error")
        else:
            _transition_job(job_id, job["status"], {
                "next_poll_at": f"NOW() + INTERVAL '{backoff} seconds'",
            }, meta_patch={
                "consecutive_errors": consecutive_errors,
                "last_poll_error": str(e)[:200],
            })
            print(f"[JOB] retry scheduled job={job_id} next_poll_at=+{backoff}s")

        return

    # Successful poll — reset consecutive errors
    status = status_resp.get("status", "pending")
    provider_status = status_resp.get("provider_status", status)
    progress = status_resp.get("progress", 0)

    print(
        f"[JOB] poll result job={job_id} upstream={upstream_id} provider={provider_name} "
        f"status={status} provider_status={provider_status} progress={progress} "
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
        print(f"[JOB] FAIL job={job_id} reason=upstream_failed error={error_code}: {error_msg}")
        _fail_job(job_id, meta, f"{error_code}: {error_msg}", error_code)
        return

    if status == "error":
        # Provider returned error status (not an exception) — same as network error
        consecutive_errors += 1
        backoff = _get_backoff(consecutive_errors)

        print(
            f"[JOB] poll status=error job={job_id} upstream={upstream_id} provider={provider_name} "
            f"consecutive_errors={consecutive_errors} backoff={backoff}s"
        )

        if consecutive_errors >= MAX_ATTEMPTS:
            _fail_job(job_id, meta, "Repeated provider errors", "seedance_poll_error")
        else:
            _transition_job(job_id, job["status"], {
                "next_poll_at": f"NOW() + INTERVAL '{backoff} seconds'",
            }, meta_patch={
                "consecutive_errors": consecutive_errors,
                "last_poll_error": "provider_error_status",
            })
            print(f"[JOB] retry scheduled job={job_id} next_poll_at=+{backoff}s")

        return

    # Pending — schedule next poll
    if status == "pending":
        past_soft = pending_elapsed >= pend_soft
        poll_interval = 15 if past_soft else POLL_SLEEP_PENDING

        _update_job_state(job_id, "provider_pending", provider_status, progress, poll_interval, {
            "pending_seconds": int(pending_elapsed),
            "consecutive_errors": 0,
        })
        _update_store(job_id, meta, upstream_id, "provider_pending", progress=progress)

    # Processing
    elif status == "processing":
        meta_patch = {"consecutive_errors": 0}
        if not processing_started_at:
            meta_patch["processing_started_at"] = now
            print(f"[JOB] job={job_id} transitioned pending->processing after {int(pending_elapsed)}s")

        past_soft = processing_elapsed >= proc_soft if processing_started_at else False
        poll_interval = 15 if past_soft else POLL_SLEEP_PROCESSING

        if status_resp.get("started_at"):
            meta_patch["started_at"] = status_resp["started_at"]

        _update_job_state(job_id, "provider_processing", provider_status, progress, poll_interval, meta_patch)
        _update_store(job_id, meta, upstream_id, "processing", progress=progress)

    else:
        # Unknown status — treat as pending, schedule normal poll
        _update_job_state(job_id, "provider_pending", provider_status, progress, POLL_SLEEP_PENDING, {
            "consecutive_errors": 0,
        })


def _get_backoff(consecutive_errors: int) -> int:
    """Get backoff delay in seconds based on error count."""
    idx = min(consecutive_errors - 1, len(BACKOFF_STEPS) - 1)
    return BACKOFF_STEPS[max(0, idx)]


def _ts(val) -> Optional[float]:
    """Convert a datetime or timestamp to epoch float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return val.timestamp()
    except AttributeError:
        return None


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

    1. Transition to 'finalizing' (prevents double finalization)
    2. Delegate to existing finalization logic (S3, credits, history)
    3. Mark as 'ready'
    """
    print(f"[JOB] finalizing job={job_id} provider={provider_name} video_url={video_url[:80]}...")

    _transition_job(job_id, "finalizing", {
        "last_provider_status": "done",
    })

    from backend.services.async_dispatch import _finalize_video_success

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

    _transition_job(job_id, "ready", {
        "result_url": video_url,
        "completed_at": "NOW()",
        "claimed_by": None,
        "claimed_at": None,
    })

    print(f"[JOB] succeeded job={job_id} provider={provider_name}")


# ── Failure Handling ────────────────────────────────────────

def _fail_job(job_id: str, meta: Dict[str, Any], error_msg: str, error_code: str):
    """
    Mark job as failed. Only releases credits for terminal error codes.

    Temporary errors (timeouts, poll errors) keep the reservation held
    so the rescue service can recover the job later if the provider
    eventually completes.
    """
    print(f"[JOB] FAIL job={job_id} code={error_code} msg={error_msg}")

    # Only release credits for confirmed terminal failures
    reservation_id = meta.get("reservation_id")
    if reservation_id and error_code in _TERMINAL_ERROR_CODES:
        from backend.services.credits_helper import release_job_credits
        try:
            release_job_credits(reservation_id, error_code, job_id)
            print(f"[JOB] credits released job={job_id} reason={error_code}")
        except Exception as e:
            print(f"[JOB] WARNING: credit release failed job={job_id}: {e}")
    elif reservation_id:
        print(
            f"[JOB] credits HELD job={job_id} reason=non_terminal_error "
            f"code={error_code} (rescue may recover)"
        )

    user_message = _FAILURE_REASON_MAP.get(error_code, error_msg)

    _transition_job(job_id, "failed", {
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
        sm["status"] = "failed"
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
        print(f"[JOB] FAIL job={job_id} reason=max_attempts attempts={attempt}/{MAX_ATTEMPTS}")
        _fail_job(job_id, meta, f"Exceeded max attempts: {error_msg}", "max_attempts_exceeded")
    else:
        backoff = _get_backoff(attempt)
        _transition_job(job_id, "stalled", {
            "last_error_code": "worker_error",
            "last_error_message": error_msg[:500],
            "next_poll_at": f"NOW() + INTERVAL '{backoff} seconds'",
        })
        print(f"[JOB] stalled job={job_id} attempt={attempt}/{MAX_ATTEMPTS} retry_in={backoff}s")


# ── Seedance Fallback ───────────────────────────────────────

def _attempt_seedance_fallback(job_id: str, meta: Dict[str, Any], provider) -> Optional[str]:
    """
    Attempt to retry a Seedance preview job with fast tier.
    Returns new upstream_id on success, None on failure.
    """
    try:
        print(f"[JOB] attempting fallback job={job_id} from=seedance-2-preview to=seedance-2-fast-preview")

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

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET upstream_job_id = %s,
                        status = 'dispatched',
                        next_poll_at = NOW() + INTERVAL '5 seconds',
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
                            "dispatched_at": time.time(),
                            "processing_started_at": None,
                            "consecutive_errors": 0,
                        }),
                        job_id,
                    ),
                )
            conn.commit()

        print(f"[JOB] fallback succeeded job={job_id} new_upstream={new_upstream}")
        return new_upstream

    except Exception as e:
        print(f"[JOB] fallback failed job={job_id}: {e}")
        return None


# ── DB State Transitions ────────────────────────────────────

def _transition_job(
    job_id: str,
    new_status: str,
    field_updates: Optional[Dict[str, Any]] = None,
    meta_patch: Optional[Dict[str, Any]] = None,
):
    """
    Atomically transition a job to a new status with optional field updates.

    field_updates: dict of column_name -> value. Raw SQL expressions like
                   'NOW()' or 'NOW() + INTERVAL ...' are detected and inlined.
    meta_patch: dict to merge into the meta JSONB column.
    """
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
        print(f"[JOB] transition error job={job_id} -> {new_status}: {e}")


def _update_job_state(
    job_id: str,
    status: str,
    provider_status: str,
    progress: int,
    next_poll_interval: int,
    extra_meta: Optional[Dict[str, Any]] = None,
):
    """Update job with current polling state and schedule next poll."""
    if not USE_DB:
        return

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
        print(f"[JOB] state update error job={job_id}: {e}")


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
    Called periodically (e.g., every 60s).
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
            print(f"[JOB] stall_detected count={len(stalled)} jobs={ids}")

        return len(stalled) if stalled else 0

    except Exception as e:
        print(f"[JOB] stall detection error: {e}")
        return 0


# ── Startup Recovery ────────────────────────────────────────

def recover_stale_jobs():
    """
    Startup recovery: find all non-terminal jobs with expired claims
    and mark them as stalled so the worker loop picks them up.
    """
    if not USE_DB:
        return {"recovered": 0}

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
            print(f"[JOB] startup recovery: marked {count} orphaned jobs as stalled")
            for r in recovered:
                print(f"[JOB] reclaimed job={r['id']} was_status={r['status']}")
        else:
            print(f"[JOB] startup recovery: no orphaned jobs found")

        return {"recovered": count}

    except Exception as e:
        print(f"[JOB] startup recovery error: {e}")
        return {"recovered": 0, "error": str(e)}


# ── Stall Detection + Rescue Thread ─────────────────────────

_stall_thread: Optional[threading.Thread] = None


def start_stall_detector(interval: int = 60):
    """
    Start a background thread that periodically:
    1. Detects stalled jobs (expired heartbeats)
    2. Runs a rescue pass for failed jobs with completed upstream
    """
    global _stall_thread

    def _loop():
        cycle = 0
        while not _worker_stop.is_set():
            try:
                detect_stalled_jobs()
            except Exception as e:
                print(f"[JOB] stall detector error: {e}")

            # Run rescue pass every 5th cycle (every ~5 minutes)
            cycle += 1
            if cycle % 5 == 0:
                try:
                    from backend.services.job_rescue import rescue_late_completed_jobs
                    result = rescue_late_completed_jobs(hours=24, dry_run=False, max_jobs=10)
                    rescued = result.get("rescued", 0)
                    if rescued > 0:
                        print(f"[JOB] rescue pass: rescued={rescued}")
                except Exception as e:
                    print(f"[JOB] rescue pass error: {e}")

            _worker_stop.wait(timeout=interval)

    _stall_thread = threading.Thread(
        target=_loop,
        name="job-stall-detector",
        daemon=True,
    )
    _stall_thread.start()
    print(f"[JOB] stall detector started (interval={interval}s, rescue every {interval * 5}s)")


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
