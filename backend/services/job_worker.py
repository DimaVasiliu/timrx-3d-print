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

Provider routing:
  Only video jobs with supported providers are claimed. Meshy 3D model
  jobs use a separate legacy dispatch path and are never touched here.
  Each provider has its own timeout config and error codes.

Lifecycle states:
  created -> queued -> dispatched -> provider_pending -> provider_processing
    -> finalizing -> ready (succeeded)
    -> failed
    -> stalled (reclaimable)
    -> refunded
    -> abandoned_legacy (too old for recovery)

Terminal states: succeeded, failed, refunded, ready, ready_unbilled,
                 abandoned_legacy, recovery_blocked
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
from backend.services.video_errors import (
    TERMINAL_STATES as _SHARED_TERMINAL_STATES,
    TERMINAL_ERROR_CODES as _SHARED_TERMINAL_ERROR_CODES,
    get_failure_message,
)

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
MAX_RECOVERY_AGE_HOURS = 48      # don't claim jobs older than this

# Stepped backoff for provider errors (indexed by consecutive_errors count)
BACKOFF_STEPS = [30, 60, 120, 300]  # seconds

# ── Provider Configuration ───────────────────────────────────
# Only these provider/stage combinations are handled by the durable worker.
# Everything else (meshy 3D, image gen, etc.) uses legacy dispatch paths.
_SUPPORTED_PROVIDERS = {"seedance", "vertex"}
_SUPPORTED_STAGES = {"video"}

# Per-provider timeout config: (pend_soft, pend_hard, proc_soft, proc_hard)
# Keys can be "provider" or "provider:task_type" for finer granularity
_PROVIDER_TIMEOUTS = {
    # Seedance task types
    "seedance:seedance-2-fast-preview": (5 * 60, 15 * 60, 10 * 60, 20 * 60),
    "seedance:seedance-2-preview":      (15 * 60, 30 * 60, 15 * 60, 30 * 60),
    "seedance":                         (5 * 60, 15 * 60, 10 * 60, 20 * 60),
    # Vertex (Veo) — faster models, tighter timeouts
    "vertex":                           (2 * 60, 6 * 60, 4 * 60, 10 * 60),
}
_DEFAULT_TIMEOUTS = (5 * 60, 15 * 60, 10 * 60, 20 * 60)

# Terminal states -- worker must never touch these (shared source of truth)
TERMINAL_STATES = _SHARED_TERMINAL_STATES

# Error codes that warrant credit release (shared source of truth).
# Timeouts and poll errors do NOT release credits — the provider may still
# complete the job, and the rescue service can recover it later.
_TERMINAL_ERROR_CODES = _SHARED_TERMINAL_ERROR_CODES


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

    if _leader_conn:
        try:
            _leader_conn.close()
        except Exception:
            pass
        _leader_conn = None

    print(f"[JOB] Worker stopped: {WORKER_ID}")


# ── Leader Election (Advisory Lock) ─────────────────────────

def _try_lock_once() -> bool:
    """Single attempt to acquire the advisory lock. Returns True if acquired."""
    global _leader_conn

    if _leader_conn:
        try:
            _leader_conn.close()
        except Exception:
            pass
        _leader_conn = None

    if _create_connection:
        _leader_conn = _create_connection()
    else:
        import psycopg
        from psycopg.rows import dict_row
        db_url = os.getenv("DATABASE_URL", "")
        _leader_conn = psycopg.connect(db_url, row_factory=dict_row)

    with _leader_conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s) AS acquired", (LEADER_LOCK_ID,))
        row = cur.fetchone()
    _leader_conn.commit()

    acquired = row and row.get("acquired", False)
    if not acquired:
        _leader_conn.close()
        _leader_conn = None
    return acquired


def _acquire_leader_lock() -> bool:
    """
    Acquire a PostgreSQL advisory lock for single-worker guarantee.

    Retries with backoff to handle deploy overlap windows where the old
    instance still holds the lock briefly (~90s total).
    """
    if not USE_DB:
        return True

    retry_delays = [5, 5, 10, 10, 15, 15, 15, 15]
    max_attempts = 1 + len(retry_delays)

    for attempt in range(max_attempts):
        try:
            if _try_lock_once():
                print(f"[JOB] Leader lock acquired by {WORKER_ID} (attempt={attempt + 1})")
                return True
        except Exception as e:
            print(f"[JOB] Leader lock error (attempt {attempt + 1}): {e} — proceeding without lock")
            return True  # Fail-open on error

        if attempt < len(retry_delays):
            delay = retry_delays[attempt]
            print(f"[JOB] Leader lock NOT acquired (attempt {attempt + 1}/{max_attempts}). Retrying in {delay}s.")
            if _worker_stop.is_set():
                return False
            _worker_stop.wait(timeout=delay)

    print(f"[JOB] Leader lock NOT acquired after {max_attempts} attempts. {WORKER_ID} giving up.")
    return False


def _release_leader_lock():
    """Release the advisory lock by closing the dedicated connection."""
    global _leader_conn
    if _leader_conn:
        try:
            _leader_conn.close()
        except Exception:
            pass
        _leader_conn = None


# ── Core Worker Loop ────────────────────────────────────────

def _worker_loop():
    """
    Persistent worker loop. Claims jobs, does one poll per claim, releases.

    Never crashes — all errors are caught per-job. The loop itself only
    exits when _worker_stop is set or leader lock is not acquired.
    """
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
                stage = job.get("stage") or "unknown"
                attempt = job.get("attempt_count", 0)
                print(
                    f"[JOB] claimed job={job_id} status={job['status']} "
                    f"provider={provider} stage={stage} upstream={upstream} attempt={attempt}"
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

    Only claims jobs that:
      - Are in a claimable status
      - Have a supported provider (seedance, vertex, google, veo)
      - Are video-stage jobs
      - Were created within MAX_RECOVERY_AGE_HOURS
      - Are not currently claimed (or have expired heartbeat)
      - Have reached their next_poll_at time
    """
    if not USE_DB:
        return None

    # Build provider IN clause
    provider_list = ", ".join(f"'{p}'" for p in _SUPPORTED_PROVIDERS)

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
                    WHERE (
                        status IN ('dispatched', 'provider_pending', 'provider_processing', 'stalled')
                        OR (status = 'queued' AND created_at < NOW() - INTERVAL '30 seconds')
                    )
                      AND provider IN ({provider_list})
                      AND stage = 'video'
                      AND created_at > NOW() - INTERVAL '{MAX_RECOVERY_AGE_HOURS} hours'
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

                attempt = (row.get("attempt_count") or 0)
                npa = row.get("next_poll_at")

                # Diagnostic: read server NOW() to compare with next_poll_at
                cur.execute("SELECT NOW() AS db_now")
                db_now = cur.fetchone()["db_now"]

                print(
                    f"[JOB][DEBUG] _claim_next_job FOUND job={row['id']} "
                    f"status={row['status']} next_poll_at={npa} db_now={db_now} "
                    f"claimed_by={row.get('claimed_by')} attempt={attempt}"
                )

                # Belt-and-suspenders: if next_poll_at is in the future despite
                # the WHERE clause, skip this claim to prevent aggressive re-poll.
                if npa is not None and npa > db_now:
                    print(
                        f"[JOB][WARN] _claim_next_job SKIPPED job={row['id']} "
                        f"next_poll_at={npa} is AFTER db_now={db_now} "
                        f"(delta={npa - db_now}) — WHERE clause did not filter"
                    )
                    return None

                # NOTE: Do NOT increment attempt_count here — it tracks
                # error retries, not normal poll cycles. Only _handle_job_error
                # should bump it.
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET claimed_by = %s,
                        claimed_at = NOW(),
                        heartbeat_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING *
                    """,
                    (WORKER_ID, row["id"]),
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

        # Post-commit verification: read next_poll_at on a SEPARATE connection
        # to confirm it survived the release transaction.
        with get_conn() as conn2:
            with conn2.cursor() as cur2:
                cur2.execute(
                    f"SELECT next_poll_at, NOW() AS db_now, claimed_by, status "
                    f"FROM {Tables.JOBS} WHERE id::text = %s",
                    (job_id,),
                )
                verify = cur2.fetchone()
        if verify:
            npa = verify["next_poll_at"]
            db_now = verify["db_now"]
            delta = f"{npa - db_now}" if npa and db_now else "N/A"
            print(
                f"[JOB][DEBUG] _release_claim job={job_id} "
                f"next_poll_at={npa} db_now={db_now} delta={delta} "
                f"claimed_by={verify['claimed_by']} status={verify['status']}"
            )
        else:
            print(f"[JOB][DEBUG] _release_claim job={job_id} — row not found in verification read")
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
    Route job to appropriate handler based on provider and status.

    1. Check provider is supported
    2. Validate required fields
    3. Route to dispatch or poll
    """
    status = job["status"]
    job_id = str(job["id"])
    meta = _parse_meta(job.get("meta"))
    provider_name = job.get("provider") or meta.get("provider", "")
    # Normalize legacy provider names to canonical
    if provider_name in ("veo", "google", "aistudio"):
        provider_name = "vertex"
    stage = job.get("stage") or meta.get("stage", "")
    upstream_id = job.get("upstream_job_id") or meta.get("upstream_id", "")

    # Guard: only supported provider/stage combos
    if provider_name not in _SUPPORTED_PROVIDERS:
        print(
            f"[JOB] BLOCKED job={job_id} provider={provider_name} stage={stage} "
            f"reason=unsupported_provider handler=none credits=preserved"
        )
        _quarantine_job(job_id, meta, "unsupported_recovery_provider",
                        f"Provider '{provider_name}' not supported by durable worker")
        return

    if stage not in _SUPPORTED_STAGES:
        print(
            f"[JOB] BLOCKED job={job_id} provider={provider_name} stage={stage} "
            f"reason=unsupported_stage handler=none credits=preserved"
        )
        _quarantine_job(job_id, meta, "unsupported_recovery_stage",
                        f"Stage '{stage}' not supported by durable worker")
        return

    # Resolve timeout family for this provider
    task_type = meta.get("task_type") or meta.get("seedance_variant") or ""
    timeout_key = f"{provider_name}:{task_type}" if task_type else provider_name
    timeouts = _PROVIDER_TIMEOUTS.get(timeout_key, _PROVIDER_TIMEOUTS.get(provider_name, _DEFAULT_TIMEOUTS))

    print(
        f"[JOB] routing job={job_id} provider={provider_name} stage={stage} "
        f"status={status} upstream={'yes' if upstream_id else 'no'} "
        f"timeout_family={timeout_key} handler=durable_video_worker"
    )

    if status in ("queued", "stalled"):
        if upstream_id:
            # Already dispatched but stalled — validate + resume poll
            if not _validate_poll_fields(job, meta):
                return
            # Ensure fresh timeout anchor for reclaimed stalled jobs
            meta = _ensure_timeout_anchor(job_id, meta)
            _poll_provider_once(job, meta, provider_name, timeouts)
        else:
            if not _validate_dispatch_fields(job, meta):
                return
            _dispatch_to_provider(job, meta)

    elif status in ("dispatched", "provider_pending", "provider_processing"):
        if not _validate_poll_fields(job, meta):
            return
        _poll_provider_once(job, meta, provider_name, timeouts)

    else:
        print(f"[JOB] skip job={job_id} reason=unexpected_status status={status}")


def _ensure_timeout_anchor(job_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure the job has a valid dispatched_at timestamp for timeout tracking.

    For stalled jobs being reclaimed, the original dispatched_at might be
    very old (or missing). In that case, set a fresh anchor so the job
    gets a fair chance to be polled before timing out.

    Returns updated meta dict.
    """
    now = time.time()
    dispatched_at = meta.get("dispatched_at")

    if not dispatched_at:
        # No dispatch timestamp at all — set fresh anchor
        meta["dispatched_at"] = now
        meta["recovery_anchor_set"] = True
        _transition_job(job_id, None, meta_patch={
            "dispatched_at": now,
            "recovery_anchor_set": True,
        })
        print(f"[JOB] set fresh timeout anchor job={job_id} reason=no_dispatched_at")
        return meta

    # Check if the dispatch timestamp is absurdly old (> MAX_RECOVERY_AGE_HOURS)
    age_hours = (now - dispatched_at) / 3600
    if age_hours > MAX_RECOVERY_AGE_HOURS:
        # Reset the timeout anchor so the job gets polled before timing out
        meta["dispatched_at"] = now
        meta["original_dispatched_at"] = dispatched_at
        meta["recovery_anchor_set"] = True
        _transition_job(job_id, None, meta_patch={
            "dispatched_at": now,
            "original_dispatched_at": dispatched_at,
            "recovery_anchor_set": True,
        })
        print(
            f"[JOB] reset timeout anchor job={job_id} "
            f"reason=stale_dispatch age={int(age_hours)}h original_at={dispatched_at}"
        )
        return meta

    return meta


def _validate_dispatch_fields(job: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    """Validate required fields before dispatching. Returns True if valid."""
    job_id = str(job["id"])
    provider_name = job.get("provider") or meta.get("provider", "")
    stage = job.get("stage") or meta.get("stage", "")
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
        _fail_job(job_id, meta, msg, "missing_fields", provider_name)
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
        _fail_job(job_id, meta, msg, "missing_fields", provider_name)
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

    if stage != "video":
        print(f"[JOB] skip job={job_id} reason=non_video_stage stage={stage} provider={provider_name}")
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

        _update_store(job_id, meta, upstream_id, "processing")

    except Exception as e:
        print(f"[JOB] dispatch FAILED job={job_id} provider={provider_name}: {e}")
        _fail_job(job_id, meta, f"dispatch_failed: {e}", "dispatch_failed", provider_name)


def _poll_provider_once(
    job: Dict[str, Any],
    meta: Dict[str, Any],
    provider_name: str,
    timeouts: tuple,
):
    """
    Single-poll-per-claim: do ONE provider status check, then either
    finalize, fail, or schedule next_poll_at and return.

    Provider-aware: uses the correct timeout config and error codes
    for the given provider.
    """
    job_id = str(job["id"])
    upstream_id = job.get("upstream_job_id") or meta.get("upstream_id")
    identity_id = str(job.get("identity_id") or meta.get("identity_id", ""))
    reservation_id = str(job.get("reservation_id") or meta.get("reservation_id", "")) or None
    task_type = meta.get("task_type") or meta.get("seedance_variant") or ""
    consecutive_errors = meta.get("consecutive_errors", 0)

    if not upstream_id:
        print(f"[JOB] FAIL job={job_id} reason=no_upstream_id provider={provider_name}")
        _fail_job(job_id, meta, "No upstream job ID", "no_upstream_id", provider_name)
        return

    # Compute elapsed times from timestamps
    dispatched_at = meta.get("dispatched_at") or _ts(job.get("created_at"))
    processing_started_at = meta.get("processing_started_at")
    now = time.time()
    pending_elapsed = now - dispatched_at if dispatched_at else 0
    processing_elapsed = (now - processing_started_at) if processing_started_at else 0

    # Unpack provider-specific timeout thresholds
    pend_soft, pend_hard, proc_soft, proc_hard = timeouts

    # Log elapsed sources for debugging
    elapsed_source = "meta.dispatched_at" if meta.get("dispatched_at") else "job.created_at"
    print(
        f"[JOB] poll prep job={job_id} provider={provider_name} upstream={upstream_id} "
        f"elapsed_source={elapsed_source} pending={int(pending_elapsed)}s "
        f"processing={int(processing_elapsed)}s timeout_hard_p={pend_hard}s timeout_hard_r={proc_hard}s"
    )

    # Hard timeout checks BEFORE polling
    last_status = job.get("last_provider_status") or meta.get("provider_status", "pending")

    # Once processing has started, skip the pending timeout entirely — use
    # the processing timeout instead (checked below).
    if last_status in ("pending", "queued", "staged") and not processing_started_at and pending_elapsed >= pend_hard:
        # Seedance-specific: attempt preview -> fast fallback
        if provider_name == "seedance" and task_type == "seedance-2-preview":
            from backend.services.video_router import resolve_video_provider
            provider_obj = resolve_video_provider(provider_name)
            fallback = _attempt_seedance_fallback(job_id, meta, provider_obj)
            if fallback:
                print(f"[JOB] fallback job={job_id} new_upstream={fallback} reason=pending_timeout provider=seedance")
                return

        error_code = f"{provider_name}_pending_timeout"
        print(f"[JOB] FAIL job={job_id} reason=pending_timeout elapsed={int(pending_elapsed)}s hard={pend_hard}s provider={provider_name}")
        _fail_job(job_id, meta, f"Provider never started after {int(pending_elapsed)}s", error_code, provider_name)
        return

    if processing_started_at and processing_elapsed >= proc_hard:
        error_code = f"{provider_name}_processing_timeout"
        print(f"[JOB] FAIL job={job_id} reason=processing_timeout elapsed={int(processing_elapsed)}s hard={proc_hard}s provider={provider_name}")
        _fail_job(job_id, meta, f"Provider did not finish after {int(processing_elapsed)}s", error_code, provider_name)
        return

    # Update heartbeat
    _update_heartbeat(job_id)

    # Do ONE provider status check
    from backend.services.video_router import resolve_video_provider
    provider_obj = resolve_video_provider(provider_name)

    try:
        if provider_obj:
            status_resp = provider_obj.check_status(upstream_id)
        else:
            from backend.services.seedance_service import check_seedance_status
            status_resp = check_seedance_status(upstream_id)
    except Exception as e:
        consecutive_errors += 1
        backoff = _get_backoff(consecutive_errors)

        print(
            f"[JOB] poll ERROR job={job_id} upstream={upstream_id} provider={provider_name} "
            f"consecutive_errors={consecutive_errors} backoff={backoff}s error={e}"
        )

        if consecutive_errors >= MAX_ATTEMPTS:
            error_code = f"{provider_name}_poll_error"
            print(f"[JOB] FAIL job={job_id} reason=max_poll_errors consecutive_errors={consecutive_errors} provider={provider_name}")
            _fail_job(job_id, meta, f"Poll error after {consecutive_errors} attempts: {e}", error_code, provider_name)
        else:
            _transition_job(job_id, job["status"], {
                "next_poll_at": f"NOW() + INTERVAL '{backoff} seconds'",
            }, meta_patch={
                "consecutive_errors": consecutive_errors,
                "last_poll_error": str(e)[:200],
            })
            print(f"[JOB] retry scheduled job={job_id} next_poll_at=+{backoff}s provider={provider_name}")

        return

    # Successful poll — parse response
    status = status_resp.get("status", "pending")
    provider_status = status_resp.get("provider_status", status)
    progress = status_resp.get("progress", 0)

    # Monotonic state guard: once processing has started locally, upstream
    # "pending" (from PiAPI "Staged" etc.) must NOT demote back to pending.
    # Two independent signals trigger the guard:
    #   1. processing_started_at is set in meta (processing was recorded)
    #   2. local DB status is already provider_processing
    current_local_status = job.get("status", "")
    already_processing = bool(processing_started_at) or current_local_status == "provider_processing"
    if status == "pending" and already_processing:
        print(
            f"[JOB] MONOTONIC GUARD: prevented backward transition job={job_id} "
            f"upstream_raw={provider_status} mapped=pending BLOCKED "
            f"local_status={current_local_status} "
            f"processing_started_at={processing_started_at} "
            f"-> treating as processing"
        )
        status = "processing"

    print(
        f"[JOB] poll result job={job_id} upstream={upstream_id} provider={provider_name} "
        f"status={status} provider_status={provider_status} progress={progress} "
        f"local_status={current_local_status} "
        f"pending={int(pending_elapsed)}s processing={int(processing_elapsed)}s"
    )

    # Route by status
    if status == "done":
        video_url = status_resp.get("video_url")
        video_bytes = status_resp.get("video_bytes")
        if video_url:
            _finalize_success(job_id, identity_id, reservation_id, video_url, meta, provider_name)
        elif video_bytes:
            _finalize_success_with_bytes(
                job_id, identity_id, reservation_id,
                video_bytes, status_resp.get("content_type", "video/mp4"),
                meta, provider_name,
            )
        else:
            error_code = f"{provider_name}_no_result_url"
            _fail_job(job_id, meta, "Completed but no video URL", error_code, provider_name)
        return

    if status == "failed":
        error_code = status_resp.get("error", f"{provider_name}_generation_failed")
        error_msg = status_resp.get("message", "Provider generation failed")
        provider_error_code = status_resp.get("provider_error_code")
        provider_logs = status_resp.get("provider_logs")
        print(
            f"[JOB] FAIL job={job_id} reason=upstream_failed error={error_code}: {error_msg} "
            f"provider={provider_name} provider_error_code={provider_error_code}"
        )
        # Enrich meta with upstream error details before failing
        if provider_error_code:
            meta["provider_error_code"] = provider_error_code
        if provider_logs:
            # Truncate logs to avoid bloating JSONB
            if isinstance(provider_logs, list):
                meta["provider_logs"] = provider_logs[:10]
            else:
                meta["provider_logs"] = str(provider_logs)[:1000]
        _fail_job(job_id, meta, f"{error_code}: {error_msg}", error_code, provider_name)
        return

    if status == "error":
        consecutive_errors += 1
        backoff = _get_backoff(consecutive_errors)

        print(
            f"[JOB] poll status=error job={job_id} upstream={upstream_id} provider={provider_name} "
            f"consecutive_errors={consecutive_errors} backoff={backoff}s"
        )

        if consecutive_errors >= MAX_ATTEMPTS:
            error_code = f"{provider_name}_poll_error"
            _fail_job(job_id, meta, "Repeated provider errors", error_code, provider_name)
        else:
            _transition_job(job_id, job["status"], {
                "next_poll_at": f"NOW() + INTERVAL '{backoff} seconds'",
            }, meta_patch={
                "consecutive_errors": consecutive_errors,
                "last_poll_error": "provider_error_status",
            })
            print(f"[JOB] retry scheduled job={job_id} next_poll_at=+{backoff}s provider={provider_name}")

        return

    # Pending — schedule next poll (with progressive backoff for long waits)
    if status == "pending":
        past_soft = pending_elapsed >= pend_soft
        if past_soft:
            # Progressive backoff: 30s → 60s → 120s based on how far past soft
            overshoot = pending_elapsed - pend_soft
            if overshoot > 1800:    # 30+ min past soft → poll every 120s
                poll_interval = 120
            elif overshoot > 600:   # 10+ min past soft → poll every 60s
                poll_interval = 60
            else:                   # just past soft → poll every 30s
                poll_interval = 30
        else:
            poll_interval = POLL_SLEEP_PENDING
        print(
            f"[JOB][DEBUG] status=pending job={job_id} poll_interval={poll_interval}s "
            f"past_soft={past_soft} pending_elapsed={int(pending_elapsed)}s"
        )

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
            print(f"[JOB] job={job_id} transitioned pending->processing after {int(pending_elapsed)}s provider={provider_name}")

        past_soft = processing_elapsed >= proc_soft if processing_started_at else False
        if past_soft:
            overshoot = processing_elapsed - proc_soft
            if overshoot > 600:
                poll_interval = 60
            else:
                poll_interval = 30
        else:
            poll_interval = POLL_SLEEP_PROCESSING
        print(
            f"[JOB][DEBUG] status=processing job={job_id} poll_interval={poll_interval}s "
            f"past_soft={past_soft} processing_elapsed={int(processing_elapsed)}s"
        )

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

def _try_transition_to_finalizing(job_id: str) -> bool:
    """
    Atomically transition job to 'finalizing' only if not already terminal.

    Returns True if this worker won the transition, False if the job was
    already in a terminal or finalizing state (e.g. another poll cycle or webhook got there first).
    """
    if not USE_DB:
        return True

    excluded = ", ".join(f"'{s}'" for s in TERMINAL_STATES | {"finalizing"})
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'finalizing',
                        last_provider_status = 'done',
                        updated_at = NOW()
                    WHERE id::text = %s
                      AND status NOT IN ({excluded})
                    RETURNING id
                    """,
                    (job_id,),
                )
                row = cur.fetchone()
            conn.commit()
            return row is not None
    except Exception as e:
        print(f"[JOB] _try_transition_to_finalizing error job={job_id}: {e}")
        return False


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

    1. Atomically transition to 'finalizing' (prevents double finalization
       from concurrent poll cycles or a future webhook delivery)
    2. Delegate to existing finalization logic (S3, credits, history)
    3. Mark as 'ready'
    """
    print(f"[JOB] finalizing job={job_id} provider={provider_name} video_url={video_url[:80]}...")

    if not _try_transition_to_finalizing(job_id):
        print(f"[JOB] skip finalize job={job_id} — already finalizing/terminal")
        return

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
        "video_uuid": meta.get("video_uuid"),
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


def _finalize_success_with_bytes(
    job_id: str,
    identity_id: str,
    reservation_id: Optional[str],
    video_bytes: bytes,
    content_type: str,
    meta: Dict[str, Any],
    provider_name: str,
):
    """
    Finalize when the provider returns inline video bytes (e.g. Vertex base64).
    Uploads to S3 first, then marks job as ready.
    """
    print(f"[JOB] finalizing job={job_id} provider={provider_name} video_bytes={len(video_bytes)} bytes")

    if not _try_transition_to_finalizing(job_id):
        print(f"[JOB] skip finalize job={job_id} — already finalizing/terminal")
        return

    from backend.services.async_dispatch import _finalize_video_success_with_bytes

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
        "task_type": meta.get("task_type", ""),
        "stage": "video",
        "task": meta.get("task", "text2video"),
        "internal_job_id": job_id,
        "video_uuid": meta.get("video_uuid"),
    }

    _finalize_video_success_with_bytes(
        internal_job_id=job_id,
        identity_id=identity_id,
        reservation_id=reservation_id,
        video_bytes=video_bytes,
        content_type=content_type,
        store_meta=store_meta,
        provider_name=provider_name,
    )

    _transition_job(job_id, "ready", {
        "completed_at": "NOW()",
        "claimed_by": None,
        "claimed_at": None,
    })

    print(f"[JOB] succeeded job={job_id} provider={provider_name} (bytes path)")


# ── Failure Handling ────────────────────────────────────────

def _fail_job(
    job_id: str,
    meta: Dict[str, Any],
    error_msg: str,
    error_code: str,
    provider_name: str = "unknown",
):
    """
    Mark job as failed. Only releases credits for terminal error codes.

    Temporary errors (timeouts, poll errors) keep the reservation held
    so the rescue service can recover the job later if the provider
    eventually completes.
    """
    print(f"[JOB] FAIL job={job_id} code={error_code} provider={provider_name} msg={error_msg}")

    # Determine if this error code is terminal (warrants credit release).
    # Check both the exact code and the generic suffix (e.g. "seedance_generation_failed" -> "generation_failed")
    reservation_id = meta.get("reservation_id")
    suffix = error_code.split("_", 1)[-1] if "_" in error_code else error_code
    is_terminal = error_code in _TERMINAL_ERROR_CODES or suffix in _TERMINAL_ERROR_CODES

    if reservation_id and is_terminal:
        from backend.services.credits_helper import release_job_credits
        try:
            release_job_credits(reservation_id, error_code, job_id)
            print(f"[JOB] credits RELEASED job={job_id} reason={error_code} provider={provider_name}")
        except Exception as e:
            print(f"[JOB] WARNING: credit release failed job={job_id}: {e}")
    elif reservation_id:
        print(
            f"[JOB] credits HELD job={job_id} reason=non_terminal_error "
            f"code={error_code} provider={provider_name} (rescue may recover)"
        )
    else:
        print(f"[JOB] credits N/A job={job_id} no_reservation provider={provider_name}")

    # Resolve user-facing message from shared error taxonomy
    user_message = get_failure_message(error_code)
    if user_message == f"Video generation failed ({error_code})":
        # No specific message for this code — try the generic suffix
        user_message = get_failure_message(suffix) if suffix != error_code else error_msg

    fail_meta = {
        "error_code": error_code,
        "error_message": error_msg,
        "failure_reason": user_message,
        "failure_provider": provider_name,
    }
    # Carry through any upstream provider error details the caller enriched
    if meta.get("provider_error_code"):
        fail_meta["provider_error_code"] = meta["provider_error_code"]
    if meta.get("provider_logs"):
        fail_meta["provider_logs"] = meta["provider_logs"]

    _transition_job(job_id, "failed", {
        "last_error_code": error_code,
        "last_error_message": error_msg[:500],
        "completed_at": "NOW()",
    }, meta_patch=fail_meta)

    # Update the early-created videos row to status='failed'
    video_uuid = meta.get("video_uuid")
    if video_uuid:
        try:
            from backend.services.history_service import update_video_record
            update_video_record(
                video_uuid,
                status="failed",
                error_message=error_msg[:500],
                meta_patch={"error_code": error_code, "failure_provider": provider_name},
            )
            print(f"[JOB] videos row updated: video_uuid={video_uuid} status=failed")
        except Exception as e:
            print(f"[JOB] WARNING: failed to update videos row {video_uuid}: {e}")

        # Write history_items row so failed video appears in user history
        try:
            identity_id = str(meta.get("identity_id") or meta.get("user_id", ""))
            from backend.services.history_service import save_failed_video_to_history
            save_failed_video_to_history(
                job_id=job_id,
                identity_id=identity_id,
                video_uuid=video_uuid,
                prompt=meta.get("prompt", ""),
                error_message=error_msg[:500],
                provider=provider_name,
                duration_seconds=meta.get("duration_seconds"),
                aspect_ratio=meta.get("aspect_ratio"),
                resolution=meta.get("resolution"),
            )
        except Exception as e:
            print(f"[JOB] WARNING: failed to write failed history for job {job_id}: {e}")

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


def _quarantine_job(job_id: str, meta: Dict[str, Any], error_code: str, reason: str):
    """
    Quarantine a job that cannot be safely recovered by the durable worker.

    - Does NOT release or finalize credits (preserves reservation state)
    - Marks job as recovery_blocked so it's excluded from future claims
    - Requires manual review or admin intervention
    """
    provider = meta.get("provider", "unknown")
    reservation_id = meta.get("reservation_id")

    print(
        f"[JOB] QUARANTINE job={job_id} code={error_code} provider={provider} "
        f"reservation={'held_safe' if reservation_id else 'none'} reason={reason}"
    )

    _transition_job(job_id, "recovery_blocked", {
        "last_error_code": error_code,
        "last_error_message": reason[:500],
    }, meta_patch={
        "quarantine_reason": reason,
        "quarantine_code": error_code,
        "quarantine_by": WORKER_ID,
        "quarantine_at": time.time(),
        "credits_action": "preserved",
    })


def _handle_job_error(job: Dict[str, Any], error_msg: str):
    """Handle unexpected errors during job processing."""
    job_id = str(job["id"])
    meta = _parse_meta(job.get("meta"))
    provider_name = job.get("provider") or "unknown"
    attempt = job.get("attempt_count", 0) + 1  # increment on error

    if attempt >= MAX_ATTEMPTS:
        print(f"[JOB] FAIL job={job_id} reason=max_attempts attempts={attempt}/{MAX_ATTEMPTS} provider={provider_name}")
        _fail_job(job_id, meta, f"Exceeded max attempts: {error_msg}", "max_attempts_exceeded", provider_name)
    else:
        backoff = _get_backoff(attempt)
        _transition_job(job_id, "stalled", {
            "attempt_count": attempt,
            "last_error_code": "worker_error",
            "last_error_message": error_msg[:500],
            "next_poll_at": f"NOW() + INTERVAL '{backoff} seconds'",
        })
        print(f"[JOB] stalled job={job_id} attempt={attempt}/{MAX_ATTEMPTS} retry_in={backoff}s provider={provider_name}")


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
    new_status: Optional[str],
    field_updates: Optional[Dict[str, Any]] = None,
    meta_patch: Optional[Dict[str, Any]] = None,
):
    """
    Atomically transition a job to a new status with optional field updates.

    If new_status is None, only applies field_updates/meta_patch without
    changing the status (useful for setting meta without state transition).

    field_updates: dict of column_name -> value. Raw SQL expressions like
                   'NOW()' or 'NOW() + INTERVAL ...' are detected and inlined.
    meta_patch: dict to merge into the meta JSONB column.
    """
    if not USE_DB:
        return

    try:
        set_clauses = ["updated_at = NOW()"]
        params: list = []

        if new_status is not None:
            set_clauses.insert(0, "status = %s")
            params.append(new_status)

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
        status_str = new_status or "(meta-only)"
        print(f"[JOB] transition error job={job_id} -> {status_str}: {e}")


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

    print(
        f"[JOB][DEBUG] _update_job_state ENTER job={job_id} status={status} "
        f"provider_status={provider_status} progress={progress} "
        f"next_poll_interval={next_poll_interval}s extra_meta={extra_meta}"
    )

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
                        next_poll_at = NOW() + %s * INTERVAL '1 second',
                        heartbeat_at = NOW(),
                        meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE id::text = %s
                    RETURNING next_poll_at, heartbeat_at
                    """,
                    (status, provider_status, progress, next_poll_interval,
                     json.dumps(meta_patch, default=str), job_id),
                )
                row = cur.fetchone()
                if row:
                    print(
                        f"[JOB][DEBUG] _update_job_state OK job={job_id} "
                        f"next_poll_at={row['next_poll_at']} heartbeat_at={row['heartbeat_at']} rowcount={cur.rowcount}"
                    )
                else:
                    print(
                        f"[JOB][DEBUG] _update_job_state NO ROW MATCHED job={job_id} "
                        f"(WHERE id::text = '{job_id}' matched 0 rows!)"
                    )
            conn.commit()
            print(f"[JOB][DEBUG] _update_job_state COMMITTED job={job_id}")
    except Exception as e:
        print(f"[JOB] state update error job={job_id}: {e}")
        import traceback as _tb
        _tb.print_exc()


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
            "dispatched_at": meta.get("dispatched_at"),
            "processing_started_at": meta.get("processing_started_at"),
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
    Only operates on supported provider/stage jobs.
    """
    if not USE_DB:
        return 0

    provider_list = ", ".join(f"'{p}'" for p in _SUPPORTED_PROVIDERS)

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
                      AND provider IN ({provider_list})
                      AND stage = 'video'
                      AND claimed_by IS NOT NULL
                      AND heartbeat_at < NOW() - INTERVAL '{STALL_TIMEOUT} seconds'
                    RETURNING id, provider
                    """,
                )
                stalled = cur.fetchall()
            conn.commit()

        if stalled:
            ids = [f"{r['id']}({r.get('provider', '?')})" for r in stalled]
            print(f"[JOB] stall_detected count={len(stalled)} jobs={ids}")

        return len(stalled) if stalled else 0

    except Exception as e:
        print(f"[JOB] stall detection error: {e}")
        return 0


# ── Stale Sweep ────────────────────────────────────────────
# Unlike stall detection (which only catches jobs with expired heartbeats),
# the stale sweep catches jobs stuck in non-terminal states beyond age
# thresholds, regardless of claim status. This covers:
# - unclaimed jobs that never got picked up
# - jobs stuck in finalizing after crash
# - jobs whose worker died without heartbeating

def run_stale_sweep():
    """
    Periodic server-side sweep for jobs stuck too long in non-terminal states.

    Uses config-driven thresholds. Actions per status:
    - queued/dispatched past age: mark stalled (worker re-claims)
    - provider_pending past age: mark stalled (worker re-evaluates)
    - provider_processing past age: mark stalled (worker re-evaluates)
    - finalizing past age: mark stalled (rescue retries finalization)

    Does NOT touch jobs actively heartbeating (within HEARTBEAT_TIMEOUT).
    Does NOT touch terminal states.
    Returns summary dict.
    """
    if not USE_DB:
        return {"swept": 0}

    from backend.config import config as _cfg

    provider_list = ", ".join(f"'{p}'" for p in _SUPPORTED_PROVIDERS)
    terminal_list = ", ".join(f"'{s}'" for s in TERMINAL_STATES)

    swept_total = 0
    details = []

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1. Queued/dispatched too long (never reached provider or got stuck)
                dispatched_age = _cfg.STALE_DISPATCHED_AGE_S
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'stalled',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        meta = COALESCE(meta, '{{}}'::jsonb) || '{{"stale_swept": true, "sweep_reason": "dispatched_age"}}'::jsonb,
                        updated_at = NOW()
                    WHERE status IN ('queued', 'dispatched')
                      AND provider IN ({provider_list})
                      AND stage = 'video'
                      AND updated_at < NOW() - INTERVAL '{dispatched_age} seconds'
                      AND (heartbeat_at IS NULL OR heartbeat_at < NOW() - INTERVAL '{HEARTBEAT_TIMEOUT} seconds')
                      AND status NOT IN ({terminal_list})
                    RETURNING id, status, provider
                    """,
                )
                rows = cur.fetchall() or []
                if rows:
                    swept_total += len(rows)
                    for r in rows:
                        details.append(f"job={r['id']} was={r['status']} provider={r.get('provider','?')} reason=dispatched_age")

                # 2. provider_pending too long (provider-aware thresholds)
                # Seedance queue can take 2+ hours under load; Vertex is typically fast.
                _pending_thresholds = {
                    "seedance": getattr(_cfg, "STALE_PENDING_AGE_SEEDANCE_S", 7200),
                    "vertex": _cfg.STALE_PENDING_AGE_S,
                }
                for _prov, _age in _pending_thresholds.items():
                    cur.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'stalled',
                            claimed_by = NULL,
                            claimed_at = NULL,
                            meta = COALESCE(meta, '{{}}'::jsonb) || '{{"stale_swept": true, "sweep_reason": "pending_age"}}'::jsonb,
                            updated_at = NOW()
                        WHERE status = 'provider_pending'
                          AND provider = %s
                          AND stage = 'video'
                          AND updated_at < NOW() - INTERVAL '{_age} seconds'
                          AND (heartbeat_at IS NULL OR heartbeat_at < NOW() - INTERVAL '{HEARTBEAT_TIMEOUT} seconds')
                        RETURNING id, status, provider
                        """,
                        (_prov,),
                    )
                    rows = cur.fetchall() or []
                    if rows:
                        swept_total += len(rows)
                        for r in rows:
                            details.append(f"job={r['id']} was=provider_pending provider={r.get('provider','?')} reason=pending_age({_age}s)")

                # 3. provider_processing too long (provider-aware thresholds)
                _processing_thresholds = {
                    "seedance": getattr(_cfg, "STALE_PROCESSING_AGE_SEEDANCE_S", 3600),
                    "vertex": _cfg.STALE_PROCESSING_AGE_S,
                }
                for _prov, _age in _processing_thresholds.items():
                    cur.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'stalled',
                            claimed_by = NULL,
                            claimed_at = NULL,
                            meta = COALESCE(meta, '{{}}'::jsonb) || '{{"stale_swept": true, "sweep_reason": "processing_age"}}'::jsonb,
                            updated_at = NOW()
                        WHERE status = 'provider_processing'
                          AND provider = %s
                          AND stage = 'video'
                          AND updated_at < NOW() - INTERVAL '{_age} seconds'
                          AND (heartbeat_at IS NULL OR heartbeat_at < NOW() - INTERVAL '{HEARTBEAT_TIMEOUT} seconds')
                        RETURNING id, status, provider
                        """,
                        (_prov,),
                    )
                    rows = cur.fetchall() or []
                    if rows:
                        swept_total += len(rows)
                        for r in rows:
                            details.append(f"job={r['id']} was=provider_processing provider={r.get('provider','?')} reason=processing_age({_age}s)")

                # 4. finalizing too long (crash during S3 upload / credit capture)
                finalizing_age = _cfg.STALE_FINALIZING_AGE_S
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'stalled',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        meta = COALESCE(meta, '{{}}'::jsonb) || '{{"stale_swept": true, "sweep_reason": "finalizing_stuck"}}'::jsonb,
                        updated_at = NOW()
                    WHERE status = 'finalizing'
                      AND provider IN ({provider_list})
                      AND stage = 'video'
                      AND updated_at < NOW() - INTERVAL '{finalizing_age} seconds'
                    RETURNING id, status, provider
                    """,
                )
                rows = cur.fetchall() or []
                if rows:
                    swept_total += len(rows)
                    for r in rows:
                        details.append(f"job={r['id']} was=finalizing provider={r.get('provider','?')} reason=finalizing_stuck")

            conn.commit()

        if swept_total > 0:
            print(f"[SWEEP] swept {swept_total} stale jobs")
            for d in details:
                print(f"[SWEEP]   {d}")
        else:
            print("[SWEEP] no stale jobs found")

        return {"swept": swept_total, "details": details}

    except Exception as e:
        print(f"[SWEEP] error: {e}")
        return {"swept": 0, "error": str(e)}


# ── Startup Recovery ────────────────────────────────────────

def recover_stale_jobs():
    """
    Startup recovery: find non-terminal video jobs with supported providers
    and mark them as stalled so the worker loop picks them up.

    Jobs outside the supported scope (meshy 3D, old legacy, unknown providers)
    are left untouched — they use separate dispatch paths.

    Very old jobs (> MAX_RECOVERY_AGE_HOURS) are marked abandoned_legacy.
    """
    if not USE_DB:
        return {"recovered": 0, "abandoned": 0}

    provider_list = ", ".join(f"'{p}'" for p in _SUPPORTED_PROVIDERS)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Recover recent supported jobs (including finalizing)
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'stalled',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        updated_at = NOW()
                    WHERE status IN (
                        'queued', 'dispatched', 'pending', 'processing',
                        'provider_pending', 'provider_processing', 'recovering',
                        'finalizing'
                    )
                    AND provider IN ({provider_list})
                    AND stage = 'video'
                    AND created_at > NOW() - INTERVAL '{MAX_RECOVERY_AGE_HOURS} hours'
                    RETURNING id, status, provider
                    """,
                )
                recovered = cur.fetchall() or []

                # Abandon very old non-terminal jobs that are past recovery age
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'abandoned_legacy',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        meta = COALESCE(meta, '{{}}'::jsonb) || '{{"abandoned_reason": "too_old_for_recovery", "abandoned_by": "startup_recovery"}}'::jsonb,
                        updated_at = NOW()
                    WHERE status IN (
                        'queued', 'dispatched', 'pending', 'processing',
                        'provider_pending', 'provider_processing', 'recovering', 'stalled',
                        'finalizing'
                    )
                    AND created_at <= NOW() - INTERVAL '{MAX_RECOVERY_AGE_HOURS} hours'
                    AND status NOT IN ({', '.join(f"'{s}'" for s in TERMINAL_STATES)})
                    RETURNING id, status, provider
                    """,
                )
                abandoned = cur.fetchall() or []

            conn.commit()

        rec_count = len(recovered)
        abn_count = len(abandoned)

        if rec_count > 0:
            print(f"[JOB] startup recovery: marked {rec_count} jobs as stalled")
            for r in recovered:
                print(f"[JOB]   reclaimed job={r['id']} was_status={r['status']} provider={r.get('provider', '?')}")
        else:
            print("[JOB] startup recovery: no recoverable jobs found")

        if abn_count > 0:
            print(f"[JOB] startup recovery: abandoned {abn_count} legacy jobs (>{MAX_RECOVERY_AGE_HOURS}h old)")
            for r in abandoned:
                print(f"[JOB]   abandoned job={r['id']} was_status={r['status']} provider={r.get('provider', '?')}")

        return {"recovered": rec_count, "abandoned": abn_count}

    except Exception as e:
        print(f"[JOB] startup recovery error: {e}")
        return {"recovered": 0, "abandoned": 0, "error": str(e)}


# ── Operations Thread (Sweep + Rescue) ─────────────────────
# Combines stall detection, stale sweep, and rescue into a single
# background loop with config-driven intervals.

_ops_thread: Optional[threading.Thread] = None


def start_operations_loop():
    """
    Start the unified background operations thread.

    Runs three periodic tasks at independent intervals:
    1. Stall detection (every sweep interval) — catches expired heartbeats
    2. Stale sweep (every sweep interval) — catches age-threshold stuck jobs
    3. Rescue (every rescue interval) — recovers late-completed upstream jobs

    Config-driven via config.STALE_SWEEP_* and config.RESCUE_*.
    Replaces the old start_stall_detector().
    """
    global _ops_thread
    if _ops_thread and _ops_thread.is_alive():
        print("[OPS] operations loop already running")
        return

    from backend.config import config as _cfg

    sweep_enabled = _cfg.STALE_SWEEP_ENABLED
    sweep_interval = _cfg.STALE_SWEEP_INTERVAL_S
    rescue_enabled = _cfg.RESCUE_ENABLED
    rescue_interval = max(_cfg.RESCUE_INTERVAL_S, sweep_interval)  # at least as often as sweep
    rescue_lookback = _cfg.RESCUE_LOOKBACK_HOURS
    rescue_max = _cfg.RESCUE_MAX_CANDIDATES

    # How many sweep cycles per rescue cycle
    rescue_every_n = max(1, rescue_interval // sweep_interval)

    def _loop():
        cycle = 0
        print(f"[OPS] operations loop started sweep_interval={sweep_interval}s "
              f"sweep_enabled={sweep_enabled} rescue_enabled={rescue_enabled} "
              f"rescue_every={rescue_every_n} cycles ({rescue_interval}s)")

        while not _worker_stop.is_set():
            cycle += 1

            # -- Stall detection (always runs, lightweight) --
            try:
                detect_stalled_jobs()
            except Exception as e:
                print(f"[OPS] stall detection error: {e}")

            # -- Stale sweep --
            if sweep_enabled:
                try:
                    run_stale_sweep()
                except Exception as e:
                    print(f"[OPS] stale sweep error: {e}")

            # -- Rescue pass --
            if rescue_enabled and cycle % rescue_every_n == 0:
                try:
                    from backend.services.job_rescue import rescue_late_completed_jobs
                    print(f"[OPS] rescue pass starting lookback={rescue_lookback}h max={rescue_max}")
                    result = rescue_late_completed_jobs(
                        hours=rescue_lookback,
                        dry_run=False,
                        max_jobs=rescue_max,
                    )
                    rescued = result.get("rescued", 0)
                    requeued = result.get("requeued", 0)
                    candidates = result.get("candidates", 0)
                    if rescued > 0 or requeued > 0:
                        print(f"[OPS] rescue pass done candidates={candidates} rescued={rescued} requeued={requeued}")
                    else:
                        print(f"[OPS] rescue pass done candidates={candidates} no_actions")
                except Exception as e:
                    print(f"[OPS] rescue pass error: {e}")

            _worker_stop.wait(timeout=sweep_interval)

    _ops_thread = threading.Thread(
        target=_loop,
        name="job-ops-loop",
        daemon=True,
    )
    _ops_thread.start()


# Legacy alias for backward compatibility
def start_stall_detector(interval: int = 60):  # noqa: ARG001
    """Start the operations loop. Legacy name kept for backward compat."""
    start_operations_loop()


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