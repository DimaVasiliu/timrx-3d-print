"""
Video Job Queue — retry queue for quota-limited video jobs.

When a provider's daily quota is exhausted, jobs are enqueued here
instead of failing immediately.  A background daemon re-tries them
periodically until the quota resets (typically 24 h for Veo).

Usage from async_dispatch:
    from backend.services.video_queue import video_queue

    try:
        resp, provider = video_router.route_text_to_video(…)
    except QuotaExhaustedError:
        video_queue.enqueue(job_data)       # → status becomes "quota_queued"
        return                              # frontend sees "queued" status

The queue is in-memory (per-process).  If the process restarts, queued
jobs stay in DB with status='quota_queued' and can be retried by calling
``video_queue.recover_from_db()`` at startup.
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from typing import Any, Dict, Optional

from backend.db import USE_DB, get_conn, Tables


# ── Configuration ─────────────────────────────────────────────
RETRY_INTERVAL_SECS = 300       # re-check every 5 minutes
MAX_RETRIES = 288               # 5 min × 288 = 24 hours
MAX_QUEUE_SIZE = 50             # prevent unbounded growth


# ── Queue entry schema ────────────────────────────────────────
class _QueueEntry:
    __slots__ = (
        "internal_job_id",
        "identity_id",
        "reservation_id",
        "payload",
        "store_meta",
        "retry_count",
        "queued_at",
    )

    def __init__(
        self,
        internal_job_id: str,
        identity_id: str,
        reservation_id: Optional[str],
        payload: dict,
        store_meta: dict,
    ):
        self.internal_job_id = internal_job_id
        self.identity_id = identity_id
        self.reservation_id = reservation_id
        self.payload = payload
        self.store_meta = store_meta
        self.retry_count = 0
        self.queued_at = time.time()


# ── VideoJobQueue ─────────────────────────────────────────────
class VideoJobQueue:
    """
    In-memory retry queue for video generation jobs blocked by quota.

    Thread-safe.  A single daemon timer fires every RETRY_INTERVAL_SECS
    and processes the head of the queue.
    """

    def __init__(self):
        self._queue: deque[_QueueEntry] = deque()
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None
        self._running = False

    # ── public API ────────────────────────────────────────────
    def enqueue(self, job_data: Dict[str, Any]) -> None:
        """
        Add a quota-blocked job to the retry queue.

        ``job_data`` must contain the same keys used by
        ``dispatch_gemini_video_async``:
            internal_job_id, identity_id, reservation_id,
            payload, store_meta
        """
        entry = _QueueEntry(
            internal_job_id=job_data["internal_job_id"],
            identity_id=job_data["identity_id"],
            reservation_id=job_data.get("reservation_id"),
            payload=job_data["payload"],
            store_meta=job_data["store_meta"],
        )

        with self._lock:
            # Prevent duplicates
            for existing in self._queue:
                if existing.internal_job_id == entry.internal_job_id:
                    print(f"[VideoQueue] Job {entry.internal_job_id} already queued, skipping")
                    return

            if len(self._queue) >= MAX_QUEUE_SIZE:
                oldest = self._queue.popleft()
                _fail_queued_job(oldest, "queue_full")

            self._queue.append(entry)
            print(
                f"[VideoQueue] Enqueued job {entry.internal_job_id}, "
                f"queue size={len(self._queue)}"
            )

        # Mark in DB + store
        _mark_job_quota_queued(entry.internal_job_id, entry.store_meta)

        self._ensure_processor_running()

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def process_queue_now(self) -> int:
        """
        Admin trigger: immediately try to process all queued jobs.
        Returns number of jobs dispatched for retry.
        """
        dispatched = 0
        with self._lock:
            entries = list(self._queue)

        for entry in entries:
            try:
                from backend.services.video_router import video_router

                providers = video_router.get_available_providers()
                if not providers:
                    print("[VideoQueue] Admin trigger: no providers available")
                    break

                with self._lock:
                    # Remove from queue
                    try:
                        self._queue.remove(entry)
                    except ValueError:
                        continue  # Already removed

                _retry_dispatch(entry)
                dispatched += 1
                print(f"[VideoQueue] Admin trigger: dispatched {entry.internal_job_id}")

            except Exception as e:
                print(f"[VideoQueue] Admin trigger error for {entry.internal_job_id}: {e}")
                break  # Stop on quota error, retry rest later

        if dispatched:
            print(f"[VideoQueue] Admin trigger: dispatched {dispatched} jobs")
        return dispatched

    def recover_from_db(self) -> int:
        """
        Re-populate queue from DB rows with status='quota_queued' or
        priority='queued_daily'.  Call once at startup.  Returns number recovered.
        """
        if not USE_DB:
            return 0
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id::text, identity_id::text, meta
                        FROM {Tables.JOBS}
                        WHERE status = 'quota_queued'
                           OR (priority = 'queued_daily' AND status NOT IN ('ready', 'failed'))
                        ORDER BY created_at ASC
                        LIMIT %s
                        """,
                        (MAX_QUEUE_SIZE,),
                    )
                    rows = cur.fetchall()

            recovered = 0
            for row in rows:
                meta = row.get("meta") or {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        meta = {}

                self.enqueue({
                    "internal_job_id": row["id"],
                    "identity_id": row["identity_id"],
                    "reservation_id": meta.get("reservation_id"),
                    "payload": meta.get("payload", {}),
                    "store_meta": meta,
                })
                recovered += 1

            if recovered:
                print(f"[VideoQueue] Recovered {recovered} quota-queued jobs from DB")
            return recovered

        except Exception as e:
            print(f"[VideoQueue] Error recovering from DB: {e}")
            return 0

    # ── background processor ──────────────────────────────────
    def _ensure_processor_running(self):
        with self._lock:
            if not self._running:
                self._running = True
                self._schedule_next()

    def _schedule_next(self):
        self._timer = threading.Timer(RETRY_INTERVAL_SECS, self._process_one)
        self._timer.daemon = True
        self._timer.start()

    def _process_one(self):
        """Try to dispatch the head-of-queue job."""
        entry: Optional[_QueueEntry] = None

        with self._lock:
            if not self._queue:
                self._running = False
                return
            entry = self._queue[0]

        if entry is None:
            return

        entry.retry_count += 1

        if entry.retry_count > MAX_RETRIES:
            with self._lock:
                self._queue.popleft()
            _fail_queued_job(entry, "max_retries_exceeded")
            self._continue_or_stop()
            return

        # Attempt dispatch via the router
        try:
            from backend.services.video_router import QuotaExhaustedError, video_router

            providers = video_router.get_available_providers()
            if not providers:
                print(f"[VideoQueue] No providers available, will retry in {RETRY_INTERVAL_SECS}s")
                self._continue_or_stop()
                return

            # Remove from queue BEFORE dispatching (prevents double-dispatch)
            with self._lock:
                self._queue.popleft()

            _retry_dispatch(entry)
            print(f"[VideoQueue] Re-dispatched job {entry.internal_job_id} (retry #{entry.retry_count})")

        except QuotaExhaustedError:
            print(
                f"[VideoQueue] Still quota-limited, retry #{entry.retry_count}. "
                f"Next check in {RETRY_INTERVAL_SECS}s"
            )
        except Exception as e:
            print(f"[VideoQueue] Error processing job {entry.internal_job_id}: {e}")

        self._continue_or_stop()

    def _continue_or_stop(self):
        with self._lock:
            if self._queue:
                self._schedule_next()
            else:
                self._running = False
                print("[VideoQueue] Queue empty, processor stopped")


# ── Helpers (module-level) ────────────────────────────────────
def _mark_job_quota_queued(job_id: str, store_meta: dict) -> None:
    """Update job + store to reflect quota_queued status."""
    from backend.services.job_service import load_store, save_store

    store = load_store()
    store_meta["status"] = "quota_queued"
    store_meta["quota_queued_at"] = time.time()
    store[job_id] = store_meta
    save_store(store)

    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'quota_queued',
                            priority = 'queued_daily',
                            meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id::text = %s
                        """,
                        (json.dumps({"quota_queued_at": time.time()}), job_id),
                    )
                conn.commit()
        except Exception as e:
            print(f"[VideoQueue] Error marking job {job_id} as quota_queued: {e}")


def _fail_queued_job(entry: _QueueEntry, reason: str) -> None:
    """Fail a queued job and release its credits."""
    from backend.services.credits_helper import release_job_credits
    from backend.services.async_dispatch import update_job_status_failed

    print(f"[VideoQueue] Failing job {entry.internal_job_id}: {reason}")

    if entry.reservation_id:
        release_job_credits(entry.reservation_id, f"queue_{reason}", entry.internal_job_id)
    update_job_status_failed(
        entry.internal_job_id,
        f"quota_queue_{reason}: Video generation could not complete within retry window",
    )


def _retry_dispatch(entry: _QueueEntry) -> None:
    """Re-dispatch a queued job through the normal async flow."""
    from backend.services.async_dispatch import dispatch_gemini_video_async, get_executor

    # Update status back to processing
    from backend.services.job_service import load_store, save_store

    store = load_store()
    entry.store_meta["status"] = "processing"
    entry.store_meta.pop("quota_queued_at", None)
    store[entry.internal_job_id] = entry.store_meta
    save_store(store)

    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'processing', priority = 'normal', updated_at = NOW()
                        WHERE id::text = %s
                        """,
                        (entry.internal_job_id,),
                    )
                conn.commit()
        except Exception as e:
            print(f"[VideoQueue] Error updating job status for retry: {e}")

    # Dispatch in background thread
    get_executor().submit(
        dispatch_gemini_video_async,
        entry.internal_job_id,
        entry.identity_id,
        entry.reservation_id,
        entry.payload,
        entry.store_meta,
    )


# Singleton instance
video_queue = VideoJobQueue()
