"""
Job Rescue Service — Late Success Recovery for Seedance Jobs.

Rescues jobs that were marked failed/provider_stalled locally but later
completed successfully at PiAPI. This happens when PiAPI queue times out
locally but the provider eventually processes the job.

Credit safety:
  - If the reservation is still 'held', finalize it normally (charge credits).
  - If the reservation was already 'released' (refunded), do NOT re-charge.
    The job is marked 'rescued_free' — the user keeps the video at no cost.
    This is the safe default: ledger correctness > lost revenue.
  - If the reservation was already 'finalized', the job already succeeded
    via another path — skip (idempotent).

All operations are idempotent. Safe to run multiple times.
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from backend.db import USE_DB, get_conn, Tables
from backend.config import AWS_BUCKET_MODELS


def rescue_late_completed_jobs(
    hours: int = 72,
    dry_run: bool = False,
    max_jobs: int = 50,
) -> Dict[str, Any]:
    """
    Find locally-failed Seedance jobs that completed upstream, and rescue them.

    Args:
        hours: Look back window in hours (default 72h).
        dry_run: If True, only report candidates without modifying anything.
        max_jobs: Max number of jobs to process per run.

    Returns:
        Summary dict with counts and details.
    """
    if not USE_DB:
        return {"error": "Database not available"}

    results = {
        "candidates": 0,
        "rescued": 0,
        "already_rescued": 0,
        "still_running": 0,
        "upstream_failed": 0,
        "upstream_not_found": 0,
        "errors": 0,
        "requeued": 0,
        "details": [],
    }

    # Step 1: Find candidate jobs
    candidates = _find_candidates(hours, max_jobs)
    results["candidates"] = len(candidates)

    if not candidates:
        print(f"[RESCUE] No candidates found in the last {hours}h")
        return results

    print(f"[RESCUE] Found {len(candidates)} candidate jobs")

    if dry_run:
        for job in candidates:
            job_id = str(job["id"])
            upstream = job.get("upstream_job_id", "?")
            status = job["status"]
            error = job.get("last_error_code") or job.get("error_message", "")[:60]
            print(f"[RESCUE] [DRY RUN] candidate job={job_id} upstream={upstream} local_status={status} error={error}")
            results["details"].append({
                "job_id": job_id,
                "upstream_job_id": upstream,
                "local_status": status,
                "action": "dry_run",
            })
        return results

    # Step 2: Process each candidate
    for job in candidates:
        job_id = str(job["id"])
        upstream_id = job.get("upstream_job_id")
        local_status = job["status"]
        meta = _parse_meta(job.get("meta"))

        print(f"[RESCUE] candidate job={job_id} upstream={upstream_id} local_status={local_status}")

        if not upstream_id:
            print(f"[RESCUE] skipped job={job_id} reason=no_upstream_id")
            results["details"].append({
                "job_id": job_id, "action": "skipped", "reason": "no_upstream_id",
            })
            continue

        try:
            result = _process_candidate(job, meta)
            action = result.get("action", "error")

            if action == "rescued":
                results["rescued"] += 1
            elif action == "already_rescued":
                results["already_rescued"] += 1
            elif action == "still_running":
                results["still_running"] += 1
            elif action == "requeued":
                results["requeued"] += 1
            elif action == "upstream_failed":
                results["upstream_failed"] += 1
            elif action == "upstream_not_found":
                results["upstream_not_found"] += 1
            else:
                results["errors"] += 1

            results["details"].append(result)

        except Exception as e:
            print(f"[RESCUE] ERROR job={job_id}: {e}")
            results["errors"] += 1
            results["details"].append({
                "job_id": job_id, "action": "error", "error": str(e),
            })

    print(f"[RESCUE] Complete: {_summary(results)}")
    return results


# ── Candidate Query ─────────────────────────────────────────

def _find_candidates(hours: int, limit: int) -> List[Dict[str, Any]]:
    """Find locally-failed Seedance jobs with an upstream_job_id."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT j.id, j.identity_id, j.provider, j.action_code,
                           j.status, j.upstream_job_id, j.reservation_id,
                           j.prompt, j.meta, j.error_message,
                           j.last_error_code, j.last_error_message,
                           j.result_url, j.cost_credits,
                           j.created_at, j.updated_at
                    FROM {Tables.JOBS} j
                    WHERE j.provider = 'seedance'
                      AND j.status IN ('failed', 'provider_stalled')
                      AND j.upstream_job_id IS NOT NULL
                      AND j.created_at > NOW() - %s * INTERVAL '1 hour'
                      AND j.result_url IS NULL
                    ORDER BY j.created_at DESC
                    LIMIT %s
                    """,
                    (hours, limit),
                )
                return cur.fetchall() or []
    except Exception as e:
        print(f"[RESCUE] Error querying candidates: {e}")
        return []


# ── Per-Job Processing ──────────────────────────────────────

def _process_candidate(job: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Check upstream status and take action.

    Returns a result dict with 'action' key indicating what happened.
    """
    job_id = str(job["id"])
    upstream_id = job["upstream_job_id"]
    identity_id = str(job.get("identity_id") or meta.get("identity_id", ""))
    reservation_id = str(job.get("reservation_id") or meta.get("reservation_id", "")) or None

    # Check if already rescued (idempotent guard)
    if job.get("result_url"):
        print(f"[RESCUE] skipped job={job_id} reason=already_succeeded")
        return {"job_id": job_id, "action": "already_rescued"}

    # Poll upstream
    from backend.services.seedance_service import check_seedance_status
    status_resp = check_seedance_status(upstream_id)
    upstream_status = status_resp.get("status", "unknown")

    print(f"[RESCUE] job={job_id} upstream_status={upstream_status}")

    if upstream_status == "done":
        video_url = status_resp.get("video_url")
        if not video_url:
            print(f"[RESCUE] failed job={job_id} reason=done_but_no_video_url")
            _enrich_error(job_id, "rescue_no_video_url", "Upstream completed but no video URL")
            return {"job_id": job_id, "action": "upstream_failed", "reason": "no_video_url"}

        return _rescue_completed_job(job, meta, video_url, reservation_id, identity_id)

    elif upstream_status in ("processing", "pending"):
        progress = status_resp.get("progress", 0)
        print(f"[RESCUE] job={job_id} still running ({upstream_status}, {progress}%), attempting requeue")
        requeued = _requeue_for_worker(job_id)
        action = "requeued" if requeued else "still_running"
        return {
            "job_id": job_id,
            "action": action,
            "upstream_status": upstream_status,
            "progress": progress,
        }

    elif upstream_status == "failed":
        error_msg = status_resp.get("message", "Provider confirmed failure")
        print(f"[RESCUE] failed job={job_id} reason=upstream_confirmed_failed")
        _enrich_error(job_id, "upstream_confirmed_failed", error_msg)
        return {"job_id": job_id, "action": "upstream_failed", "reason": error_msg}

    elif upstream_status == "error":
        print(f"[RESCUE] skipped job={job_id} reason=network_error_checking_upstream")
        return {"job_id": job_id, "action": "error", "reason": "network_error"}

    else:
        print(f"[RESCUE] skipped job={job_id} reason=unknown_status_{upstream_status}")
        return {"job_id": job_id, "action": "upstream_not_found", "reason": f"unknown_status: {upstream_status}"}


# ── Rescue a Completed Job ──────────────────────────────────

def _rescue_completed_job(
    job: Dict[str, Any],
    meta: Dict[str, Any],
    video_url: str,
    reservation_id: Optional[str],
    identity_id: str,
) -> Dict[str, Any]:
    """
    Full rescue flow for a job whose upstream completed successfully.

    1. Download video + upload to S3
    2. Inspect reservation state (held / released / finalized)
    3. If held: finalize credits (charge)
    4. If released: mark rescued_free (no charge, ledger stays correct)
    5. If finalized: already charged (idempotent)
    6. Save to history
    7. Update job row
    """
    job_id = str(job["id"])
    provider_name = job.get("provider") or meta.get("provider", "seedance")
    prompt = job.get("prompt") or meta.get("prompt", "")

    print(f"[RESCUE] completed job={job_id} video_url={video_url[:80]}...")

    # Step 1: Download and upload to S3
    s3_video_url = None
    s3_thumbnail_url = None
    final_video_url = video_url

    if AWS_BUCKET_MODELS:
        try:
            s3_result = _upload_video_to_s3(job_id, identity_id, video_url, provider_name)
            s3_video_url = s3_result.get("s3_video_url")
            s3_thumbnail_url = s3_result.get("s3_thumbnail_url")
            if s3_video_url:
                final_video_url = s3_video_url
                print(f"[RESCUE] uploaded result job={job_id}")
        except Exception as e:
            print(f"[RESCUE] S3 upload failed for job={job_id}: {e}, using provider URL")

    # Step 2: Handle credits
    credit_action = _handle_credits(job_id, reservation_id, identity_id)
    print(f"[RESCUE] credits action={credit_action} job={job_id}")

    # Step 3: Save to history (idempotent via ON CONFLICT)
    _save_to_history(job_id, identity_id, final_video_url, s3_video_url,
                     s3_thumbnail_url, prompt, meta, provider_name)

    # Step 4: Update job row
    rescued_status = "ready"  # frontend-compatible terminal success state
    _update_rescued_job(
        job_id, rescued_status, final_video_url, s3_thumbnail_url,
        credit_action, meta,
    )

    print(f"[RESCUE] completed job={job_id} uploaded=true history_saved=true credits={credit_action}")

    return {
        "job_id": job_id,
        "action": "rescued",
        "video_url": final_video_url,
        "s3_video_url": s3_video_url,
        "credit_action": credit_action,
    }


# ── Credit Safety ───────────────────────────────────────────

def _handle_credits(
    job_id: str,
    reservation_id: Optional[str],
    identity_id: str,
) -> str:
    """
    Inspect reservation state and act accordingly.

    Returns one of:
      'finalized'           - credits captured now (reservation was still held)
      'already_finalized'   - credits were already captured (idempotent)
      'released_free'       - reservation was released, user gets video free
      'no_reservation'      - no reservation found, marked rescued_free
    """
    if not reservation_id:
        print(f"[RESCUE] No reservation_id for job={job_id}, marking rescued_free")
        return "no_reservation"

    from backend.services.reservation_service import ReservationService

    # Look up current reservation state
    reservation = ReservationService.get_reservation(reservation_id)

    if not reservation:
        print(f"[RESCUE] Reservation {reservation_id} not found for job={job_id}")
        return "no_reservation"

    status = reservation.get("status")

    if status == "finalized":
        # Already charged — nothing to do
        print(f"[RESCUE] Reservation {reservation_id} already finalized for job={job_id}")
        return "already_finalized"

    if status == "released":
        # Credits were already refunded. Do NOT re-charge.
        # User gets the video for free. This is the safe default.
        print(f"[RESCUE] Reservation {reservation_id} was released (refunded) for job={job_id} — rescued_free")
        return "released_free"

    if status == "held":
        # Reservation still active — finalize normally (charge credits)
        from backend.services.credits_helper import finalize_job_credits
        result = finalize_job_credits(reservation_id, job_id, identity_id)
        if result.get("success"):
            print(f"[RESCUE] credits finalized job={job_id} reservation={reservation_id}")
            return "finalized"
        else:
            print(f"[RESCUE] credits finalize failed job={job_id}: {result}")
            return "finalize_failed"

    # Unknown state
    print(f"[RESCUE] Reservation {reservation_id} has unknown status={status}")
    return f"unknown_{status}"


# ── S3 Upload ───────────────────────────────────────────────

def _upload_video_to_s3(
    job_id: str,
    identity_id: str,
    video_url: str,
    provider_name: str,
) -> Dict[str, Any]:
    """Download video from provider and upload to S3. Returns URLs."""
    import base64
    from backend.services.s3_service import safe_upload_to_s3
    from backend.services.video_router import resolve_video_provider
    from backend.services.gemini_video_service import download_video_bytes, extract_video_thumbnail

    provider = resolve_video_provider(provider_name)

    # Download
    if provider:
        video_bytes, content_type = provider.download_video(video_url)
    else:
        video_bytes, content_type = download_video_bytes(video_url)

    ext = ".webm" if "webm" in content_type else ".mp4"

    # Upload video
    b64_data = f"data:{content_type};base64,{base64.b64encode(video_bytes).decode('utf-8')}"
    s3_video_url = safe_upload_to_s3(
        b64_data,
        content_type,
        "videos",
        f"{provider_name}_{job_id}",
        user_id=identity_id,
        key_base=f"videos/{provider_name}/{identity_id or 'public'}/{job_id}{ext}",
        provider=provider_name,
    )

    result = {"s3_video_url": s3_video_url}

    # Extract + upload thumbnail
    if s3_video_url:
        try:
            if provider:
                thumb_bytes = provider.extract_thumbnail(video_bytes, timestamp_sec=1.0)
            else:
                thumb_bytes = extract_video_thumbnail(video_bytes, timestamp_sec=1.0)

            if thumb_bytes:
                thumb_b64 = f"data:image/jpeg;base64,{base64.b64encode(thumb_bytes).decode('utf-8')}"
                result["s3_thumbnail_url"] = safe_upload_to_s3(
                    thumb_b64,
                    "image/jpeg",
                    "thumbnails",
                    f"{provider_name}_thumb_{job_id}",
                    user_id=identity_id,
                    key_base=f"thumbnails/{identity_id or 'public'}/{job_id}.jpg",
                    provider=provider_name,
                )
            else:
                result["s3_thumbnail_url"] = s3_video_url
        except Exception as e:
            print(f"[RESCUE] Thumbnail extraction failed for {job_id}: {e}")
            result["s3_thumbnail_url"] = s3_video_url

    return result


# ── History ─────────────────────────────────────────────────

def _save_to_history(
    job_id: str,
    identity_id: str,
    final_video_url: str,
    s3_video_url: Optional[str],
    s3_thumbnail_url: Optional[str],
    prompt: str,
    meta: Dict[str, Any],
    provider_name: str,
):
    """Save rescued video to normalized tables (idempotent)."""
    from backend.services.history_service import save_video_to_normalized_db

    duration_seconds = meta.get("duration_seconds")
    if duration_seconds:
        try:
            duration_seconds = int(duration_seconds)
        except (ValueError, TypeError):
            duration_seconds = None

    save_video_to_normalized_db(
        video_id=job_id,
        video_url=str(final_video_url) if final_video_url else "",
        prompt=prompt,
        duration_seconds=duration_seconds,
        resolution=meta.get("resolution"),
        aspect_ratio=meta.get("aspect_ratio"),
        thumbnail_url=str(s3_thumbnail_url) if s3_thumbnail_url else None,
        user_id=identity_id,
        provider=provider_name,
        s3_video_url=str(s3_video_url) if s3_video_url else None,
    )


# ── Job Row Update ──────────────────────────────────────────

def _update_rescued_job(
    job_id: str,
    status: str,
    result_url: str,
    thumbnail_url: Optional[str],
    credit_action: str,
    meta: Dict[str, Any],
):
    """Update the job row with rescue results."""
    try:
        meta_patch = {
            "rescued": True,
            "rescued_at": time.time(),
            "rescue_credit_action": credit_action,
            "video_url": result_url,
            "progress": 100,
        }

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = %s,
                        result_url = %s,
                        thumbnail_url = %s,
                        completed_at = NOW(),
                        last_provider_status = 'done',
                        last_error_code = NULL,
                        last_error_message = NULL,
                        error_message = NULL,
                        progress = 100,
                        meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE id::text = %s
                    """,
                    (status, result_url, thumbnail_url,
                     json.dumps(meta_patch, default=str), job_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[RESCUE] Error updating job {job_id}: {e}")


# ── Requeue for Worker ──────────────────────────────────────

def _requeue_for_worker(job_id: str) -> bool:
    """Mark a still-running job as stalled so the durable worker picks it up."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'stalled',
                        claimed_by = NULL,
                        claimed_at = NULL,
                        last_error_code = NULL,
                        last_error_message = NULL,
                        meta = COALESCE(meta, '{{}}'::jsonb) || '{{"requeued_by": "rescue"}}'::jsonb,
                        updated_at = NOW()
                    WHERE id::text = %s
                      AND claimed_by IS NULL
                    RETURNING id
                    """,
                    (job_id,),
                )
                result = cur.fetchone()
            conn.commit()

        if result:
            print(f"[RESCUE] requeued job={job_id}")
            return True
        else:
            print(f"[RESCUE] could not requeue job={job_id} (already claimed or missing)")
            return False

    except Exception as e:
        print(f"[RESCUE] Requeue error for {job_id}: {e}")
        return False


# ── Error Enrichment ────────────────────────────────────────

def _enrich_error(job_id: str, error_code: str, error_message: str):
    """Add rescue check metadata to a failed job."""
    try:
        meta_patch = {
            "rescue_checked_at": time.time(),
            "rescue_upstream_error": error_code,
        }
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                        last_error_code = COALESCE(last_error_code, %s),
                        last_error_message = COALESCE(last_error_message, %s),
                        updated_at = NOW()
                    WHERE id::text = %s
                    """,
                    (json.dumps(meta_patch, default=str), error_code,
                     error_message[:500], job_id),
                )
            conn.commit()
    except Exception:
        pass


# ── Helpers ─────────────────────────────────────────────────

def _parse_meta(meta) -> Dict[str, Any]:
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


def _summary(results: Dict[str, Any]) -> str:
    return (
        f"rescued={results['rescued']} already={results['already_rescued']} "
        f"requeued={results['requeued']} still_running={results['still_running']} "
        f"upstream_failed={results['upstream_failed']} errors={results['errors']}"
    )
