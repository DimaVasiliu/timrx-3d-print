"""
Job Service - Manages job creation and dispatch to upstream providers.

Flow:
1. create_job(identity_id, action_key, payload) ->
   a) Create job row (status=queued)
   b) Reserve credits for action
   c) Dispatch to provider (Meshy/OpenAI)
   d) Update job with upstream_job_id
   e) Return job_id + reservation_id

Providers:
- meshy: Text-to-3D, Image-to-3D, Refine, Remesh, Texture, Rig
- openai: Image Studio (DALL-E/GPT-Image)

Job Statuses:
- queued: Job created, awaiting dispatch
- pending: Dispatched to provider, awaiting completion
- succeeded: Provider completed successfully
- failed: Provider or dispatch failed
"""

import os
import uuid
import json
import requests
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from datetime import datetime

from backend.db import USE_DB, dict_row, get_conn, transaction, fetch_one, query_one, query_all, execute, Tables
from backend.utils import derive_display_title
from backend.services.reservation_service import ReservationService, ReservationStatus
from backend.services.pricing_service import PricingService


class MissingIdentityError(ValueError):
    """
    Raised when a job operation requires an identity_id but none was provided.

    This prevents orphaned jobs that cannot be retrieved by any user.
    """

    def __init__(self, operation: str, job_id: str = None):
        self.operation = operation
        self.job_id = job_id
        msg = f"identity_id is required for {operation}"
        if job_id:
            msg += f" (job_id={job_id})"
        super().__init__(msg)


class JobStatus:
    """Valid job statuses."""
    QUEUED = "queued"
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class JobProvider:
    """Valid job providers."""
    MESHY = "meshy"
    OPENAI = "openai"


# Action key to provider mapping
ACTION_PROVIDER_MAP = {
    "text_to_3d_generate": JobProvider.MESHY,
    "image_to_3d_generate": JobProvider.MESHY,
    "refine": JobProvider.MESHY,
    "remesh": JobProvider.MESHY,
    "texture": JobProvider.MESHY,
    "rig": JobProvider.MESHY,
    "image_studio_generate": JobProvider.OPENAI,
}

# Meshy API endpoints for each action
MESHY_ENDPOINTS = {
    "text_to_3d_generate": "/openapi/v2/text-to-3d",
    "image_to_3d_generate": "/openapi/v1/image-to-3d",
    "refine": "/openapi/v2/text-to-3d",  # Uses mode=refine
    "remesh": "/openapi/v2/models/remesh",
    "texture": "/openapi/v2/models/retexture",
    "rig": "/openapi/v2/models/rigging",
}


class JobService:
    """Service for managing jobs and dispatching to providers."""

    # ─────────────────────────────────────────────────────────────
    # API Configuration
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _get_meshy_api_key() -> Optional[str]:
        """Get Meshy API key from environment."""
        return os.getenv("MESHY_API_KEY")

    @staticmethod
    def _get_meshy_api_base() -> str:
        """Get Meshy API base URL."""
        return os.getenv("MESHY_API_BASE", "https://api.meshy.ai")

    @staticmethod
    def _get_openai_api_key() -> Optional[str]:
        """Get OpenAI API key from environment."""
        return os.getenv("OPENAI_API_KEY")

    @staticmethod
    def _meshy_auth_headers() -> Dict[str, str]:
        """Get Meshy API auth headers."""
        api_key = JobService._get_meshy_api_key()
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _openai_auth_headers() -> Dict[str, str]:
        """Get OpenAI API auth headers."""
        api_key = JobService._get_openai_api_key()
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    # ─────────────────────────────────────────────────────────────
    # Job Creation
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def create_job(
        identity_id: str,
        action_key: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Create a job and dispatch to the appropriate provider.

        Args:
            identity_id: The user's identity
            action_key: Frontend action key (e.g., 'text_to_3d_generate')
            payload: Action-specific payload (differs per tool)

        Returns:
            Dict with job details:
            {
                "job_id": "uuid",
                "reservation_id": "uuid",
                "upstream_job_id": "provider-id",
                "status": "pending",
                "provider": "meshy",
                "action_code": "MESHY_TEXT_TO_3D",
                "cost_credits": 20
            }

        Raises:
            ValueError: If invalid action, insufficient credits, or provider error
        """
        # Validate action
        provider = ACTION_PROVIDER_MAP.get(action_key)
        if not provider:
            raise ValueError(f"Unknown action: {action_key}")

        # Get DB action code
        action_code = PricingService.get_db_action_code(action_key)
        if not action_code:
            action_code = PricingService.map_job_type_to_action(action_key)

        # Get cost for this action
        cost_credits = PricingService.get_action_cost(action_key)
        if cost_credits == 0:
            raise ValueError(f"No cost defined for action: {action_key}")

        # Extract prompt for storage (if available)
        prompt = payload.get("prompt", "")

        # Build job meta
        job_meta = {"payload": payload}

        # Create job and reserve credits in a single transaction
        with transaction() as cur:
            # 1. Create job row (status=queued)
            cur.execute(
                f"""
                INSERT INTO {Tables.JOBS}
                (identity_id, provider, action_code, status, cost_credits, prompt, meta)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    identity_id,
                    provider,
                    action_code,
                    JobStatus.QUEUED,
                    cost_credits,
                    prompt,
                    json.dumps(job_meta),
                ),
            )
            job_row = fetch_one(cur)
            job_id = str(job_row["id"])

        # 2. Reserve credits
        reservation_id = None
        try:
            reservation_result = ReservationService.reserve_credits(
                identity_id=identity_id,
                action_key=action_key,
                job_id=job_id,
                meta={"action_key": action_key, "provider": provider},
            )
            reservation_id = reservation_result["reservation"]["id"]
        except ValueError as e:
            # Clean up job if reservation fails
            execute(
                f"UPDATE {Tables.JOBS} SET status = %s, error_message = %s WHERE id = %s",
                (JobStatus.FAILED, str(e), job_id),
            )
            raise

        # 3. Update job with reservation_id
        execute(
            f"UPDATE {Tables.JOBS} SET reservation_id = %s WHERE id = %s",
            (reservation_id, job_id),
        )

        # 4. Dispatch to provider
        try:
            upstream_job_id = JobService._dispatch_to_provider(
                provider=provider,
                action_key=action_key,
                payload=payload,
            )
        except Exception as e:
            # Release reservation on dispatch failure
            try:
                ReservationService.release_reservation(reservation_id, reason="dispatch_failed")
            except Exception:
                pass

            execute(
                f"UPDATE {Tables.JOBS} SET status = %s, error_message = %s WHERE id = %s",
                (JobStatus.FAILED, str(e), job_id),
            )
            raise ValueError(f"Provider dispatch failed: {e}")

        # 5. Update job with upstream_job_id and set status to pending
        execute(
            f"""
            UPDATE {Tables.JOBS}
            SET upstream_job_id = %s, status = %s
            WHERE id = %s
            """,
            (upstream_job_id, JobStatus.PENDING, job_id),
        )

        print(
            f"[JOB] Created job={job_id}, upstream={upstream_job_id}, "
            f"provider={provider}, action={action_code}, credits={cost_credits}"
        )

        return {
            "job_id": job_id,
            "reservation_id": reservation_id,
            "upstream_job_id": upstream_job_id,
            "status": JobStatus.PENDING,
            "provider": provider,
            "action_code": action_code,
            "cost_credits": cost_credits,
        }

    # ─────────────────────────────────────────────────────────────
    # Provider Dispatch
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _dispatch_to_provider(
        provider: str,
        action_key: str,
        payload: Dict[str, Any],
    ) -> str:
        """
        Dispatch a job to the appropriate provider.

        Returns:
            upstream_job_id from the provider

        Raises:
            RuntimeError: If dispatch fails
        """
        if provider == JobProvider.MESHY:
            return JobService._dispatch_meshy(action_key, payload)
        elif provider == JobProvider.OPENAI:
            return JobService._dispatch_openai(action_key, payload)
        else:
            raise ValueError(f"Unknown provider: {provider}")

    @staticmethod
    def _dispatch_meshy(action_key: str, payload: Dict[str, Any]) -> str:
        """Dispatch job to Meshy API."""
        api_key = JobService._get_meshy_api_key()
        if not api_key:
            raise RuntimeError("MESHY_API_KEY not configured")

        base_url = JobService._get_meshy_api_base()
        endpoint = MESHY_ENDPOINTS.get(action_key)
        if not endpoint:
            raise RuntimeError(f"No Meshy endpoint for action: {action_key}")

        # Build provider-specific payload
        meshy_payload = JobService._build_meshy_payload(action_key, payload)

        url = f"{base_url}{endpoint}"
        headers = JobService._meshy_auth_headers()

        print(f"[JOB] Dispatching to Meshy: {url}")
        print(f"[JOB] Payload: {json.dumps(meshy_payload)[:500]}")

        resp = requests.post(url, headers=headers, json=meshy_payload, timeout=60)

        if not resp.ok:
            raise RuntimeError(f"Meshy POST {endpoint} -> {resp.status_code}: {resp.text[:500]}")

        data = resp.json()

        # Meshy returns job_id as "result" or "id" depending on endpoint
        job_id = data.get("result") or data.get("id")
        if not job_id:
            raise RuntimeError(f"No job_id in Meshy response: {data}")

        return job_id

    @staticmethod
    def _build_meshy_payload(action_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Build Meshy-specific payload from generic payload."""
        if action_key == "text_to_3d_generate":
            return {
                "mode": "preview",
                "prompt": payload.get("prompt", ""),
                "ai_model": payload.get("model") or "latest",
                "art_style": payload.get("art_style") or "realistic",
                "symmetry_mode": payload.get("symmetry_mode") or "auto",
            }

        elif action_key == "image_to_3d_generate":
            return {
                "image_url": payload.get("image_url"),
                "prompt": payload.get("prompt") or "",
                "ai_model": payload.get("model") or "latest",
                "enable_pbr": payload.get("enable_pbr", True),
            }

        elif action_key == "refine":
            return {
                "mode": "refine",
                "preview_task_id": payload.get("preview_task_id"),
                "enable_pbr": payload.get("enable_pbr", True),
                "texture_prompt": payload.get("texture_prompt") or None,
            }

        elif action_key == "remesh":
            return {
                "input_task_id": payload.get("input_task_id"),
                "target_face_count": payload.get("target_face_count", 45000),
            }

        elif action_key == "texture":
            return {
                "input_task_id": payload.get("input_task_id"),
                "texture_prompt": payload.get("texture_prompt") or "",
                "resolution": payload.get("resolution") or "2k",
            }

        elif action_key == "rig":
            return {
                "input_task_id": payload.get("input_task_id"),
                "rigging_type": payload.get("rigging_type") or "auto",
            }

        else:
            # Pass through for unknown actions
            return payload

    @staticmethod
    def _dispatch_openai(action_key: str, payload: Dict[str, Any]) -> str:
        """Dispatch job to OpenAI API."""
        api_key = JobService._get_openai_api_key()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY not configured")

        prompt = payload.get("prompt", "")
        if not prompt:
            raise RuntimeError("prompt required for OpenAI image generation")

        # Normalize size
        size = payload.get("size") or payload.get("resolution") or "1024x1024"
        model = payload.get("model") or os.getenv("OPENAI_IMAGE_MODEL") or "gpt-image-1"
        n = int(payload.get("n") or 1)

        openai_payload = {
            "model": model,
            "prompt": prompt,
            "size": size,
            "n": max(1, min(4, n)),
        }

        url = "https://api.openai.com/v1/images/generations"
        headers = JobService._openai_auth_headers()

        print(f"[JOB] Dispatching to OpenAI: {url}")

        resp = requests.post(url, headers=headers, json=openai_payload, timeout=60)

        if not resp.ok:
            raise RuntimeError(f"OpenAI image -> {resp.status_code}: {resp.text[:500]}")

        data = resp.json()

        # OpenAI returns images immediately, so we generate a synthetic job_id
        # The actual result is stored in the job meta
        job_id = f"openai_{uuid.uuid4().hex[:12]}"

        # Store image URLs in DB for later retrieval
        image_urls = []
        for item in data.get("data", []):
            if item.get("url"):
                image_urls.append(item["url"])
            elif item.get("b64_json"):
                image_urls.append(f"data:image/png;base64,{item['b64_json']}")

        # Note: OpenAI returns synchronously, so we mark as succeeded immediately
        # The job status update happens in the calling code

        return job_id

    # ─────────────────────────────────────────────────────────────
    # Job Status Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_job(job_id: str) -> Optional[Dict[str, Any]]:
        """Get a job by ID."""
        job = query_one(
            f"""
            SELECT id, identity_id, provider, action_code, status,
                   cost_credits, reservation_id, upstream_job_id,
                   prompt, meta, error_message, created_at, updated_at
            FROM {Tables.JOBS}
            WHERE id = %s
            """,
            (job_id,),
        )
        if not job:
            return None

        return JobService._format_job(job)

    @staticmethod
    def get_job_by_upstream_id(provider: str, upstream_job_id: str) -> Optional[Dict[str, Any]]:
        """Get a job by provider and upstream job ID."""
        job = query_one(
            f"""
            SELECT id, identity_id, provider, action_code, status,
                   cost_credits, reservation_id, upstream_job_id,
                   prompt, meta, error_message, created_at, updated_at
            FROM {Tables.JOBS}
            WHERE provider = %s AND upstream_job_id = %s
            """,
            (provider, upstream_job_id),
        )
        if not job:
            return None

        return JobService._format_job(job)

    @staticmethod
    def get_jobs_for_identity(
        identity_id: str,
        limit: int = 50,
        offset: int = 0,
        status: Optional[str] = None,
    ) -> list:
        """Get jobs for an identity."""
        if status:
            jobs = query_all(
                f"""
                SELECT id, identity_id, provider, action_code, status,
                       cost_credits, reservation_id, upstream_job_id,
                       prompt, meta, error_message, created_at, updated_at
                FROM {Tables.JOBS}
                WHERE identity_id = %s AND status = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (identity_id, status, limit, offset),
            )
        else:
            jobs = query_all(
                f"""
                SELECT id, identity_id, provider, action_code, status,
                       cost_credits, reservation_id, upstream_job_id,
                       prompt, meta, error_message, created_at, updated_at
                FROM {Tables.JOBS}
                WHERE identity_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (identity_id, limit, offset),
            )

        return [JobService._format_job(job) for job in jobs]

    @staticmethod
    def update_job_status(
        job_id: str,
        status: str,
        error_message: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Update job status."""
        if error_message:
            execute(
                f"""
                UPDATE {Tables.JOBS}
                SET status = %s, error_message = %s
                WHERE id = %s
                """,
                (status, error_message, job_id),
            )
        else:
            execute(
                f"""
                UPDATE {Tables.JOBS}
                SET status = %s
                WHERE id = %s
                """,
                (status, job_id),
            )

        return JobService.get_job(job_id)

    @staticmethod
    def complete_job(job_id: str, success: bool = True, error_message: Optional[str] = None) -> Dict[str, Any]:
        """
        Mark a job as complete (succeeded or failed).
        Handles reservation finalization/release.

        IDEMPOTENT: If job is already completed with the same status, returns existing state.
        If job is completed with a different status, raises ValueError.

        Args:
            job_id: The job to complete
            success: Whether job succeeded
            error_message: Error message if failed

        Returns:
            Dict with job details and idempotency info:
            {
                "job": {...},
                "was_already_completed": bool
            }

        Raises:
            ValueError: If job not found or status conflict
        """
        job = JobService.get_job(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        current_status = job.get("status")
        new_status = JobStatus.SUCCEEDED if success else JobStatus.FAILED

        # Idempotency check: if already completed with same status, return existing
        if current_status in [JobStatus.SUCCEEDED, JobStatus.FAILED]:
            if current_status == new_status:
                print(f"[JOB] Idempotent: job={job_id} already {current_status}")
                return {
                    "job": job,
                    "was_already_completed": True,
                }
            else:
                # Status conflict - job completed with different outcome
                raise ValueError(
                    f"Job {job_id} already completed with status '{current_status}', "
                    f"cannot change to '{new_status}'"
                )

        reservation_id = job.get("reservation_id")

        # Update job status
        JobService.update_job_status(job_id, new_status, error_message)

        # Handle reservation (these are also idempotent)
        if reservation_id:
            try:
                if success:
                    # Finalize reservation (capture credits)
                    ReservationService.finalize_reservation(reservation_id)
                else:
                    # Release reservation (return credits)
                    ReservationService.release_reservation(reservation_id, reason=error_message or "job_failed")
            except ValueError as e:
                # Reservation already finalized/released - that's OK (idempotent)
                print(f"[JOB] Reservation {reservation_id} handling note: {e}")

        print(f"[JOB] Completed job={job_id}, success={success}, reservation={reservation_id}")

        return {
            "job": JobService.get_job(job_id),
            "was_already_completed": False,
        }

    @staticmethod
    def complete_job_by_upstream_id(
        provider: str,
        upstream_job_id: str,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Complete a job by provider and upstream job ID.
        Used for webhook callbacks from Meshy/OpenAI.

        IDEMPOTENT: Safe to call multiple times.

        Args:
            provider: Provider name ('meshy' or 'openai')
            upstream_job_id: The provider's job ID
            success: Whether job succeeded
            error_message: Error message if failed

        Returns:
            Dict with job details if found and completed, None if no matching job.
            {
                "job": {...},
                "was_already_completed": bool
            }
        """
        job = JobService.get_job_by_upstream_id(provider, upstream_job_id)
        if not job:
            print(f"[JOB] No job found for {provider}:{upstream_job_id} - may be legacy job")
            return None

        return JobService.complete_job(
            job_id=job["id"],
            success=success,
            error_message=error_message,
        )

    @staticmethod
    def cancel_job(
        job_id: str,
        reason: str = "user_cancelled",
        force: bool = False,
    ) -> Dict[str, Any]:
        """
        Cancel a job and release its credit reservation.

        Args:
            job_id: The job to cancel
            reason: Reason for cancellation
            force: If True, allow cancelling even pending jobs (admin use)

        Returns:
            Dict with cancelled job details:
            {
                "job": {...},
                "credits_returned": int
            }

        Raises:
            ValueError: If job not found, already completed, or not cancellable
        """
        job = JobService.get_job(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        current_status = job.get("status")

        # Check if job is already completed
        if current_status in [JobStatus.SUCCEEDED, JobStatus.FAILED]:
            raise ValueError(
                f"Cannot cancel job {job_id}: already completed with status '{current_status}'"
            )

        # By default, only queued jobs can be cancelled
        # Pending jobs can only be cancelled with force=True (admin)
        if current_status == JobStatus.PENDING and not force:
            raise ValueError(
                f"Cannot cancel job {job_id}: job is already in progress. "
                "Use force=True for admin cancellation."
            )

        reservation_id = job.get("reservation_id")
        credits_returned = 0

        # Release reservation if exists
        if reservation_id:
            try:
                ReservationService.release_reservation(reservation_id, reason=reason)
                credits_returned = job.get("cost_credits", 0)
            except ValueError as e:
                # Reservation already released - that's OK
                print(f"[JOB] Reservation {reservation_id} release note: {e}")

        # Update job status to failed with cancellation reason
        JobService.update_job_status(
            job_id,
            JobStatus.FAILED,
            error_message=f"Cancelled: {reason}",
        )

        print(f"[JOB] Cancelled job={job_id}, reason={reason}, credits_returned={credits_returned}")

        return {
            "job": JobService.get_job(job_id),
            "credits_returned": credits_returned,
        }

    @staticmethod
    def is_cancellable(job: Dict[str, Any], is_admin: bool = False) -> Tuple[bool, str]:
        """
        Check if a job can be cancelled.

        Args:
            job: The job dict
            is_admin: Whether the requester is an admin

        Returns:
            Tuple of (can_cancel: bool, reason: str)
        """
        status = job.get("status")

        if status == JobStatus.QUEUED:
            return True, "Job is queued and can be cancelled"

        if status == JobStatus.PENDING:
            if is_admin:
                return True, "Admin can force cancel pending jobs"
            return False, "Job is in progress. Only admins can force cancel."

        if status in [JobStatus.SUCCEEDED, JobStatus.FAILED]:
            return False, f"Job already completed with status: {status}"

        return False, f"Unknown job status: {status}"

    # ─────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _format_job(job: Dict[str, Any]) -> Dict[str, Any]:
        """Format job for API response."""
        return {
            "id": str(job["id"]),
            "identity_id": str(job["identity_id"]),
            "provider": job["provider"],
            "action_code": job["action_code"],
            "status": job["status"],
            "cost_credits": job["cost_credits"],
            "reservation_id": str(job["reservation_id"]) if job.get("reservation_id") else None,
            "upstream_job_id": job.get("upstream_job_id"),
            "prompt": job.get("prompt"),
            "meta": job.get("meta"),
            "error_message": job.get("error_message"),
            "created_at": job["created_at"].isoformat() if job.get("created_at") else None,
            "updated_at": job["updated_at"].isoformat() if job.get("updated_at") else None,
        }

# --- Phase 7: Job store helpers (standalone) ---

from backend.services.s3_service import ensure_s3_url_for_data_uri


APP_DIR = Path(__file__).resolve().parents[2]
STORE_PATH = APP_DIR / "job_store.json"
_job_store_cache: Dict[str, Any] = {}
LOCAL_DEV_MODE = not USE_DB


def load_store() -> dict:
    """
    Load job metadata store.
    - In production: returns in-memory cache only (no file I/O)
    - In dev mode: loads from job_store.json
    """
    global _job_store_cache
    if not LOCAL_DEV_MODE:
        return _job_store_cache
    if not STORE_PATH.exists():
        return _job_store_cache
    try:
        _job_store_cache = json.loads(STORE_PATH.read_text(encoding="utf-8") or "{}")
        return _job_store_cache
    except Exception:
        return _job_store_cache


def save_store(data: dict) -> None:
    """
    Save job metadata store.
    - In production: updates in-memory cache only (no file I/O)
    - In dev mode: writes to job_store.json
    """
    global _job_store_cache
    _job_store_cache = data
    if not LOCAL_DEV_MODE:
        return
    try:
        STORE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"[DEV] Failed to save job_store.json: {e}")


if LOCAL_DEV_MODE and not STORE_PATH.exists():
    save_store({})


def _map_action_code(job_type: str) -> str:
    job = (job_type or "").lower()
    mapping = {
        "text-to-3d": "MESHY_TEXT_TO_3D",
        "text_to_3d": "MESHY_TEXT_TO_3D",
        "image-to-3d": "MESHY_IMAGE_TO_3D",
        "image_to_3d": "MESHY_IMAGE_TO_3D",
        "texture": "MESHY_RETEXTURE",
        "retexture": "MESHY_RETEXTURE",
        "remesh": "MESHY_REFINE",
        "rig": "MESHY_RIG",
        "rigging": "MESHY_RIG",
        "image": "OPENAI_IMAGE",
        "openai_image": "OPENAI_IMAGE",
    }
    if job in mapping:
        return mapping[job]
    if "image" in job:
        return "OPENAI_IMAGE"
    return "MESHY_TEXT_TO_3D"


def _map_provider(job_type: str) -> str:
    job = (job_type or "").lower()
    return "openai" if "image" in job else "meshy"


def save_active_job_to_db(
    job_id: str,
    job_type: str,
    stage: str = None,
    metadata: dict = None,
    user_id: str = None,
    allow_anonymous: bool = False,
):
    """
    Persist active job metadata for recovery.

    Args:
        job_id: The upstream provider job ID
        job_type: Type of job (e.g., 'text-to-3d', 'image')
        stage: Optional stage (e.g., 'preview', 'refine')
        metadata: Job metadata dict (may contain identity_id, prompt, etc.)
        user_id: The identity_id of the job owner (required unless allow_anonymous=True)
        allow_anonymous: If True, allow jobs without identity (for legacy compatibility)

    Returns:
        True if saved successfully, False otherwise

    Raises:
        MissingIdentityError: If user_id is not provided and allow_anonymous=False
    """
    if not USE_DB:
        return False

    job_meta = metadata or {}
    if not user_id:
        user_id = job_meta.get("identity_id") or job_meta.get("user_id")

    # ENFORCE identity_id requirement - prevents orphaned jobs
    if not user_id and not allow_anonymous:
        raise MissingIdentityError("save_active_job_to_db", job_id)

    try:
        payload = dict(job_meta)
        payload.setdefault("job_type", job_type)
        payload.setdefault("stage", stage)
        payload.setdefault("original_job_id", job_id)

        try:
            uuid.UUID(str(job_id))
            history_id = str(job_id)
        except (ValueError, TypeError):
            history_id = str(uuid.uuid4())

        item_type = "image" if (job_type or "").lower() in ("image", "openai_image") else "model"
        title = derive_display_title(job_meta.get("prompt"), job_meta.get("title"))
        prompt = job_meta.get("prompt")
        root_prompt = job_meta.get("root_prompt")
        thumbnail_url = job_meta.get("thumbnail_url")
        glb_url = job_meta.get("glb_url")
        image_url = job_meta.get("image_url")

        s3_user_id = user_id or "public"
        provider = _map_provider(job_type)
        thumb_key_base = f"thumbnails/{s3_user_id}/{job_id}"
        image_key_base = f"images/{s3_user_id}/{job_id}"
        if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
            thumbnail_url = ensure_s3_url_for_data_uri(
                thumbnail_url, "thumbnails", thumb_key_base, user_id=user_id, name="thumbnail", provider=provider
            )
        if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
            image_url = ensure_s3_url_for_data_uri(
                image_url, "images", image_key_base, user_id=user_id, name="image", provider=provider
            )
        payload["thumbnail_url"] = thumbnail_url
        payload["image_url"] = image_url

        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                action_code = _map_action_code(job_type)
                provider = _map_provider(job_type)
                cur.execute(
                    f"""
                    SELECT id FROM {Tables.ACTIVE_JOBS}
                    WHERE upstream_job_id = %s
                    LIMIT 1
                    """,
                    (job_id,),
                )
                existing = cur.fetchone()
                progress = int(job_meta.get("pct") or 0)
                if existing:
                    cur.execute(
                        f"""
                        UPDATE {Tables.ACTIVE_JOBS}
                        SET identity_id = COALESCE(%s, identity_id),
                            provider = %s,
                            action_code = %s,
                            status = 'running',
                            progress = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (user_id, provider, action_code, progress, existing["id"]),
                    )
                else:
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.ACTIVE_JOBS} (
                            id, identity_id, provider, action_code, upstream_job_id,
                            status, progress
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            'running', %s
                        )
                        """,
                        (str(uuid.uuid4()), user_id, provider, action_code, job_id, progress),
                    )
            conn.commit()
        return True
    except Exception as e:
        print(f"[DB] Failed to save active job {job_id}: {e}")
        return False


def get_active_jobs_from_db(user_id: str = None):
    """Retrieve active jobs from database, filtered by user_id if provided."""
    if not USE_DB:
        return []
    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if user_id:
                    cur.execute(
                        f"""
                        SELECT aj.upstream_job_id AS job_id,
                               aj.status,
                               aj.progress,
                               aj.action_code,
                               aj.provider,
                               aj.created_at,
                               hi.stage,
                               hi.payload
                        FROM {Tables.ACTIVE_JOBS} aj
                        LEFT JOIN {Tables.HISTORY_ITEMS} hi
                            ON aj.related_history_id = hi.id
                        WHERE aj.status IN ('queued', 'running')
                          AND aj.identity_id = %s
                        ORDER BY aj.created_at DESC
                        """,
                        (user_id,),
                    )
                else:
                    # Anonymous users only see jobs without user_id
                    cur.execute(
                        f"""
                        SELECT aj.upstream_job_id AS job_id,
                               aj.status,
                               aj.progress,
                               aj.action_code,
                               aj.provider,
                               aj.created_at,
                               hi.stage,
                               hi.payload
                        FROM {Tables.ACTIVE_JOBS} aj
                        LEFT JOIN {Tables.HISTORY_ITEMS} hi
                            ON aj.related_history_id = hi.id
                        WHERE aj.status IN ('queued', 'running')
                          AND aj.identity_id IS NULL
                        ORDER BY aj.created_at DESC
                        """
                    )
                rows = cur.fetchall()
        results = []
        for row in rows:
            payload = row["payload"] if row["payload"] else {}
            if isinstance(payload, str):
                payload = json.loads(payload)
            results.append({
                "job_id": row["job_id"],
                "job_type": payload.get("job_type"),
                "stage": row["stage"] or payload.get("stage"),
                "metadata": payload,
                "status": row["status"],
                "progress": row["progress"],
                "created_at": row["created_at"],
            })
        return results
    except Exception as e:
        print(f"[DB] Failed to get active jobs: {e}")
        return []


def mark_job_completed_in_db(job_id: str, user_id: str = None):
    """Mark job as completed in database (only if user owns it or job has no user)."""
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if user_id:
                    cur.execute(
                        f"""
                        UPDATE {Tables.ACTIVE_JOBS}
                        SET status = 'succeeded', updated_at = NOW()
                        WHERE upstream_job_id = %s
                          AND (identity_id = %s OR identity_id IS NULL)
                        """,
                        (job_id, user_id),
                    )
                else:
                    # Anonymous can only complete jobs without user_id
                    cur.execute(
                        f"""
                        UPDATE {Tables.ACTIVE_JOBS}
                        SET status = 'succeeded', updated_at = NOW()
                        WHERE upstream_job_id = %s AND identity_id IS NULL
                        """,
                        (job_id,),
                    )
                # Clean up old completed jobs (keep last 100 per user)
                if user_id:
                    cur.execute(
                        f"""
                        DELETE FROM {Tables.ACTIVE_JOBS}
                        WHERE id IN (
                            SELECT id FROM {Tables.ACTIVE_JOBS}
                            WHERE status = 'succeeded' AND identity_id = %s
                            ORDER BY updated_at DESC
                            OFFSET 100
                        )
                        """,
                        (user_id,),
                    )
                else:
                    cur.execute(
                        f"""
                        DELETE FROM {Tables.ACTIVE_JOBS}
                        WHERE id IN (
                            SELECT id FROM {Tables.ACTIVE_JOBS}
                            WHERE status = 'succeeded' AND identity_id IS NULL
                            ORDER BY updated_at DESC
                            OFFSET 100
                        )
                        """
                    )
            conn.commit()
    except Exception as e:
        print(f"[DB] Failed to mark job completed {job_id}: {e}")


def delete_active_job_from_db(job_id: str, user_id: str = None):
    """Remove job from active jobs table (only if user owns it or job has no user)."""
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if user_id:
                    cur.execute(
                        f"DELETE FROM {Tables.ACTIVE_JOBS} WHERE upstream_job_id = %s AND (identity_id = %s OR identity_id IS NULL)",
                        (job_id, user_id),
                    )
                else:
                    cur.execute(
                        f"DELETE FROM {Tables.ACTIVE_JOBS} WHERE upstream_job_id = %s AND identity_id IS NULL",
                        (job_id,),
                    )
            conn.commit()
    except Exception as e:
        print(f"[DB] Failed to delete active job {job_id}: {e}")


def get_job_metadata(job_id: str, store: dict | None = None) -> dict:
    """
    Look up job metadata from local store first, then fall back to database.
    Returns dict with prompt, title, art_style, root_prompt, etc.
    """
    if not job_id:
        return {}

    if store is None:
        store = load_store()

    meta = store.get(job_id, {})
    if meta and (meta.get("prompt") or meta.get("title")):
        return meta

    if not USE_DB:
        return meta

    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    f"""
                    SELECT identity_id, related_history_id
                    FROM {Tables.ACTIVE_JOBS}
                    WHERE upstream_job_id = %s
                    LIMIT 1
                    """,
                    (job_id,),
                )
                active_job = cur.fetchone()
                active_user_id = None
                if active_job:
                    active_user_id = str(active_job["identity_id"]) if active_job["identity_id"] else None

                row = None
                if active_job and active_job.get("related_history_id"):
                    cur.execute(
                        f"""
                        SELECT id, title, prompt, stage, payload, identity_id
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE id = %s
                        LIMIT 1
                        """,
                        (active_job["related_history_id"],),
                    )
                    row = cur.fetchone()

                if not row:
                    cur.execute(
                        f"""
                        SELECT id, title, prompt, stage, payload, identity_id
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE payload->>'original_job_id' = %s
                           OR payload->>'preview_task_id' = %s
                           OR id::text = %s
                        LIMIT 1
                        """,
                        (job_id, job_id, job_id),
                    )
                    row = cur.fetchone()

            if row:
                payload = row["payload"] if row["payload"] else {}
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}
                return {
                    "prompt": row["prompt"] or payload.get("prompt"),
                    "title": row["title"] or payload.get("title"),
                    "root_prompt": payload.get("root_prompt") or row["prompt"] or payload.get("prompt"),
                    "art_style": payload.get("art_style"),
                    "stage": row["stage"] or payload.get("stage"),
                    "user_id": str(row["identity_id"]) if row["identity_id"] else active_user_id,
                }
            if active_job:
                return {"user_id": active_user_id}
    except Exception as e:
        print(f"[Metadata] ERROR: Failed to get job metadata for {job_id}: {e}")
    return meta


def resolve_meshy_job_id(input_id: str) -> str:
    """
    Resolve a TimrX internal job ID to the upstream Meshy task ID.

    The input could be:
    - A Meshy task ID (already the upstream ID) -> return as-is
    - A TimrX internal job UUID -> look up upstream_job_id in jobs table
    - A history item UUID -> look up original_job_id in payload
    """
    if not input_id:
        return input_id

    # Check local store first - if entry has upstream_job_id, use that
    store = load_store()
    if input_id in store:
        entry = store[input_id]
        # Check if this entry has a pointer to the upstream Meshy job ID
        upstream = entry.get("upstream_job_id") or entry.get("meshy_task_id") or entry.get("original_job_id")
        if upstream and upstream != input_id:
            print(f"[Resolve] Found upstream in store: {input_id} -> {upstream}")
            return upstream
        # If no upstream, input_id might itself be a Meshy task ID
        return input_id

    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # First check timrx_billing.jobs table for upstream_job_id
                    cur.execute(
                        """
                        SELECT upstream_job_id
                        FROM timrx_billing.jobs
                        WHERE id::text = %s AND upstream_job_id IS NOT NULL
                        LIMIT 1
                        """,
                        (input_id,),
                    )
                    job_row = cur.fetchone()
                    if job_row and job_row.get("upstream_job_id"):
                        upstream = job_row["upstream_job_id"]
                        print(f"[Resolve] Found upstream in jobs table: {input_id} -> {upstream}")
                        return upstream

                    # Then check history_items payload for original_job_id
                    cur.execute(
                        f"""
                        SELECT payload->>'original_job_id' as original_job_id,
                               payload->>'job_id' as job_id_field,
                               payload->>'preview_task_id' as preview_task_id
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE id::text = %s
                        LIMIT 1
                        """,
                        (input_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        original_id = row.get("original_job_id") or row.get("job_id_field") or row.get("preview_task_id")
                        if original_id and original_id != input_id:
                            print(f"[Resolve] Found upstream in history_items: {input_id} -> {original_id}")
                            return original_id
        except Exception as e:
            print(f"[Resolve] Error looking up original job ID: {e}")

    # No mapping found - assume input_id is already a Meshy task ID
    return input_id


def verify_job_ownership(job_id: str, identity_id: str) -> bool:
    store = load_store()
    if job_id in store:
        job_user_id = store[job_id].get("user_id")
        if identity_id:
            return job_user_id == identity_id or job_user_id is None
        return job_user_id is None

    if not USE_DB:
        return True

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Check active_jobs, history_items, AND timrx_billing.jobs tables
                cur.execute(
                    f"""
                    SELECT identity_id FROM {Tables.ACTIVE_JOBS} WHERE upstream_job_id = %s
                    UNION
                    SELECT identity_id FROM {Tables.HISTORY_ITEMS} WHERE id::text = %s
                       OR payload->>'original_job_id' = %s
                       OR payload->>'job_id' = %s
                    UNION
                    SELECT identity_id FROM {Tables.JOBS} WHERE id::text = %s
                       OR upstream_job_id = %s
                    LIMIT 1
                    """,
                    (job_id, job_id, job_id, job_id, job_id, job_id),
                )
                row = cur.fetchone()

            if not row:
                return False

            job_user_id = str(row[0]) if row[0] else None
            if identity_id:
                return job_user_id == identity_id or job_user_id is None
            return job_user_id is None
    except Exception as e:
        print(f"[DB] verify_job_ownership failed for {job_id}: {e}")
        return True


def create_internal_job_row(
    internal_job_id: str,
    identity_id: str,
    provider: str,
    action_key: str,
    prompt: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    reservation_id: Optional[str] = None,
    status: str = "queued",
    priority: str = "normal",
) -> bool:
    """
    Create or update a timrx_billing.jobs row for an internal job id.

    This makes status polling resilient across workers (no in-memory dependency).

    Args:
        internal_job_id: The internal job UUID
        identity_id: The identity UUID (REQUIRED - prevents orphaned jobs)
        provider: Provider name ('meshy', 'openai')
        action_key: Action key for pricing lookup
        prompt: Optional prompt text
        meta: Optional metadata dict
        reservation_id: Optional credit reservation UUID
        status: Initial job status (default: 'queued')
        priority: Job priority (default: 'normal')

    Returns:
        True if created/updated successfully, False otherwise

    Raises:
        MissingIdentityError: If identity_id is not provided
    """
    if not USE_DB:
        return False

    if not internal_job_id:
        print(f"[JOB] ERROR: create_internal_job_row called without internal_job_id")
        return False

    # ENFORCE identity_id requirement - prevents orphaned jobs
    if not identity_id:
        raise MissingIdentityError("create_internal_job_row", internal_job_id)

    # Map action_key -> DB action_code (must exist in action_costs)
    action_code = PricingService.get_db_action_code(action_key)
    if not action_code and hasattr(PricingService, "FRONTEND_ALIASES"):
        action_code = PricingService.FRONTEND_ALIASES.get(action_key)
    if not action_code:
        action_code = PricingService.map_job_type_to_action(action_key)
    if not action_code:
        # Fallbacks by provider
        if provider == "openai":
            action_code = "OPENAI_IMAGE"
        else:
            action_code = "MESHY_TEXT_TO_3D"

    cost_credits = PricingService.get_action_cost(action_key)
    meta_json = json.dumps(meta or {})

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {Tables.JOBS}
                        (id, identity_id, provider, action_code, status, cost_credits, reservation_id, prompt, meta, priority)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET provider = EXCLUDED.provider,
                        reservation_id = COALESCE(EXCLUDED.reservation_id, {Tables.JOBS}.reservation_id),
                        prompt = COALESCE(EXCLUDED.prompt, {Tables.JOBS}.prompt),
                        meta = COALESCE({Tables.JOBS}.meta, '{{}}'::jsonb) || COALESCE(EXCLUDED.meta, '{{}}'::jsonb),
                        priority = EXCLUDED.priority,
                        updated_at = NOW()
                    """,
                    (
                        internal_job_id,
                        identity_id,
                        provider,
                        action_code,
                        status,
                        max(0, int(cost_credits or 0)),
                        reservation_id,
                        prompt,
                        meta_json,
                        priority,
                    ),
                )
            conn.commit()
        return True
    except Exception as e:
        print(f"[JOB] ERROR creating internal job row {internal_job_id}: {e}")
        return False


def _update_job_status_ready(internal_job_id, upstream_job_id, model_id, glb_url):
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                meta_updates = {"progress": 100}
                if model_id:
                    meta_updates["model_id"] = model_id
                if glb_url:
                    meta_updates["glb_url"] = glb_url

                if upstream_job_id:
                    cursor.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'ready',
                            upstream_job_id = COALESCE(upstream_job_id, %s),
                            meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (upstream_job_id, json.dumps(meta_updates), internal_job_id),
                    )
                else:
                    cursor.execute(
                        f"""
                        UPDATE {Tables.JOBS}
                        SET status = 'ready',
                            meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (json.dumps(meta_updates), internal_job_id),
                    )
            conn.commit()
    except Exception as e:
        print(f"[JOB] ERROR marking job {internal_job_id} as ready: {e}")


def _update_job_status_failed(internal_job_id, error_message):
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'failed', error_message = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (error_message[:500] if error_message else None, internal_job_id),
                )
            conn.commit()
    except Exception as e:
        print(f"[JOB] ERROR marking job {internal_job_id} as failed: {e}")
