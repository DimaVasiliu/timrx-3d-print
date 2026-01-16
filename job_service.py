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
from typing import Optional, Dict, Any, Tuple
from datetime import datetime

from db import transaction, fetch_one, query_one, query_all, execute, Tables
from config import config
from reservation_service import ReservationService, ReservationStatus
from pricing_service import PricingService


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
        admin_bypass: bool = False,
    ) -> Dict[str, Any]:
        """
        Create a job and dispatch to the appropriate provider.

        Args:
            identity_id: The user's identity
            action_key: Frontend action key (e.g., 'text_to_3d_generate')
            payload: Action-specific payload (differs per tool)
            admin_bypass: If True, skip credit checks/reservations (admin testing)

        Returns:
            Dict with job details:
            {
                "job_id": "uuid",
                "reservation_id": "uuid",  # None if admin_bypass=True
                "upstream_job_id": "provider-id",
                "status": "pending",
                "provider": "meshy",
                "action_code": "MESHY_TEXT_TO_3D",
                "cost_credits": 20,
                "admin_bypass": false
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

        # Build job meta (include admin_bypass flag if applicable)
        job_meta = {"payload": payload}
        if admin_bypass:
            job_meta["admin_bypass"] = True
            print(f"[JOB] Admin bypass: skipping credit reservation for {action_key}")

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

        # 2. Reserve credits (skip if admin bypass)
        reservation_id = None
        if not admin_bypass:
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

        bypass_tag = " (admin_bypass)" if admin_bypass else ""
        print(
            f"[JOB] Created job={job_id}, upstream={upstream_job_id}, "
            f"provider={provider}, action={action_code}, credits={cost_credits}{bypass_tag}"
        )

        return {
            "job_id": job_id,
            "reservation_id": reservation_id,
            "upstream_job_id": upstream_job_id,
            "status": JobStatus.PENDING,
            "provider": provider,
            "action_code": action_code,
            "cost_credits": cost_credits,
            "admin_bypass": admin_bypass,
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
