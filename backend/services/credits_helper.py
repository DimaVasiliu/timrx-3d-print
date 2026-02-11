"""
Credits helper functions migrated from app.py.

These are kept API-compatible with the monolith so routes can migrate
incrementally without changing the credit/identity pipeline semantics.

CANONICAL ACTION KEYS:
Use these canonical keys when calling start_paid_job():
- image_generate       (5c)  - Standard AI image
- image_generate_2k    (7c)  - 2K AI image
- image_generate_4k    (10c) - 4K AI image
- text_to_3d_generate  (20c) - Text to 3D preview
- image_to_3d_generate (30c) - Image to 3D
- refine               (10c) - Refine 3D model
- remesh               (10c) - Remesh 3D model
- retexture            (15c) - Retexture 3D model
- video_generate       (70c) - Video generation
- video_text_generate  (70c) - Text to video
- video_image_animate  (70c) - Image to video

Legacy aliases are supported for backwards compatibility but will be
normalized to canonical keys internally.
"""

from __future__ import annotations

from typing import Optional, Tuple

from flask import Response, jsonify

from backend.config import ACTION_KEYS as CONFIG_ACTION_KEYS, config
from backend.services.pricing_service import (
    PricingService,
    CanonicalActions,
    normalize_action_key,
    CANONICAL_TO_DB,
)
from backend.services.reservation_service import ReservationService
from backend.services.wallet_service import WalletService
from backend.utils import log_generation_event


CREDITS_AVAILABLE = bool(getattr(config, "HAS_DATABASE", False) or getattr(config, "DATABASE_URL", ""))


def _mark_job_ready_unbilled(job_id: str | None, identity_id: str | None) -> None:
    """
    Mark a job as ready_unbilled when it completed successfully but had no reservation.

    This is a CRITICAL bug indicator - it means credits were never reserved but the job
    was dispatched anyway. These jobs need admin review for manual billing.

    The job is marked in the jobs table with status='ready_unbilled' and an error_message
    explaining the issue.
    """
    if not job_id:
        print(f"[CREDITS:ERROR] Cannot mark ready_unbilled - no job_id provided")
        return

    try:
        from backend.db import USE_DB, get_conn, Tables

        if not USE_DB:
            print(f"[CREDITS:ERROR] Cannot mark ready_unbilled - DB not available")
            return

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET status = 'ready_unbilled',
                        error_message = 'BILLING_BUG: Job completed successfully but no credit reservation was found. Manual reconciliation required.',
                        meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                        updated_at = NOW()
                    WHERE id::text = %s
                    RETURNING id
                    """,
                    (
                        __import__('json').dumps({
                            "ready_unbilled_detected_at": __import__('time').time(),
                            "identity_id_at_detection": identity_id,
                        }),
                        job_id,
                    ),
                )
                result = cur.fetchone()
            conn.commit()

        if result:
            print(f"[CREDITS:ERROR] Marked job {job_id} as ready_unbilled for admin reconciliation")
        else:
            print(f"[CREDITS:ERROR] Could not mark job {job_id} as ready_unbilled - job not found in DB")

    except Exception as e:
        print(f"[CREDITS:ERROR] Failed to mark job {job_id} as ready_unbilled: {e}")


def _make_credit_error(code: str, message: str, status: int = 400, **extra) -> Tuple:
    payload = {"ok": False, "code": code, "error": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status


def start_paid_job(identity_id, action_key, internal_job_id, job_meta) -> tuple[str | None, Response | None]:
    """
    Reserve credits for a paid job. Call this BEFORE calling upstream API.

    Args:
        identity_id: User identity ID
        action_key: Action key (canonical or alias - will be normalized)
        internal_job_id: Unique job ID
        job_meta: Additional metadata for the job

    Returns:
        (reservation_id, None) on success
        (None, error_response) on failure - return from route

    Canonical action keys:
        image_generate, text_to_3d_generate, image_to_3d_generate,
        refine, remesh, retexture, rigging,
        video_generate, video_text_generate, video_image_animate
    """
    # Normalize action key to canonical form
    canonical_action = normalize_action_key(action_key)
    db_action_code = CANONICAL_TO_DB.get(canonical_action, canonical_action.upper())

    # Log normalization if different
    # if action_key != canonical_action:
    #     print(f"[CREDITS] Action normalized: '{action_key}' -> '{canonical_action}' (DB: {db_action_code})")

    # print(f"[CREDITS:DEBUG] >>> start_paid_job: requested={action_key}, canonical={canonical_action}, db_code={db_action_code}, job_id={internal_job_id}")

    if not CREDITS_AVAILABLE:
        # print(f"[CREDITS:DEBUG] !!! SKIPPING CREDITS - system not available, allowing {canonical_action} job_id={internal_job_id}")
        return None, None

    if not identity_id:
        # print(f"[CREDITS:DEBUG] ERROR: No identity for {canonical_action} job_id={internal_job_id}")
        return None, _make_credit_error(
            "NO_SESSION",
            "A valid session is required for this action. Please sign in or restore your session.",
            401,
        )

    try:
        # Use canonical key for cost lookup
        cost = PricingService.get_action_cost(canonical_action)
        # print(f"[CREDITS:DEBUG] PricingService.get_action_cost('{canonical_action}') = {cost}")
        if cost == 0:
            # print(f"[CREDITS:DEBUG] !!! SKIPPING CREDITS - No cost for {canonical_action}, no reservation needed")
            return None, None

        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)
        available = max(0, balance - reserved)

        # print(
        #     f"[CREDITS] Reserve {canonical_action}: cost={cost}, balance={balance}, "
        #     f"reserved={reserved}, available={available}, job_id={internal_job_id}"
        # )

        if available < cost:
            return None, _make_credit_error(
                "INSUFFICIENT_CREDITS",
                f"You need {cost} credits but only have {available} available.",
                402,
                required=cost,
                available=available,
                balance=balance,
                reserved=reserved,
            )

        # Use canonical key for reservation
        result = ReservationService.reserve_credits(
            identity_id=identity_id,
            action_key=canonical_action,
            job_id=internal_job_id,
            meta={
                "action_key": canonical_action,
                "requested_action_key": action_key,  # Original key for debugging
                "source": "paid_job_pipeline",
                **(job_meta or {}),
            },
        )

        reservation_id = result["reservation"]["id"]
        is_existing = result.get("is_existing", False)

        if is_existing:
            print(f"[CREDITS] RESERVE: existing reservation (idempotent): reservation_id={reservation_id} job_id={internal_job_id} action={canonical_action} cost={cost}")
        else:
            print(f"[CREDITS] *** RESERVE SUCCESS: reservation_id={reservation_id} job_id={internal_job_id} action={canonical_action} cost={cost}")

        # Structured logging for audit trail
        log_generation_event(
            event="credits_reserved",
            provider=job_meta.get("provider", "unknown") if job_meta else "unknown",
            action_code=db_action_code,
            identity_id=identity_id,
            job_id=internal_job_id,
            reservation_id=reservation_id,
            cost=cost,
            extra={
                "is_existing": is_existing,
                "requested_action": action_key,
                "canonical_action": canonical_action,
            },
        )

        return reservation_id, None

    except ValueError as e:
        error_msg = str(e)
        print(f"[CREDITS] Reservation failed (ValueError): {error_msg}")
        if "INSUFFICIENT_CREDITS" in error_msg:
            return None, _make_credit_error("INSUFFICIENT_CREDITS", error_msg, 402)
        return None, _make_credit_error("CREDIT_ERROR", str(e), 400)

    except Exception as e:
        # CRITICAL: Do NOT return (None, None) - this allows free generations!
        # Always return an error response so the route doesn't dispatch the provider call.
        print(f"[CREDITS] CRITICAL: Unexpected error reserving credits: {e}")
        import traceback
        traceback.print_exc()

        # Return a 500 error so the route knows to abort
        return None, _make_credit_error(
            "CREDIT_RESERVATION_FAILED",
            f"Credit reservation system error. Please try again. Error: {type(e).__name__}",
            500,
            job_id=internal_job_id,
        )


def finalize_job_credits(
    reservation_id: Optional[str],
    job_id: str | None = None,
    identity_id: str | None = None,
) -> dict:
    """
    Finalize (capture) credits after successful job completion.

    Args:
        reservation_id: The credit reservation ID to finalize
        job_id: The job ID (for logging)
        identity_id: The user identity ID (for logging unbilled cases)

    Returns:
        dict with:
            - success: True if credits were finalized
            - new_balance: The user's new credit balance after finalization (or None)
            - cost: The cost that was charged (or None)

    IMPORTANT: If reservation_id is missing for a successful job, this indicates a bug
    in the credit flow. The job will be marked as ready_unbilled for admin reconciliation.
    """
    print(f"[CREDITS] >>> FINALIZE called: reservation_id={reservation_id}, job_id={job_id}")

    # If credits system unavailable, silently skip (dev mode / no DB)
    if not CREDITS_AVAILABLE:
        print(f"[CREDITS] FINALIZE SKIPPED - CREDITS_AVAILABLE=False")
        return {"success": False, "new_balance": None, "cost": None}

    # CRITICAL: Missing reservation_id for a successful job is a BUG
    # This means credits were never reserved but the job completed anyway
    if not reservation_id:
        print(f"[CREDITS:ERROR] !!! READY_UNBILLED BUG DETECTED - job_id={job_id}, identity_id={identity_id}")
        print(f"[CREDITS:ERROR] Job completed successfully but NO reservation_id to finalize!")
        print(f"[CREDITS:ERROR] This indicates a credit reservation failure that was silently bypassed.")

        # Mark job as ready_unbilled for admin reconciliation
        _mark_job_ready_unbilled(job_id, identity_id)

        # Log for audit trail
        log_generation_event(
            event="credits_ready_unbilled",
            provider="unknown",
            action_code="UNKNOWN",
            identity_id=identity_id,
            job_id=job_id,
            reservation_id=None,
            cost=None,
            status="ready_unbilled",
            error="missing_reservation_id",
        )
        return {"success": False, "new_balance": None, "cost": None}

    try:
        result = ReservationService.finalize_reservation(reservation_id)

        if result.get("not_found"):
            print(f"[CREDITS] FINALIZE: reservation not found (expired or never existed): reservation_id={reservation_id} job_id={job_id}")
            return {"success": False, "new_balance": None, "cost": None}
        elif result.get("was_already_finalized"):
            print(f"[CREDITS] FINALIZE: already finalized (idempotent): reservation_id={reservation_id} job_id={job_id}")
            # Already finalized = success, but we don't have the balance from this call
            # Fetch current balance for the caller
            current_balance = None
            if identity_id:
                try:
                    current_balance = WalletService.get_balance(identity_id)
                except Exception:
                    pass
            return {"success": True, "new_balance": current_balance, "cost": result.get("cost")}
        elif result.get("was_already_released"):
            print(f"[CREDITS] FINALIZE FAILED: already released (job was cancelled/failed): reservation_id={reservation_id} job_id={job_id}")
            return {"success": False, "new_balance": None, "cost": None}
        else:
            new_balance = result.get("balance")
            cost = result.get("cost")
            print(f"[CREDITS] *** FINALIZE SUCCESS: reservation_id={reservation_id} job_id={job_id} cost={cost} new_balance={new_balance}")

            # Structured logging for audit trail
            log_generation_event(
                event="credits_finalized",
                provider=result.get("provider", "unknown"),
                action_code=result.get("action_code", "UNKNOWN"),
                identity_id=result.get("identity_id"),
                job_id=job_id,
                reservation_id=reservation_id,
                cost=cost,
                status="captured",
            )
            return {"success": True, "new_balance": new_balance, "cost": cost}
    except Exception as e:
        print(f"[CREDITS] !!! FINALIZE ERROR: reservation_id={reservation_id} job_id={job_id} error={e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "new_balance": None, "cost": None}


def release_job_credits(reservation_id: str, reason: str = "job_failed", job_id: str | None = None) -> None:
    """Release (refund) credits after job failure/cancellation."""
    print(f"[CREDITS] >>> RELEASE called: reservation_id={reservation_id} job_id={job_id} reason={reason}")

    if not CREDITS_AVAILABLE or not reservation_id:
        print(f"[CREDITS] RELEASE SKIPPED: CREDITS_AVAILABLE={CREDITS_AVAILABLE} reservation_id={reservation_id}")
        return

    try:
        result = ReservationService.release_reservation(reservation_id, reason)
        if result.get("not_found"):
            print(f"[CREDITS] RELEASE: reservation not found (expired): reservation_id={reservation_id} job_id={job_id}")
        elif result.get("was_already_released"):
            print(f"[CREDITS] RELEASE: already released (idempotent): reservation_id={reservation_id} job_id={job_id}")
        elif result.get("was_already_finalized"):
            # IMPORTANT: This is the idempotent case - job succeeded, credits already captured
            print(f"[CREDITS] RELEASE BLOCKED: already finalized (job succeeded, credits captured): reservation_id={reservation_id} job_id={job_id}")
        else:
            cost = result.get("cost", "unknown")
            print(f"[CREDITS] *** RELEASE SUCCESS: reservation_id={reservation_id} job_id={job_id} reason={reason} cost={cost} (credits returned to wallet)")

            # Structured logging for audit trail
            log_generation_event(
                event="credits_released",
                provider=result.get("provider", "unknown"),
                action_code=result.get("action_code", "UNKNOWN"),
                identity_id=result.get("identity_id"),
                job_id=job_id,
                reservation_id=reservation_id,
                cost=result.get("cost"),
                status="released",
                error=reason,
            )
    except Exception as e:
        print(f"[CREDITS] !!! RELEASE ERROR: reservation_id={reservation_id} job_id={job_id} error={e}")


def get_current_balance(identity_id: str) -> Optional[dict]:
    """Get current credit balance info for a user."""
    if not CREDITS_AVAILABLE or not identity_id:
        return None
    try:
        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)
        available = max(0, balance - reserved)
        return {"balance": balance, "reserved": reserved, "available": available}
    except Exception as e:
        print(f"[CREDITS] Balance check error: {e}")
        return None


# DEPRECATED: Use normalize_action_key() from pricing_service instead
# This mapping is kept for backwards compatibility with routes that import it directly
ACTION_KEYS = CONFIG_ACTION_KEYS or {
    # Canonical keys (use these in new code)
    CanonicalActions.IMAGE_GENERATE: CanonicalActions.IMAGE_GENERATE,
    CanonicalActions.TEXT_TO_3D_GENERATE: CanonicalActions.TEXT_TO_3D_GENERATE,
    CanonicalActions.IMAGE_TO_3D_GENERATE: CanonicalActions.IMAGE_TO_3D_GENERATE,
    CanonicalActions.REFINE: CanonicalActions.REFINE,
    CanonicalActions.REMESH: CanonicalActions.REMESH,
    CanonicalActions.RETEXTURE: CanonicalActions.RETEXTURE,
    CanonicalActions.VIDEO_GENERATE: CanonicalActions.VIDEO_GENERATE,
    CanonicalActions.VIDEO_TEXT_GENERATE: CanonicalActions.VIDEO_TEXT_GENERATE,
    CanonicalActions.VIDEO_IMAGE_ANIMATE: CanonicalActions.VIDEO_IMAGE_ANIMATE,
    # Legacy aliases (backwards compatibility)
    "text-to-3d-preview": CanonicalActions.TEXT_TO_3D_GENERATE,
    "text-to-3d-refine": CanonicalActions.REFINE,
    "text-to-3d-remesh": CanonicalActions.REMESH,
    "image-to-3d": CanonicalActions.IMAGE_TO_3D_GENERATE,
    "texture": CanonicalActions.RETEXTURE,
    "image_studio_generate": CanonicalActions.IMAGE_GENERATE,
    "image-studio": CanonicalActions.IMAGE_GENERATE,
    "openai-image": CanonicalActions.IMAGE_GENERATE,
    "video": CanonicalActions.VIDEO_GENERATE,
    "video-generate": CanonicalActions.VIDEO_GENERATE,
    "text2video": CanonicalActions.VIDEO_TEXT_GENERATE,
    "image2video": CanonicalActions.VIDEO_IMAGE_ANIMATE,
}
