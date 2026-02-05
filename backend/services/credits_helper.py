"""
Credits helper functions migrated from app.py.

These are kept API-compatible with the monolith so routes can migrate
incrementally without changing the credit/identity pipeline semantics.

CANONICAL ACTION KEYS:
Use these canonical keys when calling start_paid_job():
- image_generate       (10c) - All 2D image providers
- text_to_3d_generate  (20c) - Text to 3D preview
- image_to_3d_generate (30c) - Image to 3D
- refine               (10c) - Refine 3D model
- remesh               (10c) - Remesh 3D model
- retexture            (15c) - Retexture 3D model
- rigging              (25c) - Rig 3D model
- video_generate       (60c) - Video generation
- video_text_generate  (60c) - Text to video
- video_image_animate  (60c) - Image to video

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
    if action_key != canonical_action:
        print(f"[CREDITS] Action normalized: '{action_key}' -> '{canonical_action}' (DB: {db_action_code})")

    print(f"[CREDITS:DEBUG] >>> start_paid_job: requested={action_key}, canonical={canonical_action}, db_code={db_action_code}, job_id={internal_job_id}")

    if not CREDITS_AVAILABLE:
        print(f"[CREDITS:DEBUG] !!! SKIPPING CREDITS - system not available, allowing {canonical_action} job_id={internal_job_id}")
        return None, None

    if not identity_id:
        print(f"[CREDITS:DEBUG] ERROR: No identity for {canonical_action} job_id={internal_job_id}")
        return None, _make_credit_error(
            "NO_SESSION",
            "A valid session is required for this action. Please sign in or restore your session.",
            401,
        )

    try:
        # Use canonical key for cost lookup
        cost = PricingService.get_action_cost(canonical_action)
        print(f"[CREDITS:DEBUG] PricingService.get_action_cost('{canonical_action}') = {cost}")
        if cost == 0:
            print(f"[CREDITS:DEBUG] !!! SKIPPING CREDITS - No cost for {canonical_action}, no reservation needed")
            return None, None

        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)
        available = max(0, balance - reserved)

        print(
            f"[CREDITS] Reserve {canonical_action}: cost={cost}, balance={balance}, "
            f"reserved={reserved}, available={available}, job_id={internal_job_id}"
        )

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
            print(f"[CREDITS] Idempotent: existing reservation {reservation_id} for job_id={internal_job_id}")
        else:
            print(f"[CREDITS] Reserved {cost} credits, reservation_id={reservation_id}, job_id={internal_job_id}")

        print(f"[JOB] started job_id={internal_job_id} reservation_id={reservation_id} action={canonical_action} cost={cost}")

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
        print(f"[CREDITS] Reservation failed: {error_msg}")
        if "INSUFFICIENT_CREDITS" in error_msg:
            return None, _make_credit_error("INSUFFICIENT_CREDITS", error_msg, 402)
        return None, _make_credit_error("CREDIT_ERROR", str(e), 400)

    except Exception as e:
        print(f"[CREDITS] Unexpected error reserving credits: {e}")
        import traceback

        traceback.print_exc()
        return None, None


def finalize_job_credits(reservation_id: str, job_id: str | None = None) -> None:
    """Finalize (capture) credits after successful job completion."""
    print(f"[CREDITS:DEBUG] >>> finalize_job_credits called: reservation_id={reservation_id}, job_id={job_id}")
    print(f"[CREDITS:DEBUG] CREDITS_AVAILABLE={CREDITS_AVAILABLE}, reservation_id is truthy={bool(reservation_id)}")

    if not CREDITS_AVAILABLE or not reservation_id:
        print(f"[CREDITS:DEBUG] !!! SKIPPING FINALIZE - CREDITS_AVAILABLE={CREDITS_AVAILABLE}, reservation_id={reservation_id}")
        return

    try:
        print(f"[CREDITS:DEBUG] Calling ReservationService.finalize_reservation({reservation_id})")
        result = ReservationService.finalize_reservation(reservation_id)
        print(f"[CREDITS:DEBUG] finalize_reservation result: {result}")

        if result.get("not_found"):
            print(f"[CREDITS:DEBUG] !!! Finalize: reservation not found (idempotent): {reservation_id} job_id={job_id}")
        elif result.get("was_already_finalized"):
            print(f"[CREDITS:DEBUG] Already finalized: reservation={reservation_id} job_id={job_id}")
        elif result.get("was_already_released"):
            print(f"[CREDITS:DEBUG] !!! Finalize skipped (already released): reservation={reservation_id} job_id={job_id}")
        else:
            new_balance = result.get("balance", "unknown")
            print(f"[CREDITS:DEBUG] *** SUCCESS: Finalized reservation={reservation_id} job_id={job_id}, new_balance={new_balance}")
            print(f"[JOB] credits_captured job_id={job_id} reservation_id={reservation_id}")

            # Structured logging for audit trail
            log_generation_event(
                event="credits_finalized",
                provider=result.get("provider", "unknown"),
                action_code=result.get("action_code", "UNKNOWN"),
                identity_id=result.get("identity_id"),
                job_id=job_id,
                reservation_id=reservation_id,
                cost=result.get("cost"),
                status="captured",
            )
    except Exception as e:
        print(f"[CREDITS:DEBUG] !!! ERROR in finalize_job_credits: {e}")
        import traceback
        traceback.print_exc()


def release_job_credits(reservation_id: str, reason: str = "job_failed", job_id: str | None = None) -> None:
    """Release (refund) credits after job failure/cancellation."""
    if not CREDITS_AVAILABLE or not reservation_id:
        return

    try:
        result = ReservationService.release_reservation(reservation_id, reason)
        if result.get("not_found"):
            print(f"[CREDITS] Release: reservation not found (idempotent): {reservation_id} job_id={job_id}")
        elif result.get("was_already_released"):
            print(f"[CREDITS] Already released: reservation={reservation_id} job_id={job_id} reason={reason}")
        elif result.get("was_already_finalized"):
            print(f"[CREDITS] Release skipped (already finalized - job succeeded): reservation={reservation_id} job_id={job_id}")
        else:
            print(f"[CREDITS] Released: reservation={reservation_id} job_id={job_id} reason={reason}")

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
        print(f"[CREDITS] Release error {reservation_id}: {e}")


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
    CanonicalActions.RIGGING: CanonicalActions.RIGGING,
    CanonicalActions.VIDEO_GENERATE: CanonicalActions.VIDEO_GENERATE,
    CanonicalActions.VIDEO_TEXT_GENERATE: CanonicalActions.VIDEO_TEXT_GENERATE,
    CanonicalActions.VIDEO_IMAGE_ANIMATE: CanonicalActions.VIDEO_IMAGE_ANIMATE,
    # Legacy aliases (backwards compatibility)
    "text-to-3d-preview": CanonicalActions.TEXT_TO_3D_GENERATE,
    "text-to-3d-refine": CanonicalActions.REFINE,
    "text-to-3d-remesh": CanonicalActions.REMESH,
    "image-to-3d": CanonicalActions.IMAGE_TO_3D_GENERATE,
    "texture": CanonicalActions.RETEXTURE,
    "rig": CanonicalActions.RIGGING,
    "image_studio_generate": CanonicalActions.IMAGE_GENERATE,
    "image-studio": CanonicalActions.IMAGE_GENERATE,
    "openai-image": CanonicalActions.IMAGE_GENERATE,
    "video": CanonicalActions.VIDEO_GENERATE,
    "video-generate": CanonicalActions.VIDEO_GENERATE,
    "text2video": CanonicalActions.VIDEO_TEXT_GENERATE,
    "image2video": CanonicalActions.VIDEO_IMAGE_ANIMATE,
}
