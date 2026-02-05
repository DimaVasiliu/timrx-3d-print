"""
Credits helper functions migrated from app.py.

These are kept API-compatible with the monolith so routes can migrate
incrementally without changing the credit/identity pipeline semantics.
"""

from __future__ import annotations

from typing import Optional, Tuple

from flask import Response, jsonify

from backend.config import ACTION_KEYS as CONFIG_ACTION_KEYS, config
from backend.services.pricing_service import PricingService
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

    Returns:
        (reservation_id, None) on success
        (None, error_response) on failure - return from route
    """
    # DEBUG: Trace credit reservation flow
    print(f"[CREDITS:DEBUG] >>> start_paid_job called: action_key={action_key}, identity_id={identity_id}, job_id={internal_job_id}")
    print(f"[CREDITS:DEBUG] CREDITS_AVAILABLE={CREDITS_AVAILABLE}")

    if not CREDITS_AVAILABLE:
        print(f"[CREDITS:DEBUG] !!! SKIPPING CREDITS - system not available, allowing {action_key} job_id={internal_job_id}")
        return None, None

    if not identity_id:
        print(f"[CREDITS:DEBUG] ERROR: No identity for {action_key} job_id={internal_job_id}")
        return None, _make_credit_error(
            "NO_SESSION",
            "A valid session is required for this action. Please sign in or restore your session.",
            401,
        )

    try:
        cost = PricingService.get_action_cost(action_key)
        print(f"[CREDITS:DEBUG] PricingService.get_action_cost('{action_key}') = {cost}")
        if cost == 0:
            print(f"[CREDITS:DEBUG] !!! SKIPPING CREDITS - No cost for {action_key}, no reservation needed")
            return None, None

        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)
        available = max(0, balance - reserved)

        print(
            f"[CREDITS] Reserve {action_key}: cost={cost}, balance={balance}, "
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

        result = ReservationService.reserve_credits(
            identity_id=identity_id,
            action_key=action_key,
            job_id=internal_job_id,
            meta={
                "action_key": action_key,
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

        print(f"[JOB] started job_id={internal_job_id} reservation_id={reservation_id} action={action_key} cost={cost}")

        # Structured logging for audit trail
        action_code = CONFIG_ACTION_KEYS.get(action_key, action_key).upper() if CONFIG_ACTION_KEYS else action_key.upper()
        log_generation_event(
            event="credits_reserved",
            provider=job_meta.get("provider", "unknown") if job_meta else "unknown",
            action_code=action_code,
            identity_id=identity_id,
            job_id=internal_job_id,
            reservation_id=reservation_id,
            cost=cost,
            extra={"is_existing": is_existing},
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


ACTION_KEYS = CONFIG_ACTION_KEYS or {
    "text-to-3d-preview": "text_to_3d_generate",
    "text-to-3d-refine": "refine",
    "text-to-3d-remesh": "remesh",
    "image-to-3d": "image_to_3d_generate",
    "remesh": "remesh",
    "retexture": "texture",
    "texture": "texture",
    "rig": "rig",
    "rigging": "rig",
    "image_generate": "image_studio_generate",  # Canonical key for image generation
    "image-studio": "image_studio_generate",    # Alias (legacy)
    "openai-image": "image_studio_generate",    # Alias (legacy)
    "video": "video",
    "video-generate": "video",
    "text2video": "video_text_generate",
    "image2video": "video_image_animate",
    "video_text_generate": "video_text_generate",
    "video_image_animate": "video_image_animate",
}
