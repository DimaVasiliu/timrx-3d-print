"""
Error sanitization layer for user-facing API responses.

Provider errors (Meshy, Seedance, OpenAI, Gemini, etc.) must NEVER leak
to end users.  This module provides:

  1. user_safe_error()   — build a sanitized JSON-safe error dict
  2. sanitize_provider_error() — log raw provider error + return safe message
  3. WALLET_KEYWORDS       — patterns that indicate provider wallet exhaustion

Usage in route handlers:

    from backend.services.error_sanitizer import sanitize_provider_error

    except Exception as e:
        return jsonify(sanitize_provider_error(
            provider="meshy",
            error=e,
            job_id=internal_job_id,
            code="MODEL_GENERATION_FAILED",
        )), 502
"""

from __future__ import annotations

from typing import Any, Dict, Optional


# ── Standard error codes ──────────────────────────────────────────────────
# These are the ONLY codes that should appear in user-facing responses.

GENERATION_FAILED = "GENERATION_FAILED"
GENERATION_TIMEOUT = "GENERATION_TIMEOUT"
GENERATION_TEMPORARILY_UNAVAILABLE = "GENERATION_TEMPORARILY_UNAVAILABLE"
MODEL_GENERATION_FAILED = "MODEL_GENERATION_FAILED"
IMAGE_GENERATION_FAILED = "IMAGE_GENERATION_FAILED"
VIDEO_GENERATION_FAILED = "VIDEO_GENERATION_FAILED"
INVALID_PROMPT = "INVALID_PROMPT"
INPUT_VALIDATION_FAILED = "INPUT_VALIDATION_FAILED"
SERVER_ERROR = "SERVER_ERROR"
FETCH_FAILED = "FETCH_FAILED"

# ── Default user-safe messages per code ───────────────────────────────────

_DEFAULT_MESSAGES: Dict[str, str] = {
    GENERATION_FAILED: "Generation failed. Please try again.",
    GENERATION_TIMEOUT: "Generation timed out. Please try again.",
    GENERATION_TEMPORARILY_UNAVAILABLE: "Generation service temporarily unavailable. Please try again shortly.",
    MODEL_GENERATION_FAILED: "3D model generation failed. Please try again.",
    IMAGE_GENERATION_FAILED: "Image generation failed. Please try again.",
    VIDEO_GENERATION_FAILED: "Video generation failed. Please try again.",
    INVALID_PROMPT: "Invalid prompt. Please revise and try again.",
    INPUT_VALIDATION_FAILED: "Invalid input. Please check your request and try again.",
    SERVER_ERROR: "Something went wrong. Please try again.",
    FETCH_FAILED: "Failed to fetch the requested resource. Please try again.",
}

# ── Provider wallet exhaustion keywords ───────────────────────────────────
# If any of these appear in a provider error message (case-insensitive),
# the system should alert the admin about wallet depletion.

WALLET_KEYWORDS = (
    "insufficient credits",
    "insufficient balance",
    "wallet empty",
    "balance too low",
    "quota exceeded",
    "rate limit",
    "billing",
    "payment required",
    "account suspended",
    "credit limit",
    "out of credits",
    "no remaining",
)


def user_safe_error(
    code: str,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a sanitized error dict safe for user-facing JSON responses.

    Args:
        code: One of the standard error codes above.
        message: Optional override for the user message.
                 Falls back to _DEFAULT_MESSAGES[code].

    Returns:
        {"ok": False, "error": code, "message": "..."}
    """
    return {
        "ok": False,
        "error": code,
        "message": message or _DEFAULT_MESSAGES.get(code, "Something went wrong. Please try again."),
    }


def _is_wallet_exhaustion(error_str: str) -> bool:
    """Check if the error string looks like a provider wallet/quota issue."""
    lower = error_str.lower()
    return any(kw in lower for kw in WALLET_KEYWORDS)


def sanitize_provider_error(
    *,
    provider: str,
    error: Exception | str,
    job_id: Optional[str] = None,
    code: str = GENERATION_FAILED,
    message: Optional[str] = None,
    alert_on_wallet: bool = True,
) -> Dict[str, Any]:
    """Log the raw provider error server-side and return a sanitized dict.

    This is the main entry point for route handlers catching provider errors.

    Args:
        provider: Provider name (meshy, seedance, openai, etc.)
        error: The raw exception or error string.
        job_id: Optional job ID for log correlation.
        code: Standard error code for the user response.
        message: Optional override user message.
        alert_on_wallet: If True, send admin alert on wallet exhaustion.

    Returns:
        A dict safe for jsonify(): {"ok": False, "error": "...", "message": "..."}
    """
    error_str = str(error)

    # Structured server-side log
    print(
        f"[PROVIDER_ERROR] provider={provider} "
        f"job_id={job_id or 'N/A'} "
        f"error={error_str[:500]}"
    )

    # Check for provider wallet exhaustion
    if alert_on_wallet and _is_wallet_exhaustion(error_str):
        _send_wallet_alert(provider, error_str, job_id)
        # Override code to signal temporary unavailability
        code = GENERATION_TEMPORARILY_UNAVAILABLE
        message = message or "Generation temporarily unavailable. Please try again shortly."

    return user_safe_error(code, message)


def sanitize_internal_error(
    *,
    context: str,
    error: Exception | str,
) -> Dict[str, Any]:
    """Log an internal (non-provider) error and return a safe dict.

    Use for database errors, S3 errors, and other internal failures
    in non-admin routes.
    """
    print(f"[INTERNAL_ERROR] context={context} error={str(error)[:500]}")
    return user_safe_error(SERVER_ERROR)


def sanitize_job_error_message(raw_error: Optional[str]) -> Optional[str]:
    """Sanitize an error_message stored in the jobs table before returning to users.

    The jobs table stores raw provider errors from async_dispatch.
    This strips provider details and returns a generic message.
    """
    if not raw_error:
        return None

    lower = raw_error.lower()

    # Check for wallet/quota — return availability message
    if any(kw in lower for kw in WALLET_KEYWORDS):
        return "Generation temporarily unavailable. Please try again shortly."

    # Expired preview / model — pass through so frontend shows the nice modal
    if "preview task not found" in lower or "task not found" in lower:
        return "Preview task not found — this model's source data has expired. Please generate a new preview."

    # Check for common provider error patterns
    if any(p in lower for p in ("api_error", "status_code", "http ", "->", "non-json")):
        return "Generation failed. Please try again."

    # Check for timeout
    if "timeout" in lower or "timed out" in lower:
        return "Generation timed out. Please try again."

    # If it doesn't look like a provider error, allow it through
    # (e.g. "Meshy refine returned no task ID" → generic)
    if any(p in lower for p in ("meshy", "seedance", "openai", "gemini", "replicate", "fal", "piapi", "vertex")):
        return "Generation failed. Please try again."

    # Fallback: return generic if over 100 chars (likely internal)
    if len(raw_error) > 100:
        return "Generation failed. Please try again."

    return raw_error


def _send_wallet_alert(provider: str, error_str: str, job_id: Optional[str]) -> None:
    """Send admin alert about provider wallet depletion (deduplicated)."""
    try:
        from backend.services.alert_service import send_admin_alert_once
        send_admin_alert_once(
            alert_key=f"provider_wallet_depleted:{provider}",
            alert_type="wallet_depleted",
            subject=f"Provider wallet depleted — {provider}",
            message=(
                f"Provider wallet/quota exhaustion detected.\n\n"
                f"Provider: {provider}\n"
                f"Job ID: {job_id or 'N/A'}\n"
                f"Error: {error_str[:300]}\n\n"
                f"Refill provider wallet immediately."
            ),
            severity="critical",
            provider=provider,
            related_job_id=job_id,
            metadata={
                "error": error_str[:500],
                "alert_type": "wallet_depleted",
            },
            cooldown_minutes=15,
        )
    except Exception as e:
        print(f"[PROVIDER_ERROR] Failed to send wallet alert: {e}")
