"""
Shared Video Error Taxonomy and Provider Status Mapping.

Single source of truth for:
- PiAPI/Seedance status mapping to internal statuses
- Normalized error categories for video/provider failures
- Quota/billing error detection
- Terminal job states

Used by: seedance_service, webhooks, job_worker, job_rescue, video_router,
         seedance_provider, vertex_provider.
"""

from __future__ import annotations


# ── PiAPI Status Mapping ─────────────────────────────────────
# Maps raw PiAPI status strings to internal status values.
# Used by seedance_service.check_seedance_status() and webhooks.

PIAPI_STATUS_MAP = {
    "Completed": "done",
    "completed": "done",
    "Processing": "processing",
    "processing": "processing",
    "Pending": "pending",
    "pending": "pending",
    "Staged": "pending",
    "staged": "pending",
    "Failed": "failed",
    "failed": "failed",
}

# Zero-value timestamp from PiAPI means "not started yet"
PIAPI_ZERO_TIMESTAMPS = frozenset({"0001-01-01T00:00:00Z", "", None})


# ── Terminal Job States ──────────────────────────────────────
# Worker must never touch jobs in these states.

TERMINAL_STATES = frozenset({
    "succeeded", "failed", "refunded", "ready", "ready_unbilled",
    "abandoned_legacy", "recovery_blocked",
})

# States that must never be overwritten by a webhook or sweep.
TERMINAL_AND_FINALIZING = frozenset(TERMINAL_STATES | {"finalizing"})


# ── Active Job States ────────────────────────────────────────
# Jobs in these states are considered "in-flight" for concurrency/rate limits.

ACTIVE_VIDEO_STATUSES = frozenset({
    "queued", "dispatched", "processing",
    "provider_pending", "provider_processing", "finalizing",
})


# ── Normalized Error Categories ──────────────────────────────
# Each video/provider error should map to one of these categories.
# Provider-specific detail is preserved in error_message / meta,
# but the error_code uses these normalized values.

class ErrorCategory:
    """Normalized error categories for video pipeline failures."""
    AUTH = "auth"                             # Provider auth/key failure
    QUOTA = "quota"                           # Provider quota/billing exhausted
    VALIDATION = "validation"                 # Bad request params (caller's fault)
    PENDING_TIMEOUT = "pending_timeout"       # Provider queue timed out
    PROCESSING_TIMEOUT = "processing_timeout" # Provider started but didn't finish
    NETWORK = "network"                       # Network error reaching provider
    MALFORMED_RESPONSE = "malformed_response" # Provider returned unparseable data
    NO_OUTPUT = "no_output"                   # Provider completed but no result URL
    WEBHOOK_INVALID = "webhook_invalid"       # Webhook payload validation failed
    DUPLICATE_EVENT = "duplicate_event"       # Duplicate webhook/completion event
    FINALIZATION_FAILED = "finalization_failed"  # S3 upload / credit capture failed
    DISPATCH_FAILED = "dispatch_failed"       # Could not dispatch to provider
    MAX_RETRIES = "max_retries"              # Exhausted retry attempts
    INTERNAL = "internal"                    # Unexpected internal error
    UNKNOWN = "unknown"                      # Unclassified


# Map provider-specific error strings/codes to normalized categories.
# Keys are substrings matched case-insensitively against error messages.
_PROVIDER_ERROR_MAP = {
    # Auth
    "auth": ErrorCategory.AUTH,
    "401": ErrorCategory.AUTH,
    "403": ErrorCategory.AUTH,
    "seedance_auth_error": ErrorCategory.AUTH,
    # Quota
    "quota": ErrorCategory.QUOTA,
    "billing": ErrorCategory.QUOTA,
    "resource_exhausted": ErrorCategory.QUOTA,
    "rate_limit": ErrorCategory.QUOTA,
    "429": ErrorCategory.QUOTA,
    "too_many": ErrorCategory.QUOTA,
    # Network
    "network": ErrorCategory.NETWORK,
    "timeout": ErrorCategory.NETWORK,
    "connection": ErrorCategory.NETWORK,
    # No output
    "no_video_url": ErrorCategory.NO_OUTPUT,
    "no_result_url": ErrorCategory.NO_OUTPUT,
    "seedance_no_video_url": ErrorCategory.NO_OUTPUT,
    # Generation failures
    "generation_failed": ErrorCategory.INTERNAL,
    "seedance_generation_failed": ErrorCategory.INTERNAL,
}


def classify_error(error_code: str = "", error_message: str = "") -> str:
    """
    Classify a provider error into a normalized ErrorCategory.

    Checks error_code first (exact match), then scans error_message
    for known substrings. Returns ErrorCategory.UNKNOWN if unclassified.
    """
    # Direct code match
    if error_code in _PROVIDER_ERROR_MAP:
        return _PROVIDER_ERROR_MAP[error_code]

    # Substring scan on combined text
    combined = f"{error_code} {error_message}".lower()
    for pattern, category in _PROVIDER_ERROR_MAP.items():
        if pattern in combined:
            return category

    return ErrorCategory.UNKNOWN


def is_quota_error(error_msg: str) -> bool:
    """
    Detect quota/billing/rate-limit errors from error messages.

    Replaces the duplicated _is_quota_error() helpers in video_router,
    seedance_provider, and vertex_provider.
    """
    lower = error_msg.lower()
    return any(tok in lower for tok in (
        "quota", "billing", "resource_exhausted", "rate_limit", "429", "too_many",
    ))


# ── Error codes that warrant credit release ──────────────────
# Confirmed terminal failures where the provider will NOT complete the job.
# Timeouts and poll errors do NOT release credits -- the provider may
# still complete, and the rescue service can recover later.

TERMINAL_ERROR_CODES = frozenset({
    # Normalized categories
    ErrorCategory.AUTH,
    ErrorCategory.NO_OUTPUT,
    ErrorCategory.DISPATCH_FAILED,
    ErrorCategory.MAX_RETRIES,
    ErrorCategory.VALIDATION,
    # Legacy codes (backward compat with existing DB rows)
    "generation_failed",
    "no_result_url",
    "auth_error",
    "seedance_generation_failed",
    "seedance_no_video_url",
    "seedance_auth_error",
    "max_attempts_exceeded",
    "dispatch_failed",
    "no_upstream_id",
    "missing_fields",
    "unsupported_recovery_provider",
})


# ── Human-readable failure messages ──────────────────────────
# Maps error codes to user-friendly messages for logs and API responses.

FAILURE_MESSAGES = {
    # Normalized
    ErrorCategory.PENDING_TIMEOUT: "Provider queue timed out -- job was not started in time",
    ErrorCategory.PROCESSING_TIMEOUT: "Render timed out -- provider started but did not finish in time",
    ErrorCategory.NETWORK: "Lost connection to provider during generation",
    ErrorCategory.AUTH: "Provider authentication failed",
    ErrorCategory.NO_OUTPUT: "Generation completed but no result was returned",
    ErrorCategory.FINALIZATION_FAILED: "Video completed but post-processing failed",
    ErrorCategory.DISPATCH_FAILED: "Could not dispatch to provider",
    ErrorCategory.MAX_RETRIES: "Exhausted all retry attempts",
    ErrorCategory.INTERNAL: "Provider rejected this generation",
    # Legacy (backward compat)
    "pending_timeout": "Provider queue timed out -- job was not started in time",
    "processing_timeout": "Render timed out -- provider started but did not finish in time",
    "poll_error": "Lost connection to provider during generation",
    "generation_failed": "Provider rejected this generation",
    "no_result_url": "Generation completed but no result was returned",
    "auth_error": "Provider authentication failed",
    "seedance_pending_timeout": "Provider queue timed out -- Seedance did not start this job in time",
    "seedance_processing_timeout": "Render timed out -- Seedance started but did not finish in time",
    "seedance_poll_error": "Lost connection to provider during generation",
    "seedance_generation_failed": "Seedance rejected this generation",
    "seedance_no_video_url": "Generation completed but no video was returned",
    "seedance_auth_error": "Provider authentication failed",
}


def get_failure_message(error_code: str) -> str:
    """Get a human-readable failure message for an error code."""
    return FAILURE_MESSAGES.get(error_code, f"Video generation failed ({error_code})")
