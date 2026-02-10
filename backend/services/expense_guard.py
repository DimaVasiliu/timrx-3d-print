"""
Stability Guardrails for Generation Jobs.

Provides stability features for image/video generation:
1. Per-request hard limits (API constraints - max images, max video seconds)
2. Idempotency key tracking (prevent duplicate charges on retries)
3. Concurrent job limits (prevent queue flooding)

NOTE: This module does NOT enforce cost limits for paying users.
      Cost enforcement is handled ONLY by the credits system (start_paid_job).
      If user has credits -> allowed. No credits -> blocked.

Usage:
    from backend.services.expense_guard import ExpenseGuard

    # Check before dispatching (stability only, not cost)
    error = ExpenseGuard.check_image_request(n=4)
    if error:
        return error

    # Track idempotency
    if ExpenseGuard.is_duplicate_request(idempotency_key):
        return existing_response
"""

from __future__ import annotations

import time
import hashlib
from dataclasses import dataclass
from typing import Optional, Dict, Tuple
from flask import jsonify

# In-memory caches (sufficient for single-instance deployment)
_idempotency_cache: Dict[str, Tuple[float, dict]] = {}  # key -> (timestamp, response)
_active_jobs: Dict[str, float] = {}  # job_id -> start_time

# Cache TTLs
IDEMPOTENCY_TTL = 3600  # 1 hour
ACTIVE_JOB_TTL = 600    # 10 minutes (max job duration)


@dataclass
class ExpenseConfig:
    """
    Stability guardrail configuration.
    Override via environment variables with EXPENSE_ prefix.
    """
    # Per-request hard limits (API constraints, not arbitrary throttles)
    MAX_IMAGES_PER_REQUEST: int = 4      # Max images in single request (API limit)
    MAX_VIDEO_SECONDS: int = 8           # Max video duration (Veo API max is 8)
    MAX_CONCURRENT_JOBS: int = 5         # Max concurrent generation jobs (queue stability)

    # Master switch
    ENABLED: bool = True

    @classmethod
    def from_env(cls) -> "ExpenseConfig":
        """Load config from environment variables."""
        import os

        def get_int(key: str, default: int) -> int:
            return int(os.getenv(f"EXPENSE_{key}", default))

        def get_bool(key: str, default: bool) -> bool:
            val = os.getenv(f"EXPENSE_{key}", str(default)).lower()
            return val in ("true", "1", "yes")

        return cls(
            MAX_IMAGES_PER_REQUEST=get_int("MAX_IMAGES_PER_REQUEST", cls.MAX_IMAGES_PER_REQUEST),
            MAX_VIDEO_SECONDS=get_int("MAX_VIDEO_SECONDS", cls.MAX_VIDEO_SECONDS),
            MAX_CONCURRENT_JOBS=get_int("MAX_CONCURRENT_JOBS", cls.MAX_CONCURRENT_JOBS),
            ENABLED=get_bool("ENABLED", cls.ENABLED),
        )


# Global config instance
config = ExpenseConfig.from_env()


def _cleanup_caches():
    """Clean up expired entries from caches."""
    now = time.time()

    # Clean idempotency cache
    expired_keys = [k for k, (ts, _) in _idempotency_cache.items() if now - ts > IDEMPOTENCY_TTL]
    for k in expired_keys:
        del _idempotency_cache[k]

    # Clean active jobs
    expired_jobs = [k for k, ts in _active_jobs.items() if now - ts > ACTIVE_JOB_TTL]
    for k in expired_jobs:
        del _active_jobs[k]


def _make_error(code: str, message: str, status: int = 400, **extra) -> Tuple:
    """Create a Flask error response."""
    payload = {"ok": False, "code": code, "error": message}
    if extra:
        payload.update(extra)
    return jsonify(payload), status


class ExpenseGuard:
    """
    Pre-flight stability checks for generation operations.
    Call before dispatching image/video generation jobs.

    NOTE: Does NOT enforce cost limits. Credits system handles that.
    """

    @staticmethod
    def check_image_request(n: int = 1) -> Optional[Tuple]:
        """
        Check if an image generation request is allowed (stability only).

        Args:
            n: Number of images requested

        Returns:
            None if allowed, error response tuple if blocked.
        """
        if not config.ENABLED:
            return None

        # Per-request hard limit (API constraint)
        if n > config.MAX_IMAGES_PER_REQUEST:
            return _make_error(
                "IMAGE_LIMIT_EXCEEDED",
                f"Cannot generate {n} images at once. Maximum is {config.MAX_IMAGES_PER_REQUEST} per request.",
                400,
                requested=n,
                maximum=config.MAX_IMAGES_PER_REQUEST,
            )

        # Concurrent job limit (queue stability)
        _cleanup_caches()
        if len(_active_jobs) >= config.MAX_CONCURRENT_JOBS:
            return _make_error(
                "TOO_MANY_JOBS",
                f"Too many jobs in progress ({len(_active_jobs)}). Wait for current jobs to complete.",
                429,
                active_jobs=len(_active_jobs),
                maximum=config.MAX_CONCURRENT_JOBS,
            )

        return None

    @staticmethod
    def check_video_request(duration_seconds: int = 6) -> Optional[Tuple]:
        """
        Check if a video generation request is allowed (stability only).

        Args:
            duration_seconds: Video duration in seconds

        Returns:
            None if allowed, error response tuple if blocked.
        """
        if not config.ENABLED:
            return None

        # Per-request hard limit (API constraint)
        if duration_seconds > config.MAX_VIDEO_SECONDS:
            return _make_error(
                "VIDEO_DURATION_EXCEEDED",
                f"Video duration {duration_seconds}s exceeds maximum of {config.MAX_VIDEO_SECONDS}s.",
                400,
                requested=duration_seconds,
                maximum=config.MAX_VIDEO_SECONDS,
            )

        # Concurrent job limit (queue stability)
        _cleanup_caches()
        if len(_active_jobs) >= config.MAX_CONCURRENT_JOBS:
            return _make_error(
                "TOO_MANY_JOBS",
                f"Too many jobs in progress ({len(_active_jobs)}). Wait for current jobs to complete.",
                429,
                active_jobs=len(_active_jobs),
                maximum=config.MAX_CONCURRENT_JOBS,
            )

        return None

    @staticmethod
    def compute_idempotency_key(
        identity_id: str,
        action: str,
        prompt: str,
        **params
    ) -> str:
        """
        Compute an idempotency key for a request.

        Same key = same request, should return cached response instead of
        creating a new job.
        """
        # Build a stable string from parameters
        parts = [
            identity_id or "",
            action,
            prompt[:200] if prompt else "",  # Truncate long prompts
        ]

        # Add sorted params for stability
        for k in sorted(params.keys()):
            v = params[k]
            if v is not None:
                parts.append(f"{k}={v}")

        key_string = "|".join(parts)
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]

    @staticmethod
    def is_duplicate_request(idempotency_key: str) -> Optional[dict]:
        """
        Check if this request is a duplicate.

        Returns:
            Cached response if duplicate, None if new request.
        """
        if not idempotency_key:
            return None

        _cleanup_caches()

        entry = _idempotency_cache.get(idempotency_key)
        if entry:
            timestamp, response = entry
            # Return cached response if within TTL
            if time.time() - timestamp < IDEMPOTENCY_TTL:
                print(f"[EXPENSE_GUARD] Idempotent hit: returning cached response for key={idempotency_key[:8]}...")
                return response

        return None

    @staticmethod
    def cache_response(idempotency_key: str, response: dict):
        """Cache a response for idempotency."""
        if not idempotency_key:
            return

        _idempotency_cache[idempotency_key] = (time.time(), response)

    @staticmethod
    def register_active_job(job_id: str):
        """Register a job as active (for concurrent limit tracking)."""
        _cleanup_caches()
        _active_jobs[job_id] = time.time()

    @staticmethod
    def unregister_active_job(job_id: str):
        """Mark a job as completed."""
        _active_jobs.pop(job_id, None)

    @staticmethod
    def get_active_job_count() -> int:
        """Get count of currently active jobs."""
        _cleanup_caches()
        return len(_active_jobs)

    @staticmethod
    def get_status() -> dict:
        """Get current guardrail status for debugging/display."""
        _cleanup_caches()
        return {
            "enabled": config.ENABLED,
            "active_jobs": len(_active_jobs),
            "max_concurrent_jobs": config.MAX_CONCURRENT_JOBS,
            "idempotency_cache_size": len(_idempotency_cache),
            "limits": {
                "max_images_per_request": config.MAX_IMAGES_PER_REQUEST,
                "max_video_seconds": config.MAX_VIDEO_SECONDS,
            },
        }
