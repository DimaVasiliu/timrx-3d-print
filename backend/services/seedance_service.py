"""
Seedance Video Generation Service (via PiAPI).

Wraps the PiAPI unified API for Seedance 2.0 video generation.
Endpoints:
  - POST https://api.piapi.ai/api/v1/task  (create task)
  - GET  https://api.piapi.ai/api/v1/task/{task_id}  (poll status)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import requests

from backend.config import config


# ── Constants ────────────────────────────────────────────────
PIAPI_BASE = "https://api.piapi.ai/api/v1"
PIAPI_TIMEOUT = 30  # seconds


# ── Errors ───────────────────────────────────────────────────
class SeedanceConfigError(Exception):
    """Raised when PIAPI_API_KEY is not configured."""
    pass


class SeedanceAuthError(Exception):
    """Raised on 401/403 from PiAPI."""
    pass


class SeedanceQuotaError(Exception):
    """Raised on 429 / quota exhaustion from PiAPI."""
    pass


# ── Configuration check ─────────────────────────────────────
def check_seedance_configured() -> Tuple[bool, Optional[str]]:
    """Check if Seedance (PiAPI) is configured."""
    api_key = getattr(config, "PIAPI_API_KEY", "")
    if not api_key:
        return False, "PIAPI_API_KEY not set"
    return True, None


def _get_headers() -> Dict[str, str]:
    """Build PiAPI request headers."""
    api_key = getattr(config, "PIAPI_API_KEY", "")
    if not api_key:
        raise SeedanceConfigError("PIAPI_API_KEY not set")
    return {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
    }


# ── Create task ──────────────────────────────────────────────
def create_seedance_task(
    prompt: str,
    duration: int = 5,
    aspect_ratio: str = "16:9",
    image_urls: Optional[List[str]] = None,
    task_type: str = "seedance-2-preview",
) -> Dict[str, Any]:
    """
    Create a Seedance video generation task via PiAPI.

    Args:
        prompt: Text prompt for video generation.
        duration: Video duration in seconds (5, 10, or 15).
        aspect_ratio: Output aspect ratio (16:9, 9:16, 4:3, 3:4).
        image_urls: Optional list of image URLs for image-to-video.
        task_type: PiAPI task type (seedance-2-preview or seedance-2-fast-preview).

    Returns:
        {"task_id": "...", "status": "processing"}

    Raises:
        SeedanceConfigError: API key not set.
        SeedanceAuthError: Authentication failed.
        SeedanceQuotaError: Quota exhausted or rate limited.
        RuntimeError: Other API errors.
    """
    headers = _get_headers()

    body = {
        "model": "seedance",
        "task_type": task_type,
        "input": {
            "prompt": prompt,
            "duration": duration,
            "aspect_ratio": aspect_ratio,
        },
    }

    if image_urls:
        body["input"]["image_urls"] = image_urls

    try:
        resp = requests.post(
            f"{PIAPI_BASE}/task",
            json=body,
            headers=headers,
            timeout=PIAPI_TIMEOUT,
        )
    except requests.RequestException as e:
        raise RuntimeError(f"seedance_network_error: {e}")

    if resp.status_code == 401 or resp.status_code == 403:
        raise SeedanceAuthError(f"PiAPI auth failed: {resp.status_code} {resp.text[:200]}")

    if resp.status_code == 429:
        raise SeedanceQuotaError(f"PiAPI rate limited: {resp.text[:200]}")

    if resp.status_code >= 400:
        raise RuntimeError(f"seedance_api_error: {resp.status_code} {resp.text[:300]}")

    data = resp.json()

    # PiAPI returns {"code": 200, "data": {"task_id": "...", ...}}
    task_data = data.get("data", data)
    task_id = task_data.get("task_id")

    if not task_id:
        raise RuntimeError(f"seedance_no_task_id: {data}")

    print(f"[Seedance] Task created: {task_id}")

    return {
        "task_id": task_id,
        "status": "processing",
    }


# ── Check status ─────────────────────────────────────────────
# PiAPI status → internal status.
# "Pending"/"Staged" are now distinguished from "Processing".
_STATUS_MAP = {
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
_ZERO_TIMESTAMPS = {"0001-01-01T00:00:00Z", "", None}


def check_seedance_status(task_id: str) -> Dict[str, Any]:
    """
    Check the status of a Seedance task via PiAPI.

    Returns a rich dict:
        status:          done | processing | pending | failed | error
        provider_status: raw PiAPI status string
        started_at:      ISO timestamp or None
        ended_at:        ISO timestamp or None
        progress:        0-100
        video_url:       (only when done)
        error/message:   (only when failed)
    """
    headers = _get_headers()

    try:
        resp = requests.get(
            f"{PIAPI_BASE}/task/{task_id}",
            headers=headers,
            timeout=PIAPI_TIMEOUT,
        )
    except requests.RequestException as e:
        return {"status": "error", "message": f"Network error: {e}"}

    if resp.status_code == 401 or resp.status_code == 403:
        return {"status": "failed", "error": "seedance_auth_error", "message": "PiAPI auth failed"}

    if resp.status_code >= 400:
        return {"status": "error", "message": f"PiAPI error: {resp.status_code}"}

    data = resp.json()
    task_data = data.get("data", data)

    piapi_status = task_data.get("status", "unknown")
    internal_status = _STATUS_MAP.get(piapi_status, "pending")

    # Extract timing metadata
    started_at = task_data.get("started_at") or task_data.get("start_time")
    ended_at = task_data.get("ended_at") or task_data.get("end_time")
    actually_started = started_at not in _ZERO_TIMESTAMPS

    # If provider says Processing but started_at is zero, treat as pending
    if internal_status == "processing" and not actually_started:
        internal_status = "pending"

    result: Dict[str, Any] = {
        "status": internal_status,
        "provider_status": piapi_status,
        "started_at": started_at if actually_started else None,
        "ended_at": ended_at if ended_at not in _ZERO_TIMESTAMPS else None,
        "progress": task_data.get("progress", 0),
    }

    if internal_status == "done":
        # Extract video URL from output
        output = task_data.get("output") or {}
        video_url = (
            output.get("video")
            or output.get("video_url")
            or output.get("video_urls", [None])[0]
        )

        if not video_url:
            video_url = task_data.get("video_url") or task_data.get("video")

        if video_url:
            result["video_url"] = video_url
        else:
            result["status"] = "failed"
            result["error"] = "seedance_no_video_url"
            result["message"] = "Task completed but no video URL found"

    elif internal_status == "failed":
        error = task_data.get("error", {})
        result["error"] = "seedance_generation_failed"
        result["message"] = (
            error.get("message", "")
            if isinstance(error, dict)
            else str(error) or "Seedance generation failed"
        )

    return result


# ── Download video ───────────────────────────────────────────
def download_seedance_video(video_url: str) -> Tuple[bytes, str]:
    """
    Download video bytes from a Seedance result URL.

    Returns:
        (video_bytes, content_type)
    """
    try:
        resp = requests.get(video_url, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"seedance_download_error: {e}")

    content_type = resp.headers.get("Content-Type", "video/mp4")
    return resp.content, content_type
