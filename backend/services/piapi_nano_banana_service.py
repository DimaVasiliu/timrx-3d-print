"""
PiAPI Nano Banana 2 Image Generation Service.

Uses PiAPI for text-to-image generation via the Nano Banana 2 model.
Authentication: PIAPI_API_KEY via X-API-Key header.

Create task: POST https://api.piapi.ai/api/v1/task
Get task:    GET  https://api.piapi.ai/api/v1/task/{task_id}
"""

from __future__ import annotations

import time
import os
import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError
from typing import Dict, Any, Optional, Tuple

from backend.config import config

# Timeouts
PIAPI_TIMEOUT = (15, 120)  # (connect_timeout, read_timeout)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2

# PiAPI base URL
PIAPI_API_BASE = "https://api.piapi.ai/api/v1"

# Polling settings for get-task
POLL_INTERVAL_INITIAL = 3       # seconds
POLL_INTERVAL_MAX = 8           # seconds
POLL_TIMEOUT = 180              # seconds total before giving up

# Allowed parameter values
ALLOWED_ASPECT_RATIOS = {"1:1", "9:16", "16:9", "3:4", "4:3"}
ALLOWED_RESOLUTIONS = {"1K", "2K", "4K"}
ALLOWED_OUTPUT_FORMATS = {"png", "jpg"}


class PiAPIConfigError(Exception):
    """Raised when PiAPI is not configured (missing API key)."""
    pass


class PiAPIAuthError(Exception):
    """Raised when PiAPI authentication fails (401/403)."""
    pass


class PiAPIValidationError(Exception):
    """Raised for parameter validation errors."""
    def __init__(self, field: str, value: Any, allowed: list, message: Optional[str] = None):
        self.field = field
        self.value = value
        self.allowed = allowed
        self.message = message if message else f"Invalid {field}: {value}. Allowed: {allowed}"
        super().__init__(self.message)


class PiAPIServerError(Exception):
    """Raised for 5xx errors from PiAPI (retryable)."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


class PiAPITaskError(Exception):
    """Raised when a PiAPI task fails or times out."""
    pass


def _get_api_key() -> str:
    """Get the PiAPI API key from config/environment."""
    key = getattr(config, 'PIAPI_API_KEY', None) or os.getenv("PIAPI_API_KEY") or ""
    if not key:
        raise PiAPIConfigError(
            "PIAPI_API_KEY is not set. "
            "Get your API key from https://piapi.ai"
        )
    return key


def _get_headers() -> Dict[str, str]:
    """Get headers for PiAPI requests."""
    return {
        "Content-Type": "application/json",
        "X-API-Key": _get_api_key(),
    }


def check_piapi_configured() -> Tuple[bool, Optional[str]]:
    """Check if PiAPI is configured. Returns (is_configured, error_message)."""
    try:
        _get_api_key()
        return True, None
    except PiAPIConfigError as e:
        return False, str(e)


def validate_nano_banana_params(aspect_ratio: str, resolution: str, output_format: str) -> None:
    """Validate Nano Banana parameters. Raises PiAPIValidationError if invalid."""
    if aspect_ratio not in ALLOWED_ASPECT_RATIOS:
        raise PiAPIValidationError("aspect_ratio", aspect_ratio, list(ALLOWED_ASPECT_RATIOS))
    if resolution not in ALLOWED_RESOLUTIONS:
        raise PiAPIValidationError("resolution", resolution, list(ALLOWED_RESOLUTIONS))
    if output_format not in ALLOWED_OUTPUT_FORMATS:
        raise PiAPIValidationError("output_format", output_format, list(ALLOWED_OUTPUT_FORMATS))


def create_nano_banana_task(
    prompt: str,
    aspect_ratio: str = "1:1",
    resolution: str = "1K",
    output_format: str = "png",
) -> Dict[str, Any]:
    """
    Create a Nano Banana 2 image generation task on PiAPI.

    Args:
        prompt: Text description of the image to generate.
        aspect_ratio: "1:1", "9:16", "16:9", "3:4", "4:3"
        resolution: "1K", "2K", or "4K"
        output_format: "png" or "jpg"

    Returns:
        Dict with task_id and raw response.

    Raises:
        PiAPIConfigError: If PIAPI_API_KEY not set.
        PiAPIValidationError: If parameters are invalid.
        PiAPIAuthError: If authentication fails.
        PiAPIServerError: For 5xx errors.
        RuntimeError: For other API errors.
    """
    validate_nano_banana_params(aspect_ratio, resolution, output_format)

    url = f"{PIAPI_API_BASE}/task"
    payload = {
        "model": "gemini",
        "task_type": "nano-banana-2",
        "input": {
            "prompt": prompt,
            "aspect_ratio": aspect_ratio,
            "resolution": resolution,
            "output_format": output_format,
        },
        "config": {
            "webhook_config": {
                "endpoint": "",
                "secret": ""
            }
        }
    }

    print(f"[PiAPI NanoBanana] Creating task: aspect_ratio={aspect_ratio}, resolution={resolution}, format={output_format}")

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(url, headers=_get_headers(), json=payload, timeout=PIAPI_TIMEOUT)

            if not r.ok:
                error_text = r.text[:500] if r.text else "No error details"
                print(f"[PiAPI NanoBanana] Error {r.status_code}: {error_text}")

                if r.status_code in (401, 403):
                    raise PiAPIAuthError(
                        "PiAPI authentication failed. Check your PIAPI_API_KEY."
                    )

                if 400 <= r.status_code < 500:
                    raise RuntimeError(f"piapi_task_failed: {error_text}")

                raise PiAPIServerError(r.status_code, f"PiAPI server error {r.status_code}: {error_text}")

            result = r.json()
            task_id = result.get("data", {}).get("task_id") or result.get("task_id")

            if not task_id:
                raise RuntimeError(f"piapi_task_failed: No task_id in response: {str(result)[:200]}")

            print(f"[PiAPI NanoBanana] Task created: task_id={task_id}")
            return {"task_id": task_id, "raw": result}

        except (PiAPIAuthError, PiAPIConfigError):
            raise
        except (Timeout, RequestsConnectionError, PiAPIServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[PiAPI NanoBanana] Attempt {attempt} failed, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[PiAPI NanoBanana] All {MAX_RETRIES} attempts failed")
        except RuntimeError:
            raise

    raise RuntimeError(f"piapi_task_failed: Create task failed after {MAX_RETRIES} attempts: {last_error}")


def get_nano_banana_task(task_id: str) -> Dict[str, Any]:
    """
    Get the status of a PiAPI task.

    Args:
        task_id: The PiAPI task ID.

    Returns:
        Dict with status, image_urls (if completed), and raw response.

    Raises:
        PiAPIConfigError: If PIAPI_API_KEY not set.
        PiAPIAuthError: If authentication fails.
        RuntimeError: For other API errors.
    """
    url = f"{PIAPI_API_BASE}/task/{task_id}"

    r = requests.get(url, headers=_get_headers(), timeout=PIAPI_TIMEOUT)

    if not r.ok:
        error_text = r.text[:500] if r.text else "No error details"
        if r.status_code in (401, 403):
            raise PiAPIAuthError("PiAPI authentication failed. Check your PIAPI_API_KEY.")
        raise RuntimeError(f"piapi_status_failed: HTTP {r.status_code}: {error_text}")

    result = r.json()
    data = result.get("data", {})
    status = data.get("status", "unknown")

    # Extract image URLs from output
    image_urls = []
    output = data.get("output", {})
    if isinstance(output, dict):
        image_urls = output.get("image_urls", [])
        # Some responses use image_url (singular)
        if not image_urls and output.get("image_url"):
            image_urls = [output["image_url"]]

    return {
        "status": status,
        "task_id": task_id,
        "image_urls": image_urls,
        "raw": result,
    }


def poll_nano_banana_task(task_id: str) -> Dict[str, Any]:
    """
    Poll a PiAPI task until completion, failure, or timeout.

    Args:
        task_id: The PiAPI task ID to poll.

    Returns:
        Dict with final status and image_urls if completed.

    Raises:
        PiAPITaskError: If task fails or times out.
        PiAPIConfigError: If API key missing.
        PiAPIAuthError: If auth fails.
    """
    start_time = time.time()
    interval = POLL_INTERVAL_INITIAL

    while True:
        elapsed = time.time() - start_time
        if elapsed > POLL_TIMEOUT:
            raise PiAPITaskError(f"Task {task_id} timed out after {POLL_TIMEOUT}s")

        time.sleep(interval)
        interval = min(POLL_INTERVAL_MAX, interval * 1.5)

        try:
            result = get_nano_banana_task(task_id)
        except Exception as e:
            print(f"[PiAPI NanoBanana] Poll error for {task_id}: {e}")
            continue

        status = result["status"]
        print(f"[PiAPI NanoBanana] Poll task_id={task_id} status={status} elapsed={elapsed:.0f}s")

        if status == "completed":
            if not result["image_urls"]:
                raise PiAPITaskError(f"Task {task_id} completed but no image URLs returned")
            return result

        if status in ("failed", "error"):
            error_msg = result.get("raw", {}).get("data", {}).get("error", {}).get("message", "Unknown error")
            raise PiAPITaskError(f"Task {task_id} failed: {error_msg}")

        # Continue polling for pending/processing statuses
