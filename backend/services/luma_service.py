"""
Luma Dream Machine API HTTP Client.

Handles authentication, headers, error parsing, and retries for the
Luma video generation API.

Base URL: https://api.lumalabs.ai
Auth:     Authorization: Bearer <LUMA_API_KEY>

Endpoints used:
  POST /dream-machine/v1/generations                → create generation
  GET  /dream-machine/v1/generations/{id}           → get generation status
  DELETE /dream-machine/v1/generations/{id}         → cancel generation

The video URLs returned on completion are ephemeral (may expire).
Always download and persist to S3 immediately.

Luma API docs: https://docs.lumalabs.ai/docs/video-generation
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout

from backend.config import config


# ── Timeouts ─────────────────────────────────────────────────
CONNECT_TIMEOUT = 15        # seconds
READ_TIMEOUT = 60           # seconds for task creation
DOWNLOAD_TIMEOUT = 300      # seconds for video download
MAX_RETRIES = 2
BASE_RETRY_DELAY = 2        # exponential backoff base


# ── Exceptions ───────────────────────────────────────────────
class LumaError(Exception):
    """Typed exception for Luma API errors."""

    def __init__(self, status_code: int, message: str, *, retryable: bool = False):
        self.status_code = status_code
        self.message = message
        self.retryable = retryable
        super().__init__(f"Luma API error {status_code}: {message}")


class LumaConfigError(LumaError):
    """Raised when Luma is not configured (missing API key)."""

    def __init__(self, message: str = "LUMA_API_KEY is not set"):
        super().__init__(status_code=0, message=message, retryable=False)


class LumaAuthError(LumaError):
    """Raised for 401/403 authentication failures."""

    def __init__(self, message: str = "Luma authentication failed"):
        super().__init__(status_code=401, message=message, retryable=False)


class LumaQuotaError(LumaError):
    """Raised for 429 rate-limit / quota exhaustion."""

    def __init__(self, message: str = "Luma rate limit exceeded"):
        super().__init__(status_code=429, message=message, retryable=True)


class LumaValidationError(LumaError):
    """Raised for 400 validation errors."""

    def __init__(self, message: str = "Luma validation failed"):
        super().__init__(status_code=400, message=message, retryable=False)


# ── Internal helpers ─────────────────────────────────────────
def _get_api_key() -> str:
    key = getattr(config, "LUMA_API_KEY", None) or os.getenv("LUMA_API_KEY") or ""
    if not key:
        raise LumaConfigError()
    return key


def _get_base_url() -> str:
    return (
        getattr(config, "LUMA_BASE_URL", None)
        or os.getenv("LUMA_BASE_URL")
        or "https://api.lumalabs.ai"
    ).rstrip("/")


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_api_key()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _parse_error(r: requests.Response) -> LumaError:
    """Convert a non-2xx response into a typed LumaError."""
    body_text = r.text[:500] if r.text else ""

    try:
        body = r.json()
        # Luma API returns errors as { "detail": "message" } or { "error": "message" }
        msg = body.get("detail", body.get("error", body.get("message", body_text)))
    except Exception:
        msg = body_text

    if r.status_code in (401, 403):
        return LumaAuthError(str(msg))
    if r.status_code == 429:
        return LumaQuotaError(str(msg))
    if r.status_code == 400:
        return LumaValidationError(str(msg))

    retryable = r.status_code >= 500
    return LumaError(r.status_code, str(msg), retryable=retryable)


# ── Public API ───────────────────────────────────────────────
def check_luma_configured() -> Tuple[bool, Optional[str]]:
    """Check whether Luma API key is set.  Returns (ok, error_msg)."""
    try:
        _get_api_key()
        return True, None
    except LumaConfigError as e:
        return False, e.message


def luma_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST to a Luma API endpoint with retries on 5xx.

    Args:
        path: e.g. "/dream-machine/v1/generations"
        payload: JSON body

    Returns:
        Parsed JSON response

    Raises:
        LumaConfigError, LumaAuthError, LumaQuotaError, LumaError
    """
    url = f"{_get_base_url()}{path}"
    headers = _headers()

    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 2):  # 1-indexed, +1 for initial try
        try:
            r = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
            )

            if r.ok:
                return r.json()

            err = _parse_error(r)
            if not err.retryable or attempt > MAX_RETRIES:
                raise err

            last_err = err
            delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
            print(f"[Luma] POST {path} retry {attempt}/{MAX_RETRIES} after {delay}s: {err.message}")
            time.sleep(delay)

        except (Timeout, RequestsConnectionError) as e:
            last_err = e
            if attempt > MAX_RETRIES:
                raise LumaError(0, f"Connection error: {e}", retryable=True) from e
            delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
            print(f"[Luma] POST {path} connection error, retry {attempt}/{MAX_RETRIES} after {delay}s")
            time.sleep(delay)

    # Should not reach here, but just in case
    raise LumaError(0, f"Max retries exceeded: {last_err}", retryable=False)


def luma_get(path: str) -> Dict[str, Any]:
    """
    GET from a Luma API endpoint.

    Args:
        path: e.g. "/dream-machine/v1/generations/{id}"

    Returns:
        Parsed JSON response

    Raises:
        LumaConfigError, LumaAuthError, LumaQuotaError, LumaError
    """
    url = f"{_get_base_url()}{path}"
    headers = _headers()
    headers.pop("Content-Type", None)  # no body on GET

    try:
        r = requests.get(url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))

        if r.ok:
            return r.json()

        raise _parse_error(r)

    except (Timeout, RequestsConnectionError) as e:
        raise LumaError(0, f"Connection error: {e}", retryable=True) from e


def luma_download(url: str) -> Tuple[bytes, str]:
    """
    Download a video from a Luma output URL.

    These URLs may be pre-signed — no Luma auth needed.

    Returns:
        (video_bytes, content_type)
    """
    print(f"[Luma] Downloading video from: {url[:100]}...")
    try:
        r = requests.get(url, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True, stream=True)
        if not r.ok:
            raise LumaError(r.status_code, f"Failed to download output: HTTP {r.status_code}")

        content_type = r.headers.get("Content-Type", "video/mp4")
        video_bytes = r.content
        print(f"[Luma] Downloaded {len(video_bytes)} bytes, type={content_type}")
        return video_bytes, content_type

    except (Timeout, RequestsConnectionError) as e:
        raise LumaError(0, f"Download connection error: {e}", retryable=True) from e


# ── High-level Luma API wrappers ─────────────────────────────
def luma_create_generation(
    prompt: str,
    model: str = "ray-2",
    aspect_ratio: str = "16:9",
    duration_seconds: int = 5,
    loop: bool = False,
    keyframes: Optional[Dict[str, Any]] = None,
    resolution: str = "720p",
    concept: str = "auto",
) -> Dict[str, Any]:
    """
    Create a new Luma video generation.

    Args:
        prompt: Text prompt for video generation
        model: "ray-2", "ray-flash-2" (fast), etc.
        aspect_ratio: "16:9", "9:16", "1:1", etc.
        duration_seconds: 5 or 10 (Luma native)
        loop: Whether video should loop
        keyframes: Optional keyframes for image-to-video
        resolution: "720p" or "1080p"
        concept: Luma Concept ID or "auto" for default

    Returns:
        Generation response with 'id' for polling
    """
    payload: Dict[str, Any] = {
        "prompt": prompt[:2000],  # Luma prompt limit
        "model": model,
        "aspect_ratio": aspect_ratio,
        "loop": loop,
    }

    # Add resolution for models that support it
    if resolution == "1080p" and model == "ray-2":
        payload["resolution"] = "1080p"
    # ray-flash-2 only supports 720p

    # Add concept if not auto (Luma Concepts API)
    if concept and concept != "auto":
        payload["concept"] = concept

    # Add keyframes for image-to-video
    if keyframes:
        payload["keyframes"] = keyframes

    resp = luma_post("/dream-machine/v1/generations", payload)
    return resp


def luma_get_generation(generation_id: str) -> Dict[str, Any]:
    """
    Get the status of a Luma generation.

    Returns:
        Generation object with:
        - id: generation ID
        - state: "queued", "dreaming", "completed", "failed"
        - failure_reason: error message if failed
        - assets: { video: "url" } if completed
        - created_at, estimated_time_remaining, etc.
    """
    resp = luma_get(f"/dream-machine/v1/generations/{generation_id}")
    return resp


def luma_cancel_generation(generation_id: str) -> Dict[str, Any]:
    """Cancel a running generation."""
    url = f"{_get_base_url()}/dream-machine/v1/generations/{generation_id}"
    headers = _headers()

    try:
        r = requests.delete(url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if r.ok:
            return r.json() if r.text else {"success": True}
        raise _parse_error(r)
    except (Timeout, RequestsConnectionError) as e:
        raise LumaError(0, f"Connection error: {e}", retryable=True) from e


def luma_get_concepts() -> Dict[str, Any]:
    """
    Fetch the list of available Luma Concepts for styling video generations.

    Returns:
        List of concept objects with:
        - id: concept ID to use in generation requests
        - name: display name
        - description: concept description
        - thumbnail_url: preview image
    """
    try:
        resp = luma_get("/dream-machine/v1/concepts")
        return resp
    except LumaError:
        # If concepts endpoint doesn't exist or fails, return empty list
        return {"concepts": []}


# ── Status normalization ─────────────────────────────────────
def normalize_luma_status(generation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize Luma generation status to our standard format.

    Luma states: "queued", "dreaming", "completed", "failed"
    Our states: "processing", "done", "failed", "error"

    Returns:
        {
            "status": "processing" | "done" | "failed" | "error",
            "progress": 0-100,
            "video_url": str (on done),
            "error": str (on failed),
            "message": str,
            "luma_generation": dict (raw response)
        }
    """
    state = generation.get("state", "unknown").lower()
    gen_id = generation.get("id", "")

    if state == "completed":
        assets = generation.get("assets", {})
        video_url = assets.get("video", "")
        return {
            "status": "done",
            "progress": 100,
            "video_url": video_url,
            "luma_generation": generation,
        }

    if state == "failed":
        failure_reason = generation.get("failure_reason", "Unknown failure")
        return {
            "status": "failed",
            "error": "luma_generation_failed",
            "message": str(failure_reason),
            "luma_generation": generation,
        }

    if state == "dreaming":
        # Luma "dreaming" = actively generating
        return {
            "status": "processing",
            "progress": 50,  # Luma doesn't expose granular progress
            "message": "Luma is generating the video...",
            "luma_generation": generation,
        }

    if state == "queued":
        return {
            "status": "processing",
            "progress": 0,
            "message": "Generation queued...",
            "luma_generation": generation,
        }

    # Unknown state — treat as transient
    return {
        "status": "error",
        "error": "luma_unknown_status",
        "message": f"Unknown Luma state: {state}",
        "luma_generation": generation,
    }
