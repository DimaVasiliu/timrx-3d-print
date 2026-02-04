"""
Runway API HTTP Client.

Handles authentication, headers, error parsing, and retries for the
Runway video generation API.

Base URL: https://api.dev.runwayml.com
Auth:     Authorization: Bearer <RUNWAY_API_KEY>
Version:  X-Runway-Version: 2024-11-06

Endpoints used:
  POST /v1/text_to_video   → start text-to-video task
  POST /v1/image_to_video  → start image-to-video task
  GET  /v1/tasks/{id}      → poll task status

The output URLs returned on SUCCEEDED are ephemeral (expire 24-48 h).
Always download and persist to S3 immediately.
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
class RunwayError(Exception):
    """Typed exception for Runway API errors."""

    def __init__(self, status_code: int, message: str, *, retryable: bool = False):
        self.status_code = status_code
        self.message = message
        self.retryable = retryable
        super().__init__(f"Runway API error {status_code}: {message}")


class RunwayConfigError(RunwayError):
    """Raised when Runway is not configured (missing API key)."""

    def __init__(self, message: str = "RUNWAY_API_KEY is not set"):
        super().__init__(status_code=0, message=message, retryable=False)


class RunwayAuthError(RunwayError):
    """Raised for 401/403 authentication failures."""

    def __init__(self, message: str = "Runway authentication failed"):
        super().__init__(status_code=401, message=message, retryable=False)


class RunwayQuotaError(RunwayError):
    """Raised for 429 rate-limit / quota exhaustion."""

    def __init__(self, message: str = "Runway rate limit exceeded"):
        super().__init__(status_code=429, message=message, retryable=True)


# ── Internal helpers ─────────────────────────────────────────
def _get_api_key() -> str:
    key = getattr(config, "RUNWAY_API_KEY", None) or os.getenv("RUNWAY_API_KEY") or ""
    if not key:
        raise RunwayConfigError()
    return key


def _get_base_url() -> str:
    return (
        getattr(config, "RUNWAY_API_BASE", None)
        or os.getenv("RUNWAY_API_BASE")
        or "https://api.dev.runwayml.com"
    ).rstrip("/")


def _get_version() -> str:
    return (
        getattr(config, "RUNWAY_API_VERSION", None)
        or os.getenv("RUNWAY_API_VERSION")
        or "2024-11-06"
    )


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_api_key()}",
        "X-Runway-Version": _get_version(),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _parse_error(r: requests.Response) -> RunwayError:
    """Convert a non-2xx response into a typed RunwayError."""
    body_text = r.text[:500] if r.text else ""

    try:
        body = r.json()
        msg = body.get("error", body.get("message", body_text))
    except Exception:
        msg = body_text

    if r.status_code in (401, 403):
        return RunwayAuthError(str(msg))
    if r.status_code == 429:
        return RunwayQuotaError(str(msg))

    retryable = r.status_code >= 500
    return RunwayError(r.status_code, str(msg), retryable=retryable)


# ── Public API ───────────────────────────────────────────────
def check_runway_configured() -> Tuple[bool, Optional[str]]:
    """Check whether Runway API key is set.  Returns (ok, error_msg)."""
    try:
        _get_api_key()
        return True, None
    except RunwayConfigError as e:
        return False, e.message


def runway_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST to a Runway API endpoint with retries on 5xx.

    Args:
        path: e.g. "/v1/text_to_video"
        payload: JSON body

    Returns:
        Parsed JSON response (typically ``{"id": "<task-uuid>"}``)

    Raises:
        RunwayConfigError, RunwayAuthError, RunwayQuotaError, RunwayError
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
            print(f"[Runway] POST {path} retry {attempt}/{MAX_RETRIES} after {delay}s: {err.message}")
            time.sleep(delay)

        except (Timeout, RequestsConnectionError) as e:
            last_err = e
            if attempt > MAX_RETRIES:
                raise RunwayError(0, f"Connection error: {e}", retryable=True) from e
            delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
            print(f"[Runway] POST {path} connection error, retry {attempt}/{MAX_RETRIES} after {delay}s")
            time.sleep(delay)

    # Should not reach here, but just in case
    raise RunwayError(0, f"Max retries exceeded: {last_err}", retryable=False)


def runway_get(path: str) -> Dict[str, Any]:
    """
    GET from a Runway API endpoint.

    Args:
        path: e.g. "/v1/tasks/<task_id>"

    Returns:
        Parsed JSON response

    Raises:
        RunwayConfigError, RunwayAuthError, RunwayQuotaError, RunwayError
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
        raise RunwayError(0, f"Connection error: {e}", retryable=True) from e


def runway_download(url: str) -> Tuple[bytes, str]:
    """
    Download a video from a Runway ephemeral output URL.

    These URLs are pre-signed CloudFront links — no Runway auth needed.

    Returns:
        (video_bytes, content_type)
    """
    print(f"[Runway] Downloading video from: {url[:100]}...")
    try:
        r = requests.get(url, timeout=DOWNLOAD_TIMEOUT, allow_redirects=True, stream=True)
        if not r.ok:
            raise RunwayError(r.status_code, f"Failed to download output: HTTP {r.status_code}")

        content_type = r.headers.get("Content-Type", "video/mp4")
        video_bytes = r.content
        print(f"[Runway] Downloaded {len(video_bytes)} bytes, type={content_type}")
        return video_bytes, content_type

    except (Timeout, RequestsConnectionError) as e:
        raise RunwayError(0, f"Download connection error: {e}", retryable=True) from e
