"""
fal.ai Seedance 1.5 Pro Video Generation Service.

Wraps the fal.ai REST API for Seedance 1.5 Pro video generation.
Uses the submit → poll pattern (no webhooks).

Endpoints:
  - POST https://queue.fal.run/{model_id}  (submit task)
  - GET  https://queue.fal.run/{model_id}/requests/{request_id}/status  (poll)
  - GET  https://queue.fal.run/{model_id}/requests/{request_id}  (get result)
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import requests

from backend.config import config


# ── Constants ────────────────────────────────────────────────
FAL_QUEUE_BASE = "https://queue.fal.run"
FAL_TIMEOUT = 30  # seconds

# Default model IDs (overridable via config)
DEFAULT_T2V_MODEL = "fal-ai/bytedance/seedance/v1.5/pro/text-to-video"
DEFAULT_I2V_MODEL = "fal-ai/bytedance/seedance/v1.5/pro/image-to-video"

# Startup diagnostic (safe — never logs the key itself)
_api_key_present = bool(getattr(config, "FAL_KEY", ""))
_enabled = getattr(config, "FAL_SEEDANCE_ENABLED", True)
print(f"[FAL_SEEDANCE] api_key configured={_api_key_present} enabled={_enabled}")
if not _api_key_present:
    print("[FAL_SEEDANCE] WARNING: FAL_KEY is not set — fal Seedance video generation will fail")


# ── Errors ───────────────────────────────────────────────────
class FalSeedanceConfigError(Exception):
    """Raised when FAL_KEY is not configured."""
    pass


class FalSeedanceAuthError(Exception):
    """Raised on 401/403 from fal.ai."""
    pass


class FalSeedanceQuotaError(Exception):
    """Raised on 429 / quota exhaustion from fal.ai."""
    pass


# ── Configuration check ─────────────────────────────────────
def check_fal_seedance_configured() -> Tuple[bool, Optional[str]]:
    """Check if fal.ai Seedance is configured and enabled."""
    enabled = getattr(config, "FAL_SEEDANCE_ENABLED", True)
    if not enabled:
        return False, "FAL_SEEDANCE_ENABLED is False"
    api_key = getattr(config, "FAL_KEY", "")
    if not api_key:
        return False, "FAL_KEY not set"
    return True, None


def _get_headers() -> Dict[str, str]:
    """Build fal.ai request headers."""
    api_key = getattr(config, "FAL_KEY", "")
    if not api_key:
        raise FalSeedanceConfigError("FAL_KEY not set")
    return {
        "Authorization": f"Key {api_key}",
        "Content-Type": "application/json",
    }


def _get_model_id(task: str = "text2video") -> str:
    """Get the fal model ID for the given task type."""
    if task in ("image2video", "image_to_video", "i2v"):
        return getattr(config, "FAL_SEEDANCE_I2V_MODEL", DEFAULT_I2V_MODEL)
    return getattr(config, "FAL_SEEDANCE_T2V_MODEL", DEFAULT_T2V_MODEL)


# ── Submit task ──────────────────────────────────────────────
def submit_fal_seedance_task(
    prompt: str,
    duration: int = 5,
    aspect_ratio: str = "16:9",
    image_url: Optional[str] = None,
    end_image_url: Optional[str] = None,
    task: str = "text2video",
) -> Dict[str, Any]:
    """
    Submit a Seedance 1.5 Pro video generation task to fal.ai queue.

    Args:
        prompt: Text prompt for video generation.
        duration: Video duration in seconds (5 or 10).
        aspect_ratio: Output aspect ratio (16:9, 9:16, 1:1).
        image_url: Start/source image URL for image-to-video (must be publicly accessible).
        end_image_url: End image URL for image transition (must be publicly accessible).
        task: "text2video" or "image2video".

    Returns:
        {"request_id": "...", "status": "processing"}

    Raises:
        FalSeedanceConfigError: FAL_KEY not set.
        FalSeedanceAuthError: Authentication failed.
        FalSeedanceQuotaError: Quota exhausted or rate limited.
        RuntimeError: Other API errors.
    """
    headers = _get_headers()
    model_id = _get_model_id(task)

    # Build request body per fal Seedance 1.5 Pro API
    # NOTE: fal expects duration as a string enum ("5", "10"), not an integer
    body: Dict[str, Any] = {
        "prompt": prompt,
        "duration": str(duration),
        "aspect_ratio": aspect_ratio,
        "generate_audio": getattr(config, "FAL_SEEDANCE_GENERATE_AUDIO", True),
        "enable_safety_checker": getattr(config, "FAL_SEEDANCE_ENABLE_SAFETY_CHECKER", True),
    }

    # Image-to-video: add image_url (start/source image)
    if task in ("image2video", "image_to_video", "i2v") and image_url:
        body["image_url"] = image_url

    # Image transition: add end_image_url
    if end_image_url:
        body["end_image_url"] = end_image_url

    # Optional: fixed camera (fal expects boolean, not object)
    if getattr(config, "FAL_SEEDANCE_CAMERA_FIXED", False):
        body["camera_fixed"] = True

    # Log request (safe: no secrets)
    _prompt_preview = (prompt or "")[:60]
    if end_image_url:
        _mode = "transition"
    elif image_url:
        _mode = "animate"
    else:
        _mode = "text2video"
    print(
        f"[FAL_SEEDANCE] SUBMIT model={model_id} task={task} mode={_mode} "
        f"dur={duration}s ar={aspect_ratio} image_url={'yes' if image_url else 'no'} "
        f"end_image_url={'yes' if end_image_url else 'no'} prompt={_prompt_preview!r}"
    )

    try:
        resp = requests.post(
            f"{FAL_QUEUE_BASE}/{model_id}",
            json=body,
            headers=headers,
            timeout=FAL_TIMEOUT,
        )
    except requests.RequestException as e:
        raise RuntimeError(f"fal_seedance_network_error: {e}")

    # Log response
    print(
        f"[FAL_SEEDANCE] RESPONSE status={resp.status_code} "
        f"body={resp.text[:500]}"
    )

    if resp.status_code in (401, 403):
        raise FalSeedanceAuthError(f"fal.ai auth failed: {resp.status_code} {resp.text[:200]}")

    if resp.status_code == 429:
        raise FalSeedanceQuotaError(f"fal.ai rate limited: {resp.text[:200]}")

    if resp.status_code >= 400:
        raise RuntimeError(f"fal_seedance_api_error: {resp.status_code} {resp.text[:300]}")

    data = resp.json()

    # fal queue returns {"request_id": "...", "status_url": "...", "response_url": "...", "cancel_url": "..."}
    request_id = data.get("request_id")
    if not request_id:
        raise RuntimeError(f"fal_seedance_no_request_id: {data}")

    status_url = data.get("status_url", "")
    response_url = data.get("response_url", "")
    cancel_url = data.get("cancel_url", "")

    print(
        f"[FAL_SEEDANCE] Task submitted: request_id={request_id} "
        f"status_url={status_url[:80] or 'NONE'} response_url={response_url[:80] or 'NONE'}"
    )

    return {
        "request_id": request_id,
        "status": "processing",
        "fal_model_id": model_id,
        "fal_status_url": status_url,
        "fal_response_url": response_url,
        "fal_cancel_url": cancel_url,
    }


# ── Check status ─────────────────────────────────────────────
# fal status values → internal status
_FAL_STATUS_MAP = {
    "COMPLETED": "done",
    "IN_PROGRESS": "processing",
    "IN_QUEUE": "pending",
    "FAILED": "failed",
}


def check_fal_seedance_status(
    request_id: str,
    model_id: str | None = None,
    status_url: str | None = None,
    response_url: str | None = None,
) -> Dict[str, Any]:
    """
    Check the status of a fal.ai Seedance task.

    Args:
        request_id: The fal request ID.
        model_id: The fal model ID used for submission. If None, defaults to t2v model.
        status_url: Exact status URL returned by fal on submit (preferred over reconstruction).
        response_url: Exact result URL returned by fal on submit (preferred over reconstruction).

    Returns a rich dict:
        status:          done | processing | pending | failed | error
        provider_status: raw fal status string
        video_url:       (only when done)
        error/message:   (only when failed)
    """
    headers = _get_headers()
    if not model_id:
        model_id = _get_model_id("text2video")

    # Prefer exact URL from fal submit response; fall back to reconstruction
    poll_url = status_url or f"{FAL_QUEUE_BASE}/{model_id}/requests/{request_id}/status"

    print(f"[FAL_SEEDANCE] polling url={poll_url}")

    try:
        resp = requests.get(poll_url, headers=headers, timeout=FAL_TIMEOUT)
    except requests.RequestException as e:
        print(f"[FAL_SEEDANCE] POLL NETWORK ERROR url={poll_url} error={e}")
        return {"status": "error", "provider_status": "network_error", "message": f"Network error polling fal: {e}"}

    if resp.status_code in (401, 403):
        print(f"[FAL_SEEDANCE] POLL AUTH ERROR url={poll_url} status={resp.status_code} body={resp.text[:300]}")
        return {"status": "failed", "provider_status": "auth_error", "error": "auth", "message": f"fal.ai auth failed: {resp.status_code}"}

    if resp.status_code >= 400:
        print(
            f"[FAL_SEEDANCE] POLL HTTP ERROR url={poll_url} status={resp.status_code} "
            f"body={resp.text[:500]}"
        )
        return {
            "status": "error",
            "provider_status": f"http_{resp.status_code}",
            "error": "network",
            "message": f"fal.ai poll error: {resp.status_code} {resp.text[:200]}",
        }

    data = resp.json()
    fal_status = data.get("status", "UNKNOWN")
    internal_status = _FAL_STATUS_MAP.get(fal_status, "pending")

    print(
        f"[FAL_SEEDANCE] status check request={request_id[:12]}... "
        f"raw_status={fal_status!r} -> {internal_status} keys={list(data.keys())[:8]}"
    )

    result: Dict[str, Any] = {
        "status": internal_status,
        "provider_status": fal_status,
    }

    if internal_status == "done":
        # Fetch the full result to get the video URL
        result_url = response_url or f"{FAL_QUEUE_BASE}/{model_id}/requests/{request_id}"
        video_result = _get_fal_result_from_url(result_url, headers)
        if video_result.get("video_url"):
            result["video_url"] = video_result["video_url"]
        else:
            result["status"] = "failed"
            result["error"] = "no_output"
            result["message"] = "Task completed but no video URL found"

    elif internal_status == "failed":
        result["error"] = "internal"
        err_data = data.get("error", "fal Seedance generation failed")
        if isinstance(err_data, dict):
            result["message"] = err_data.get("message", "") or str(err_data)
        else:
            result["message"] = str(err_data)

    return result


def _get_fal_result_from_url(result_url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Fetch the full result of a completed fal task using the exact result URL."""
    print(f"[FAL_SEEDANCE] fetching result url={result_url}")
    try:
        resp = requests.get(result_url, headers=headers, timeout=FAL_TIMEOUT)
    except requests.RequestException as e:
        print(f"[FAL_SEEDANCE] ERROR fetching result url={result_url} error={e}")
        return {}

    if resp.status_code >= 400:
        print(f"[FAL_SEEDANCE] ERROR fetching result url={result_url} status={resp.status_code} body={resp.text[:500]}")
        return {}

    data = resp.json()

    # fal Seedance returns {"video": {"url": "..."}} or similar
    video = data.get("video") or {}
    video_url = None

    if isinstance(video, dict):
        video_url = video.get("url")
    elif isinstance(video, str):
        video_url = video

    # Fallback: check other common output shapes
    if not video_url:
        video_url = data.get("video_url") or data.get("output", {}).get("video_url")

    print(f"[FAL_SEEDANCE] result parsed: video_url={'YES' if video_url else 'NONE'} keys={list(data.keys())[:10]}")
    return {"video_url": video_url}


# ── Download video ───────────────────────────────────────────
def download_fal_seedance_video(video_url: str) -> Tuple[bytes, str]:
    """
    Download video bytes from a fal.ai result URL.

    Returns:
        (video_bytes, content_type)
    """
    try:
        resp = requests.get(video_url, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"fal_seedance_download_error: {e}")

    content_type = resp.headers.get("Content-Type", "video/mp4")
    return resp.content, content_type
