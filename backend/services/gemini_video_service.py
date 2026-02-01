"""
Gemini Video Generation Service.

Handles video generation via Google's Gemini API (Veo model).
Supports both text-to-video and image-to-video generation.
"""

from __future__ import annotations

import time
import base64
import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError
from typing import Optional, Dict, Any

from backend.config import config

# Gemini video generation can take significant time
GEMINI_TIMEOUT = (15, 300)  # (connect_timeout, read_timeout) - 5 minutes for generation
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2  # seconds (exponential backoff: 2s, 4s, 8s)

# Gemini API base URL
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"


class GeminiServerError(Exception):
    """Raised for 5xx errors from Gemini (retryable)."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


def _get_api_key() -> str:
    """Get the Google API key from config."""
    key = getattr(config, 'GOOGLE_API_KEY', None) or ""
    if not key:
        raise RuntimeError("GOOGLE_API_KEY not configured")
    return key


def gemini_text_to_video(
    prompt: str,
    duration_sec: int = 5,
    fps: int = 24,
    aspect_ratio: str = "16:9",
    resolution: str = "1080p",
    audio: bool = False,
    loop_seamlessly: bool = False,
) -> Dict[str, Any]:
    """
    Generate a video from a text prompt using Gemini Veo.

    Args:
        prompt: Text description of the video to generate
        duration_sec: Video duration in seconds (5 or 10)
        fps: Frames per second (24, 30, or 60)
        aspect_ratio: Aspect ratio ("16:9", "9:16", or "1:1")
        resolution: Video resolution ("720p", "1080p", or "4K")
        audio: Whether to generate audio
        loop_seamlessly: Whether video should loop seamlessly

    Returns:
        Dict with video_url, task_id, and other metadata
    """
    api_key = _get_api_key()

    # Build the request for Gemini's video generation
    url = f"{GEMINI_API_BASE}/models/veo-2.0-generate-001:predictLongRunning"

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key,
    }

    # Map resolution to dimensions
    resolution_map = {
        "720p": {"width": 1280, "height": 720},
        "1080p": {"width": 1920, "height": 1080},
        "4K": {"width": 3840, "height": 2160},
    }
    dims = resolution_map.get(resolution, resolution_map["1080p"])

    # Adjust dimensions for aspect ratio
    if aspect_ratio == "9:16":
        dims = {"width": dims["height"], "height": dims["width"]}
    elif aspect_ratio == "1:1":
        size = min(dims["width"], dims["height"])
        dims = {"width": size, "height": size}

    payload = {
        "instances": [{
            "prompt": prompt,
        }],
        "parameters": {
            "sampleCount": 1,
            "durationSeconds": duration_sec,
            "fps": fps,
            "aspectRatio": aspect_ratio,
            "personGeneration": "allow_adult",
        }
    }

    return _execute_gemini_request(url, headers, payload, "text-to-video")


def gemini_image_to_video(
    image_data: str,
    motion_prompt: str = "",
    duration_sec: int = 5,
    fps: int = 24,
    aspect_ratio: str = "16:9",
    resolution: str = "1080p",
    audio: bool = False,
    loop_seamlessly: bool = False,
) -> Dict[str, Any]:
    """
    Generate a video from an image using Gemini Veo.

    Args:
        image_data: Base64-encoded image data or URL
        motion_prompt: Description of motion/camera movement
        duration_sec: Video duration in seconds
        fps: Frames per second
        aspect_ratio: Aspect ratio
        resolution: Video resolution
        audio: Whether to generate audio
        loop_seamlessly: Whether video should loop seamlessly

    Returns:
        Dict with video_url, task_id, and other metadata
    """
    api_key = _get_api_key()

    url = f"{GEMINI_API_BASE}/models/veo-2.0-generate-001:predictLongRunning"

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key,
    }

    # Handle image data - could be base64 or URL
    image_content = {}
    if image_data.startswith("data:"):
        # Data URL - extract base64
        parts = image_data.split(",", 1)
        if len(parts) == 2:
            mime_type = parts[0].split(";")[0].replace("data:", "")
            image_content = {
                "bytesBase64Encoded": parts[1],
                "mimeType": mime_type or "image/png",
            }
    elif image_data.startswith("http"):
        # URL - Gemini might support direct URLs
        image_content = {
            "fileUri": image_data,
        }
    else:
        # Assume raw base64
        image_content = {
            "bytesBase64Encoded": image_data,
            "mimeType": "image/png",
        }

    payload = {
        "instances": [{
            "prompt": motion_prompt or "Animate this image with natural motion",
            "image": image_content,
        }],
        "parameters": {
            "sampleCount": 1,
            "durationSeconds": duration_sec,
            "fps": fps,
            "aspectRatio": aspect_ratio,
        }
    }

    return _execute_gemini_request(url, headers, payload, "image-to-video")


def gemini_video_status(operation_name: str) -> Dict[str, Any]:
    """
    Check the status of a long-running video generation operation.

    Args:
        operation_name: The operation name returned from generate call

    Returns:
        Dict with status, progress, video_url (if complete), error (if failed)
    """
    api_key = _get_api_key()

    # Operation name format: operations/{operation_id}
    url = f"{GEMINI_API_BASE}/{operation_name}"

    headers = {
        "x-goog-api-key": api_key,
    }

    try:
        print(f"[Gemini] Checking status for operation: {operation_name}")
        r = requests.get(url, headers=headers, timeout=GEMINI_TIMEOUT)

        if not r.ok:
            if 400 <= r.status_code < 500:
                return {
                    "status": "failed",
                    "error": f"Gemini API error {r.status_code}: {r.text[:200]}",
                }
            raise GeminiServerError(r.status_code, f"Gemini server error {r.status_code}")

        result = r.json()

        # Parse the operation response
        if result.get("done"):
            if "error" in result:
                return {
                    "status": "failed",
                    "error": result["error"].get("message", "Unknown error"),
                }

            # Extract video from response
            response = result.get("response", {})
            predictions = response.get("predictions", [])

            if predictions and len(predictions) > 0:
                video_data = predictions[0]
                video_url = video_data.get("videoUri") or video_data.get("video", {}).get("uri")

                return {
                    "status": "done",
                    "video_url": video_url,
                    "metadata": video_data.get("metadata", {}),
                }

            return {
                "status": "failed",
                "error": "No video in response",
            }

        # Still processing
        metadata = result.get("metadata", {})
        progress = metadata.get("progressPercent", 0)

        return {
            "status": "processing",
            "progress": progress,
        }

    except (Timeout, RequestsConnectionError) as e:
        return {
            "status": "error",
            "error": f"Connection error: {str(e)}",
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
        }


def _execute_gemini_request(
    url: str,
    headers: Dict[str, str],
    payload: Dict[str, Any],
    action: str,
) -> Dict[str, Any]:
    """
    Execute a Gemini API request with retries.

    Returns:
        Dict with operation_name for polling, or immediate result
    """
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[Gemini] Attempt {attempt}/{MAX_RETRIES}: {action} (timeout={GEMINI_TIMEOUT[1]}s)")
            r = requests.post(url, headers=headers, json=payload, timeout=GEMINI_TIMEOUT)

            if not r.ok:
                # Don't retry 4xx errors
                if 400 <= r.status_code < 500:
                    error_detail = r.text[:500]
                    print(f"[Gemini] Client error {r.status_code}: {error_detail}")
                    raise RuntimeError(f"Gemini API error {r.status_code}: {error_detail}")
                # Retry 5xx errors
                raise GeminiServerError(r.status_code, f"Gemini server error {r.status_code}: {r.text[:200]}")

            result = r.json()
            print(f"[Gemini] Request successful on attempt {attempt}")

            # For long-running operations, return operation name for polling
            if "name" in result:
                return {
                    "operation_name": result["name"],
                    "status": "processing",
                }

            # Immediate result (unlikely for video)
            return result

        except (Timeout, RequestsConnectionError, GeminiServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                error_type = type(e).__name__
                if isinstance(e, GeminiServerError):
                    error_type = f"HTTP {e.status_code}"
                print(f"[Gemini] Attempt {attempt} failed ({error_type}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[Gemini] All {MAX_RETRIES} attempts failed")
        except RuntimeError:
            raise

    raise RuntimeError(f"Gemini request failed after {MAX_RETRIES} attempts: {last_error}")
