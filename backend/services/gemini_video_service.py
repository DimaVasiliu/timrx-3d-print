"""
Gemini Video Generation Service.

Uses the Gemini Developer API (AI Studio) for video generation.
Authentication: GEMINI_API_KEY only (no GCP project, OAuth, or service accounts).

Supports:
- Text-to-video generation via Veo model
- Image-to-video generation via Veo model
"""

from __future__ import annotations

import time
import base64
import os
import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError
from typing import Optional, Dict, Any

from backend.config import config

# Timeouts for video generation (can take a while)
GEMINI_TIMEOUT = (15, 300)  # (connect_timeout, read_timeout)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2  # seconds (exponential backoff)

# Gemini Developer API base URL
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"

# Video generation model
VEO_MODEL = "veo-2.0-generate-001"


class GeminiAuthError(Exception):
    """Raised when Gemini authentication fails."""
    pass


class GeminiServerError(Exception):
    """Raised for 5xx errors from Gemini (retryable)."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


def _get_api_key() -> str:
    """
    Get the Gemini API key from config/environment.

    Raises:
        GeminiAuthError: If GEMINI_API_KEY is not configured.
    """
    # Try config first, then environment
    key = getattr(config, 'GEMINI_API_KEY', None) or os.getenv("GEMINI_API_KEY") or ""

    if not key:
        raise GeminiAuthError(
            "GEMINI_API_KEY is not set. "
            "Get your API key from https://aistudio.google.com/apikey"
        )
    return key


def validate_api_key() -> bool:
    """
    Validate that GEMINI_API_KEY is configured.
    Call this at startup for fail-fast behavior.

    Returns:
        True if configured, raises otherwise.
    """
    _get_api_key()
    return True


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
        duration_sec: Video duration in seconds (5 or 8)
        fps: Frames per second (24)
        aspect_ratio: Aspect ratio ("16:9", "9:16", or "1:1")
        resolution: Video resolution ("720p", "1080p")
        audio: Whether to generate audio (not yet supported)
        loop_seamlessly: Whether video should loop seamlessly

    Returns:
        Dict with operation_name for polling, or error
    """
    api_key = _get_api_key()

    # Veo supports 5s or 8s videos
    if duration_sec > 6:
        duration_sec = 8
    else:
        duration_sec = 5

    # Build API URL with key parameter
    url = f"{GEMINI_API_BASE}/models/{VEO_MODEL}:predictLongRunning?key={api_key}"

    headers = {
        "Content-Type": "application/json",
    }

    payload = {
        "instances": [{
            "prompt": prompt,
        }],
        "parameters": {
            "aspectRatio": aspect_ratio,
            "personGeneration": "allow_adult",
            "durationSeconds": duration_sec,
            "enhancePrompt": True,
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
        image_data: Base64-encoded image data or data URL
        motion_prompt: Description of motion/camera movement
        duration_sec: Video duration in seconds
        fps: Frames per second
        aspect_ratio: Aspect ratio
        resolution: Video resolution
        audio: Whether to generate audio
        loop_seamlessly: Whether video should loop seamlessly

    Returns:
        Dict with operation_name for polling, or error
    """
    api_key = _get_api_key()

    # Handle image data - extract base64
    image_bytes = ""
    mime_type = "image/png"

    if image_data.startswith("data:"):
        # Data URL - extract base64
        parts = image_data.split(",", 1)
        if len(parts) == 2:
            header = parts[0]
            image_bytes = parts[1]
            if "image/jpeg" in header or "image/jpg" in header:
                mime_type = "image/jpeg"
            elif "image/png" in header:
                mime_type = "image/png"
            elif "image/webp" in header:
                mime_type = "image/webp"
    elif image_data.startswith("http"):
        # URL - download and convert to base64
        try:
            resp = requests.get(image_data, timeout=30)
            if resp.ok:
                image_bytes = base64.b64encode(resp.content).decode('utf-8')
                content_type = resp.headers.get('content-type', 'image/png')
                if 'jpeg' in content_type or 'jpg' in content_type:
                    mime_type = "image/jpeg"
        except Exception as e:
            raise RuntimeError(f"Failed to download image from URL: {e}")
    else:
        # Assume raw base64
        image_bytes = image_data

    if not image_bytes:
        raise RuntimeError("No valid image data provided")

    # Veo supports 5s or 8s videos
    if duration_sec > 6:
        duration_sec = 8
    else:
        duration_sec = 5

    # Build API URL with key parameter
    url = f"{GEMINI_API_BASE}/models/{VEO_MODEL}:predictLongRunning?key={api_key}"

    headers = {
        "Content-Type": "application/json",
    }

    payload = {
        "instances": [{
            "prompt": motion_prompt or "Animate this image with natural, smooth motion",
            "image": {
                "bytesBase64Encoded": image_bytes,
                "mimeType": mime_type,
            }
        }],
        "parameters": {
            "aspectRatio": aspect_ratio,
            "durationSeconds": duration_sec,
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

    # Build status URL
    # Operation name format: operations/{operation_id}
    if operation_name.startswith("operations/"):
        url = f"{GEMINI_API_BASE}/{operation_name}?key={api_key}"
    else:
        url = f"{GEMINI_API_BASE}/operations/{operation_name}?key={api_key}"

    headers = {
        "Content-Type": "application/json",
    }

    try:
        print(f"[Gemini] Checking status for operation: {operation_name}")
        r = requests.get(url, headers=headers, timeout=GEMINI_TIMEOUT)

        if not r.ok:
            error_text = r.text[:200] if r.text else "No error details"

            # Check for auth errors
            if r.status_code == 401 or r.status_code == 403:
                print(f"[Gemini] Authentication failed – check GEMINI_API_KEY")
                return {
                    "status": "failed",
                    "error": "Gemini authentication failed – check GEMINI_API_KEY",
                }

            if 400 <= r.status_code < 500:
                return {
                    "status": "failed",
                    "error": f"Gemini API error {r.status_code}: {error_text}",
                }
            raise GeminiServerError(r.status_code, f"Gemini server error {r.status_code}")

        result = r.json()
        print(f"[Gemini] Operation response: {result}")

        # Parse the operation response
        if result.get("done"):
            if "error" in result:
                error_info = result["error"]
                error_msg = error_info.get("message", "Unknown error")
                return {
                    "status": "failed",
                    "error": error_msg,
                }

            # Extract video from response
            response = result.get("response", {})

            # Veo returns generated videos in different formats
            generated_samples = response.get("generatedSamples", [])
            if generated_samples:
                video_data = generated_samples[0]
                video_uri = video_data.get("video", {}).get("uri")
                video_gcs = video_data.get("video", {}).get("gcsUri")

                video_url = video_uri or video_gcs
                if video_url:
                    return {
                        "status": "done",
                        "video_url": video_url,
                        "metadata": video_data.get("video", {}),
                    }

            # Alternative response format
            predictions = response.get("predictions", [])
            if predictions:
                video_data = predictions[0]
                video_url = video_data.get("videoUri") or video_data.get("video", {}).get("uri")
                if video_url:
                    return {
                        "status": "done",
                        "video_url": video_url,
                        "metadata": video_data,
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
        print(f"[Gemini] Error checking status: {e}")
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
            print(f"[Gemini] Attempt {attempt}/{MAX_RETRIES}: {action}")
            r = requests.post(url, headers=headers, json=payload, timeout=GEMINI_TIMEOUT)

            if not r.ok:
                error_text = r.text[:500] if r.text else "No error details"
                print(f"[Gemini] Error response: {error_text}")

                # Check for auth errors - don't retry these
                if r.status_code == 401 or r.status_code == 403:
                    print(f"[Gemini] Authentication failed – check GEMINI_API_KEY")
                    raise GeminiAuthError("Gemini authentication failed – check GEMINI_API_KEY")

                # Don't retry other 4xx errors
                if 400 <= r.status_code < 500:
                    try:
                        error_json = r.json()
                        error_msg = error_json.get("error", {}).get("message", error_text)
                    except:
                        error_msg = error_text
                    raise RuntimeError(f"Gemini API error {r.status_code}: {error_msg}")

                # Retry 5xx errors
                raise GeminiServerError(r.status_code, f"Gemini server error {r.status_code}: {error_text}")

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

        except GeminiAuthError:
            # Don't retry auth errors
            raise
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
