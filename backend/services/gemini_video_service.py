"""
Gemini Video Generation Service (Veo 3.1).

Uses the Gemini Developer API for video generation via Veo model.
Authentication: GEMINI_API_KEY only (via x-goog-api-key header).

Start Endpoint: POST https://generativelanguage.googleapis.com/v1beta/models/veo-3.1-generate-preview:predictLongRunning
Poll Endpoint: GET https://generativelanguage.googleapis.com/v1beta/<operation_name>

CURL Test Examples:

1) Start video generation:
    curl -X POST \
      'https://generativelanguage.googleapis.com/v1beta/models/veo-3.1-generate-preview:predictLongRunning' \
      -H 'x-goog-api-key: YOUR_GEMINI_API_KEY' \
      -H 'Content-Type: application/json' \
      -d '{
        "instances": [{"prompt": "A cat walking on a beach at sunset"}],
        "parameters": {"aspectRatio": "16:9", "resolution": "720p", "durationSeconds": 6}
      }'

2) Poll operation status:
    curl -X GET \
      'https://generativelanguage.googleapis.com/v1beta/operations/YOUR_OPERATION_NAME' \
      -H 'x-goog-api-key: YOUR_GEMINI_API_KEY'

CRITICAL CONSTRAINTS:
- aspectRatio: ONLY "16:9" or "9:16" (NO "1:1" for video!)
- resolution: ONLY "720p", "1080p", "4k" (lowercase 4k, NOT "4K")
- durationSeconds: ONLY 4, 6, 8 (as integers, NOT strings!)
- If resolution is "1080p" or "4k", durationSeconds MUST be 8

NOTE: Requires Gemini API Paid tier. Get your key from https://aistudio.google.com/apikey
"""

from __future__ import annotations

import time
import base64
import os
import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError
from typing import Dict, Any, Optional, Tuple

from backend.config import config

# Timeouts for video generation (can take a while)
GEMINI_TIMEOUT = (15, 300)  # (connect_timeout, read_timeout)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2  # seconds (exponential backoff)

# Gemini Developer API base URL
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"

# Veo model for video generation
VEO_MODEL = "veo-3.1-generate-preview"

# Allowed parameter values - STRICT validation
ALLOWED_VIDEO_ASPECT_RATIOS = {"16:9", "9:16"}  # NO "1:1" for video!
ALLOWED_RESOLUTIONS = {"720p", "1080p", "4k"}  # lowercase 4k
ALLOWED_DURATIONS = {4, 6, 8}  # as integers (NOT strings!)

# Resolution constraint: 1080p/4k REQUIRE 8 seconds
HIGH_RES_REQUIRES_8S = {"1080p", "4k"}


class GeminiAuthError(Exception):
    """Raised when Gemini authentication fails."""
    pass


class GeminiConfigError(Exception):
    """Raised when Gemini is not configured."""
    pass


class GeminiValidationError(Exception):
    """Raised for parameter validation errors."""
    def __init__(self, field: str, value: Any, allowed: list, message: Optional[str] = None):
        self.field = field
        self.value = value
        self.allowed = allowed
        self.message = message if message else f"Invalid {field}: {value}. Allowed: {allowed}"
        super().__init__(self.message)


class GeminiServerError(Exception):
    """Raised for 5xx errors from Gemini (retryable)."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


def _get_api_key() -> str:
    """
    Get the Gemini API key from config/environment.
    Falls back to GOOGLE_API_KEY for backward compatibility.
    """
    key = getattr(config, 'GEMINI_API_KEY', None) or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""
    if not key:
        raise GeminiConfigError(
            "GEMINI_API_KEY is not set. "
            "Get your API key from https://aistudio.google.com/apikey"
        )
    return key


def _get_headers() -> Dict[str, str]:
    """Get headers for Gemini API requests."""
    return {
        "Content-Type": "application/json",
        "x-goog-api-key": _get_api_key(),
    }


def check_gemini_configured() -> Tuple[bool, Optional[str]]:
    """
    Check if Gemini is configured. Returns (is_configured, error_message).
    Use this for fail-fast checks in routes.
    """
    try:
        _get_api_key()
        return True, None
    except GeminiConfigError as e:
        return False, str(e)


def validate_video_params(
    aspect_ratio: str,
    resolution: str,
    duration_seconds: Any,
) -> Tuple[str, str, int]:
    """
    Validate and normalize Veo video parameters.

    Returns normalized (aspect_ratio, resolution, duration_seconds).
    Raises GeminiValidationError if invalid.

    CRITICAL:
    - durationSeconds MUST be an integer (4, 6, or 8), NOT a string!
    - 1080p/4k requires 8 seconds duration.
    """
    # Normalize resolution (handle uppercase 4K -> 4k)
    if resolution == "4K":
        resolution = "4k"

    # Normalize duration to integer (parse from string if needed)
    try:
        # Handle "4 sec", "4s", "4", 4, etc.
        if isinstance(duration_seconds, str):
            # Strip common suffixes
            duration_str = duration_seconds.lower().replace("sec", "").replace("s", "").strip()
            duration_int = int(duration_str)
        else:
            duration_int = int(duration_seconds)
    except (ValueError, TypeError):
        raise GeminiValidationError(
            "durationSeconds",
            duration_seconds,
            list(ALLOWED_DURATIONS),
            f"Duration must be a number (4, 6, or 8). Got: {duration_seconds} (type: {type(duration_seconds).__name__})"
        )

    # Validate aspect ratio
    if aspect_ratio not in ALLOWED_VIDEO_ASPECT_RATIOS:
        raise GeminiValidationError(
            "aspectRatio",
            aspect_ratio,
            list(ALLOWED_VIDEO_ASPECT_RATIOS),
            f"Video aspect ratio must be 16:9 or 9:16. Got: {aspect_ratio}. "
            "Note: 1:1 is not supported for video generation."
        )

    # Validate resolution
    if resolution not in ALLOWED_RESOLUTIONS:
        raise GeminiValidationError(
            "resolution",
            resolution,
            list(ALLOWED_RESOLUTIONS),
            f"Resolution must be 720p, 1080p, or 4k (lowercase). Got: {resolution}"
        )

    # Validate duration
    if duration_int not in ALLOWED_DURATIONS:
        raise GeminiValidationError(
            "durationSeconds",
            duration_int,
            list(ALLOWED_DURATIONS),
            f"Duration must be 4, 6, or 8 seconds. Got: {duration_int}"
        )

    # CRITICAL CONSTRAINT: 1080p/4k requires 8 seconds
    if resolution in HIGH_RES_REQUIRES_8S and duration_int != 8:
        raise GeminiValidationError(
            "durationSeconds",
            duration_int,
            [8],
            f"Resolution {resolution} requires 8 seconds duration. "
            f"Either choose 720p for {duration_int}s, or use 8s for {resolution}."
        )

    return aspect_ratio, resolution, duration_int


def gemini_text_to_video(
    prompt: str,
    aspect_ratio: str = "16:9",
    resolution: str = "720p",
    duration_seconds: Any = 6,
    negative_prompt: Optional[str] = None,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Start a video generation from a text prompt using Gemini Veo 3.1.

    Args:
        prompt: Text description of the video to generate
        aspect_ratio: "16:9" or "9:16" (NO "1:1" for video)
        resolution: "720p", "1080p", or "4k" (lowercase)
        duration_seconds: 4, 6, or 8 (integer, 1080p/4k requires 8)
        negative_prompt: Things to avoid (if supported)
        seed: Random seed for reproducibility (if supported)

    Returns:
        Dict with operation_name for polling

    Raises:
        GeminiConfigError: If GEMINI_API_KEY not set
        GeminiValidationError: If parameters are invalid
        GeminiAuthError: If authentication fails
        RuntimeError: For other API errors
    """
    # Validate and normalize parameters (ensures duration_seconds is int)
    aspect_ratio, resolution, duration_int = validate_video_params(
        aspect_ratio, resolution, duration_seconds
    )

    # Build API URL
    url = f"{GEMINI_API_BASE}/models/{VEO_MODEL}:predictLongRunning"

    # Build payload - durationSeconds MUST be an integer
    payload = {
        "instances": [{"prompt": prompt}],
        "parameters": {
            "aspectRatio": aspect_ratio,
            "resolution": resolution,
            "durationSeconds": duration_int,  # integer, not string!
        }
    }

    # Add optional parameters if supported by API
    if negative_prompt:
        payload["parameters"]["negativePrompt"] = negative_prompt
    if seed is not None:
        payload["parameters"]["seed"] = int(seed)

    # Debug log: show payload types to verify durationSeconds is int
    print(f"[Gemini Veo] text-to-video payload types: "
          f"durationSeconds={duration_int} (type={type(duration_int).__name__}), "
          f"aspectRatio={aspect_ratio}, resolution={resolution}")

    return _execute_video_start_request(url, payload, "text-to-video")


def gemini_image_to_video(
    image_data: str,
    motion_prompt: str = "",
    aspect_ratio: str = "16:9",
    resolution: str = "720p",
    duration_seconds: Any = 6,
    negative_prompt: Optional[str] = None,
    seed: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Start a video generation from an image using Gemini Veo 3.1.

    Args:
        image_data: Base64-encoded image data or data URL
        motion_prompt: Description of motion/camera movement
        aspect_ratio: "16:9" or "9:16"
        resolution: "720p", "1080p", or "4k"
        duration_seconds: 4, 6, or 8 (integer)
        negative_prompt: Things to avoid
        seed: Random seed for reproducibility

    Returns:
        Dict with operation_name for polling
    """
    # Validate and normalize parameters (ensures duration_seconds is int)
    aspect_ratio, resolution, duration_int = validate_video_params(
        aspect_ratio, resolution, duration_seconds
    )

    # Parse image data
    image_bytes, mime_type = _parse_image_data(image_data)
    if not image_bytes:
        raise RuntimeError("gemini_video_failed: No valid image data provided")

    # Build API URL
    url = f"{GEMINI_API_BASE}/models/{VEO_MODEL}:predictLongRunning"

    # Build payload with image - durationSeconds MUST be an integer
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
            "resolution": resolution,
            "durationSeconds": duration_int,  # integer, not string!
        }
    }

    # Add optional parameters
    if negative_prompt:
        payload["parameters"]["negativePrompt"] = negative_prompt
    if seed is not None:
        payload["parameters"]["seed"] = int(seed)

    # Debug log: show payload types to verify durationSeconds is int
    print(f"[Gemini Veo] image-to-video payload types: "
          f"durationSeconds={duration_int} (type={type(duration_int).__name__}), "
          f"aspectRatio={aspect_ratio}, resolution={resolution}")

    return _execute_video_start_request(url, payload, "image-to-video")


def _parse_image_data(image_data: str) -> Tuple[str, str]:
    """Parse image data from various formats."""
    image_bytes = ""
    mime_type = "image/png"

    if image_data.startswith("data:"):
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
        try:
            resp = requests.get(image_data, timeout=30)
            if resp.ok:
                image_bytes = base64.b64encode(resp.content).decode('utf-8')
                content_type = resp.headers.get('content-type', 'image/png')
                if 'jpeg' in content_type or 'jpg' in content_type:
                    mime_type = "image/jpeg"
                elif 'webp' in content_type:
                    mime_type = "image/webp"
        except Exception as e:
            raise RuntimeError(f"gemini_video_failed: Failed to download image from URL: {e}")
    else:
        image_bytes = image_data

    return image_bytes, mime_type


def _execute_video_start_request(
    url: str,
    payload: Dict[str, Any],
    action: str,
) -> Dict[str, Any]:
    """
    Execute a Gemini Veo start request with retries.
    Returns operation_name for polling.
    """
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[Gemini Veo] Attempt {attempt}/{MAX_RETRIES}: {action}")

            r = requests.post(url, headers=_get_headers(), json=payload, timeout=GEMINI_TIMEOUT)

            if not r.ok:
                error_text = r.text[:500] if r.text else "No error details"
                print(f"[Gemini Veo] Error {r.status_code}: {error_text}")

                # Auth errors - don't retry
                if r.status_code in (401, 403):
                    raise GeminiAuthError(
                        "Gemini authentication failed. Check your GEMINI_API_KEY. "
                        "Ensure you have a paid tier API key from https://aistudio.google.com/apikey"
                    )

                # Parse error message
                error_msg = error_text
                try:
                    error_json = r.json()
                    error_msg = error_json.get("error", {}).get("message", error_text)
                except Exception:
                    pass

                # Check for quota/billing errors
                if r.status_code == 429 or "quota" in error_msg.lower() or "billing" in error_msg.lower():
                    raise RuntimeError(f"gemini_quota_or_billing: {error_msg}")

                # Don't retry 4xx errors
                if 400 <= r.status_code < 500:
                    raise RuntimeError(f"gemini_video_failed: {error_msg}")

                # Retry 5xx errors
                raise GeminiServerError(r.status_code, f"Gemini server error {r.status_code}: {error_text}")

            result = r.json()
            operation_name = result.get("name")

            if operation_name:
                print(f"[Gemini Veo] Operation started: {operation_name}")
                return {
                    "operation_name": operation_name,
                    "status": "processing",
                }
            else:
                raise RuntimeError("gemini_video_failed: No operation name in response")

        except (GeminiAuthError, GeminiConfigError):
            raise
        except (Timeout, RequestsConnectionError, GeminiServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[Gemini Veo] Attempt {attempt} failed, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[Gemini Veo] All {MAX_RETRIES} attempts failed")
        except RuntimeError:
            raise

    raise RuntimeError(f"gemini_video_failed: Request failed after {MAX_RETRIES} attempts: {last_error}")


def gemini_video_status(operation_name: str) -> Dict[str, Any]:
    """
    Check the status of a long-running video generation operation.

    Args:
        operation_name: The operation name returned from generate call
                        Format: "models/veo-3.1-generate-preview/operations/XYZ"
                        or just "operations/XYZ"

    Returns:
        Dict with:
        - status: "processing", "done", "failed", or "error"
        - progress: percentage (if processing)
        - video_url: URI to video (if done)
        - error: error message (if failed)
    """
    # Build status URL
    # Gemini returns operation names like "models/veo-3.1-generate-preview/operations/XYZ"
    # or sometimes just "operations/XYZ" - we need to handle both correctly
    if operation_name.startswith("models/") or operation_name.startswith("operations/"):
        # Full path - just prepend base URL
        url = f"{GEMINI_API_BASE}/{operation_name}"
    else:
        # Just the operation ID - need to add operations/ prefix
        url = f"{GEMINI_API_BASE}/operations/{operation_name}"

    try:
        print(f"[Gemini Veo] Polling operation: {operation_name}")
        print(f"[Gemini Veo] Poll URL: {url}")
        r = requests.get(url, headers=_get_headers(), timeout=GEMINI_TIMEOUT)

        if not r.ok:
            error_text = r.text[:300] if r.text else "No error details"
            print(f"[Gemini Veo] Poll failed: {r.status_code} - {error_text}")
            print(f"[Gemini Veo] Failed poll URL was: {url}")

            if r.status_code == 404:
                # 404 usually means wrong URL format or operation doesn't exist
                print(f"[Gemini Veo] 404 Not Found - operation_name={operation_name}")
                return {
                    "status": "failed",
                    "error": "gemini_video_failed",
                    "message": f"Gemini API error 404: Operation not found. URL: {url}",
                }

            if r.status_code in (401, 403):
                return {
                    "status": "failed",
                    "error": "gemini_auth_failed",
                    "message": "Gemini authentication failed – check GEMINI_API_KEY",
                }

            if 400 <= r.status_code < 500:
                return {
                    "status": "failed",
                    "error": "gemini_video_failed",
                    "message": f"Gemini API error {r.status_code}: {error_text}",
                }

            # Server error - mark as transient error (can retry)
            return {
                "status": "error",
                "error": "gemini_server_error",
                "message": f"Gemini server error {r.status_code}",
            }

        result = r.json()

        # Check if operation is done
        if result.get("done"):
            # Check for error in response
            if "error" in result:
                error_info = result["error"]
                error_msg = error_info.get("message", "Unknown error")
                print(f"[Gemini Veo] Operation failed: {error_msg}")
                return {
                    "status": "failed",
                    "error": "gemini_video_failed",
                    "message": error_msg,
                }

            # Extract video URL from response
            video_url = _extract_video_url(result)
            if video_url:
                print(f"[Gemini Veo] Video ready: {video_url[:100]}...")
                return {
                    "status": "done",
                    "video_url": video_url,
                }
            else:
                print(f"[Gemini Veo] No video URL found in response: {result}")
                return {
                    "status": "failed",
                    "error": "gemini_video_failed",
                    "message": "No video in response",
                }

        # Still processing
        metadata = result.get("metadata", {})
        progress = metadata.get("progressPercent", 0)

        print(f"[Gemini Veo] Still processing, progress={progress}%")
        return {
            "status": "processing",
            "progress": progress,
        }

    except GeminiAuthError:
        return {
            "status": "failed",
            "error": "gemini_auth_failed",
            "message": "Gemini authentication failed – check GEMINI_API_KEY",
        }
    except (Timeout, RequestsConnectionError) as e:
        return {
            "status": "error",
            "error": "connection_error",
            "message": f"Connection error: {str(e)}",
        }
    except Exception as e:
        print(f"[Gemini Veo] Error checking status: {e}")
        return {
            "status": "error",
            "error": "unknown_error",
            "message": str(e),
        }


def _extract_video_url(result: Dict[str, Any]) -> Optional[str]:
    """
    Extract video URL from completed operation response.

    Tries multiple response formats that Veo might use.
    """
    response = result.get("response", {})

    # Format 1: generateVideoResponse.generatedSamples[0].video.uri
    video_response = response.get("generateVideoResponse", {})
    generated_samples = video_response.get("generatedSamples", [])
    if generated_samples:
        video_data = generated_samples[0]
        video_info = video_data.get("video", {})
        video_url = video_info.get("uri")
        if video_url:
            return video_url

    # Format 2: direct generatedSamples
    generated_samples = response.get("generatedSamples", [])
    if generated_samples:
        video_data = generated_samples[0]
        video_url = video_data.get("video", {}).get("uri")
        if video_url:
            return video_url

    # Format 3: predictions format
    predictions = response.get("predictions", [])
    if predictions:
        video_data = predictions[0]
        video_url = video_data.get("videoUri") or video_data.get("video", {}).get("uri")
        if video_url:
            return video_url

    return None


def download_video_bytes(video_url: str) -> Tuple[bytes, str]:
    """
    Download video bytes from Gemini's generated video URL.

    Args:
        video_url: The video URI from Veo response

    Returns:
        Tuple of (video_bytes, content_type)
    """
    headers = _get_headers()
    # Remove Content-Type for GET request
    headers.pop("Content-Type", None)

    print(f"[Gemini Veo] Downloading video from: {video_url[:100]}...")

    try:
        r = requests.get(video_url, headers=headers, timeout=120, allow_redirects=True)

        if not r.ok:
            raise RuntimeError(f"Failed to download video: HTTP {r.status_code}")

        content_type = r.headers.get("Content-Type", "video/mp4")
        print(f"[Gemini Veo] Downloaded {len(r.content)} bytes, type={content_type}")

        return r.content, content_type

    except Exception as e:
        raise RuntimeError(f"gemini_video_failed: Failed to download video: {e}")


def extract_video_thumbnail(video_bytes: bytes, timestamp_sec: float = 1.0) -> Optional[bytes]:
    """
    Extract a thumbnail frame from video bytes using ffmpeg.

    Args:
        video_bytes: The video file bytes
        timestamp_sec: Time in seconds to extract the frame (default 1.0)

    Returns:
        JPEG image bytes, or None if extraction fails
    """
    import subprocess
    import tempfile
    import os

    temp_video = None
    temp_thumb = None

    try:
        # Write video to temp file
        with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as f:
            f.write(video_bytes)
            temp_video = f.name

        # Create temp file for thumbnail
        temp_thumb = tempfile.mktemp(suffix=".jpg")

        # Extract frame using ffmpeg
        # -ss before -i for faster seeking
        # -vframes 1 to get single frame
        # -q:v 2 for high quality JPEG
        cmd = [
            "ffmpeg",
            "-y",  # Overwrite output
            "-ss", str(timestamp_sec),
            "-i", temp_video,
            "-vframes", "1",
            "-q:v", "2",
            "-vf", "scale='min(640,iw)':'-1'",  # Max width 640px, preserve aspect
            temp_thumb
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=30
        )

        if result.returncode != 0:
            print(f"[Thumbnail] ffmpeg failed: {result.stderr.decode('utf-8', errors='ignore')[:200]}")
            return None

        # Read thumbnail
        if os.path.exists(temp_thumb) and os.path.getsize(temp_thumb) > 0:
            with open(temp_thumb, "rb") as f:
                thumb_bytes = f.read()
            print(f"[Thumbnail] Extracted {len(thumb_bytes)} bytes at {timestamp_sec}s")
            return thumb_bytes

        return None

    except subprocess.TimeoutExpired:
        print("[Thumbnail] ffmpeg timed out")
        return None
    except FileNotFoundError:
        print("[Thumbnail] ffmpeg not found - install ffmpeg for video thumbnails")
        return None
    except Exception as e:
        print(f"[Thumbnail] Error: {e}")
        return None
    finally:
        # Cleanup temp files
        if temp_video and os.path.exists(temp_video):
            try:
                os.unlink(temp_video)
            except:
                pass
        if temp_thumb and os.path.exists(temp_thumb):
            try:
                os.unlink(temp_thumb)
            except:
                pass
