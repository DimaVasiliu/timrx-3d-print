"""
Vertex AI Veo Video Generation Service.

Uses Vertex AI's Long-Running Operations (LRO) API for video generation via Veo models.
Authentication: Service Account JSON (GOOGLE_APPLICATION_CREDENTIALS_JSON env var).

===============================================================================
ENVIRONMENT VARIABLES REQUIRED
===============================================================================

For Vertex AI (Production):
  VIDEO_PROVIDER=vertex                    # Use Vertex AI as primary (default)
  GOOGLE_CLOUD_PROJECT=timrx-20b4b         # Your GCP project ID
  GOOGLE_APPLICATION_CREDENTIALS_JSON=...  # Full JSON string of service account key
  VERTEX_LOCATION=us-central1              # MUST be us-central1 for Veo quota
  VIDEO_QUALITY=fast                       # "fast" or "hq"
  VERTEX_MODEL_FAST=veo-3.1-fast-generate-001   # Fast model (default)
  VERTEX_MODEL_HQ=veo-3.1-generate-001          # High quality model

For AI Studio (Fallback):
  VIDEO_PROVIDER=aistudio                  # Use AI Studio as primary
  GEMINI_API_KEY=your_api_key              # From https://aistudio.google.com/apikey

==============================================================================="""
Vertex AI Veo Video Generation Service.

Uses Vertex AI's Long-Running Operations (LRO) API for video generation via Veo models.
Authentication: Service Account JSON (GOOGLE_APPLICATION_CREDENTIALS_JSON env var).

===============================================================================
ENVIRONMENT VARIABLES REQUIRED
===============================================================================

For Vertex AI (Production):
  VIDEO_PROVIDER=vertex                    # Use Vertex AI as primary (default)
  GOOGLE_CLOUD_PROJECT=timrx-20b4b         # Your GCP project ID
  GOOGLE_APPLICATION_CREDENTIALS_JSON=...  # Full JSON string of service account key
  VERTEX_LOCATION=us-central1              # MUST be us-central1 for Veo quota
  VIDEO_QUALITY=fast                       # "fast" or "hq"
  VERTEX_MODEL_FAST=veo-3.1-fast-generate-001   # Fast model (default)
  VERTEX_MODEL_HQ=veo-3.1-generate-001          # High quality model

For AI Studio (Fallback):
  VIDEO_PROVIDER=aistudio                  # Use AI Studio as primary
  GEMINI_API_KEY=your_api_key              # From https://aistudio.google.com/apikey

===============================================================================
MODELS SUPPORTED
===============================================================================

- veo-3.1-fast-generate-001: Fast model, lower latency, good for testing
- veo-3.1-generate-001: High quality model, longer generation time

===============================================================================
API ENDPOINTS
===============================================================================

Start job:
  POST https://{LOCATION}-aiplatform.googleapis.com/v1/projects/{PROJECT}/locations/{LOCATION}/publishers/google/models/{MODEL}:predictLongRunning

Poll status:
  GET https://{LOCATION}-aiplatform.googleapis.com/v1/{operation_name}

===============================================================================
CRITICAL NOTES
===============================================================================

1. VERTEX_LOCATION MUST be us-central1 for Veo - quota is only available there
2. Service account needs "Vertex AI User" role (roles/aiplatform.user)
3. Video constraints are the same as AI Studio:
   - aspectRatio: ONLY "16:9" or "9:16" (NO "1:1")
   - resolution: "720p", "1080p", "4k"
   - durationSeconds: 4, 6, 8 (integers)
   - 1080p/4k requires 8 seconds duration

===============================================================================
ADMIN ENDPOINTS FOR TESTING
===============================================================================

Get provider info:
  GET /api/_mod/video/admin/provider-info

Run smoke test:
  POST /api/_mod/video/admin/smoke-test
  Body: {"provider": "vertex", "prompt": "A cat on a beach"}

Check smoke test status:
  GET /api/_mod/video/admin/smoke-test/status?op=<operation_name>&provider=vertex

===============================================================================
SWITCHING PROVIDERS
===============================================================================

To use Vertex (production default):
  VIDEO_PROVIDER=vertex

To use AI Studio (fallback):
  VIDEO_PROVIDER=aistudio

The router automatically falls back to the next provider if the primary is:
- Not configured
- Quota exhausted
- Authentication failed
"""

from __future__ import annotations

import base64
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError

from backend.config import config


# Timeouts for Vertex AI operations
VERTEX_TIMEOUT = (15, 300)  # (connect_timeout, read_timeout)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2

# OAuth scope for Vertex AI
VERTEX_OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

# Token cache (in-memory, refreshed before expiry)
_token_cache: Dict[str, Any] = {}


class VertexAuthError(Exception):
    """Raised when Vertex AI authentication fails."""
    pass


class VertexConfigError(Exception):
    """Raised when Vertex AI is not configured."""
    pass


class VertexValidationError(Exception):
    """Raised for parameter validation errors."""
    def __init__(self, field: str, value: Any, allowed: list, message: Optional[str] = None):
        self.field = field
        self.value = value
        self.allowed = allowed
        self.message = message if message else f"Invalid {field}: {value}. Allowed: {allowed}"
        super().__init__(self.message)


class VertexServerError(Exception):
    """Raised for 5xx errors from Vertex AI (retryable)."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


class VertexQuotaError(Exception):
    """Raised when Vertex AI quota is exhausted."""
    pass


def _get_service_account_info() -> Dict[str, Any]:
    """
    Get service account credentials from environment.

    Supports:
    - GOOGLE_APPLICATION_CREDENTIALS_JSON: Full JSON string (for Render/cloud deployments)
    - GOOGLE_APPLICATION_CREDENTIALS: Path to JSON file (for local dev)
    """
    # Try JSON string first (preferred for cloud deployments)
    json_str = getattr(config, 'GOOGLE_APPLICATION_CREDENTIALS_JSON', None) or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if json_str:
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise VertexConfigError(f"Invalid GOOGLE_APPLICATION_CREDENTIALS_JSON: {e}")

    # Fall back to file path
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        try:
            with open(creds_path, 'r') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            raise VertexConfigError(f"Failed to read credentials file: {e}")

    raise VertexConfigError(
        "No service account credentials configured. "
        "Set GOOGLE_APPLICATION_CREDENTIALS_JSON (JSON string) or "
        "GOOGLE_APPLICATION_CREDENTIALS (file path)."
    )


def _create_jwt_for_token(service_account: Dict[str, Any]) -> str:
    """
    Create a signed JWT for exchanging to an access token.

    Uses the service account's private key to sign the JWT.
    """
    import jwt  # PyJWT

    now = int(time.time())
    payload = {
        "iss": service_account["client_email"],
        "sub": service_account["client_email"],
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,  # 1 hour
        "scope": VERTEX_OAUTH_SCOPE,
    }

    private_key = service_account["private_key"]

    return jwt.encode(payload, private_key, algorithm="RS256")


def _get_access_token() -> str:
    """
    Get a valid OAuth access token for Vertex AI.

    Uses cached token if still valid (with 5-minute buffer).
    Otherwise, creates a new JWT and exchanges it for an access token.
    """
    global _token_cache

    # Check cache
    if _token_cache.get("access_token"):
        expires_at = _token_cache.get("expires_at", 0)
        if time.time() < expires_at - 300:  # 5-minute buffer
            return _token_cache["access_token"]

    # Get service account and create JWT
    service_account = _get_service_account_info()
    signed_jwt = _create_jwt_for_token(service_account)

    # Exchange JWT for access token
    token_url = "https://oauth2.googleapis.com/token"
    token_data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": signed_jwt,
    }

    try:
        r = requests.post(token_url, data=token_data, timeout=30)
        if not r.ok:
            error_text = r.text[:500]
            raise VertexAuthError(f"Token exchange failed: {r.status_code} - {error_text}")

        token_resp = r.json()
        access_token = token_resp["access_token"]
        expires_in = token_resp.get("expires_in", 3600)

        # Cache token
        _token_cache = {
            "access_token": access_token,
            "expires_at": time.time() + expires_in,
        }

        print(f"[Vertex AI] Obtained access token, expires in {expires_in}s")
        return access_token

    except requests.RequestException as e:
        raise VertexAuthError(f"Token exchange request failed: {e}")


def _get_headers() -> Dict[str, str]:
    """Get headers for Vertex AI API requests."""
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_get_access_token()}",
    }


def _get_project_id() -> str:
    """Get Google Cloud project ID."""
    project = getattr(config, 'GOOGLE_CLOUD_PROJECT', None) or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project:
        # Try to get from service account
        try:
            sa = _get_service_account_info()
            project = sa.get("project_id")
        except VertexConfigError:
            pass

    if not project:
        raise VertexConfigError(
            "GOOGLE_CLOUD_PROJECT not set. "
            "Set the environment variable or include project_id in service account JSON."
        )
    return project


def _get_vertex_location() -> str:
    """Get Vertex AI location (MUST be us-central1 for Veo)."""
    return getattr(config, 'VERTEX_LOCATION', None) or os.getenv("VERTEX_LOCATION", "us-central1")


def _get_veo_model() -> str:
    """Get the Veo model to use based on VIDEO_QUALITY setting."""
    quality = getattr(config, 'VIDEO_QUALITY', None) or os.getenv("VIDEO_QUALITY", "fast")
    if quality.lower() == "hq":
        return getattr(config, 'VERTEX_MODEL_HQ', None) or os.getenv("VERTEX_MODEL_HQ", "veo-3.1-generate-001")
    return getattr(config, 'VERTEX_MODEL_FAST', None) or os.getenv("VERTEX_MODEL_FAST", "veo-3.1-fast-generate-001")


def check_vertex_configured() -> Tuple[bool, Optional[str]]:
    """
    Check if Vertex AI is configured for video generation.
    Returns (is_configured, error_message).
    """
    try:
        _get_project_id()
        _get_service_account_info()
        return True, None
    except VertexConfigError as e:
        return False, str(e)


def vertex_text_to_video(
    prompt: str,
    aspect_ratio: str = "16:9",
    resolution: str = "720p",
    duration_seconds: Any = 6,
    negative_prompt: Optional[str] = None,
    seed: Optional[int] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Start a video generation from a text prompt using Vertex AI Veo.

    Args:
        prompt: Text description of the video to generate
        aspect_ratio: "16:9" or "9:16"
        resolution: "720p", "1080p", or "4k"
        duration_seconds: 4, 6, or 8 (integer)
        negative_prompt: Things to avoid (optional)
        seed: Random seed for reproducibility (optional)
        model: Override model (optional, uses VIDEO_QUALITY setting otherwise)

    Returns:
        Dict with operation_name for polling
    """
    # Normalize duration to int
    duration_int = _normalize_duration(duration_seconds)

    # Validate parameters
    _validate_params(aspect_ratio, resolution, duration_int)

    # Get project, location, model
    project = _get_project_id()
    location = _get_vertex_location()
    model_id = model or _get_veo_model()

    # Build URL
    url = (
        f"https://{location}-aiplatform.googleapis.com/v1/"
        f"projects/{project}/locations/{location}/"
        f"publishers/google/models/{model_id}:predictLongRunning"
    )

    # Build payload
    payload = {
        "instances": [{"prompt": prompt}],
        "parameters": {
            "aspectRatio": aspect_ratio,
            "resolution": resolution,
            "durationSeconds": duration_int,
        }
    }

    if negative_prompt:
        payload["parameters"]["negativePrompt"] = negative_prompt
    if seed is not None:
        payload["parameters"]["seed"] = int(seed)

    print(f"[Vertex Veo] text-to-video: model={model_id}, duration={duration_int}s, "
          f"aspect={aspect_ratio}, resolution={resolution}")

    return _execute_video_start_request(url, payload, "text-to-video")


def vertex_image_to_video(
    image_data: str,
    motion_prompt: str = "",
    aspect_ratio: str = "16:9",
    resolution: str = "720p",
    duration_seconds: Any = 6,
    negative_prompt: Optional[str] = None,
    seed: Optional[int] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Start a video generation from an image using Vertex AI Veo.

    Args:
        image_data: Base64-encoded image or data URL
        motion_prompt: Description of motion/camera movement
        aspect_ratio: "16:9" or "9:16"
        resolution: "720p", "1080p", or "4k"
        duration_seconds: 4, 6, or 8 (integer)
        negative_prompt: Things to avoid (optional)
        seed: Random seed (optional)
        model: Override model (optional)

    Returns:
        Dict with operation_name for polling
    """
    # Normalize duration to int
    duration_int = _normalize_duration(duration_seconds)

    # Validate parameters
    _validate_params(aspect_ratio, resolution, duration_int)

    # Parse image data
    image_bytes, mime_type = _parse_image_data(image_data)
    if not image_bytes:
        raise RuntimeError("vertex_video_failed: No valid image data provided")

    # Get project, location, model
    project = _get_project_id()
    location = _get_vertex_location()
    model_id = model or _get_veo_model()

    # Build URL
    url = (
        f"https://{location}-aiplatform.googleapis.com/v1/"
        f"projects/{project}/locations/{location}/"
        f"publishers/google/models/{model_id}:predictLongRunning"
    )

    # Build payload with image
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
            "durationSeconds": duration_int,
        }
    }

    if negative_prompt:
        payload["parameters"]["negativePrompt"] = negative_prompt
    if seed is not None:
        payload["parameters"]["seed"] = int(seed)

    print(f"[Vertex Veo] image-to-video: model={model_id}, duration={duration_int}s, "
          f"aspect={aspect_ratio}, resolution={resolution}")

    return _execute_video_start_request(url, payload, "image-to-video")


def vertex_video_status(operation_name: str) -> Dict[str, Any]:
    """
    Check the status of a long-running video generation operation.

    Args:
        operation_name: The operation name EXACTLY as returned by Vertex.
                        Format: "projects/.../locations/.../publishers/google/models/.../operations/..."
                        DO NOT modify or normalize this path.

    Returns:
        Dict with:
        - status: "processing", "done", "failed", or "error"
        - progress: percentage (if processing)
        - video_url: URI to video (if done)
        - error: error message (if failed)
    """
    location = _get_vertex_location()

    # Use operation_name EXACTLY as returned by Vertex - do NOT normalize or modify it
    # Vertex returns full paths like:
    #   projects/{project}/locations/{location}/publishers/google/models/{model}/operations/{op_id}
    # This is the correct path for polling - do not strip /publishers/google/models/ segment

    if operation_name.startswith("projects/"):
        # Full path from Vertex - use as-is
        url = f"https://{location}-aiplatform.googleapis.com/v1/{operation_name}"
    else:
        # Fallback: assume it's just an operation ID (shouldn't happen with Vertex)
        project = _get_project_id()
        url = f"https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/operations/{operation_name}"
        print(f"[Vertex Veo] WARNING: operation_name doesn't start with 'projects/', using fallback URL")

    try:
        print(f"[Vertex Veo] Polling operation (raw): {operation_name}")
        print(f"[Vertex Veo] Poll URL: {url}")
        r = requests.get(url, headers=_get_headers(), timeout=VERTEX_TIMEOUT)

        if not r.ok:
            # Check if response is HTML or non-JSON (wrong endpoint/host)
            content_type = r.headers.get("Content-Type", "")
            if "text/html" in content_type:
                error_text = f"[HTML Response - wrong endpoint?] {r.text[:200]}"
            elif "application/json" not in content_type:
                error_text = f"[Non-JSON Response: {content_type}] {r.text[:200]}"
            else:
                error_text = r.text[:300] if r.text else "No error details"
            print(f"[Vertex Veo] Poll failed: {r.status_code} - {error_text}")

            if r.status_code == 404:
                return {
                    "status": "failed",
                    "error": "vertex_video_failed",
                    "message": f"Operation not found: {operation_name}",
                }

            if r.status_code in (401, 403):
                return {
                    "status": "failed",
                    "error": "vertex_auth_failed",
                    "message": "Vertex AI authentication failed",
                }

            if 400 <= r.status_code < 500:
                return {
                    "status": "failed",
                    "error": "vertex_video_failed",
                    "message": f"Vertex API error {r.status_code}: {error_text}",
                }

            return {
                "status": "error",
                "error": "vertex_server_error",
                "message": f"Vertex server error {r.status_code}",
            }

        result = r.json()

        # Check if operation is done
        if result.get("done"):
            # Check for error
            if "error" in result:
                error_info = result["error"]
                error_msg = error_info.get("message", "Unknown error")
                code = error_info.get("code", 0)
                print(f"[Vertex Veo] Operation failed: {code} - {error_msg}")
                return {
                    "status": "failed",
                    "error": "vertex_video_failed",
                    "message": error_msg,
                }

            # Extract video URL from response
            video_url = _extract_video_url(result)
            if video_url:
                print(f"[Vertex Veo] Video ready: {video_url[:100]}...")
                return {
                    "status": "done",
                    "video_url": video_url,
                }
            else:
                # Check for content filtering
                response = result.get("response", {})
                video_response = response.get("generateVideoResponse", {})
                filtered_reasons = video_response.get("raiMediaFilteredReasons", [])

                if filtered_reasons:
                    reasons_str = ", ".join(str(r) for r in filtered_reasons)
                    print(f"[Vertex Veo] Content filtered: {reasons_str}")
                    return {
                        "status": "failed",
                        "error": "provider_filtered_content",
                        "message": "Content blocked by safety filters. Try removing faces/logos/copyrighted content.",
                        "filtered_reasons": filtered_reasons,
                    }

                print(f"[Vertex Veo] No video URL in response: {result}")
                return {
                    "status": "failed",
                    "error": "vertex_video_failed",
                    "message": "No video in response",
                }

        # Still processing
        metadata = result.get("metadata", {})
        progress = metadata.get("progressPercent", 0)

        print(f"[Vertex Veo] Processing, progress={progress}%")
        return {
            "status": "processing",
            "progress": progress,
        }

    except VertexAuthError as e:
        return {
            "status": "failed",
            "error": "vertex_auth_failed",
            "message": str(e),
        }
    except (Timeout, RequestsConnectionError) as e:
        return {
            "status": "error",
            "error": "connection_error",
            "message": f"Connection error: {str(e)}",
        }
    except Exception as e:
        print(f"[Vertex Veo] Error checking status: {e}")
        return {
            "status": "error",
            "error": "unknown_error",
            "message": str(e),
        }


def download_video_bytes(video_url: str) -> Tuple[bytes, str]:
    """
    Download video bytes from Vertex AI's generated video URL.

    For GCS URIs (gs://...), uses authenticated access.
    For HTTPS URLs, downloads directly with auth headers.
    """
    headers = _get_headers()
    headers.pop("Content-Type", None)

    print(f"[Vertex Veo] Downloading video from: {video_url[:100]}...")

    try:
        # Handle GCS URIs
        if video_url.startswith("gs://"):
            # Convert gs:// to authenticated HTTPS URL
            # gs://bucket/path -> https://storage.googleapis.com/bucket/path
            parts = video_url[5:].split("/", 1)
            bucket = parts[0]
            path = parts[1] if len(parts) > 1 else ""
            https_url = f"https://storage.googleapis.com/{bucket}/{path}"
            r = requests.get(https_url, headers=headers, timeout=120, allow_redirects=True)
        else:
            r = requests.get(video_url, headers=headers, timeout=120, allow_redirects=True)

        if not r.ok:
            raise RuntimeError(f"Failed to download video: HTTP {r.status_code}")

        content_type = r.headers.get("Content-Type", "video/mp4")
        print(f"[Vertex Veo] Downloaded {len(r.content)} bytes, type={content_type}")

        return r.content, content_type

    except Exception as e:
        raise RuntimeError(f"vertex_video_failed: Failed to download video: {e}")


# ─── Internal helpers ─────────────────────────────────────────────


def _normalize_duration(duration_seconds: Any) -> int:
    """Normalize duration to integer."""
    try:
        if isinstance(duration_seconds, str):
            return int(duration_seconds.lower().replace("sec", "").replace("s", "").strip())
        return int(duration_seconds)
    except (ValueError, TypeError):
        return 6  # Default


def _validate_params(aspect_ratio: str, resolution: str, duration_int: int) -> None:
    """Validate video parameters."""
    ALLOWED_ASPECTS = {"16:9", "9:16"}
    ALLOWED_RESOLUTIONS = {"720p", "1080p", "4k"}
    ALLOWED_DURATIONS = {4, 6, 8}
    HIGH_RES_REQUIRES_8S = {"1080p", "4k"}

    if aspect_ratio not in ALLOWED_ASPECTS:
        raise VertexValidationError("aspectRatio", aspect_ratio, list(ALLOWED_ASPECTS))

    # Normalize resolution
    if resolution == "4K":
        resolution = "4k"

    if resolution not in ALLOWED_RESOLUTIONS:
        raise VertexValidationError("resolution", resolution, list(ALLOWED_RESOLUTIONS))

    if duration_int not in ALLOWED_DURATIONS:
        raise VertexValidationError("durationSeconds", duration_int, list(ALLOWED_DURATIONS))

    if resolution in HIGH_RES_REQUIRES_8S and duration_int != 8:
        raise VertexValidationError(
            "durationSeconds",
            duration_int,
            [8],
            f"Resolution {resolution} requires 8 seconds duration"
        )


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
            raise RuntimeError(f"Failed to download image from URL: {e}")
    else:
        image_bytes = image_data

    return image_bytes, mime_type


def _execute_video_start_request(
    url: str,
    payload: Dict[str, Any],
    action: str,
) -> Dict[str, Any]:
    """Execute a Vertex AI video start request with retries."""
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[Vertex Veo] Attempt {attempt}/{MAX_RETRIES}: {action}")

            r = requests.post(url, headers=_get_headers(), json=payload, timeout=VERTEX_TIMEOUT)

            if not r.ok:
                error_text = r.text[:500] if r.text else "No error details"
                print(f"[Vertex Veo] Error {r.status_code}: {error_text}")

                # Auth errors - don't retry
                if r.status_code in (401, 403):
                    raise VertexAuthError(
                        "Vertex AI authentication failed. Check service account credentials."
                    )

                # Parse error message
                error_msg = error_text
                try:
                    error_json = r.json()
                    error_msg = error_json.get("error", {}).get("message", error_text)
                except Exception:
                    pass

                # Check for quota errors
                if r.status_code == 429 or "quota" in error_msg.lower() or "RESOURCE_EXHAUSTED" in error_msg:
                    raise VertexQuotaError(f"Vertex AI quota exhausted: {error_msg}")

                # Don't retry 4xx errors
                if 400 <= r.status_code < 500:
                    raise RuntimeError(f"vertex_video_failed: {error_msg}")

                # Retry 5xx errors
                raise VertexServerError(r.status_code, f"Vertex server error {r.status_code}")

            result = r.json()
            operation_name = result.get("name")

            if operation_name:
                print(f"[Vertex Veo] Operation started: {operation_name[:80]}...")
                return {
                    "operation_name": operation_name,
                    "status": "processing",
                }
            else:
                raise RuntimeError("vertex_video_failed: No operation name in response")

        except (VertexAuthError, VertexConfigError, VertexQuotaError):
            raise
        except (Timeout, RequestsConnectionError, VertexServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[Vertex Veo] Attempt {attempt} failed, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[Vertex Veo] All {MAX_RETRIES} attempts failed")
        except RuntimeError:
            raise

    raise RuntimeError(f"vertex_video_failed: Request failed after {MAX_RETRIES} attempts: {last_error}")


def _extract_video_url(result: Dict[str, Any]) -> Optional[str]:
    """Extract video URL from completed operation response."""
    response = result.get("response", {})

    # Format 1: generateVideoResponse.generatedSamples[0].video.uri
    video_response = response.get("generateVideoResponse", {})
    generated_samples = video_response.get("generatedSamples", [])
    if generated_samples:
        video_info = generated_samples[0].get("video", {})
        video_url = video_info.get("uri")
        if video_url:
            return video_url

    # Format 2: direct generatedSamples
    generated_samples = response.get("generatedSamples", [])
    if generated_samples:
        video_url = generated_samples[0].get("video", {}).get("uri")
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
MODELS SUPPORTED
===============================================================================

- veo-3.1-fast-generate-001: Fast model, lower latency, good for testing
- veo-3.1-generate-001: High quality model, longer generation time

===============================================================================
API ENDPOINTS
===============================================================================

Start job:
  POST https://{LOCATION}-aiplatform.googleapis.com/v1/projects/{PROJECT}/locations/{LOCATION}/publishers/google/models/{MODEL}:predictLongRunning

Poll status:
  GET https://{LOCATION}-aiplatform.googleapis.com/v1/{operation_name}

===============================================================================
CRITICAL NOTES
===============================================================================

1. VERTEX_LOCATION MUST be us-central1 for Veo - quota is only available there
2. Service account needs "Vertex AI User" role (roles/aiplatform.user)
3. Video constraints are the same as AI Studio:
   - aspectRatio: ONLY "16:9" or "9:16" (NO "1:1")
   - resolution: "720p", "1080p", "4k"
   - durationSeconds: 4, 6, 8 (integers)
   - 1080p/4k requires 8 seconds duration

===============================================================================
ADMIN ENDPOINTS FOR TESTING
===============================================================================

Get provider info:
  GET /api/_mod/video/admin/provider-info

Run smoke test:
  POST /api/_mod/video/admin/smoke-test
  Body: {"provider": "vertex", "prompt": "A cat on a beach"}

Check smoke test status:
  GET /api/_mod/video/admin/smoke-test/status?op=<operation_name>&provider=vertex

===============================================================================
SWITCHING PROVIDERS
===============================================================================

To use Vertex (production default):
  VIDEO_PROVIDER=vertex

To use AI Studio (fallback):
  VIDEO_PROVIDER=aistudio

The router automatically falls back to the next provider if the primary is:
- Not configured
- Quota exhausted
- Authentication failed
"""

from __future__ import annotations

import base64
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError

from backend.config import config


# Timeouts for Vertex AI operations
VERTEX_TIMEOUT = (15, 300)  # (connect_timeout, read_timeout)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2

# OAuth scope for Vertex AI
VERTEX_OAUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

# Token cache (in-memory, refreshed before expiry)
_token_cache: Dict[str, Any] = {}


class VertexAuthError(Exception):
    """Raised when Vertex AI authentication fails."""
    pass


class VertexConfigError(Exception):
    """Raised when Vertex AI is not configured."""
    pass


class VertexValidationError(Exception):
    """Raised for parameter validation errors."""
    def __init__(self, field: str, value: Any, allowed: list, message: Optional[str] = None):
        self.field = field
        self.value = value
        self.allowed = allowed
        self.message = message if message else f"Invalid {field}: {value}. Allowed: {allowed}"
        super().__init__(self.message)


class VertexServerError(Exception):
    """Raised for 5xx errors from Vertex AI (retryable)."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


class VertexQuotaError(Exception):
    """Raised when Vertex AI quota is exhausted."""
    pass


def _get_service_account_info() -> Dict[str, Any]:
    """
    Get service account credentials from environment.

    Supports:
    - GOOGLE_APPLICATION_CREDENTIALS_JSON: Full JSON string (for Render/cloud deployments)
    - GOOGLE_APPLICATION_CREDENTIALS: Path to JSON file (for local dev)
    """
    # Try JSON string first (preferred for cloud deployments)
    json_str = getattr(config, 'GOOGLE_APPLICATION_CREDENTIALS_JSON', None) or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if json_str:
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            raise VertexConfigError(f"Invalid GOOGLE_APPLICATION_CREDENTIALS_JSON: {e}")

    # Fall back to file path
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if creds_path and os.path.exists(creds_path):
        try:
            with open(creds_path, 'r') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            raise VertexConfigError(f"Failed to read credentials file: {e}")

    raise VertexConfigError(
        "No service account credentials configured. "
        "Set GOOGLE_APPLICATION_CREDENTIALS_JSON (JSON string) or "
        "GOOGLE_APPLICATION_CREDENTIALS (file path)."
    )


def _create_jwt_for_token(service_account: Dict[str, Any]) -> str:
    """
    Create a signed JWT for exchanging to an access token.

    Uses the service account's private key to sign the JWT.
    """
    import jwt  # PyJWT

    now = int(time.time())
    payload = {
        "iss": service_account["client_email"],
        "sub": service_account["client_email"],
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,  # 1 hour
        "scope": VERTEX_OAUTH_SCOPE,
    }

    private_key = service_account["private_key"]

    return jwt.encode(payload, private_key, algorithm="RS256")


def _get_access_token() -> str:
    """
    Get a valid OAuth access token for Vertex AI.

    Uses cached token if still valid (with 5-minute buffer).
    Otherwise, creates a new JWT and exchanges it for an access token.
    """
    global _token_cache

    # Check cache
    if _token_cache.get("access_token"):
        expires_at = _token_cache.get("expires_at", 0)
        if time.time() < expires_at - 300:  # 5-minute buffer
            return _token_cache["access_token"]

    # Get service account and create JWT
    service_account = _get_service_account_info()
    signed_jwt = _create_jwt_for_token(service_account)

    # Exchange JWT for access token
    token_url = "https://oauth2.googleapis.com/token"
    token_data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": signed_jwt,
    }

    try:
        r = requests.post(token_url, data=token_data, timeout=30)
        if not r.ok:
            error_text = r.text[:500]
            raise VertexAuthError(f"Token exchange failed: {r.status_code} - {error_text}")

        token_resp = r.json()
        access_token = token_resp["access_token"]
        expires_in = token_resp.get("expires_in", 3600)

        # Cache token
        _token_cache = {
            "access_token": access_token,
            "expires_at": time.time() + expires_in,
        }

        print(f"[Vertex AI] Obtained access token, expires in {expires_in}s")
        return access_token

    except requests.RequestException as e:
        raise VertexAuthError(f"Token exchange request failed: {e}")


def _get_headers() -> Dict[str, str]:
    """Get headers for Vertex AI API requests."""
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_get_access_token()}",
    }


def _get_project_id() -> str:
    """Get Google Cloud project ID."""
    project = getattr(config, 'GOOGLE_CLOUD_PROJECT', None) or os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project:
        # Try to get from service account
        try:
            sa = _get_service_account_info()
            project = sa.get("project_id")
        except VertexConfigError:
            pass

    if not project:
        raise VertexConfigError(
            "GOOGLE_CLOUD_PROJECT not set. "
            "Set the environment variable or include project_id in service account JSON."
        )
    return project


def _get_vertex_location() -> str:
    """Get Vertex AI location (MUST be us-central1 for Veo)."""
    return getattr(config, 'VERTEX_LOCATION', None) or os.getenv("VERTEX_LOCATION", "us-central1")


def _get_veo_model() -> str:
    """Get the Veo model to use based on VIDEO_QUALITY setting."""
    quality = getattr(config, 'VIDEO_QUALITY', None) or os.getenv("VIDEO_QUALITY", "fast")
    if quality.lower() == "hq":
        return getattr(config, 'VERTEX_MODEL_HQ', None) or os.getenv("VERTEX_MODEL_HQ", "veo-3.1-generate-001")
    return getattr(config, 'VERTEX_MODEL_FAST', None) or os.getenv("VERTEX_MODEL_FAST", "veo-3.1-fast-generate-001")


def check_vertex_configured() -> Tuple[bool, Optional[str]]:
    """
    Check if Vertex AI is configured for video generation.
    Returns (is_configured, error_message).
    """
    try:
        _get_project_id()
        _get_service_account_info()
        return True, None
    except VertexConfigError as e:
        return False, str(e)


def vertex_text_to_video(
    prompt: str,
    aspect_ratio: str = "16:9",
    resolution: str = "720p",
    duration_seconds: Any = 6,
    negative_prompt: Optional[str] = None,
    seed: Optional[int] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Start a video generation from a text prompt using Vertex AI Veo.

    Args:
        prompt: Text description of the video to generate
        aspect_ratio: "16:9" or "9:16"
        resolution: "720p", "1080p", or "4k"
        duration_seconds: 4, 6, or 8 (integer)
        negative_prompt: Things to avoid (optional)
        seed: Random seed for reproducibility (optional)
        model: Override model (optional, uses VIDEO_QUALITY setting otherwise)

    Returns:
        Dict with operation_name for polling
    """
    # Normalize duration to int
    duration_int = _normalize_duration(duration_seconds)

    # Validate parameters
    _validate_params(aspect_ratio, resolution, duration_int)

    # Get project, location, model
    project = _get_project_id()
    location = _get_vertex_location()
    model_id = model or _get_veo_model()

    # Build URL
    url = (
        f"https://{location}-aiplatform.googleapis.com/v1/"
        f"projects/{project}/locations/{location}/"
        f"publishers/google/models/{model_id}:predictLongRunning"
    )

    # Build payload
    payload = {
        "instances": [{"prompt": prompt}],
        "parameters": {
            "aspectRatio": aspect_ratio,
            "resolution": resolution,
            "durationSeconds": duration_int,
        }
    }

    if negative_prompt:
        payload["parameters"]["negativePrompt"] = negative_prompt
    if seed is not None:
        payload["parameters"]["seed"] = int(seed)

    print(f"[Vertex Veo] text-to-video: model={model_id}, duration={duration_int}s, "
          f"aspect={aspect_ratio}, resolution={resolution}")

    return _execute_video_start_request(url, payload, "text-to-video")


def vertex_image_to_video(
    image_data: str,
    motion_prompt: str = "",
    aspect_ratio: str = "16:9",
    resolution: str = "720p",
    duration_seconds: Any = 6,
    negative_prompt: Optional[str] = None,
    seed: Optional[int] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Start a video generation from an image using Vertex AI Veo.

    Args:
        image_data: Base64-encoded image or data URL
        motion_prompt: Description of motion/camera movement
        aspect_ratio: "16:9" or "9:16"
        resolution: "720p", "1080p", or "4k"
        duration_seconds: 4, 6, or 8 (integer)
        negative_prompt: Things to avoid (optional)
        seed: Random seed (optional)
        model: Override model (optional)

    Returns:
        Dict with operation_name for polling
    """
    # Normalize duration to int
    duration_int = _normalize_duration(duration_seconds)

    # Validate parameters
    _validate_params(aspect_ratio, resolution, duration_int)

    # Parse image data
    image_bytes, mime_type = _parse_image_data(image_data)
    if not image_bytes:
        raise RuntimeError("vertex_video_failed: No valid image data provided")

    # Get project, location, model
    project = _get_project_id()
    location = _get_vertex_location()
    model_id = model or _get_veo_model()

    # Build URL
    url = (
        f"https://{location}-aiplatform.googleapis.com/v1/"
        f"projects/{project}/locations/{location}/"
        f"publishers/google/models/{model_id}:predictLongRunning"
    )

    # Build payload with image
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
            "durationSeconds": duration_int,
        }
    }

    if negative_prompt:
        payload["parameters"]["negativePrompt"] = negative_prompt
    if seed is not None:
        payload["parameters"]["seed"] = int(seed)

    print(f"[Vertex Veo] image-to-video: model={model_id}, duration={duration_int}s, "
          f"aspect={aspect_ratio}, resolution={resolution}")

    return _execute_video_start_request(url, payload, "image-to-video")


def _normalize_operation_name(operation_name: str) -> str:
    """
    Normalize operation name to the correct polling path.

    Vertex returns operation names like:
      projects/{project}/locations/{location}/publishers/google/models/{model}/operations/{op_id}

    But polling should use:
      projects/{project}/locations/{location}/operations/{op_id}

    This function extracts the operation ID and builds the correct path.
    """
    # If it contains /publishers/google/models/, extract just the operation ID
    # and rebuild with the correct path
    if "/publishers/google/models/" in operation_name and "/operations/" in operation_name:
        # Extract: projects/{project}/locations/{location} and operations/{op_id}
        # From: projects/{project}/locations/{location}/publishers/google/models/{model}/operations/{op_id}
        parts = operation_name.split("/operations/")
        if len(parts) == 2:
            op_id = parts[1]
            # Extract project and location from the beginning
            # projects/{project}/locations/{location}/publishers/...
            prefix_parts = parts[0].split("/publishers/")[0]  # projects/{project}/locations/{location}
            normalized = f"{prefix_parts}/operations/{op_id}"
            print(f"[Vertex Veo] Normalized operation path: {operation_name[:60]}... -> {normalized}")
            return normalized

    # Already in correct format or just an operation ID
    return operation_name


def vertex_video_status(operation_name: str) -> Dict[str, Any]:
    """
    Check the status of a long-running video generation operation.

    Args:
        operation_name: The operation name from the start response
                        Format: "projects/.../locations/.../operations/..."
                        or: "projects/.../publishers/google/models/.../operations/..."

    Returns:
        Dict with:
        - status: "processing", "done", "failed", or "error"
        - progress: percentage (if processing)
        - video_url: URI to video (if done)
        - error: error message (if failed)
    """
    location = _get_vertex_location()
    project = _get_project_id()

    # Normalize the operation name to the correct polling path
    normalized_op = _normalize_operation_name(operation_name)

    # Build status URL
    if normalized_op.startswith("projects/"):
        url = f"https://{location}-aiplatform.googleapis.com/v1/{normalized_op}"
    else:
        # Assume it's just the operation ID - build full path
        url = f"https://{location}-aiplatform.googleapis.com/v1/projects/{project}/locations/{location}/operations/{normalized_op}"

    try:
        print(f"[Vertex Veo] Polling operation: {normalized_op}")
        print(f"[Vertex Veo] Poll URL: {url}")
        r = requests.get(url, headers=_get_headers(), timeout=VERTEX_TIMEOUT)

        if not r.ok:
            # Check if response is HTML (wrong endpoint/host)
            content_type = r.headers.get("Content-Type", "")
            if "text/html" in content_type:
                error_text = f"[HTML Response - wrong endpoint?] {r.text[:200]}"
            else:
                error_text = r.text[:300] if r.text else "No error details"
            print(f"[Vertex Veo] Poll failed: {r.status_code} - {error_text}")

            if r.status_code == 404:
                return {
                    "status": "failed",
                    "error": "vertex_video_failed",
                    "message": f"Operation not found: {operation_name}",
                }

            if r.status_code in (401, 403):
                return {
                    "status": "failed",
                    "error": "vertex_auth_failed",
                    "message": "Vertex AI authentication failed",
                }

            if 400 <= r.status_code < 500:
                return {
                    "status": "failed",
                    "error": "vertex_video_failed",
                    "message": f"Vertex API error {r.status_code}: {error_text}",
                }

            return {
                "status": "error",
                "error": "vertex_server_error",
                "message": f"Vertex server error {r.status_code}",
            }

        result = r.json()

        # Check if operation is done
        if result.get("done"):
            # Check for error
            if "error" in result:
                error_info = result["error"]
                error_msg = error_info.get("message", "Unknown error")
                code = error_info.get("code", 0)
                print(f"[Vertex Veo] Operation failed: {code} - {error_msg}")
                return {
                    "status": "failed",
                    "error": "vertex_video_failed",
                    "message": error_msg,
                }

            # Extract video URL from response
            video_url = _extract_video_url(result)
            if video_url:
                print(f"[Vertex Veo] Video ready: {video_url[:100]}...")
                return {
                    "status": "done",
                    "video_url": video_url,
                }
            else:
                # Check for content filtering
                response = result.get("response", {})
                video_response = response.get("generateVideoResponse", {})
                filtered_reasons = video_response.get("raiMediaFilteredReasons", [])

                if filtered_reasons:
                    reasons_str = ", ".join(str(r) for r in filtered_reasons)
                    print(f"[Vertex Veo] Content filtered: {reasons_str}")
                    return {
                        "status": "failed",
                        "error": "provider_filtered_content",
                        "message": "Content blocked by safety filters. Try removing faces/logos/copyrighted content.",
                        "filtered_reasons": filtered_reasons,
                    }

                print(f"[Vertex Veo] No video URL in response: {result}")
                return {
                    "status": "failed",
                    "error": "vertex_video_failed",
                    "message": "No video in response",
                }

        # Still processing
        metadata = result.get("metadata", {})
        progress = metadata.get("progressPercent", 0)

        print(f"[Vertex Veo] Processing, progress={progress}%")
        return {
            "status": "processing",
            "progress": progress,
        }

    except VertexAuthError as e:
        return {
            "status": "failed",
            "error": "vertex_auth_failed",
            "message": str(e),
        }
    except (Timeout, RequestsConnectionError) as e:
        return {
            "status": "error",
            "error": "connection_error",
            "message": f"Connection error: {str(e)}",
        }
    except Exception as e:
        print(f"[Vertex Veo] Error checking status: {e}")
        return {
            "status": "error",
            "error": "unknown_error",
            "message": str(e),
        }


def download_video_bytes(video_url: str) -> Tuple[bytes, str]:
    """
    Download video bytes from Vertex AI's generated video URL.

    For GCS URIs (gs://...), uses authenticated access.
    For HTTPS URLs, downloads directly with auth headers.
    """
    headers = _get_headers()
    headers.pop("Content-Type", None)

    print(f"[Vertex Veo] Downloading video from: {video_url[:100]}...")

    try:
        # Handle GCS URIs
        if video_url.startswith("gs://"):
            # Convert gs:// to authenticated HTTPS URL
            # gs://bucket/path -> https://storage.googleapis.com/bucket/path
            parts = video_url[5:].split("/", 1)
            bucket = parts[0]
            path = parts[1] if len(parts) > 1 else ""
            https_url = f"https://storage.googleapis.com/{bucket}/{path}"
            r = requests.get(https_url, headers=headers, timeout=120, allow_redirects=True)
        else:
            r = requests.get(video_url, headers=headers, timeout=120, allow_redirects=True)

        if not r.ok:
            raise RuntimeError(f"Failed to download video: HTTP {r.status_code}")

        content_type = r.headers.get("Content-Type", "video/mp4")
        print(f"[Vertex Veo] Downloaded {len(r.content)} bytes, type={content_type}")

        return r.content, content_type

    except Exception as e:
        raise RuntimeError(f"vertex_video_failed: Failed to download video: {e}")


# ─── Internal helpers ─────────────────────────────────────────────


def _normalize_duration(duration_seconds: Any) -> int:
    """Normalize duration to integer."""
    try:
        if isinstance(duration_seconds, str):
            return int(duration_seconds.lower().replace("sec", "").replace("s", "").strip())
        return int(duration_seconds)
    except (ValueError, TypeError):
        return 6  # Default


def _validate_params(aspect_ratio: str, resolution: str, duration_int: int) -> None:
    """Validate video parameters."""
    ALLOWED_ASPECTS = {"16:9", "9:16"}
    ALLOWED_RESOLUTIONS = {"720p", "1080p", "4k"}
    ALLOWED_DURATIONS = {4, 6, 8}
    HIGH_RES_REQUIRES_8S = {"1080p", "4k"}

    if aspect_ratio not in ALLOWED_ASPECTS:
        raise VertexValidationError("aspectRatio", aspect_ratio, list(ALLOWED_ASPECTS))

    # Normalize resolution
    if resolution == "4K":
        resolution = "4k"

    if resolution not in ALLOWED_RESOLUTIONS:
        raise VertexValidationError("resolution", resolution, list(ALLOWED_RESOLUTIONS))

    if duration_int not in ALLOWED_DURATIONS:
        raise VertexValidationError("durationSeconds", duration_int, list(ALLOWED_DURATIONS))

    if resolution in HIGH_RES_REQUIRES_8S and duration_int != 8:
        raise VertexValidationError(
            "durationSeconds",
            duration_int,
            [8],
            f"Resolution {resolution} requires 8 seconds duration"
        )


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
            raise RuntimeError(f"Failed to download image from URL: {e}")
    else:
        image_bytes = image_data

    return image_bytes, mime_type


def _execute_video_start_request(
    url: str,
    payload: Dict[str, Any],
    action: str,
) -> Dict[str, Any]:
    """Execute a Vertex AI video start request with retries."""
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[Vertex Veo] Attempt {attempt}/{MAX_RETRIES}: {action}")

            r = requests.post(url, headers=_get_headers(), json=payload, timeout=VERTEX_TIMEOUT)

            if not r.ok:
                error_text = r.text[:500] if r.text else "No error details"
                print(f"[Vertex Veo] Error {r.status_code}: {error_text}")

                # Auth errors - don't retry
                if r.status_code in (401, 403):
                    raise VertexAuthError(
                        "Vertex AI authentication failed. Check service account credentials."
                    )

                # Parse error message
                error_msg = error_text
                try:
                    error_json = r.json()
                    error_msg = error_json.get("error", {}).get("message", error_text)
                except Exception:
                    pass

                # Check for quota errors
                if r.status_code == 429 or "quota" in error_msg.lower() or "RESOURCE_EXHAUSTED" in error_msg:
                    raise VertexQuotaError(f"Vertex AI quota exhausted: {error_msg}")

                # Don't retry 4xx errors
                if 400 <= r.status_code < 500:
                    raise RuntimeError(f"vertex_video_failed: {error_msg}")

                # Retry 5xx errors
                raise VertexServerError(r.status_code, f"Vertex server error {r.status_code}")

            result = r.json()
            operation_name = result.get("name")

            if operation_name:
                print(f"[Vertex Veo] Operation started: {operation_name[:80]}...")
                return {
                    "operation_name": operation_name,
                    "status": "processing",
                }
            else:
                raise RuntimeError("vertex_video_failed: No operation name in response")

        except (VertexAuthError, VertexConfigError, VertexQuotaError):
            raise
        except (Timeout, RequestsConnectionError, VertexServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[Vertex Veo] Attempt {attempt} failed, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[Vertex Veo] All {MAX_RETRIES} attempts failed")
        except RuntimeError:
            raise

    raise RuntimeError(f"vertex_video_failed: Request failed after {MAX_RETRIES} attempts: {last_error}")


def _extract_video_url(result: Dict[str, Any]) -> Optional[str]:
    """Extract video URL from completed operation response."""
    response = result.get("response", {})

    # Format 1: generateVideoResponse.generatedSamples[0].video.uri
    video_response = response.get("generateVideoResponse", {})
    generated_samples = video_response.get("generatedSamples", [])
    if generated_samples:
        video_info = generated_samples[0].get("video", {})
        video_url = video_info.get("uri")
        if video_url:
            return video_url

    # Format 2: direct generatedSamples
    generated_samples = response.get("generatedSamples", [])
    if generated_samples:
        video_url = generated_samples[0].get("video", {}).get("uri")
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
