"""
Gemini Image Generation Service (Imagen 4.0).

Uses the Gemini Developer API for image generation via Imagen model.
Authentication: GEMINI_API_KEY only (via x-goog-api-key header).

Endpoint: POST https://generativelanguage.googleapis.com/v1beta/models/imagen-4.0-generate-001:predict

CURL Test Example:
    curl -X POST \
      'https://generativelanguage.googleapis.com/v1beta/models/imagen-4.0-generate-001:predict' \
      -H 'x-goog-api-key: YOUR_GEMINI_API_KEY' \
      -H 'Content-Type: application/json' \
      -d '{
        "instances": [{"prompt": "A cute robot painting a sunset"}],
        "parameters": {"sampleCount": 1, "imageSize": "1K", "aspectRatio": "1:1"}
      }'

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

# Timeouts
GEMINI_TIMEOUT = (15, 120)  # (connect_timeout, read_timeout)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2

# Gemini Developer API base URL
GEMINI_API_BASE = "https://generativelanguage.googleapis.com/v1beta"

# Imagen model for image generation
IMAGEN_MODEL = "imagen-4.0-generate-001"

# Allowed parameter values
ALLOWED_ASPECT_RATIOS = {"1:1", "3:4", "4:3", "9:16", "16:9"}
ALLOWED_IMAGE_SIZES = {"1K", "2K"}


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


def validate_image_params(aspect_ratio: str, image_size: str) -> None:
    """
    Validate Imagen parameters. Raises GeminiValidationError if invalid.
    """
    if aspect_ratio not in ALLOWED_ASPECT_RATIOS:
        raise GeminiValidationError("aspectRatio", aspect_ratio, list(ALLOWED_ASPECT_RATIOS))
    if image_size not in ALLOWED_IMAGE_SIZES:
        raise GeminiValidationError("imageSize", image_size, list(ALLOWED_IMAGE_SIZES))


def gemini_generate_image(
    prompt: str,
    aspect_ratio: str = "1:1",
    image_size: str = "1K",
    sample_count: int = 1,
    negative_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate an image from a text prompt using Gemini Imagen 4.0.

    Args:
        prompt: Text description of the image to generate
        aspect_ratio: "1:1", "3:4", "4:3", "9:16", "16:9"
        image_size: "1K" or "2K"
        sample_count: Number of images to generate (1-4)
        negative_prompt: What to avoid in the image (not supported by Imagen, kept for API compat)

    Returns:
        Dict with image_url (data URL), image_base64, etc.

    Raises:
        GeminiConfigError: If GEMINI_API_KEY not set
        GeminiValidationError: If parameters are invalid
        GeminiAuthError: If authentication fails
        RuntimeError: For other API errors
    """
    # Validate parameters
    validate_image_params(aspect_ratio, image_size)

    # Clamp sample_count
    sample_count = max(1, min(4, sample_count))

    # Build API URL
    url = f"{GEMINI_API_BASE}/models/{IMAGEN_MODEL}:predict"

    # Build payload
    payload = {
        "instances": [{"prompt": prompt}],
        "parameters": {
            "sampleCount": sample_count,
            "imageSize": image_size,
            "aspectRatio": aspect_ratio,
        }
    }

    print(f"[Gemini Imagen] Request: model={IMAGEN_MODEL}, aspectRatio={aspect_ratio}, imageSize={image_size}")

    return _execute_image_request(url, payload)


def _execute_image_request(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a Gemini Imagen request with retries."""
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[Gemini Imagen] Attempt {attempt}/{MAX_RETRIES}")

            r = requests.post(url, headers=_get_headers(), json=payload, timeout=GEMINI_TIMEOUT)

            if not r.ok:
                error_text = r.text[:500] if r.text else "No error details"
                print(f"[Gemini Imagen] Error {r.status_code}: {error_text}")

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

                if 400 <= r.status_code < 500:
                    raise RuntimeError(f"gemini_image_failed: {error_msg}")

                raise GeminiServerError(r.status_code, f"Gemini server error {r.status_code}: {error_text}")

            result = r.json()
            print(f"[Gemini Imagen] Request successful")

            # Parse response - extract images
            return _parse_imagen_response(result)

        except (GeminiAuthError, GeminiConfigError):
            raise
        except (Timeout, RequestsConnectionError, GeminiServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[Gemini Imagen] Attempt {attempt} failed, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[Gemini Imagen] All {MAX_RETRIES} attempts failed")
        except RuntimeError:
            raise

    raise RuntimeError(f"gemini_image_failed: Request failed after {MAX_RETRIES} attempts: {last_error}")


def _parse_imagen_response(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse the Imagen response to extract images.

    Imagen response format:
    {
      "predictions": [
        {
          "bytesBase64Encoded": "<base64_image_data>",
          "mimeType": "image/png"
        }
      ]
    }
    """
    images = []

    predictions = result.get("predictions", [])
    if not predictions:
        # Check for error in response
        error = result.get("error", {})
        if error:
            error_msg = error.get("message", "Unknown error")
            raise RuntimeError(f"gemini_image_failed: {error_msg}")
        raise RuntimeError("gemini_image_failed: No images generated in response")

    for pred in predictions:
        image_base64 = pred.get("bytesBase64Encoded", "")
        mime_type = pred.get("mimeType", "image/png")

        if image_base64:
            data_url = f"data:{mime_type};base64,{image_base64}"
            images.append({
                "url": data_url,
                "base64": image_base64,
                "mime_type": mime_type,
            })

    if not images:
        raise RuntimeError("gemini_image_failed: No valid images in response")

    # Return first image as primary, all as list
    return {
        "ok": True,
        "image_url": images[0]["url"],
        "image_base64": images[0]["base64"],
        "mime_type": images[0]["mime_type"],
        "image_urls": [img["url"] for img in images],
        "images": images,
        "provider": "google",
        "model": IMAGEN_MODEL,
    }
