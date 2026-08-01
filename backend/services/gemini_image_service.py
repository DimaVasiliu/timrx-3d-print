"""
Google Image Generation Service (Gemini image models, "Nano Banana").

Uses the Gemini Developer API. Authentication: GEMINI_API_KEY (x-goog-api-key).

Endpoint: POST {base}/models/{model}:generateContent

MIGRATION (2026-08): this service used to call Imagen via the :predict API on
`imagen-4.0-fast-generate-001`. Google deprecated the entire Imagen line —
imagen-3.0-capability-001 was discontinued 2026-06-30 and every imagen-4.0-*
endpoint shut down 2026-08-17 — with `gemini-3.1-flash-image` as the documented
replacement for all of them.

That is not a model-string swap: Imagen used :predict with
{instances:[{prompt}], parameters:{sampleCount, imageSize, aspectRatio}} and
returned `predictions[].bytesBase64Encoded`, whereas the Gemini image models use
:generateContent with {contents:[{parts}], generationConfig:{responseModalities,
imageConfig}} and return `candidates[].content.parts[].inlineData`. Both the
request builder and the response parser below were rewritten accordingly.

Behaviour changes inherited from the new API:
  - sample_count is no longer supported (Gemini returns one image per request);
    the parameter is kept for call-compatibility and ignored.
  - negative_prompt is not supported (it never was on Imagen either).
  - 4K output is now available in addition to 1K/2K.

Model IDs are configured centrally — see config.GOOGLE_IMAGE_MODEL.

NOTE: Requires Gemini API paid tier. Key: https://aistudio.google.com/apikey
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

# Google image model. Env-overridable via GOOGLE_IMAGE_MODEL so the next vendor
# bump does not need a code change. `IMAGEN_MODEL` is kept as an alias because
# other modules import it by that name.
GOOGLE_IMAGE_MODEL = getattr(config, "GOOGLE_IMAGE_MODEL", None) or "gemini-3.1-flash-image"
IMAGEN_MODEL = GOOGLE_IMAGE_MODEL  # legacy alias

# Allowed parameter values. The Gemini image models accept a wider set of aspect
# ratios than Imagen did, and add 4K.
ALLOWED_ASPECT_RATIOS = {"1:1", "2:3", "3:2", "3:4", "4:3", "4:5", "5:4", "9:16", "16:9", "21:9"}
ALLOWED_IMAGE_SIZES = {"1K", "2K", "4K"}


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
    reference_images: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Generate (or reference-edit) an image using the configured Gemini image model.

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

    url = f"{GEMINI_API_BASE}/models/{GOOGLE_IMAGE_MODEL}:generateContent"

    parts = [{"text": prompt}]
    if reference_images:
        for src in reference_images:
            img_bytes, mime = _fetch_reference_bytes(src)
            parts.append({
                "inline_data": {
                    "mime_type": mime,
                    "data": base64.b64encode(img_bytes).decode("utf-8"),
                }
            })

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {
            "responseModalities": ["IMAGE"],
            "imageConfig": {
                "aspectRatio": aspect_ratio,
                "imageSize": image_size,
            },
        },
    }

    if sample_count and int(sample_count) > 1:
        # Imagen could return up to 4 per call; the Gemini image API returns one.
        print(f"[Google Image] sample_count={sample_count} ignored — the Gemini image API returns one image per request")

    print(
        f"[Google Image] Request: model={GOOGLE_IMAGE_MODEL}, aspectRatio={aspect_ratio}, "
        f"imageSize={image_size}, refs={len(reference_images or [])}"
    )

    return _execute_image_request(url, payload)


def _fetch_reference_bytes(src: str) -> Tuple[bytes, str]:
    """Decode a reference image (data URI or URL) into (bytes, mime) for inline_data."""
    from backend.services.video_providers.reference_media import (
        ReferenceMediaError,
        prepare_image,
    )
    try:
        prepared = prepare_image(src, provider="vertex")  # jpeg/png — safest for Gemini
        return prepared.data, prepared.mime
    except ReferenceMediaError as exc:
        raise GeminiValidationError("reference_images", "<image>", [], str(exc)) from exc


def _execute_image_request(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a Gemini Imagen request with retries."""
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[Google Image] Attempt {attempt}/{MAX_RETRIES}")

            r = requests.post(url, headers=_get_headers(), json=payload, timeout=GEMINI_TIMEOUT)

            if not r.ok:
                error_text = r.text[:500] if r.text else "No error details"
                print(f"[Google Image] Error {r.status_code}: {error_text}")

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
            print(f"[Google Image] Request successful")

            # Parse response - extract images
            return _parse_imagen_response(result)

        except (GeminiAuthError, GeminiConfigError):
            raise
        except (Timeout, RequestsConnectionError, GeminiServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[Google Image] Attempt {attempt} failed, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[Google Image] All {MAX_RETRIES} attempts failed")
        except RuntimeError:
            raise

    raise RuntimeError(f"gemini_image_failed: Request failed after {MAX_RETRIES} attempts: {last_error}")


def _parse_imagen_response(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse a Gemini image :generateContent response.

    Response shape:
    {
      "candidates": [
        {"content": {"parts": [{"inlineData": {"mimeType": "...", "data": "<b64>"}}]}}
      ]
    }

    (Named `_parse_imagen_response` for backwards compatibility with importers;
    the Imagen `predictions[]` shape it used to parse no longer exists.)
    """
    images = []

    for candidate in result.get("candidates") or []:
        content = candidate.get("content") or {}
        for part in content.get("parts") or []:
            inline = part.get("inlineData") or part.get("inline_data") or {}
            data = inline.get("data")
            mime_type = inline.get("mimeType") or inline.get("mime_type") or "image/png"
            if data:
                images.append({
                    "url": f"data:{mime_type};base64,{data}",
                    "base64": data,
                    "mime_type": mime_type,
                })

    if not images:
        error = result.get("error", {})
        if error:
            raise RuntimeError(f"gemini_image_failed: {error.get('message', 'Unknown error')}")
        # A prompt blocked by safety filters comes back with no parts and a reason.
        blocked = (result.get("promptFeedback") or {}).get("blockReason")
        if blocked:
            raise RuntimeError(f"gemini_image_failed: prompt blocked ({blocked})")
        raise RuntimeError("gemini_image_failed: No images generated in response")

    return {
        "ok": True,
        "image_url": images[0]["url"],
        "image_base64": images[0]["base64"],
        "mime_type": images[0]["mime_type"],
        "image_urls": [img["url"] for img in images],
        "images": images,
        "provider": "google",
        "model": GOOGLE_IMAGE_MODEL,
    }
