"""
Direct Google Gemini image generation service for TimrX.

Uses Gemini's native image model instead of PiAPI. This is kept beside the
existing PiAPI Nano Banana route so TimrX can compare latency, reliability,
and cost before changing defaults.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout

from backend.config import config

GOOGLE_NANO_TIMEOUT = (15, 180)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2
GOOGLE_API_BASE = "https://generativelanguage.googleapis.com/v1beta"
GOOGLE_NANO_MODEL = "gemini-2.5-flash-image"
ALLOWED_ASPECT_RATIOS = {"1:1", "2:3", "3:2", "3:4", "4:3", "4:5", "5:4", "9:16", "16:9", "21:9"}
ALLOWED_IMAGE_SIZES = {"1K"}


class GoogleNanoAuthError(Exception):
    pass


class GoogleNanoConfigError(Exception):
    pass


class GoogleNanoValidationError(Exception):
    def __init__(self, field: str, value: Any, allowed: list[str], message: Optional[str] = None):
        self.field = field
        self.value = value
        self.allowed = allowed
        self.message = message or f"Invalid {field}: {value}. Allowed: {allowed}"
        super().__init__(self.message)


class GoogleNanoServerError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


def _get_api_key() -> str:
    key = getattr(config, "GEMINI_API_KEY", None) or os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""
    if not key:
        raise GoogleNanoConfigError(
            "GEMINI_API_KEY is not set. "
            "Get your API key from https://aistudio.google.com/apikey"
        )
    return key


def _get_headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "x-goog-api-key": _get_api_key(),
    }


def check_google_nano_configured() -> Tuple[bool, Optional[str]]:
    try:
        _get_api_key()
        return True, None
    except GoogleNanoConfigError as e:
        return False, str(e)


def validate_google_nano_params(aspect_ratio: str, image_size: str) -> None:
    if aspect_ratio not in ALLOWED_ASPECT_RATIOS:
        raise GoogleNanoValidationError("aspect_ratio", aspect_ratio, sorted(ALLOWED_ASPECT_RATIOS))
    if image_size not in ALLOWED_IMAGE_SIZES:
        raise GoogleNanoValidationError("image_size", image_size, sorted(ALLOWED_IMAGE_SIZES))


def google_nano_generate_image(
    prompt: str,
    aspect_ratio: str = "1:1",
    image_size: str = "1K",
) -> Dict[str, Any]:
    validate_google_nano_params(aspect_ratio, image_size)

    url = f"{GOOGLE_API_BASE}/models/{GOOGLE_NANO_MODEL}:generateContent"
    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
            ]
        }],
        "generationConfig": {
            "responseModalities": ["IMAGE"],
            "imageConfig": {
                "aspectRatio": aspect_ratio,
                "imageSize": image_size,
            },
        },
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, headers=_get_headers(), json=payload, timeout=GOOGLE_NANO_TIMEOUT)
            if not response.ok:
                error_text = response.text[:500] if response.text else "No error details"
                if response.status_code in (401, 403):
                    raise GoogleNanoAuthError("Google Nano authentication failed. Check GEMINI_API_KEY.")

                error_message = error_text
                try:
                    error_json = response.json()
                    error_message = error_json.get("error", {}).get("message", error_text)
                except Exception:
                    pass

                if response.status_code == 429 or "quota" in error_message.lower() or "billing" in error_message.lower():
                    raise RuntimeError(f"google_nano_quota_or_billing: {error_message}")
                if 400 <= response.status_code < 500:
                    raise RuntimeError(f"google_nano_failed: {error_message}")
                raise GoogleNanoServerError(response.status_code, error_message)

            return _parse_google_nano_response(response.json())
        except (GoogleNanoAuthError, GoogleNanoConfigError, GoogleNanoValidationError):
            raise
        except (Timeout, RequestsConnectionError, GoogleNanoServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(BASE_RETRY_DELAY * (2 ** (attempt - 1)))
                continue
        except RuntimeError:
            raise

    raise RuntimeError(f"google_nano_failed: Request failed after {MAX_RETRIES} attempts: {last_error}")


def _parse_google_nano_response(result: Dict[str, Any]) -> Dict[str, Any]:
    images = []
    candidates = result.get("candidates") or []

    for candidate in candidates:
        content = candidate.get("content") or {}
        parts = content.get("parts") or []
        for part in parts:
            inline = part.get("inlineData") or part.get("inline_data") or {}
            data = inline.get("data")
            mime_type = inline.get("mimeType") or inline.get("mime_type") or "image/png"
            if data:
                images.append({
                    "base64": data,
                    "mime_type": mime_type,
                    "url": f"data:{mime_type};base64,{data}",
                })

    if not images:
        error = result.get("error", {})
        if error:
            raise RuntimeError(f"google_nano_failed: {error.get('message', 'Unknown error')}")
        raise RuntimeError("google_nano_failed: No image returned by Google Nano")

    return {
        "ok": True,
        "image_url": images[0]["url"],
        "image_base64": images[0]["base64"],
        "mime_type": images[0]["mime_type"],
        "image_urls": [img["url"] for img in images],
        "images": images,
        "provider": "google_nano",
        "provider_variant": "direct_google",
        "model": GOOGLE_NANO_MODEL,
    }
