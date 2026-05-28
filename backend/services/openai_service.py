"""
OpenAI image generation service.

Model history:
  - gpt-image-1   : original (still works)
  - gpt-image-1.5 : latest — faster, better instruction following (March 2026)
  - DALL·E 2/3    : deprecated, shutting down May 12, 2026

Capabilities:
  - openai_image_generate(...) : text-to-image  via POST /v1/images/generations
  - openai_image_edit(...)     : image-to-image via POST /v1/images/edits
                                 (one or more reference images + prompt; optional mask)
"""

from __future__ import annotations

import base64
import time
from typing import List, Optional, Tuple

import requests
from requests.exceptions import Timeout, ConnectionError as RequestsConnectionError

from backend.config import config

# OpenAI image generation can take a while, especially for high-quality images
# gpt-image-1 can take 60-120s for complex prompts; allow generous timeout
OPENAI_TIMEOUT = (15, 180)  # (connect_timeout, read_timeout) - 3 minutes for generation
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2  # seconds (exponential backoff: 2s, 4s, 8s)


class OpenAIServerError(Exception):
    """Raised for 5xx errors from OpenAI (retryable)."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


def openai_image_generate(
    prompt: str,
    size: str = "1024x1024",
    model: str = "gpt-image-1.5",
    n: int = 1,
    response_format: str = "url",
) -> dict:
    if not config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    url = "https://api.openai.com/v1/images/generations"
    headers = {
        "Authorization": f"Bearer {config.OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "prompt": prompt,
        "size": size,
        "n": max(1, min(4, int(n or 1))),
    }
    # gpt-image-1 and gpt-image-1.5 don't support response_format param
    if model not in ("gpt-image-1", "gpt-image-1.5"):
        payload["response_format"] = response_format

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[OpenAI] Attempt {attempt}/{MAX_RETRIES}: generating image (timeout={OPENAI_TIMEOUT[1]}s)")
            r = requests.post(url, headers=headers, json=payload, timeout=OPENAI_TIMEOUT)
            if not r.ok:
                # Don't retry 4xx errors (client errors) - they won't succeed
                if 400 <= r.status_code < 500:
                    raise RuntimeError(f"OpenAI image -> {r.status_code}: {r.text[:500]}")
                # For 5xx errors, raise retryable exception
                raise OpenAIServerError(r.status_code, f"OpenAI server error {r.status_code}: {r.text[:200]}")
            try:
                result = r.json()
                print(f"[OpenAI] Image generated successfully on attempt {attempt}")
                return result
            except Exception as exc:
                raise RuntimeError(f"OpenAI image returned non-JSON: {r.text[:200]}") from exc
        except (Timeout, RequestsConnectionError, OpenAIServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                # Exponential backoff: 2s, 4s, 8s
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                error_type = type(e).__name__
                if isinstance(e, OpenAIServerError):
                    error_type = f"HTTP {e.status_code}"
                print(f"[OpenAI] Attempt {attempt} failed ({error_type}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[OpenAI] All {MAX_RETRIES} attempts failed after exponential backoff")
        except RuntimeError:
            # Re-raise RuntimeError (non-retryable errors like 4xx)
            raise

    # If we get here, all retries failed
    raise RuntimeError(f"OpenAI request failed after {MAX_RETRIES} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Image-to-image / edit via POST /v1/images/edits
# Supports gpt-image-1 and gpt-image-1.5. Accepts up to 10 reference images
# (sent as image[] multipart files) and an optional mask for inpainting.
# ---------------------------------------------------------------------------
_OPENAI_FETCH_TIMEOUT = (15, 60)


def _fetch_image_bytes(img_src: str) -> Tuple[bytes, str]:
    """Resolve a URL or data: URL into (bytes, mime_type). Raises RuntimeError on failure."""
    if not img_src:
        raise RuntimeError("openai_image_edit: empty image source")
    src = str(img_src).strip()
    if src.startswith("data:"):
        try:
            header, _, b64data = src.partition(",")
            mime = "image/png"
            if ":" in header and ";" in header:
                mime = header.split(":", 1)[1].split(";", 1)[0] or "image/png"
            return base64.b64decode(b64data), mime
        except Exception as exc:
            raise RuntimeError(f"openai_image_edit: invalid data URL: {exc}") from exc
    try:
        resp = requests.get(src, timeout=_OPENAI_FETCH_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"openai_image_edit: failed to fetch reference image: {exc}") from exc
    mime = (resp.headers.get("content-type") or "image/png").split(";")[0].strip() or "image/png"
    return resp.content, mime


def openai_image_edit(
    prompt: str,
    reference_images: List[str],
    mask_image: Optional[str] = None,
    size: str = "1024x1024",
    model: str = "gpt-image-1.5",
    n: int = 1,
) -> dict:
    """
    Edit / generate an image conditioned on one or more reference images + prompt.

    Args:
        prompt: Text instruction describing the desired output.
        reference_images: List of URLs (http/https) or data: URLs (1-10).
        mask_image: Optional URL/data: URL for inpainting (transparent = region to edit).
        size: "1024x1024" | "1024x1536" | "1536x1024"
        model: "gpt-image-1" or "gpt-image-1.5" (default)
        n: 1-4 images

    Returns:
        Raw OpenAI response dict (with data[].b64_json or data[].url).
    """
    if not config.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set")
    if not reference_images:
        raise RuntimeError("openai_image_edit requires at least one reference image")

    url = "https://api.openai.com/v1/images/edits"
    headers = {"Authorization": f"Bearer {config.OPENAI_API_KEY}"}

    # Build multipart files list (resolved on every attempt — see retry loop below)
    def _build_files() -> list:
        files: list = []
        for idx, src in enumerate(reference_images[:10]):
            img_bytes, mime = _fetch_image_bytes(src)
            ext = (mime.split("/")[-1] or "png").lower()
            if ext == "jpeg":
                ext = "jpg"
            files.append(("image[]", (f"ref_{idx}.{ext}", img_bytes, mime)))
        if mask_image:
            mask_bytes, mask_mime = _fetch_image_bytes(mask_image)
            files.append(("mask", ("mask.png", mask_bytes, mask_mime)))
        return files

    data = {
        "prompt": prompt,
        "model": model,
        "size": size,
        "n": str(max(1, min(4, int(n or 1)))),
    }

    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            files = _build_files()
            print(f"[OpenAI Edit] Attempt {attempt}/{MAX_RETRIES}: editing with "
                  f"{len(reference_images)} reference(s)" + (" + mask" if mask_image else ""))
            r = requests.post(url, headers=headers, data=data, files=files, timeout=OPENAI_TIMEOUT)
            if not r.ok:
                if 400 <= r.status_code < 500:
                    raise RuntimeError(f"OpenAI image edit -> {r.status_code}: {r.text[:500]}")
                raise OpenAIServerError(
                    r.status_code,
                    f"OpenAI server error {r.status_code}: {r.text[:200]}",
                )
            try:
                result = r.json()
                print(f"[OpenAI Edit] Image edited successfully on attempt {attempt}")
                return result
            except Exception as exc:
                raise RuntimeError(f"OpenAI image edit returned non-JSON: {r.text[:200]}") from exc
        except (Timeout, RequestsConnectionError, OpenAIServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                error_type = type(e).__name__
                if isinstance(e, OpenAIServerError):
                    error_type = f"HTTP {e.status_code}"
                print(f"[OpenAI Edit] Attempt {attempt} failed ({error_type}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[OpenAI Edit] All {MAX_RETRIES} attempts failed")
        except RuntimeError:
            raise

    raise RuntimeError(f"OpenAI image edit failed after {MAX_RETRIES} attempts: {last_error}")
