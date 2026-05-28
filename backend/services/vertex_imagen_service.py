"""
Vertex AI Imagen service.

Provides image-to-image / reference-guided editing via Vertex AI's Imagen
capability model, plus an optional text-to-image fallback that runs directly
on Vertex (the existing gemini_image_service.py keeps using the Gemini
Developer API for plain text-to-image).

Authentication: reuses the service-account helpers in vertex_video_service.py
(same Veo setup — no new credentials needed). The Vertex AI service account
role `roles/aiplatform.user` already covers Imagen `:predict`.

Endpoint:
  POST https://{LOCATION}-aiplatform.googleapis.com/v1/projects/{PROJECT}/locations/{LOCATION}/publishers/google/models/{MODEL}:predict

Default models:
  - VERTEX_IMAGEN_EDIT_MODEL : imagen-3.0-capability-001 (image editing)
  - VERTEX_IMAGEN_GEN_MODEL  : imagen-4.0-fast-generate-001 (text-to-image)

Edit modes supported by imagen-3.0-capability-001:
  EDIT_MODE_DEFAULT         — general reference-guided edit (no mask required)
  EDIT_MODE_INPAINT_INSERTION — insert content into masked region
  EDIT_MODE_INPAINT_REMOVAL   — remove content from masked region
  EDIT_MODE_OUTPAINT        — expand image beyond original bounds
  EDIT_MODE_BGSWAP          — swap background while preserving subject
  EDIT_MODE_PRODUCT_IMAGE   — product photo customization
"""

from __future__ import annotations

import base64
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout

from backend.config import config
from backend.services.vertex_video_service import (
    VertexAuthError,
    VertexConfigError,
    VertexServerError,
    _get_access_token,
    _get_project_id,
)

VERTEX_TIMEOUT = (15, 180)  # (connect, read)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2
_FETCH_TIMEOUT = (15, 60)

# Models (overridable via env)
IMAGEN_EDIT_MODEL = (
    getattr(config, "VERTEX_IMAGEN_EDIT_MODEL", None)
    or os.getenv("VERTEX_IMAGEN_EDIT_MODEL")
    or "imagen-3.0-capability-001"
)
IMAGEN_GEN_MODEL = (
    getattr(config, "VERTEX_IMAGEN_GEN_MODEL", None)
    or os.getenv("VERTEX_IMAGEN_GEN_MODEL")
    or "imagen-4.0-fast-generate-001"
)

# Validation sets
ALLOWED_EDIT_MODES = {
    "EDIT_MODE_DEFAULT",
    "EDIT_MODE_INPAINT_INSERTION",
    "EDIT_MODE_INPAINT_REMOVAL",
    "EDIT_MODE_OUTPAINT",
    "EDIT_MODE_BGSWAP",
    "EDIT_MODE_PRODUCT_IMAGE",
}
ALLOWED_ASPECT_RATIOS = {"1:1", "3:4", "4:3", "9:16", "16:9"}


class VertexImagenError(Exception):
    """Raised for parameter / response errors specific to Vertex Imagen."""


def _get_location() -> str:
    return (
        getattr(config, "VERTEX_LOCATION", None)
        or os.getenv("VERTEX_LOCATION")
        or "us-central1"
    )


def _get_headers() -> Dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_get_access_token()}",
    }


def check_vertex_imagen_configured() -> Tuple[bool, Optional[str]]:
    """Return (ok, error_message)."""
    try:
        _get_access_token()
        _get_project_id()
        return True, None
    except (VertexConfigError, VertexAuthError) as e:
        return False, str(e)


def _fetch_image_b64(src: str) -> Tuple[str, str]:
    """Resolve URL or data: URL to (base64_string, mime_type)."""
    if not src:
        raise VertexImagenError("empty image source")
    src = str(src).strip()
    if src.startswith("data:"):
        try:
            header, _, b64data = src.partition(",")
            mime = "image/png"
            if ":" in header and ";" in header:
                mime = header.split(":", 1)[1].split(";", 1)[0] or "image/png"
            # Re-encode to ensure clean base64 (strip whitespace/newlines).
            raw = base64.b64decode(b64data)
            return base64.b64encode(raw).decode("utf-8"), mime
        except Exception as exc:
            raise VertexImagenError(f"invalid data URL: {exc}") from exc
    try:
        r = requests.get(src, timeout=_FETCH_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as exc:
        raise VertexImagenError(f"failed to fetch reference image: {exc}") from exc
    mime = (r.headers.get("content-type") or "image/png").split(";")[0].strip() or "image/png"
    return base64.b64encode(r.content).decode("utf-8"), mime


def _build_endpoint(model: str) -> str:
    project = _get_project_id()
    location = _get_location()
    return (
        f"https://{location}-aiplatform.googleapis.com/v1/"
        f"projects/{project}/locations/{location}/publishers/google/models/{model}:predict"
    )


def _request_with_retries(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[Vertex Imagen] Attempt {attempt}/{MAX_RETRIES}")
            r = requests.post(url, headers=_get_headers(), json=payload, timeout=VERTEX_TIMEOUT)
            if not r.ok:
                txt = r.text[:500] if r.text else "No error details"
                if r.status_code in (401, 403):
                    raise VertexAuthError(f"Vertex Imagen auth failed ({r.status_code}): {txt}")
                # Pull a friendlier error message if present.
                error_msg = txt
                try:
                    error_msg = r.json().get("error", {}).get("message", txt) or txt
                except Exception:
                    pass
                if r.status_code == 429 or "quota" in error_msg.lower() or "billing" in error_msg.lower():
                    raise VertexImagenError(f"vertex_imagen_quota_or_billing: {error_msg}")
                if 400 <= r.status_code < 500:
                    raise VertexImagenError(f"vertex_imagen_failed: HTTP {r.status_code}: {error_msg}")
                raise VertexServerError(r.status_code, error_msg)
            return r.json()
        except (Timeout, RequestsConnectionError, VertexServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                delay = BASE_RETRY_DELAY * (2 ** (attempt - 1))
                print(f"[Vertex Imagen] Attempt {attempt} failed ({type(e).__name__}), retrying in {delay}s...")
                time.sleep(delay)
                continue
        except (VertexAuthError, VertexConfigError, VertexImagenError):
            raise
    raise VertexImagenError(f"vertex_imagen_failed after {MAX_RETRIES} attempts: {last_error}")


# ---------------------------------------------------------------------------
# Public: text-to-image via Vertex Imagen 4 (optional)
# ---------------------------------------------------------------------------
def vertex_imagen_generate_image(
    prompt: str,
    aspect_ratio: str = "1:1",
    sample_count: int = 1,
    negative_prompt: Optional[str] = None,
    model: str = IMAGEN_GEN_MODEL,
) -> Dict[str, Any]:
    if aspect_ratio not in ALLOWED_ASPECT_RATIOS:
        raise VertexImagenError(f"invalid aspect_ratio: {aspect_ratio}")

    instance: Dict[str, Any] = {"prompt": prompt}
    if negative_prompt:
        instance["negativePrompt"] = negative_prompt

    payload: Dict[str, Any] = {
        "instances": [instance],
        "parameters": {
            "sampleCount": max(1, min(4, int(sample_count or 1))),
            "aspectRatio": aspect_ratio,
        },
    }
    return _parse_imagen_response(
        _request_with_retries(_build_endpoint(model), payload),
        model=model,
        operation="generate",
    )


# ---------------------------------------------------------------------------
# Public: image-to-image / reference-guided edit via Imagen 3 Capability
# ---------------------------------------------------------------------------
def vertex_imagen_edit_image(
    prompt: str,
    reference_images: List[str],
    mask_image: Optional[str] = None,
    edit_mode: str = "EDIT_MODE_DEFAULT",
    sample_count: int = 1,
    negative_prompt: Optional[str] = None,
    model: str = IMAGEN_EDIT_MODEL,
) -> Dict[str, Any]:
    """
    Edit / generate an image conditioned on one or more reference images + prompt.

    Args:
        prompt: instruction text.
        reference_images: list of URLs or data: URLs. First image is the RAW base.
        mask_image: optional mask URL/data URL (for inpaint/outpaint modes).
        edit_mode: one of ALLOWED_EDIT_MODES. EDIT_MODE_DEFAULT works as a generic
                   reference-guided image-to-image (no mask required).
    """
    if not reference_images:
        raise VertexImagenError("reference_images required for edit")
    if edit_mode not in ALLOWED_EDIT_MODES:
        raise VertexImagenError(f"invalid edit_mode: {edit_mode}")

    refs: List[Dict[str, Any]] = []
    ref_id = 1

    # First reference = RAW base image
    base_b64, _ = _fetch_image_b64(reference_images[0])
    refs.append({
        "referenceType": "REFERENCE_TYPE_RAW",
        "referenceId": ref_id,
        "referenceImage": {"bytesBase64Encoded": base_b64},
    })
    ref_id += 1

    # Additional reference images → treated as SUBJECT references (max 4 extras).
    for extra in reference_images[1:5]:
        b64, _ = _fetch_image_b64(extra)
        refs.append({
            "referenceType": "REFERENCE_TYPE_SUBJECT",
            "referenceId": ref_id,
            "referenceImage": {"bytesBase64Encoded": b64},
            "subjectImageConfig": {
                "subjectDescription": (prompt or "subject")[:120],
                "subjectType": "SUBJECT_TYPE_DEFAULT",
            },
        })
        ref_id += 1

    # Optional mask (for inpaint/outpaint modes)
    if mask_image:
        mask_b64, _ = _fetch_image_b64(mask_image)
        refs.append({
            "referenceType": "REFERENCE_TYPE_MASK",
            "referenceId": ref_id,
            "referenceImage": {"bytesBase64Encoded": mask_b64},
            "maskImageConfig": {"maskMode": "MASK_MODE_USER_PROVIDED"},
        })

    instance: Dict[str, Any] = {"prompt": prompt, "referenceImages": refs}
    if negative_prompt:
        instance["negativePrompt"] = negative_prompt

    payload: Dict[str, Any] = {
        "instances": [instance],
        "parameters": {
            "editMode": edit_mode,
            "sampleCount": max(1, min(4, int(sample_count or 1))),
        },
    }
    return _parse_imagen_response(
        _request_with_retries(_build_endpoint(model), payload),
        model=model,
        operation="edit",
    )


# ---------------------------------------------------------------------------
# Response parsing — Vertex Imagen returns predictions[].bytesBase64Encoded
# ---------------------------------------------------------------------------
def _parse_imagen_response(
    result: Dict[str, Any],
    *,
    model: str,
    operation: str = "generate",
) -> Dict[str, Any]:
    images: List[Dict[str, str]] = []
    for pred in result.get("predictions", []) or []:
        b64 = pred.get("bytesBase64Encoded")
        mime = pred.get("mimeType", "image/png")
        if b64:
            images.append({
                "base64": b64,
                "mime_type": mime,
                "url": f"data:{mime};base64,{b64}",
            })

    if not images:
        err = result.get("error", {}) or {}
        msg = err.get("message", "no predictions returned")
        raise VertexImagenError(f"vertex_imagen_no_images: {msg}")

    return {
        "ok": True,
        "image_url": images[0]["url"],
        "image_base64": images[0]["base64"],
        "mime_type": images[0]["mime_type"],
        "image_urls": [img["url"] for img in images],
        "images": images,
        "provider": "google",
        "provider_variant": "vertex",
        "model": model,
        "operation": operation,
        "image_count": len(images),
    }
