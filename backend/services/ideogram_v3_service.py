"""
Ideogram V3 image generation and editing service.

TimrX keeps the public endpoint unified while this adapter handles the
provider-specific multipart contract for generation, transparent background,
remix, edit, reframe, background replacement, and upscale.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout

from backend.config import config
from backend.services.image_asset_utils import (
    build_multipart_file,
    color_members_from_hex_list,
    normalize_asset_list,
    normalize_string_list,
)

IDEOGRAM_API_BASE = "https://api.ideogram.ai"
IDEOGRAM_TIMEOUT = (15, 180)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2
IDEOGRAM_MODEL = "ideogram-v3"
IDEOGRAM_RENDERING_SPEEDS = {"FLASH", "TURBO", "DEFAULT", "QUALITY"}
IDEOGRAM_MAGIC_PROMPTS = {"AUTO", "ON", "OFF"}
IDEOGRAM_STYLE_TYPES = {"AUTO", "GENERAL", "REALISTIC", "DESIGN", "FICTION"}
IDEOGRAM_OPERATIONS = {
    "generate": "/v1/ideogram-v3/generate",
    "generate_transparent": "/v1/ideogram-v3/generate-transparent",
    "edit": "/v1/ideogram-v3/edit",
    "remix": "/v1/ideogram-v3/remix",
    "reframe": "/v1/ideogram-v3/reframe",
    "replace_background": "/v1/ideogram-v3/replace-background",
    "upscale": "/upscale",
}


class IdeogramConfigError(Exception):
    pass


class IdeogramAuthError(Exception):
    pass


class IdeogramValidationError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class IdeogramServerError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


def _get_api_key() -> str:
    key = getattr(config, "IDEOGRAM_API_KEY", None) or os.getenv("IDEOGRAM_API_KEY") or ""
    if not key:
        raise IdeogramConfigError("IDEOGRAM_API_KEY is not set.")
    return key


def check_ideogram_v3_configured() -> Tuple[bool, Optional[str]]:
    try:
        _get_api_key()
        return True, None
    except IdeogramConfigError as e:
        return False, str(e)


def ideogram_v3_generate_image(options: Dict[str, Any]) -> Dict[str, Any]:
    operation = str(options.get("operation") or "generate").strip().lower()
    if operation not in IDEOGRAM_OPERATIONS:
        raise IdeogramValidationError(
            f"Unsupported Ideogram operation '{operation}'. Allowed: {sorted(IDEOGRAM_OPERATIONS)}"
        )

    url = f"{IDEOGRAM_API_BASE}{IDEOGRAM_OPERATIONS[operation]}"
    multipart = _build_ideogram_multipart(options, operation=operation)
    headers = {"Api-Key": _get_api_key()}

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, headers=headers, files=multipart, timeout=IDEOGRAM_TIMEOUT)
            if not response.ok:
                error_text = response.text[:500] if response.text else "No error details"
                if response.status_code in (401, 403):
                    raise IdeogramAuthError("Ideogram authentication failed. Check IDEOGRAM_API_KEY.")
                if 400 <= response.status_code < 500:
                    raise RuntimeError(f"ideogram_v3_failed: {error_text}")
                raise IdeogramServerError(response.status_code, error_text)
            result = response.json()
            return _parse_ideogram_response(result, operation=operation)
        except (IdeogramAuthError, IdeogramConfigError, IdeogramValidationError):
            raise
        except (Timeout, RequestsConnectionError, IdeogramServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(BASE_RETRY_DELAY * (2 ** (attempt - 1)))
                continue
        except RuntimeError:
            raise

    raise RuntimeError(f"ideogram_v3_failed: Request failed after {MAX_RETRIES} attempts: {last_error}")


def _build_ideogram_multipart(options: Dict[str, Any], *, operation: str) -> list[tuple[str, tuple | str]]:
    prompt = str(options.get("prompt") or "").strip()
    negative_prompt = str(options.get("negative_prompt") or "").strip()
    rendering_speed = str(options.get("rendering_speed") or "DEFAULT").strip().upper()
    magic_prompt = str(options.get("magic_prompt") or "AUTO").strip().upper()
    style_type = str(options.get("style_type") or "").strip().upper()
    style_preset = str(options.get("style_preset") or "").strip()

    if operation != "upscale" and not prompt and operation not in {"reframe"}:
        raise IdeogramValidationError("prompt is required")
    if rendering_speed and rendering_speed not in IDEOGRAM_RENDERING_SPEEDS:
        raise IdeogramValidationError(
            f"Invalid rendering_speed '{rendering_speed}'. Allowed: {sorted(IDEOGRAM_RENDERING_SPEEDS)}"
        )
    if magic_prompt and magic_prompt not in IDEOGRAM_MAGIC_PROMPTS:
        raise IdeogramValidationError(
            f"Invalid magic_prompt '{magic_prompt}'. Allowed: {sorted(IDEOGRAM_MAGIC_PROMPTS)}"
        )
    if style_type and style_type not in IDEOGRAM_STYLE_TYPES:
        raise IdeogramValidationError(
            f"Invalid style_type '{style_type}'. Allowed: {sorted(IDEOGRAM_STYLE_TYPES)}"
        )

    files: list[tuple[str, tuple | str]] = []

    if operation == "upscale":
        source_image = _require_single_asset(options.get("source_image"), "source_image is required for Ideogram upscale")
        files.append(("image_file", build_multipart_file(source_image, default_name="ideogram-upscale")))
        image_request = {
            "prompt": prompt or None,
            "detail": int(options.get("detail") or 50),
            "resemblance": int(options.get("resemblance") or 50),
        }
        files.append(("image_request", (None, json.dumps(image_request), "application/json")))
        return files

    if prompt:
        files.append(("prompt", (None, prompt)))

    seed = options.get("seed")
    if seed is not None:
        files.append(("seed", (None, str(int(seed)))))

    files.append(("rendering_speed", (None, rendering_speed)))
    if magic_prompt:
        files.append(("magic_prompt", (None, magic_prompt)))
    if negative_prompt:
        files.append(("negative_prompt", (None, negative_prompt)))

    if operation == "reframe":
        source_image = _require_single_asset(options.get("source_image"), "source_image is required for Ideogram reframe")
        files.append(("image", build_multipart_file(source_image, default_name="ideogram-reframe")))
        aspect_ratio = str(options.get("aspect_ratio") or "").strip()
        resolution = str(options.get("resolution") or "").strip()
        if resolution:
            files.append(("resolution", (None, resolution)))
        elif aspect_ratio:
            files.append(("aspect_ratio", (None, aspect_ratio)))
        else:
            raise IdeogramValidationError("Either aspect_ratio or resolution is required for Ideogram reframe")

    if operation in {"edit", "replace_background", "remix"}:
        source_image = _require_single_asset(options.get("source_image"), f"source_image is required for Ideogram {operation}")
        files.append(("image", build_multipart_file(source_image, default_name=f"ideogram-{operation}")))

    if operation == "edit":
        mask_image = _require_single_asset(options.get("mask_image"), "mask_image is required for Ideogram edit")
        files.append(("mask", build_multipart_file(mask_image, default_name="ideogram-mask")))
    if operation == "remix" and options.get("image_weight") is not None:
        files.append(("image_weight", (None, str(float(options["image_weight"])))))

    if operation in {"generate", "generate_transparent"}:
        aspect_ratio = str(options.get("aspect_ratio") or "").strip()
        resolution = str(options.get("resolution") or "").strip()
        if resolution:
            files.append(("resolution", (None, resolution)))
        elif aspect_ratio:
            files.append(("aspect_ratio", (None, aspect_ratio)))
        else:
            files.append(("aspect_ratio", (None, "1x1")))

    if operation == "generate_transparent":
        upscale_factor = str(options.get("upscale_factor") or "X1").strip().upper()
        files.append(("upscale_factor", (None, upscale_factor)))

    num_images = int(options.get("num_images") or 1)
    files.append(("num_images", (None, str(max(1, min(num_images, 8))))))

    if style_type:
        files.append(("style_type", (None, style_type)))
    if style_preset:
        files.append(("style_preset", (None, style_preset)))

    style_codes = normalize_string_list(options.get("style_codes"))
    for code in style_codes:
        files.append(("style_codes", (None, code)))

    color_palette = _build_color_palette(options)
    if color_palette:
        files.append(("color_palette", (None, json.dumps(color_palette), "application/json")))

    for index, asset in enumerate(normalize_asset_list(options.get("style_reference_images")), start=1):
        files.append(
            (
                "style_reference_images",
                build_multipart_file(asset, default_name=f"ideogram-style-ref-{index}"),
            )
        )

    character_reference_images = normalize_asset_list(options.get("character_reference_images"))
    for index, asset in enumerate(character_reference_images, start=1):
        files.append(
            (
                "character_reference_images",
                build_multipart_file(asset, default_name=f"ideogram-char-ref-{index}"),
            )
        )
    for index, asset in enumerate(normalize_asset_list(options.get("character_reference_masks")), start=1):
        files.append(
            (
                "character_reference_images_mask",
                build_multipart_file(asset, default_name=f"ideogram-char-mask-{index}"),
            )
        )

    return files


def _build_color_palette(options: Dict[str, Any]) -> Dict[str, Any] | None:
    palette_name = str(options.get("color_palette_name") or "").strip()
    if palette_name:
        return {"name": palette_name}
    members = color_members_from_hex_list(options.get("color_palette_members"))
    if members:
        return {"members": members}
    return None


def _require_single_asset(value: Any, message: str) -> str:
    assets = normalize_asset_list(value)
    if not assets:
        raise IdeogramValidationError(message)
    return assets[0]


def _parse_ideogram_response(result: Dict[str, Any], *, operation: str) -> Dict[str, Any]:
    if operation == "upscale":
        data = result.get("data") or []
        if data and isinstance(data[0], dict):
            image_url = data[0].get("url")
            if image_url:
                return {
                    "ok": True,
                    "image_url": image_url,
                    "image_urls": [item.get("url") for item in data if isinstance(item, dict) and item.get("url")],
                    "provider": "ideogram_v3",
                    "model": IDEOGRAM_MODEL,
                    "operation": operation,
                    "raw": result,
                }

    data = result.get("data") or []
    if not data:
        raise RuntimeError("ideogram_v3_failed: No images returned by Ideogram V3")

    first = data[0] if isinstance(data[0], dict) else {}
    image_url = first.get("url")
    if not image_url:
        raise RuntimeError("ideogram_v3_failed: Ideogram V3 response did not include an image URL")

    return {
        "ok": True,
        "image_url": image_url,
        "image_urls": [item.get("url") for item in data if isinstance(item, dict) and item.get("url")],
        "provider": "ideogram_v3",
        "model": IDEOGRAM_MODEL,
        "operation": operation,
        "raw": result,
    }
