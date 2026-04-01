"""
Recraft image generation and editing service.

Supports Recraft's practical workspace feature set:
- V4 / V4 Pro raster and vector generation
- V3 / V3 Vector editing and styled generation
- image-to-image, inpaint, replace/generate background
- vectorize, remove background, crisp/creative upscale, erase region, remix
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
    normalize_asset_list,
    normalize_string_list,
)

RECRAFT_API_BASE = "https://external.api.recraft.ai/v1"
RECRAFT_TIMEOUT = (15, 180)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2
ALLOWED_OUTPUT_MODES = {"raster", "vector_svg"}
RECRAFT_V3_MODELS = {"recraftv3", "recraftv3_vector"}
RECRAFT_V2_V3_GENERATE_MODELS = {"recraftv2", "recraftv2_vector", "recraftv3", "recraftv3_vector"}
RECRAFT_OPERATIONS = {
    "generate": {"path": "/images/generations", "multipart": False},
    "image_to_image": {"path": "/images/imageToImage", "multipart": True},
    "inpaint": {"path": "/images/inpaint", "multipart": True},
    "replace_background": {"path": "/images/replaceBackground", "multipart": True},
    "generate_background": {"path": "/images/generateBackground", "multipart": True},
    "vectorize": {"path": "/images/vectorize", "multipart": True},
    "remove_background": {"path": "/images/removeBackground", "multipart": True},
    "crisp_upscale": {"path": "/images/crispUpscale", "multipart": True},
    "creative_upscale": {"path": "/images/creativeUpscale", "multipart": True},
    "erase_region": {"path": "/images/eraseRegion", "multipart": True},
    "remix": {"path": "/images/variateImage", "multipart": True},
}


class RecraftConfigError(Exception):
    pass


class RecraftAuthError(Exception):
    pass


class RecraftValidationError(Exception):
    def __init__(self, field: str, value: Any, allowed: list[str], message: Optional[str] = None):
        self.field = field
        self.value = value
        self.allowed = allowed
        self.message = message or f"Invalid {field}: {value}. Allowed: {allowed}"
        super().__init__(self.message)


class RecraftServerError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


def _get_api_key() -> str:
    key = getattr(config, "RECRAFT_API_KEY", None) or os.getenv("RECRAFT_API_KEY") or ""
    if not key:
        raise RecraftConfigError("RECRAFT_API_KEY is not set.")
    return key


def check_recraft_v4_configured() -> Tuple[bool, Optional[str]]:
    try:
        _get_api_key()
        return True, None
    except RecraftConfigError as e:
        return False, str(e)


def validate_recraft_params(options: Dict[str, Any]) -> None:
    operation = str(options.get("operation") or "generate").strip().lower()
    if operation not in RECRAFT_OPERATIONS:
        raise RecraftValidationError("operation", operation, sorted(RECRAFT_OPERATIONS))

    output_mode = str(options.get("output_mode") or "raster").lower()
    if output_mode not in ALLOWED_OUTPUT_MODES:
        raise RecraftValidationError("output_mode", output_mode, sorted(ALLOWED_OUTPUT_MODES))

    resolved_model = (
        _resolve_edit_model(options) if operation in {"image_to_image", "inpaint", "replace_background", "generate_background"}
        else _resolve_generate_model(options)
    ).strip().lower()

    style = str(options.get("style") or "").strip()
    style_id = str(options.get("style_id") or "").strip()
    negative_prompt = str(options.get("negative_prompt") or "").strip()
    text_layout = _parse_json_or_passthrough(options.get("text_layout"))
    if style and style_id:
        raise RecraftValidationError("style", style, [], "Use either style or style_id, not both.")

    if operation == "generate":
        supports_styles = resolved_model in RECRAFT_V2_V3_GENERATE_MODELS
        supports_negative_prompt = resolved_model in RECRAFT_V2_V3_GENERATE_MODELS
        supports_text_layout = resolved_model in RECRAFT_V3_MODELS
    elif operation in {"image_to_image", "inpaint", "replace_background", "generate_background"}:
        if resolved_model not in RECRAFT_V3_MODELS:
            raise RecraftValidationError(
                "model_variant",
                resolved_model,
                sorted(RECRAFT_V3_MODELS),
                f"{operation} is available only with Recraft V3 or Recraft V3 Vector.",
            )
        supports_styles = True
        supports_negative_prompt = True
        supports_text_layout = True
    else:
        supports_styles = False
        supports_negative_prompt = False
        supports_text_layout = False

    if style and not supports_styles:
        raise RecraftValidationError(
            "style",
            style,
            [],
            f"Styles are not supported for {resolved_model or operation} in {operation}.",
        )
    if style_id and not supports_styles:
        raise RecraftValidationError(
            "style_id",
            style_id,
            [],
            f"style_id is not supported for {resolved_model or operation} in {operation}.",
        )
    if negative_prompt and not supports_negative_prompt:
        raise RecraftValidationError(
            "negative_prompt",
            negative_prompt,
            [],
            f"negative_prompt is not supported for {resolved_model or operation} in {operation}.",
        )
    if text_layout and not supports_text_layout:
        raise RecraftValidationError(
            "text_layout",
            text_layout,
            [],
            f"text_layout is available only for Recraft V3 and Recraft V3 Vector generation/edit modes.",
        )

    if operation in {"image_to_image", "inpaint", "replace_background", "generate_background", "erase_region", "remix"}:
        if not normalize_asset_list(options.get("source_image")):
            raise RecraftValidationError("source_image", None, [], f"source_image is required for {operation}")
    if operation in {"inpaint", "generate_background", "erase_region"} and not normalize_asset_list(options.get("mask_image")):
        raise RecraftValidationError("mask_image", None, [], f"mask_image is required for {operation}")
    if operation == "image_to_image" and options.get("strength") is None:
        raise RecraftValidationError("strength", None, [], "strength is required for image_to_image")
    if operation == "generate" and not str(options.get("prompt") or "").strip():
        raise RecraftValidationError("prompt", "", [], "prompt is required for image generation")
    if operation in {"image_to_image", "inpaint", "replace_background", "generate_background"} and not str(options.get("prompt") or "").strip():
        raise RecraftValidationError("prompt", "", [], f"prompt is required for {operation}")
    if operation == "remix" and not str(options.get("size") or "").strip():
        raise RecraftValidationError("size", "", [], "size is required for remix")


def recraft_v4_generate_image(options: Dict[str, Any]) -> Dict[str, Any]:
    validate_recraft_params(options)

    operation = str(options.get("operation") or "generate").strip().lower()
    spec = RECRAFT_OPERATIONS[operation]
    url = f"{RECRAFT_API_BASE}{spec['path']}"
    headers = {"Authorization": f"Bearer {_get_api_key()}"}

    request_kwargs = _build_request_kwargs(options, operation=operation, multipart=bool(spec["multipart"]))
    request_headers = request_kwargs.pop("headers", None) or {}
    if request_headers:
        headers.update(request_headers)

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, headers=headers, timeout=RECRAFT_TIMEOUT, **request_kwargs)
            if not response.ok:
                error_text = response.text[:500] if response.text else "No error details"
                if response.status_code in (401, 403):
                    raise RecraftAuthError("Recraft authentication failed. Check RECRAFT_API_KEY.")
                if 400 <= response.status_code < 500:
                    raise RuntimeError(f"recraft_v4_failed: {error_text}")
                raise RecraftServerError(response.status_code, error_text)
            result = response.json()
            return _parse_recraft_response(result, options=options, operation=operation)
        except (RecraftAuthError, RecraftConfigError, RecraftValidationError):
            raise
        except (Timeout, RequestsConnectionError, RecraftServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(BASE_RETRY_DELAY * (2 ** (attempt - 1)))
                continue
        except RuntimeError:
            raise

    raise RuntimeError(f"recraft_v4_failed: Request failed after {MAX_RETRIES} attempts: {last_error}")


def _build_request_kwargs(options: Dict[str, Any], *, operation: str, multipart: bool) -> Dict[str, Any]:
    if not multipart:
        payload = _build_json_payload(options)
        return {
            "json": payload,
            "headers": {
                "Content-Type": "application/json",
            },
        }

    files, data = _build_multipart_payload(options, operation=operation)
    return {
        "files": files,
        "data": data,
    }


def _build_json_payload(options: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "prompt": str(options.get("prompt") or "").strip(),
        "model": _resolve_generate_model(options),
        "size": str(options.get("size") or "1:1").strip(),
        "n": 1,
        "response_format": str(options.get("response_format") or "url").strip(),
    }
    style = str(options.get("style") or "").strip()
    style_id = str(options.get("style_id") or "").strip()
    negative_prompt = str(options.get("negative_prompt") or "").strip()
    if style:
        payload["style"] = style
    if style_id:
        payload["style_id"] = style_id
    if negative_prompt:
        payload["negative_prompt"] = negative_prompt

    text_layout = _parse_json_or_passthrough(options.get("text_layout"))
    if text_layout:
        payload["text_layout"] = text_layout

    controls = _build_controls(options)
    if controls:
        payload["controls"] = controls
    return payload


def _build_multipart_payload(options: Dict[str, Any], *, operation: str) -> tuple[list[tuple[str, tuple]], dict[str, Any]]:
    files: list[tuple[str, tuple]] = []
    data: dict[str, Any] = {
        "response_format": str(options.get("response_format") or "url").strip(),
    }

    prompt = str(options.get("prompt") or "").strip()
    if prompt and operation not in {"vectorize", "remove_background", "crisp_upscale", "creative_upscale", "erase_region", "remix"}:
        data["prompt"] = prompt
    if operation == "remix":
        data["size"] = str(options.get("size") or "1:1").strip()

    if operation in {"image_to_image", "inpaint", "replace_background", "generate_background"}:
        data["model"] = _resolve_edit_model(options)
        style = str(options.get("style") or "").strip()
        style_id = str(options.get("style_id") or "").strip()
        negative_prompt = str(options.get("negative_prompt") or "").strip()
        if style:
            data["style"] = style
        if style_id:
            data["style_id"] = style_id
        if negative_prompt:
            data["negative_prompt"] = negative_prompt
        controls = _build_controls(options)
        if controls:
            data["controls"] = json.dumps(controls)
        text_layout = _parse_json_or_passthrough(options.get("text_layout"))
        if text_layout:
            data["text_layout"] = json.dumps(text_layout)

    if operation == "image_to_image":
        data["strength"] = str(float(options.get("strength") or 0.35))

    if operation == "remix" and options.get("seed") is not None:
        data["random_seed"] = str(int(options["seed"]))

    source_image = normalize_asset_list(options.get("source_image"))
    mask_image = normalize_asset_list(options.get("mask_image"))

    if operation in {"image_to_image", "inpaint", "replace_background", "generate_background", "erase_region", "remix"}:
        files.append(("image", build_multipart_file(source_image[0], default_name=f"recraft-{operation}")))
    elif operation in {"vectorize", "remove_background", "crisp_upscale", "creative_upscale"}:
        files.append(("file", build_multipart_file(source_image[0], default_name=f"recraft-{operation}")))

    if operation in {"inpaint", "generate_background", "erase_region"}:
        files.append(("mask", build_multipart_file(mask_image[0], default_name="recraft-mask")))

    if operation == "vectorize":
        if options.get("svg_compression") is not None:
            data["svg_compression"] = "on" if bool(options.get("svg_compression")) else "off"
        if options.get("limit_num_shapes") is not None:
            data["limit_num_shapes"] = "on" if bool(options.get("limit_num_shapes")) else "off"
        if options.get("max_num_shapes") is not None:
            data["max_num_shapes"] = str(int(options["max_num_shapes"]))

    return files, data


def _resolve_generate_model(options: Dict[str, Any]) -> str:
    requested = str(options.get("model_variant") or "").strip()
    if requested:
        return requested
    output_mode = str(options.get("output_mode") or "raster").strip().lower()
    return "recraftv4_vector" if output_mode == "vector_svg" else "recraftv4"


def _resolve_edit_model(options: Dict[str, Any]) -> str:
    requested = str(options.get("model_variant") or "").strip()
    if requested:
        return requested
    output_mode = str(options.get("output_mode") or "raster").strip().lower()
    return "recraftv3_vector" if output_mode == "vector_svg" else "recraftv3"


def _build_controls(options: Dict[str, Any]) -> Dict[str, Any] | None:
    controls: Dict[str, Any] = {}
    background_color = str(options.get("background_color") or "").strip()
    if background_color:
        controls["background_color"] = {"rgb": _hex_to_rgb(background_color)}

    colors = []
    for color_text in normalize_string_list(options.get("preferred_colors")):
        rgb = _hex_to_rgb(color_text)
        if rgb:
            colors.append({"rgb": rgb})
    if colors:
        controls["colors"] = colors

    if options.get("artistic_level") is not None and str(options.get("artistic_level")).strip() != "":
        controls["artistic_level"] = int(options["artistic_level"])
    if options.get("no_text") is not None:
        controls["no_text"] = bool(options["no_text"])

    return controls or None


def _hex_to_rgb(value: str) -> list[int] | None:
    text = str(value).strip().lower().lstrip("#")
    if len(text) != 6 or any(c not in "0123456789abcdef" for c in text):
        return None
    return [int(text[0:2], 16), int(text[2:4], 16), int(text[4:6], 16)]


def _parse_json_or_passthrough(value: Any) -> Any:
    if value is None or value == "":
        return None
    if isinstance(value, (dict, list)):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _parse_recraft_response(result: Dict[str, Any], *, options: Dict[str, Any], operation: str) -> Dict[str, Any]:
    data = result.get("data") or []
    image_node = result.get("image") if isinstance(result.get("image"), dict) else None

    image_url = None
    image_urls: list[str] = []
    image_base64 = None

    if data:
        for item in data:
            if not isinstance(item, dict):
                continue
            if item.get("url"):
                image_urls.append(item["url"])
            elif item.get("b64_json"):
                image_base64 = image_base64 or item["b64_json"]
        image_url = image_urls[0] if image_urls else None
    elif image_node:
        image_url = image_node.get("url")
        image_base64 = image_node.get("b64_json")
        if image_url:
            image_urls = [image_url]

    if not image_url and image_base64:
        artifact_format = _artifact_format_for_recraft(options, operation)
        mime_type = "image/svg+xml" if artifact_format == "svg" else "image/png"
        image_url = f"data:{mime_type};base64,{image_base64}"
        image_urls = [image_url]

    if not image_url:
        raise RuntimeError("recraft_v4_failed: Recraft response did not include an image")

    artifact_format = _artifact_format_for_recraft(options, operation)
    mime_type = "image/svg+xml" if artifact_format == "svg" else "image/png"

    return {
        "ok": True,
        "image_url": image_url,
        "image_urls": image_urls,
        "image_base64": image_base64,
        "mime_type": mime_type,
        "provider": "recraft_v4",
        "model": str(options.get("model_variant") or _resolve_generate_model(options)),
        "output_mode": "vector_svg" if artifact_format == "svg" else "raster",
        "artifact_format": artifact_format,
        "operation": operation,
        "raw": result,
    }


def _artifact_format_for_recraft(options: Dict[str, Any], operation: str) -> str:
    model_variant = str(options.get("model_variant") or _resolve_generate_model(options)).lower()
    if operation == "vectorize" or "vector" in model_variant or str(options.get("output_mode") or "").lower() == "vector_svg":
        return "svg"
    return "png"
