"""
Black Forest Labs FLUX.2 service.

Supports the practical FLUX.2 image feature set TimrX exposes today:
- text-to-image
- image-to-image / reference-image guided generation
- FLUX.2 Pro, Pro Preview, and Flex model variants
- seed, prompt upsampling, transparency, output format, and Flex controls
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout

from backend.config import config

BFL_API_BASE = "https://api.bfl.ai/v1"
BFL_TIMEOUT = (15, 180)
MAX_RETRIES = 3
BASE_RETRY_DELAY = 2
POLL_INTERVAL_INITIAL = 2
POLL_INTERVAL_MAX = 8
POLL_TIMEOUT = 300

FLUX_MODEL_ENDPOINTS = {
    "pro": "flux-2-pro",
    "pro_preview": "flux-2-pro-preview",
    "flex": "flux-2-flex",
}
ALLOWED_OUTPUT_FORMATS = {"jpeg", "png", "webp"}


class FluxProConfigError(Exception):
    pass


class FluxProAuthError(Exception):
    pass


class FluxProValidationError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class FluxProServerError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(message)


class FluxProTaskError(Exception):
    pass


def _get_api_key() -> str:
    key = getattr(config, "BFL_API_KEY", None) or os.getenv("BFL_API_KEY") or ""
    if not key:
        raise FluxProConfigError("BFL_API_KEY is not set.")
    return key


def _get_headers() -> Dict[str, str]:
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "x-key": _get_api_key(),
    }


def check_flux_pro_configured() -> Tuple[bool, Optional[str]]:
    try:
        _get_api_key()
        return True, None
    except FluxProConfigError as e:
        return False, str(e)


def validate_flux_dimensions(width: int, height: int) -> None:
    if int(width or 0) < 64 or int(height or 0) < 64:
        raise FluxProValidationError("width and height must be at least 64")


def validate_flux_request(options: Dict[str, Any]) -> None:
    model_variant = str(options.get("model_variant") or "pro").strip().lower()
    operation = str(options.get("operation") or "generate").strip().lower()
    if model_variant not in FLUX_MODEL_ENDPOINTS:
        raise FluxProValidationError(
            f"Unsupported FLUX model variant '{model_variant}'. Allowed: {sorted(FLUX_MODEL_ENDPOINTS)}"
        )

    validate_flux_dimensions(int(options.get("width") or 0), int(options.get("height") or 0))

    output_format = str(options.get("output_format") or "jpeg").strip().lower()
    if output_format not in ALLOWED_OUTPUT_FORMATS:
        raise FluxProValidationError(
            f"Unsupported FLUX output format '{output_format}'. Allowed: {sorted(ALLOWED_OUTPUT_FORMATS)}"
        )

    safety_tolerance = int(options.get("safety_tolerance") or 2)
    if safety_tolerance < 0 or safety_tolerance > 5:
        raise FluxProValidationError("safety_tolerance must be between 0 and 5")

    reference_images = list(options.get("reference_images") or [])
    if len(reference_images) > 8:
        raise FluxProValidationError("FLUX supports at most 8 reference images")
    if operation == "edit" and not reference_images:
        raise FluxProValidationError("FLUX edit mode requires at least one source or reference image")

    if model_variant == "flex":
        guidance = options.get("guidance")
        steps = options.get("steps")
        if guidance is not None and not (1.5 <= float(guidance) <= 10):
            raise FluxProValidationError("guidance must be between 1.5 and 10 for FLUX Flex")
        if steps is not None and not (1 <= int(steps) <= 50):
            raise FluxProValidationError("steps must be between 1 and 50 for FLUX Flex")


def create_flux_pro_task(options: Dict[str, Any]) -> Dict[str, Any]:
    validate_flux_request(options)

    model_variant = str(options.get("model_variant") or "pro").strip().lower()
    endpoint = FLUX_MODEL_ENDPOINTS[model_variant]
    url = f"{BFL_API_BASE}/{endpoint}"
    payload = _build_flux_payload(options, model_variant=model_variant)

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(url, headers=_get_headers(), json=payload, timeout=BFL_TIMEOUT)
            if not response.ok:
                error_text = response.text[:500] if response.text else "No error details"
                if response.status_code in (401, 403):
                    raise FluxProAuthError("BFL authentication failed. Check BFL_API_KEY.")
                if 400 <= response.status_code < 500:
                    raise RuntimeError(f"flux_pro_task_failed: {error_text}")
                raise FluxProServerError(response.status_code, error_text)
            result = response.json()
            task_id = result.get("id")
            polling_url = result.get("polling_url")
            if not task_id or not polling_url:
                raise RuntimeError(f"flux_pro_task_failed: Missing task id or polling_url: {str(result)[:200]}")
            return {
                "task_id": task_id,
                "polling_url": polling_url,
                "cost": result.get("cost"),
                "model_variant": model_variant,
                "model": endpoint,
                "raw": result,
            }
        except (FluxProAuthError, FluxProConfigError, FluxProValidationError):
            raise
        except (Timeout, RequestsConnectionError, FluxProServerError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                time.sleep(BASE_RETRY_DELAY * (2 ** (attempt - 1)))
                continue
        except RuntimeError:
            raise

    raise RuntimeError(f"flux_pro_task_failed: Create task failed after {MAX_RETRIES} attempts: {last_error}")


def poll_flux_pro_task(task_id: str, polling_url: str) -> Dict[str, Any]:
    start_time = time.time()
    interval = POLL_INTERVAL_INITIAL

    while True:
        if time.time() - start_time > POLL_TIMEOUT:
            raise FluxProTaskError(f"Task {task_id} timed out after {POLL_TIMEOUT}s")

        time.sleep(interval)
        interval = min(POLL_INTERVAL_MAX, interval * 1.5)

        response = requests.get(
            polling_url,
            headers={"accept": "application/json", "x-key": _get_api_key()},
            timeout=BFL_TIMEOUT,
        )
        if not response.ok:
            error_text = response.text[:500] if response.text else "No error details"
            if response.status_code in (401, 403):
                raise FluxProAuthError("BFL authentication failed while polling.")
            raise FluxProTaskError(f"Task {task_id} poll failed: HTTP {response.status_code}: {error_text}")

        result = response.json()
        status = str(result.get("status") or "").strip() or "Pending"
        status_key = status.lower()

        if status_key == "ready":
            sample = _extract_flux_image_url(result)
            if not sample:
                raise FluxProTaskError(f"Task {task_id} completed without an image URL")
            return {
                "status": "completed",
                "task_id": task_id,
                "image_url": sample,
                "upstream_cost": result.get("cost"),
                "raw": result,
            }

        if status_key in {"error", "failed", "request moderated", "content moderated"}:
            error_message = result.get("error") or result.get("message") or status
            raise FluxProTaskError(f"Task {task_id} failed: {error_message}")


def _build_flux_payload(options: Dict[str, Any], *, model_variant: str) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "prompt": str(options.get("prompt") or "").strip(),
        "width": int(options.get("width") or 1024),
        "height": int(options.get("height") or 1024),
        "output_format": str(options.get("output_format") or "jpeg").strip().lower(),
        "safety_tolerance": int(options.get("safety_tolerance") or 2),
    }
    if options.get("seed") is not None:
        payload["seed"] = int(options["seed"])

    reference_images = list(options.get("reference_images") or [])
    for index, image_url in enumerate(reference_images[:8], start=1):
        payload["input_image" if index == 1 else f"input_image_{index}"] = image_url

    prompt_upsampling = bool(options.get("prompt_upsampling", True))
    if model_variant == "flex":
        payload["prompt_upsampling"] = prompt_upsampling
        if options.get("guidance") is not None:
            payload["guidance"] = float(options["guidance"])
        if options.get("steps") is not None:
            payload["steps"] = int(options["steps"])
    else:
        payload["disable_pup"] = not prompt_upsampling
        if options.get("transparent_background") is not None:
            payload["transparent_bg"] = bool(options["transparent_background"])
    return payload


def _extract_flux_image_url(result: Dict[str, Any]) -> Optional[str]:
    payload = result.get("result")
    if isinstance(payload, dict):
        sample = payload.get("sample")
        if isinstance(sample, str):
            return sample
        if isinstance(sample, dict):
            return sample.get("url") or sample.get("uri")
        samples = payload.get("samples")
        if isinstance(samples, list):
            for item in samples:
                if isinstance(item, str):
                    return item
                if isinstance(item, dict):
                    return item.get("url") or item.get("uri")
    return result.get("sample") or result.get("url")
