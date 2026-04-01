"""
Image Generation Routes Blueprint (Modular)
------------------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

import base64
import os
import time
import traceback
import uuid
from urllib.parse import urlparse

import requests
from flask import Blueprint, Response, jsonify, request

from backend.config import OPENAI_API_KEY, config
from backend.db import USE_DB, get_conn, dict_row, Tables
from backend.middleware import with_session, with_session_readonly
from backend.services.async_dispatch import (
    get_executor,
    _dispatch_openai_image_async,
    dispatch_flux_pro_image_async,
    dispatch_gemini_image_async,
    dispatch_google_nano_image_async,
    dispatch_ideogram_v3_image_async,
    dispatch_piapi_nano_banana_async,
    dispatch_recraft_v4_image_async,
    update_job_status_ready,
    update_job_status_failed,
)
from backend.services.credits_helper import get_current_balance, start_paid_job
from backend.services.expense_guard import ExpenseGuard
from backend.services.gemini_image_service import (
    gemini_generate_image,
    check_gemini_configured,
    GeminiAuthError,
    GeminiConfigError,
    GeminiValidationError,
    ALLOWED_ASPECT_RATIOS,
    ALLOWED_IMAGE_SIZES,
)
from backend.services.google_nano_image_service import (
    check_google_nano_configured,
    ALLOWED_ASPECT_RATIOS as GOOGLE_NANO_ALLOWED_ASPECT_RATIOS,
    ALLOWED_IMAGE_SIZES as GOOGLE_NANO_ALLOWED_IMAGE_SIZES,
)
from backend.services.flux_pro_service import check_flux_pro_configured
from backend.services.ideogram_v3_service import check_ideogram_v3_configured
from backend.services.piapi_nano_banana_service import (
    check_piapi_configured,
    ALLOWED_ASPECT_RATIOS as PIAPI_ALLOWED_ASPECT_RATIOS,
    ALLOWED_RESOLUTIONS as PIAPI_ALLOWED_RESOLUTIONS,
)
from backend.services.recraft_image_service import (
    check_recraft_v4_configured,
    ALLOWED_OUTPUT_MODES as RECRAFT_ALLOWED_OUTPUT_MODES,
    RecraftValidationError,
    validate_recraft_params,
)
from backend.services.image_asset_utils import (
    coerce_bool,
    coerce_float,
    coerce_int,
    ensure_asset_url,
    normalize_asset_list,
    normalize_string_list,
)
from backend.services.image_provider_registry import (
    IMAGE_PROVIDER_REGISTRY,
    get_allowed_image_providers,
    get_enabled_image_providers,
    get_image_action_key as get_registry_image_action_key,
    get_image_provider_spec,
    is_image_provider_enabled,
)
from backend.services.history_service import get_canonical_image_row, save_image_to_normalized_db
from backend.services.identity_service import require_identity
from backend.services.job_service import create_internal_job_row, load_store, save_store
from backend.services.s3_service import is_s3_url, parse_s3_key, presign_s3_url
from backend.services.prompt_safety_service import check_prompt_safety
from backend.utils.helpers import now_s, log_event

bp = Blueprint("image_gen", __name__)


def _get_image_action_key(image_size: str = "1K", provider: str = "openai") -> str:
    """Return the provider-specific canonical action key for the given quality tier."""
    return get_registry_image_action_key(provider=provider, image_size=image_size)


def _validate_provider_image_size(image_size: str, provider: str):
    """Reject unsupported size tiers per provider. Returns error response or None."""
    spec = get_image_provider_spec(provider)
    requested = (image_size or "").upper()
    if not spec or not requested:
        return None
    allowed = list(spec.image_sizes)
    if requested not in spec.image_sizes:
        return jsonify({
            "error": "invalid_params",
            "message": f"{requested} resolution is not available with provider '{provider}'.",
            "field": "image_size",
            "allowed": allowed,
        }), 400
    return None


FLUX_PRO_RESOLUTION_MAP = {
    "square": (1024, 1024),
    "portrait": (1024, 1536),
    "landscape": (1536, 1024),
}

IDEOGRAM_ASPECT_RATIO_MAP = {
    "square": "1x1",
    "portrait": "2x3",
    "landscape": "3x2",
}

IDEOGRAM_REFRAME_RESOLUTION_MAP = {
    "square": "1024x1024",
    "portrait": "512x1536",
    "landscape": "1280x800",
}

RECRAFT_SIZE_MAP = {
    "square": "1024x1024",
    "portrait": "1024x1536",
    "landscape": "1536x1024",
}


def _provider_disabled_response(provider: str):
    return jsonify({
        "error": "provider_disabled",
        "message": f"Image provider '{provider}' is disabled.",
        "allowed": get_enabled_image_providers(),
    }), 400


def _parse_resolution_pair(raw: str | None, fallback: tuple[int, int]) -> tuple[int, int]:
    text = (raw or "").strip().lower()
    if "x" in text:
        try:
            width_text, height_text = text.split("x", 1)
            return int(width_text), int(height_text)
        except Exception:
            return fallback
    return fallback


def _body_value(body: dict, *keys: str, default=None):
    for key in keys:
        if key in body and body.get(key) is not None:
            return body.get(key)
    return default


def _normalize_image_inputs(body: dict, *keys: str) -> list[str]:
    for key in keys:
        value = body.get(key)
        if value is not None:
            return normalize_asset_list(value)
    return []


# OpenAI blob hosts (from monolith)
ALLOWED_IMAGE_HOSTS = {
    "oaidalleapiprodscus.blob.core.windows.net",
    "oaidalleapiprodscus.bblob.core.windows.net",
    "oaidalleapiprodscus.blob.core.windows.net:443",
}

_IMAGE_PROVIDER_PUBLIC_ORDER = (
    "nano_banana",
    "openai",
    "google",
    "google_nano",
    "flux_pro",
    "ideogram_v3",
    "recraft_v4",
)


@bp.route("/image/providers", methods=["GET"])
@with_session_readonly
def image_provider_catalog():
    enabled_set = set(get_enabled_image_providers())
    ordered_enabled = [provider for provider in _IMAGE_PROVIDER_PUBLIC_ORDER if provider in enabled_set]
    if not ordered_enabled:
        ordered_enabled = [provider for provider in IMAGE_PROVIDER_REGISTRY if provider in enabled_set]

    providers = []
    for provider in ordered_enabled:
        spec = get_image_provider_spec(provider)
        if not spec:
            continue
        providers.append(
            {
                "id": spec.provider,
                "name": spec.display_name,
                "image_sizes": list(spec.image_sizes),
                "default_image_size": spec.default_image_size,
                "output_modes": list(spec.output_modes),
                "provider_variant": spec.provider_variant,
                "model": spec.model,
            }
        )

    default_provider = ordered_enabled[0] if ordered_enabled else None

    return jsonify(
        {
            "ok": True,
            "providers": providers,
            "enabled_providers": ordered_enabled,
            "default_provider": default_provider,
        }
    )


@bp.route("/image/generate", methods=["POST", "OPTIONS"])
@with_session
def image_generate_unified():
    """
    Unified image generation endpoint that routes based on provider.

    Request body:
    {
        "provider": "google" | "openai" | "nano_banana" | "google_nano" | "flux_pro" | "ideogram_v3" | "recraft_v4",
        "prompt": "A beautiful sunset...",
        "aspect_ratio": "16:9",           # For Google: "1:1", "3:4", "4:3", "9:16", "16:9"
        "image_size": "1K",               # For Google: "1K" or "2K"
        "size": "1024x1024",              # For OpenAI
        "model": "gpt-image-1.5",          # For OpenAI (default; gpt-image-1 still supported)
        "n": 1                            # Number of images
    }

    Response (success):
    {
        "ok": true,
        "image_url": "...",
        "image_id": "uuid",
        "job_id": "uuid",
        "provider": "google" | "openai"
    }

    Response (error):
    {
        "error": "<machine_code>",
        "message": "<human readable>",
        "details": {...}
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    body = request.get_json(silent=True) or {}
    provider = (body.get("provider") or "openai").lower()

    spec = get_image_provider_spec(provider)
    if not spec:
        return jsonify({
            "error": "invalid_provider",
            "message": f"Unknown image provider: {provider}",
            "allowed": get_allowed_image_providers(),
        }), 400
    if not is_image_provider_enabled(provider):
        return _provider_disabled_response(provider)

    req_size = (body.get("image_size") or body.get("imageSize") or body.get("quality_tier") or "").upper()
    if req_size:
        err = _validate_provider_image_size(req_size, provider)
        if err:
            return err

    # ── Prompt safety preflight ──
    raw_prompt = (body.get("prompt") or "").strip()
    if raw_prompt:
        from flask import g
        user_id = getattr(g, "identity_id", None)
        safety = check_prompt_safety(raw_prompt, medium="image", provider=provider, user_id=user_id)
        if safety["decision"] in ("block", "warn"):
            status_code = 451 if safety["decision"] == "block" else 422
            return jsonify({
                "ok": False,
                "error": "prompt_safety",
                "safety": safety,
            }), status_code

    try:
        if provider == "nano_banana":
            # Route to PiAPI Nano Banana 2
            return _handle_nano_banana_image_generate(body)
        elif provider == "google":
            # Route to Gemini Imagen
            return _handle_gemini_image_generate(body)
        elif provider == "google_nano":
            return _handle_google_nano_image_generate(body)
        elif provider == "flux_pro":
            return _handle_flux_pro_image_generate(body)
        elif provider == "ideogram_v3":
            return _handle_ideogram_v3_image_generate(body)
        elif provider == "recraft_v4":
            return _handle_recraft_v4_image_generate(body)
        elif provider == "openai":
            # Route to OpenAI (via existing endpoint logic)
            return _handle_openai_image_generate(body)
        else:
            return jsonify({
                "error": "invalid_provider",
                "message": f"Unknown image provider: {provider}",
                "allowed": get_allowed_image_providers(),
            }), 400
    except Exception as e:
        print(f"[IMAGE_API] Unhandled error provider={provider}: {e}")
        print(traceback.format_exc())
        return jsonify({
            "error": "image_generate_internal_error",
            "message": "Image generation failed before dispatch. Please try again.",
            "details": {"provider": provider},
        }), 500


def _handle_nano_banana_image_generate(body: dict):
    """Handle PiAPI Nano Banana 2 image generation (async, returns job_id for polling)."""
    # Fail-fast: Check if PiAPI is configured
    is_configured, config_error = check_piapi_configured()
    if not is_configured:
        return jsonify({
            "error": "piapi_not_configured",
            "message": "Nano Banana image provider is not configured. Set PIAPI_API_KEY.",
            "details": {"hint": config_error}
        }), 500

    # Require authentication
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({
            "error": "invalid_params",
            "message": "prompt is required",
            "field": "prompt"
        }), 400

    # Parse options — map from UI format to PiAPI format
    # UI sends aspect_ratio in Google-style ("1:1", "9:16", "16:9")
    aspect_ratio = body.get("aspect_ratio") or body.get("aspectRatio") or "1:1"
    # UI sends image_size ("1K", "2K") — map to PiAPI resolution
    resolution = body.get("image_size") or body.get("imageSize") or body.get("resolution") or "1K"
    output_format = body.get("output_format") or "png"

    # Stability guardrails
    guard_error = ExpenseGuard.check_image_request(n=1)
    if guard_error:
        return guard_error

    # Idempotency check
    idempotency_key = ExpenseGuard.compute_idempotency_key(
        identity_id or "", "image_generate", prompt,
        provider="nano_banana", aspect_ratio=aspect_ratio, resolution=resolution
    )
    cached = ExpenseGuard.is_duplicate_request(idempotency_key)
    if cached:
        return jsonify(cached)

    # Validate aspect_ratio
    if aspect_ratio not in PIAPI_ALLOWED_ASPECT_RATIOS:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid aspect_ratio: {aspect_ratio}",
            "field": "aspect_ratio",
            "allowed": list(PIAPI_ALLOWED_ASPECT_RATIOS)
        }), 400

    # Validate resolution
    if resolution not in PIAPI_ALLOWED_RESOLUTIONS:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid resolution: {resolution}",
            "field": "resolution",
            "allowed": list(PIAPI_ALLOWED_RESOLUTIONS)
        }), 400

    # Generate job ID
    internal_job_id = str(uuid.uuid4())

    # Reserve credits (provider-specific: Nano Banana is premium)
    action_key = _get_image_action_key(resolution, "nano_banana")
    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "model": "gemini-2.5-flash-image", "provider": "nano_banana"},
    )
    if credit_error:
        return credit_error

    # Store metadata for async processing
    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": "gemini-2.5-flash-image",
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "output_format": output_format,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
        "provider": "nano_banana",
    }

    # Save to in-memory store
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    # Create job record for tracking
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="nano_banana",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    # Register active job for per-identity concurrent limit tracking
    ExpenseGuard.register_active_job(internal_job_id, identity_id)

    # Dispatch async — PiAPI task creation + polling happens in background thread
    get_executor().submit(
        dispatch_piapi_nano_banana_async,
        internal_job_id,
        identity_id,
        reservation_id,
        prompt,
        aspect_ratio,
        resolution,
        output_format,
        store_meta,
    )

    log_event("image/generate:nano_banana:queued", {"internal_job_id": internal_job_id})

    # Return immediately with job_id for polling
    balance_info = get_current_balance(identity_id)
    response_data = {
        "ok": True,
        "job_id": internal_job_id,
        "image_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "model": "gemini-2.5-flash-image",
        "provider": "nano_banana",
    }

    # Cache response for idempotency
    ExpenseGuard.cache_response(idempotency_key, response_data)

    return jsonify(response_data)


def _handle_gemini_image_generate(body: dict):
    """Handle Gemini Imagen image generation (async, returns job_id for polling)."""
    # Fail-fast: Check if Gemini is configured
    is_configured, config_error = check_gemini_configured()
    if not is_configured:
        return jsonify({
            "error": "gemini_not_configured",
            "message": "Gemini image provider is not configured. Set GEMINI_API_KEY.",
            "details": {"hint": config_error}
        }), 500

    # Require authentication
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({
            "error": "invalid_params",
            "message": "prompt is required",
            "field": "prompt"
        }), 400

    # Parse options with defaults
    aspect_ratio = body.get("aspect_ratio") or body.get("aspectRatio") or "1:1"
    image_size = body.get("image_size") or body.get("imageSize") or "1K"
    sample_count = int(body.get("sample_count") or body.get("sampleCount") or body.get("n") or 1)

    # Stability guardrails: check API limits and concurrent jobs
    guard_error = ExpenseGuard.check_image_request(n=sample_count)
    if guard_error:
        return guard_error

    # Idempotency check: return cached response if duplicate
    idempotency_key = ExpenseGuard.compute_idempotency_key(
        identity_id or "", "image_generate", prompt,
        provider="google", aspect_ratio=aspect_ratio, image_size=image_size, n=sample_count
    )
    cached = ExpenseGuard.is_duplicate_request(idempotency_key)
    if cached:
        return jsonify(cached)

    # Validate aspect_ratio
    if aspect_ratio not in ALLOWED_ASPECT_RATIOS:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid aspect_ratio: {aspect_ratio}",
            "field": "aspect_ratio",
            "allowed": list(ALLOWED_ASPECT_RATIOS)
        }), 400

    # Validate image_size
    if image_size not in ALLOWED_IMAGE_SIZES:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid image_size: {image_size}",
            "field": "image_size",
            "allowed": list(ALLOWED_IMAGE_SIZES)
        }), 400

    # Generate job ID
    internal_job_id = str(uuid.uuid4())

    # Reserve credits (provider-specific: Gemini uses google tier)
    action_key = _get_image_action_key(image_size, "google")
    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "model": "imagen-4.0", "provider": "google"},
    )
    if credit_error:
        return credit_error

    # Store metadata for async processing
    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": "imagen-4.0",
        "aspect_ratio": aspect_ratio,
        "image_size": image_size,
        "sample_count": sample_count,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
        "provider": "google",
    }

    # Save to in-memory store
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    # Create job record for tracking (same as OpenAI flow)
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="google",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    # Register active job for per-identity concurrent limit tracking
    ExpenseGuard.register_active_job(internal_job_id, identity_id)

    # Dispatch async - Gemini API call happens in background thread
    get_executor().submit(
        dispatch_gemini_image_async,
        internal_job_id,
        identity_id,
        reservation_id,
        prompt,
        aspect_ratio,
        image_size,
        sample_count,
        store_meta,
    )

    log_event("image/generate:gemini:queued", {"internal_job_id": internal_job_id})

    # Return immediately with job_id for polling
    # Credits are now held in DB - frontend can see via /api/credits/wallet
    balance_info = get_current_balance(identity_id)
    response_data = {
        "ok": True,
        "job_id": internal_job_id,
        "image_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "model": "imagen-4.0",
        "provider": "google",
    }

    # Cache response for idempotency
    ExpenseGuard.cache_response(idempotency_key, response_data)

    return jsonify(response_data)


def _handle_google_nano_image_generate(body: dict):
    """Handle direct Google Nano image generation (async)."""
    is_configured, config_error = check_google_nano_configured()
    if not is_configured:
        return jsonify({
            "error": "google_nano_not_configured",
            "message": "Google Nano image provider is not configured. Set GEMINI_API_KEY.",
            "details": {"hint": config_error},
        }), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "invalid_params", "message": "prompt is required", "field": "prompt"}), 400

    aspect_ratio = body.get("aspect_ratio") or body.get("aspectRatio") or "1:1"
    image_size = (body.get("image_size") or body.get("imageSize") or "1K").upper()

    guard_error = ExpenseGuard.check_image_request(n=1)
    if guard_error:
        return guard_error

    idempotency_key = ExpenseGuard.compute_idempotency_key(
        identity_id or "", "image_generate", prompt,
        provider="google_nano", aspect_ratio=aspect_ratio, image_size=image_size,
    )
    cached = ExpenseGuard.is_duplicate_request(idempotency_key)
    if cached:
        return jsonify(cached)

    if aspect_ratio not in GOOGLE_NANO_ALLOWED_ASPECT_RATIOS:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid aspect_ratio: {aspect_ratio}",
            "field": "aspect_ratio",
            "allowed": list(GOOGLE_NANO_ALLOWED_ASPECT_RATIOS),
        }), 400
    if image_size not in GOOGLE_NANO_ALLOWED_IMAGE_SIZES:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid image_size: {image_size}",
            "field": "image_size",
            "allowed": list(GOOGLE_NANO_ALLOWED_IMAGE_SIZES),
        }), 400

    internal_job_id = str(uuid.uuid4())
    action_key = get_registry_image_action_key("google_nano", image_size)
    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "model": "gemini-2.5-flash-image", "provider": "google_nano"},
    )
    if credit_error:
        return credit_error

    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": "gemini-2.5-flash-image",
        "aspect_ratio": aspect_ratio,
        "image_size": image_size,
        "provider_variant": "direct_google",
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
        "provider": "google_nano",
    }
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="google_nano",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )
    ExpenseGuard.register_active_job(internal_job_id, identity_id)
    get_executor().submit(
        dispatch_google_nano_image_async,
        internal_job_id,
        identity_id,
        reservation_id,
        prompt,
        aspect_ratio,
        image_size,
        store_meta,
    )

    balance_info = get_current_balance(identity_id)
    response_data = {
        "ok": True,
        "job_id": internal_job_id,
        "image_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "model": "gemini-2.5-flash-image",
        "provider": "google_nano",
        "provider_variant": "direct_google",
    }
    ExpenseGuard.cache_response(idempotency_key, response_data)
    return jsonify(response_data)


def _handle_flux_pro_image_generate(body: dict):
    """Handle BFL FLUX.2 image generation / editing (async)."""
    is_configured, config_error = check_flux_pro_configured()
    if not is_configured:
        return jsonify({
            "error": "flux_pro_not_configured",
            "message": "FLUX.2 Pro is not configured. Set BFL_API_KEY and enable IMAGE_PROVIDER_FLUX_PRO_ENABLED.",
            "details": {"hint": config_error},
        }), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "invalid_params", "message": "prompt is required", "field": "prompt"}), 400

    shape = body.get("shape") or "square"
    width, height = _parse_resolution_pair(
        _body_value(body, "resolution", "size"),
        FLUX_PRO_RESOLUTION_MAP.get(shape, FLUX_PRO_RESOLUTION_MAP["square"]),
    )
    model_variant = str(_body_value(body, "model_variant", "modelVariant", default="pro") or "pro").strip().lower()
    operation = str(_body_value(body, "operation", default="generate") or "generate").strip().lower()
    source_images = _normalize_image_inputs(body, "source_image", "sourceImage", "input_image")
    extra_references = _normalize_image_inputs(body, "reference_images", "referenceImages", "input_images")
    reference_images = []
    for index, asset in enumerate(source_images + extra_references, start=1):
        asset_url = ensure_asset_url(
            asset,
            provider="flux_pro",
            identity_id=identity_id,
            prefix="source_images",
            name=f"flux-ref-{index}",
        )
        if asset_url:
            if asset_url.startswith("data:"):
                return jsonify({
                    "error": "invalid_params",
                    "message": "FLUX reference images must resolve to accessible URLs. AWS-backed uploads are required for local data URLs.",
                    "field": "source_image",
                }), 400
            reference_images.append(asset_url)
    if reference_images and operation == "generate":
        operation = "edit"
    if operation == "edit" and not reference_images:
        return jsonify({
            "error": "invalid_params",
            "message": "FLUX Reference / Edit mode requires at least one source or reference image.",
            "field": "source_image",
        }), 400

    request_options = {
        "prompt": prompt,
        "operation": operation,
        "model_variant": model_variant,
        "width": width,
        "height": height,
        "reference_images": reference_images,
        "prompt_upsampling": coerce_bool(_body_value(body, "prompt_upsampling", "promptUpsampling"), True),
        "seed": coerce_int(body.get("seed")),
        "guidance": coerce_float(body.get("guidance")),
        "steps": coerce_int(body.get("steps")),
        "safety_tolerance": coerce_int(_body_value(body, "safety_tolerance", "safetyTolerance"), 2),
        "output_format": str(_body_value(body, "output_format", "outputFormat", default="jpeg") or "jpeg").strip().lower(),
        "transparent_background": coerce_bool(_body_value(body, "transparent_background", "transparentBackground"), False),
    }

    guard_error = ExpenseGuard.check_image_request(n=1)
    if guard_error:
        return guard_error

    idempotency_key = ExpenseGuard.compute_idempotency_key(
        identity_id or "", "image_generate", prompt,
        provider="flux_pro",
        width=width,
        height=height,
        model_variant=model_variant,
        operation=operation,
        source=(reference_images[0] if reference_images else "")[:96],
        refs=len(reference_images),
    )
    cached = ExpenseGuard.is_duplicate_request(idempotency_key)
    if cached:
        return jsonify(cached)

    internal_job_id = str(uuid.uuid4())
    action_key = get_registry_image_action_key("flux_pro", "1K")
    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "model": f"flux-2-{model_variant}", "provider": "flux_pro", "operation": operation},
    )
    if credit_error:
        return credit_error

    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": f"flux-2-{model_variant}",
        "width": width,
        "height": height,
        "operation": operation,
        "model_variant": model_variant,
        "reference_count": len(reference_images),
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
        "provider": "flux_pro",
    }
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="flux_pro",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )
    ExpenseGuard.register_active_job(internal_job_id, identity_id)
    get_executor().submit(
        dispatch_flux_pro_image_async,
        internal_job_id,
        identity_id,
        reservation_id,
        request_options,
        store_meta,
    )

    balance_info = get_current_balance(identity_id)
    response_data = {
        "ok": True,
        "job_id": internal_job_id,
        "image_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "model": f"flux-2-{model_variant}",
        "provider": "flux_pro",
        "provider_variant": model_variant,
        "operation": operation,
    }
    ExpenseGuard.cache_response(idempotency_key, response_data)
    return jsonify(response_data)


def _handle_ideogram_v3_image_generate(body: dict):
    """Handle Ideogram V3 generation/edit operations (async wrapper over sync upstream)."""
    is_configured, config_error = check_ideogram_v3_configured()
    if not is_configured:
        return jsonify({
            "error": "ideogram_v3_not_configured",
            "message": "Ideogram V3 is not configured. Set IDEOGRAM_API_KEY and enable IMAGE_PROVIDER_IDEOGRAM_V3_ENABLED.",
            "details": {"hint": config_error},
        }), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    operation = str(_body_value(body, "operation", default="generate") or "generate").strip().lower()
    prompt = (body.get("prompt") or "").strip()
    if not prompt and operation not in {"reframe", "upscale"}:
        return jsonify({"error": "invalid_params", "message": "prompt is required", "field": "prompt"}), 400

    shape = body.get("shape") or "square"
    aspect_ratio = str(_body_value(body, "aspect_ratio", "aspectRatio", default="") or "").strip()
    resolution = str(_body_value(body, "resolution", "size", default="") or "").strip()

    if operation == "reframe":
        if not aspect_ratio and not resolution:
            resolution = IDEOGRAM_REFRAME_RESOLUTION_MAP.get(shape, IDEOGRAM_REFRAME_RESOLUTION_MAP["square"])
    elif operation != "upscale":
        # Ideogram V3 standard generate/edit/remix flows should use aspect ratios.
        # Older frontends may still send generic 1024x1536-style resolutions, which
        # Ideogram rejects, so ignore those here and normalize to provider ratios.
        resolution = ""
        if not aspect_ratio:
            aspect_ratio = IDEOGRAM_ASPECT_RATIO_MAP.get(shape, IDEOGRAM_ASPECT_RATIO_MAP["square"])

    request_options = {
        "prompt": prompt,
        "operation": operation,
        "aspect_ratio": aspect_ratio,
        "resolution": resolution,
        "source_image": _body_value(body, "source_image", "sourceImage", "input_image"),
        "mask_image": _body_value(body, "mask_image", "maskImage"),
        "style_reference_images": _body_value(body, "style_reference_images", "styleReferenceImages"),
        "character_reference_images": _body_value(body, "character_reference_images", "characterReferenceImages"),
        "character_reference_masks": _body_value(body, "character_reference_masks", "characterReferenceMasks"),
        "seed": coerce_int(body.get("seed")),
        "rendering_speed": str(_body_value(body, "rendering_speed", "renderingSpeed", default="DEFAULT") or "DEFAULT").strip().upper(),
        "magic_prompt": str(_body_value(body, "magic_prompt", "magicPrompt", default="AUTO") or "AUTO").strip().upper(),
        "negative_prompt": str(_body_value(body, "negative_prompt", "negativePrompt", default="") or "").strip(),
        "style_type": str(_body_value(body, "style_type", "styleType", default="") or "").strip().upper(),
        "style_preset": str(_body_value(body, "style_preset", "stylePreset", default="") or "").strip(),
        "style_codes": _body_value(body, "style_codes", "styleCodes"),
        "color_palette_name": str(_body_value(body, "color_palette_name", "colorPaletteName", default="") or "").strip(),
        "color_palette_members": _body_value(body, "color_palette_members", "colorPaletteMembers"),
        "image_weight": coerce_float(_body_value(body, "image_weight", "imageWeight")),
        "upscale_factor": str(_body_value(body, "upscale_factor", "upscaleFactor", default="X1") or "X1").strip().upper(),
        "detail": coerce_int(body.get("detail"), 50),
        "resemblance": coerce_int(body.get("resemblance"), 50),
        "num_images": 1,
    }

    guard_error = ExpenseGuard.check_image_request(n=1)
    if guard_error:
        return guard_error

    idempotency_key = ExpenseGuard.compute_idempotency_key(
        identity_id or "", "image_generate", prompt,
        provider="ideogram_v3",
        resolution=resolution,
        operation=operation,
        source=str(request_options.get("source_image") or "")[:96],
        mask=str(request_options.get("mask_image") or "")[:96],
    )
    cached = ExpenseGuard.is_duplicate_request(idempotency_key)
    if cached:
        return jsonify(cached)

    internal_job_id = str(uuid.uuid4())
    action_key = get_registry_image_action_key("ideogram_v3", "1K")
    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "model": "ideogram-v3", "provider": "ideogram_v3", "operation": operation},
    )
    if credit_error:
        return credit_error

    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": "ideogram-v3",
        "resolution": resolution,
        "aspect_ratio": aspect_ratio,
        "operation": operation,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
        "provider": "ideogram_v3",
    }
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="ideogram_v3",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )
    ExpenseGuard.register_active_job(internal_job_id, identity_id)
    get_executor().submit(
        dispatch_ideogram_v3_image_async,
        internal_job_id,
        identity_id,
        reservation_id,
        request_options,
        store_meta,
    )

    balance_info = get_current_balance(identity_id)
    response_data = {
        "ok": True,
        "job_id": internal_job_id,
        "image_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "model": "ideogram-v3",
        "provider": "ideogram_v3",
        "provider_variant": operation,
        "operation": operation,
    }
    ExpenseGuard.cache_response(idempotency_key, response_data)
    return jsonify(response_data)


def _handle_recraft_v4_image_generate(body: dict):
    """Handle Recraft generation/edit/vector operations."""
    is_configured, config_error = check_recraft_v4_configured()
    if not is_configured:
        return jsonify({
            "error": "recraft_v4_not_configured",
            "message": "Recraft V4 is not configured. Set RECRAFT_API_KEY and enable IMAGE_PROVIDER_RECRAFT_V4_ENABLED.",
            "details": {"hint": config_error},
        }), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    operation = str(_body_value(body, "operation", default="generate") or "generate").strip().lower()
    prompt = (body.get("prompt") or "").strip()
    if operation in {"generate", "image_to_image", "inpaint", "replace_background", "generate_background"} and not prompt:
        return jsonify({"error": "invalid_params", "message": "prompt is required", "field": "prompt"}), 400

    shape = body.get("shape") or "square"
    size = str(_body_value(body, "size", "resolution", default=RECRAFT_SIZE_MAP.get(shape, RECRAFT_SIZE_MAP["square"])) or "").strip()
    model_variant = str(_body_value(body, "model_variant", "modelVariant", default="") or "").strip()
    output_mode = (body.get("output_mode") or body.get("outputMode") or "raster").lower()
    if not model_variant:
        model_variant = "recraftv4_vector" if output_mode == "vector_svg" else "recraftv4"
    if operation == "vectorize":
        output_mode = "vector_svg"
    if "vector" in model_variant.lower():
        output_mode = "vector_svg"
    if output_mode not in RECRAFT_ALLOWED_OUTPUT_MODES:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid output_mode: {output_mode}",
            "field": "output_mode",
            "allowed": list(RECRAFT_ALLOWED_OUTPUT_MODES),
        }), 400

    request_options = {
        "prompt": prompt,
        "operation": operation,
        "size": size,
        "shape": shape,
        "model_variant": model_variant,
        "output_mode": output_mode,
        "style": str(body.get("style") or "").strip(),
        "style_id": str(_body_value(body, "style_id", "styleId", default="") or "").strip(),
        "negative_prompt": str(_body_value(body, "negative_prompt", "negativePrompt", default="") or "").strip(),
        "source_image": _body_value(body, "source_image", "sourceImage", "input_image"),
        "mask_image": _body_value(body, "mask_image", "maskImage"),
        "strength": coerce_float(body.get("strength")),
        "seed": coerce_int(body.get("seed")),
        "response_format": str(_body_value(body, "response_format", "responseFormat", default="url") or "url").strip(),
        "background_color": str(_body_value(body, "background_color", "backgroundColor", default="") or "").strip(),
        "preferred_colors": _body_value(body, "preferred_colors", "preferredColors"),
        "artistic_level": coerce_int(_body_value(body, "artistic_level", "artisticLevel")),
        "no_text": coerce_bool(_body_value(body, "no_text", "noText"), False),
        "svg_compression": coerce_bool(_body_value(body, "svg_compression", "svgCompression"), False),
        "limit_num_shapes": coerce_bool(_body_value(body, "limit_num_shapes", "limitNumShapes"), False),
        "max_num_shapes": coerce_int(_body_value(body, "max_num_shapes", "maxNumShapes")),
        "text_layout": _body_value(body, "text_layout", "textLayout"),
    }

    try:
        validate_recraft_params(request_options)
    except RecraftValidationError as e:
        payload = {
            "error": "invalid_params",
            "message": e.message,
            "field": e.field,
        }
        if e.allowed:
            payload["allowed"] = e.allowed
        return jsonify(payload), 400

    guard_error = ExpenseGuard.check_image_request(n=1)
    if guard_error:
        return guard_error

    idempotency_key = ExpenseGuard.compute_idempotency_key(
        identity_id or "", "image_generate", prompt,
        provider="recraft_v4",
        size=size,
        output_mode=output_mode,
        operation=operation,
        model_variant=model_variant,
        source=str(request_options.get("source_image") or "")[:96],
        mask=str(request_options.get("mask_image") or "")[:96],
    )
    cached = ExpenseGuard.is_duplicate_request(idempotency_key)
    if cached:
        return jsonify(cached)

    internal_job_id = str(uuid.uuid4())
    action_key = get_registry_image_action_key("recraft_v4", "1K", output_mode)
    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {
            "prompt": prompt[:100],
            "model": model_variant,
            "provider": "recraft_v4",
            "output_mode": output_mode,
            "operation": operation,
        },
    )
    if credit_error:
        return credit_error

    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": model_variant,
        "size": size,
        "output_mode": output_mode,
        "operation": operation,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
        "provider": "recraft_v4",
    }
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="recraft_v4",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )
    ExpenseGuard.register_active_job(internal_job_id, identity_id)
    get_executor().submit(
        dispatch_recraft_v4_image_async,
        internal_job_id,
        identity_id,
        reservation_id,
        request_options,
        store_meta,
    )

    balance_info = get_current_balance(identity_id)
    response_data = {
        "ok": True,
        "job_id": internal_job_id,
        "image_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "model": model_variant,
        "provider": "recraft_v4",
        "output_mode": output_mode,
        "provider_variant": operation,
        "operation": operation,
    }
    ExpenseGuard.cache_response(idempotency_key, response_data)
    return jsonify(response_data)


def _handle_openai_image_generate(body: dict):
    """Handle OpenAI image generation (async, returns job_id for polling)."""
    if not OPENAI_API_KEY:
        return jsonify({
            "error": "openai_not_configured",
            "message": "OpenAI image provider is not configured. Set OPENAI_API_KEY."
        }), 500

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({
            "error": "invalid_params",
            "message": "prompt required",
            "field": "prompt"
        }), 400

    size_raw = (body.get("size") or body.get("resolution") or "1024x1024").lower()
    size_map = {
        "1024x1024": "1024x1024",
        "1024x1536": "1024x1536",
        "1536x1024": "1536x1024",
    }
    size = "1024x1024"
    for key in size_map:
        if key in size_raw:
            size = size_map[key]
            break

    model = (body.get("model") or os.getenv("OPENAI_IMAGE_MODEL") or "gpt-image-1.5").strip()
    n = int(body.get("n") or 1)
    response_format = (body.get("response_format") or "url").strip()

    # Stability guardrails: check API limits and concurrent jobs
    guard_error = ExpenseGuard.check_image_request(n=n)
    if guard_error:
        return guard_error

    # Idempotency check: return cached response if duplicate
    idempotency_key = ExpenseGuard.compute_idempotency_key(
        identity_id or "", "image_generate", prompt,
        provider="openai", size=size, model=model, n=n
    )
    cached = ExpenseGuard.is_duplicate_request(idempotency_key)
    if cached:
        return jsonify(cached)

    internal_job_id = str(uuid.uuid4())
    # Resolve provider-specific action key from image_size if provided
    image_size = (body.get("image_size") or body.get("imageSize") or "1K").upper()
    action_key = _get_image_action_key(image_size, "openai")

    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "n": n, "model": model, "size": size, "provider": "openai"},
    )
    if credit_error:
        return credit_error

    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": model,
        "size": size,
        "n": n,
        "response_format": response_format,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
        "provider": "openai",
    }

    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="openai",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    # Register active job for per-identity concurrent limit tracking
    ExpenseGuard.register_active_job(internal_job_id, identity_id)

    get_executor().submit(
        _dispatch_openai_image_async,
        internal_job_id,
        identity_id,
        reservation_id,
        {"prompt": prompt, "size": size, "model": model, "n": n, "response_format": response_format},
        store_meta,
    )

    log_event("image/generate:openai", {"internal_job_id": internal_job_id})

    balance_info = get_current_balance(identity_id)
    response_data = {
        "ok": True,
        "job_id": internal_job_id,
        "image_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "model": model,
        "size": size,
        "provider": "openai",
    }

    # Cache response for idempotency
    ExpenseGuard.cache_response(idempotency_key, response_data)

    return jsonify(response_data)


@bp.route("/image/gemini", methods=["POST", "OPTIONS"])
@with_session
def gemini_image_mod():
    """
    Generate an image using Gemini Imagen 4.0.

    Request body:
    {
        "prompt": "A beautiful sunset over mountains",
        "aspect_ratio": "16:9",     # "1:1", "3:4", "4:3", "9:16", "16:9"
        "image_size": "1K",         # "1K" or "2K"
        "sample_count": 1           # Number of images (1-4)
    }

    Response (success):
    {
        "ok": true,
        "image_url": "data:image/png;base64,...",
        "image_base64": "...",
        "image_id": "uuid",
        "provider": "google"
    }

    Response (error):
    {
        "error": "<machine_code>",
        "message": "<human readable>",
        "details": {...}
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    # Fail-fast: Check if Gemini is configured
    is_configured, config_error = check_gemini_configured()
    if not is_configured:
        return jsonify({
            "error": "gemini_not_configured",
            "message": "Set GEMINI_API_KEY environment variable",
            "details": {"hint": config_error}
        }), 500

    # Require authentication
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({
            "error": "invalid_params",
            "message": "prompt is required",
            "field": "prompt"
        }), 400

    # Parse options with defaults
    aspect_ratio = body.get("aspect_ratio") or body.get("aspectRatio") or "1:1"
    image_size = body.get("image_size") or body.get("imageSize") or "1K"
    sample_count = int(body.get("sample_count") or body.get("sampleCount") or 1)

    # Validate aspect_ratio
    if aspect_ratio not in ALLOWED_ASPECT_RATIOS:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid aspect_ratio: {aspect_ratio}",
            "field": "aspect_ratio",
            "allowed": list(ALLOWED_ASPECT_RATIOS)
        }), 400

    # Validate image_size
    if image_size not in ALLOWED_IMAGE_SIZES:
        return jsonify({
            "error": "invalid_params",
            "message": f"Invalid image_size: {image_size}",
            "field": "image_size",
            "allowed": list(ALLOWED_IMAGE_SIZES)
        }), 400

    # Generate job ID
    internal_job_id = str(uuid.uuid4())

    # Reserve credits (provider-specific: Gemini uses google tier)
    action_key = _get_image_action_key(image_size, "google")
    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "model": "imagen-4.0", "provider": "google"},
    )
    if credit_error:
        return credit_error

    # Store metadata for async processing
    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": "imagen-4.0",
        "aspect_ratio": aspect_ratio,
        "image_size": image_size,
        "sample_count": sample_count,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
        "provider": "google",
    }

    # Save to in-memory store
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    # Create job record for tracking (same as OpenAI flow)
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="google",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    # Dispatch async - Gemini API call happens in background thread
    get_executor().submit(
        dispatch_gemini_image_async,
        internal_job_id,
        identity_id,
        reservation_id,
        prompt,
        aspect_ratio,
        image_size,
        sample_count,
        store_meta,
    )

    log_event("image/gemini:queued", {"internal_job_id": internal_job_id})

    # Return immediately with job_id for polling
    # Credits are now held in DB - frontend can see via /api/credits/wallet
    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": internal_job_id,
        "image_id": internal_job_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "status": "queued",
        "model": "imagen-4.0",
        "provider": "google",
    })


# NOTE: Image editing endpoint removed - Imagen 4.0 is text-to-image only.
# For image editing, consider using a different model or workflow.


@bp.route("/image/openai", methods=["POST", "OPTIONS"])
@with_session
def openai_image_mod():
    if request.method == "OPTIONS":
        return ("", 204)

    if not OPENAI_API_KEY:
        return jsonify({"error": "OPENAI_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "prompt required"}), 400

    size_raw = (body.get("size") or body.get("resolution") or "1024x1024").lower()
    # GPT Image 1 supported sizes (gpt-image-1)
    size_map = {
        "1024x1024": "1024x1024",
        "1024x1536": "1024x1536",
        "1536x1024": "1536x1024",
    }
    size = "1024x1024"
    for key in size_map:
        if key in size_raw:
            size = size_map[key]
            break

    model = (body.get("model") or os.getenv("OPENAI_IMAGE_MODEL") or "gpt-image-1.5").strip()
    n = int(body.get("n") or 1)
    response_format = (body.get("response_format") or "url").strip()

    internal_job_id = str(uuid.uuid4())
    # Resolve provider-specific action key from image_size if provided
    image_size = (body.get("image_size") or body.get("imageSize") or "1K").upper()
    action_key = _get_image_action_key(image_size, "openai")

    # DEBUG: Trace OpenAI image credit flow
    print(f"[OPENAI_IMAGE:DEBUG] >>> Route handler: identity_id={identity_id}, action_key={action_key}, job_id={internal_job_id}")

    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "n": n, "model": model, "size": size},
    )

    print(f"[OPENAI_IMAGE:DEBUG] start_paid_job returned: reservation_id={reservation_id}, credit_error={credit_error is not None}")

    if credit_error:
        print(f"[OPENAI_IMAGE:DEBUG] !!! Credit error returned, aborting")
        return credit_error

    store_meta = {
        "stage": "image",
        "created_at": now_s() * 1000,
        "prompt": prompt,
        "model": model,
        "size": size,
        "n": n,
        "response_format": response_format,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
        "status": "queued",
    }

    # Persist immediately so status polling works across workers
    store = load_store()
    store[internal_job_id] = store_meta
    save_store(store)

    # Persist job row so status polling works across workers
    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="openai",
        action_key=action_key,
        prompt=prompt,
        meta=store_meta,
        reservation_id=reservation_id,
        status="queued",
    )

    get_executor().submit(
        _dispatch_openai_image_async,
        internal_job_id,
        identity_id,
        reservation_id,
        {"prompt": prompt, "size": size, "model": model, "n": n, "response_format": response_format},
        store_meta,
    )

    log_event("image/openai:dispatched[mod]", {"internal_job_id": internal_job_id})

    balance_info = get_current_balance(identity_id)
    # DEBUG: Log wallet state after reservation
    print(f"[OPENAI_IMAGE:DEBUG] Job dispatched. balance_info={balance_info}, reservation_id={reservation_id}")
    if balance_info:
        print(f"[OPENAI_IMAGE:DEBUG] Wallet state: balance={balance_info.get('balance')}, reserved={balance_info.get('reserved')}, available={balance_info.get('available')}")

    return jsonify(
        {
            "ok": True,
            "job_id": internal_job_id,
            "image_id": internal_job_id,
            "reservation_id": reservation_id,
            "new_balance": balance_info["available"] if balance_info else None,
            "status": "queued",
            "model": model,
            "size": size,
            "source": "modular",
        }
    )


# ─────────────────────────────────────────────────────────────
# Unified Image Status Endpoint
# ─────────────────────────────────────────────────────────────
# Single canonical endpoint for all image providers.
# Resolves provider from the DB job row, then returns a consistent
# response shape regardless of which provider generated the image.
# Legacy provider-specific endpoints below are kept for backward compat.


def _image_status_handler(job_id: str, identity_id: str):
    """
    Shared image status logic for all providers.

    Checks DB job row first (authoritative), falls back to in-memory store.
    Returns a consistent response dict or a Flask response tuple on error.
    """
    store = load_store()
    meta = store.get(job_id) or {}

    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, error_message, meta, provider
                        FROM timrx_billing.jobs
                        WHERE id::text = %s AND identity_id = %s
                        LIMIT 1
                        """,
                        (job_id, identity_id),
                    )
                    job = cur.fetchone()

            if job:
                job_meta = job.get("meta") or {}
                if isinstance(job_meta, str):
                    try:
                        job_meta = __import__('json').loads(job_meta)
                    except Exception:
                        job_meta = {}

                provider = job.get("provider") or meta.get("provider") or job_meta.get("provider") or "unknown"

                if job["status"] in ("queued", "processing"):
                    return jsonify({"ok": True, "status": "queued", "job_id": job_id, "provider": provider, "message": "Generating image..."})

                if job["status"] == "failed":
                    return jsonify({"ok": False, "status": "failed", "job_id": job_id, "provider": provider, "error": job.get("error_message", "Image generation failed")})

                if job["status"] == "ready":
                    image_url = meta.get("image_url") or job_meta.get("image_url")
                    image_urls = meta.get("image_urls") or job_meta.get("image_urls") or ([] if not image_url else [image_url])
                    artifact_format = meta.get("artifact_format") or job_meta.get("artifact_format")
                    provider_variant = meta.get("provider_variant") or job_meta.get("provider_variant")
                    output_mode = meta.get("output_mode") or job_meta.get("output_mode") or "raster"
                    upstream_request_id = meta.get("upstream_request_id") or job_meta.get("upstream_request_id")
                    upstream_cost = meta.get("upstream_cost") or job_meta.get("upstream_cost")
                    mime_type = meta.get("mime_type") or job_meta.get("mime_type")

                    canonical = get_canonical_image_row(
                        identity_id,
                        upstream_id=job_id,
                        alt_upstream_id=job_meta.get("image_id") or meta.get("image_id"),
                    )
                    thumbnail_url = None
                    if canonical:
                        if canonical.get("image_url"):
                            image_url = canonical["image_url"]
                            image_urls = [image_url]
                        if canonical.get("thumbnail_url"):
                            thumbnail_url = canonical["thumbnail_url"]
                        canonical_meta = canonical.get("meta") or {}
                        if isinstance(canonical_meta, str):
                            try:
                                import json as _json
                                canonical_meta = _json.loads(canonical_meta)
                            except Exception:
                                canonical_meta = {}
                        if isinstance(canonical_meta, dict):
                            artifact_format = canonical_meta.get("artifact_format") or canonical_meta.get("format") or artifact_format
                            provider_variant = canonical_meta.get("provider_variant") or provider_variant
                            output_mode = canonical_meta.get("output_mode") or output_mode
                            upstream_request_id = canonical_meta.get("upstream_request_id") or upstream_request_id
                            upstream_cost = canonical_meta.get("upstream_cost") or upstream_cost
                            mime_type = canonical_meta.get("mime_type") or mime_type

                    balance_info = get_current_balance(identity_id) if identity_id else None

                    return jsonify({
                        "ok": True,
                        "status": "done",
                        "job_id": job_id,
                        "image_id": job_id,
                        "image_url": image_url,
                        "image_urls": image_urls,
                        "thumbnail_url": thumbnail_url,
                        "image_base64": meta.get("image_base64") or job_meta.get("image_base64"),
                        "mime_type": mime_type,
                        "model": meta.get("model") or job_meta.get("model"),
                        "provider": provider,
                        "artifact_format": artifact_format,
                        "provider_variant": provider_variant,
                        "output_mode": output_mode,
                        "upstream_request_id": upstream_request_id,
                        "upstream_cost": upstream_cost,
                        "new_balance": balance_info["available"] if balance_info else None,
                    })
        except Exception as e:
            print(f"[STATUS][mod] Error checking image job {job_id}: {e}")

    # Fallback to in-memory store
    if meta.get("status") == "done":
        provider = meta.get("provider") or "unknown"
        canonical = get_canonical_image_row(
            identity_id,
            upstream_id=job_id,
            alt_upstream_id=meta.get("image_id"),
        )
        thumbnail_url = None
        if canonical:
            if canonical.get("image_url"):
                meta["image_url"] = canonical["image_url"]
                meta["image_urls"] = [canonical["image_url"]]
            if canonical.get("thumbnail_url"):
                thumbnail_url = canonical["thumbnail_url"]
            canonical_meta = canonical.get("meta") or {}
            if isinstance(canonical_meta, str):
                try:
                    import json as _json
                    canonical_meta = _json.loads(canonical_meta)
                except Exception:
                    canonical_meta = {}
            if isinstance(canonical_meta, dict):
                meta.setdefault("artifact_format", canonical_meta.get("artifact_format") or canonical_meta.get("format"))
                meta.setdefault("provider_variant", canonical_meta.get("provider_variant"))
                meta.setdefault("output_mode", canonical_meta.get("output_mode"))
                meta.setdefault("upstream_request_id", canonical_meta.get("upstream_request_id"))
                meta.setdefault("upstream_cost", canonical_meta.get("upstream_cost"))
                meta.setdefault("mime_type", canonical_meta.get("mime_type"))

        balance_info = get_current_balance(identity_id) if identity_id else None

        return jsonify({
            "ok": True,
            "status": "done",
            "job_id": job_id,
            "image_id": job_id,
            "image_url": meta.get("image_url"),
            "image_urls": meta.get("image_urls", []),
            "thumbnail_url": thumbnail_url,
            "image_base64": meta.get("image_base64"),
            "mime_type": meta.get("mime_type"),
            "model": meta.get("model"),
            "provider": provider,
            "artifact_format": meta.get("artifact_format"),
            "provider_variant": meta.get("provider_variant"),
            "output_mode": meta.get("output_mode") or "raster",
            "upstream_request_id": meta.get("upstream_request_id"),
            "upstream_cost": meta.get("upstream_cost"),
            "new_balance": balance_info["available"] if balance_info else None,
        })

    return jsonify({"error": "Job not found"}), 404


@bp.route("/image/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def image_status_unified(job_id: str):
    """
    Canonical image status endpoint — works for all providers.

    Resolves provider from the DB job row. Returns a consistent response shape:
    - status: "queued" | "done" | "failed"
    - On done: image_url, image_urls, thumbnail_url, model, provider, new_balance
    - On failed: error message
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    return _image_status_handler(job_id, identity_id)


# ─────────────────────────────────────────────────────────────
# Legacy Provider-Specific Status Endpoints (kept for backward compat)
# ─────────────────────────────────────────────────────────────
# These delegate to the shared handler above. The canonical endpoint
# is /image/status/<job_id> — frontend should migrate to that.


@bp.route("/image/openai/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def openai_image_status_mod(job_id: str):
    """Legacy OpenAI image status — delegates to unified handler."""
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error
    return _image_status_handler(job_id, identity_id)


@bp.route("/image/gemini/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def gemini_image_status_mod(job_id: str):
    """Legacy Gemini image status — delegates to unified handler."""
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error
    return _image_status_handler(job_id, identity_id)


@bp.route("/image/piapi/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def piapi_image_status_mod(job_id: str):
    """Legacy PiAPI image status — delegates to unified handler."""
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error
    return _image_status_handler(job_id, identity_id)


@bp.route("/proxy-image")
@with_session
def proxy_image_mod():
    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    url = request.args.get("u") or ""
    if not url:
        return jsonify({"error": "Missing url"}), 400

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return jsonify({"error": "Invalid scheme"}), 400
    host = (parsed.hostname or "").lower()
    if host not in ALLOWED_IMAGE_HOSTS and (not config.AWS_BUCKET_MODELS or not host.startswith(config.AWS_BUCKET_MODELS.lower())):
        return jsonify({"error": "Host not allowed"}), 400

    if not USE_DB:
        return jsonify({"error": "db_unavailable"}), 503

    s3_key = parse_s3_key(url) if is_s3_url(url) else None
    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                if s3_key:
                    cur.execute(
                        f"""
                        SELECT 1
                        FROM {Tables.IMAGES}
                        WHERE identity_id = %s AND (image_s3_key = %s OR thumbnail_s3_key = %s OR source_s3_key = %s)
                        UNION
                        SELECT 1
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE identity_id = %s AND (image_url = %s OR thumbnail_url = %s)
                        LIMIT 1
                        """,
                        (identity_id, s3_key, s3_key, s3_key, identity_id, url, url),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT 1
                        FROM {Tables.IMAGES}
                        WHERE identity_id = %s AND (image_url = %s OR thumbnail_url = %s)
                        UNION
                        SELECT 1
                        FROM {Tables.HISTORY_ITEMS}
                        WHERE identity_id = %s AND (image_url = %s OR thumbnail_url = %s)
                        LIMIT 1
                        """,
                        (identity_id, url, url, identity_id, url, url),
                    )
                row = cur.fetchone()
        if not row:
            return jsonify({"error": "not_found"}), 404
    except Exception as e:
        print(f"[proxy-image][mod] ownership check failed: {e}")
        return jsonify({"error": "ownership_check_failed"}), 500

    if s3_key and config.AWS_BUCKET_MODELS:
        signed = presign_s3_url(url)
        if signed:
            url = signed

    try:
        r = requests.get(url, stream=True, timeout=30)
    except Exception as e:
        print(f"[PROVIDER_ERROR] provider=image_proxy error={e}")
        return jsonify({"error": "FETCH_FAILED", "message": "Failed to fetch the requested resource. Please try again."}), 502

    if not r.ok:
        print(f"[PROVIDER_ERROR] provider=image_proxy error=upstream_http_{r.status_code}")
        return jsonify({"error": "FETCH_FAILED", "message": "Failed to fetch the requested resource. Please try again."}), 502

    content_type = r.headers.get("Content-Type", "application/octet-stream")
    return Response(r.content, status=200, mimetype=content_type)


@bp.route("/cache-image", methods=["POST", "OPTIONS"])
def cache_image_mod():
    if request.method == "OPTIONS":
        return ("", 204)

    body = request.get_json(silent=True) or {}
    data_url = body.get("data_url") or ""
    if not data_url.startswith("data:"):
        return jsonify({"error": "data_url is required and must be a data URI"}), 400

    max_bytes = int(os.getenv("CACHE_IMAGE_MAX_BYTES", "5242880"))
    allowed_mimes = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
    try:
        header, b64data = data_url.split(",", 1)
        meta = header.split(";")[0]
        mime = meta.replace("data:", "") or "image/png"
        if mime.lower() not in allowed_mimes:
            return jsonify({"error": "mime not allowed"}), 400
        if (len(b64data) * 3) / 4 > max_bytes:
            return jsonify({"error": "image too large"}), 400
        ext = ".png" if "png" in mime else ".jpg"
        file_id = f"{int(time.time()*1000)}"
        file_path = config.CACHE_DIR / f"{file_id}{ext}"
        file_path.write_bytes(base64.b64decode(b64data))
    except Exception as e:
        print(f"[INTERNAL_ERROR] context=data_url_decode error={e}")
        return jsonify({"error": "INPUT_VALIDATION_FAILED", "message": "Failed to decode image data. Please try again with a different image."}), 400

    return jsonify({"url": f"/api/cache-image/{file_path.name}", "mime": mime})


@bp.route("/cache-image/<path:filename>", methods=["GET"])
def cache_image_get_mod(filename: str):
    # Path traversal protection: resolve and verify target stays within CACHE_DIR
    cache_dir = config.CACHE_DIR.resolve()
    target = (cache_dir / filename).resolve()
    if not str(target).startswith(str(cache_dir) + os.sep) and target != cache_dir:
        return jsonify({"error": "Not found"}), 404
    if not target.exists() or not target.is_file():
        return jsonify({"error": "Not found"}), 404
    return Response(target.read_bytes(), mimetype="image/png")
