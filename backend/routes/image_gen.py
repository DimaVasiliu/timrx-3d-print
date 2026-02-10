"""
Image Generation Routes Blueprint (Modular)
------------------------------------------
Registered under /api/_mod.
"""

from __future__ import annotations

import base64
import os
import time
import uuid
from urllib.parse import urlparse

import requests
from flask import Blueprint, Response, jsonify, request

from backend.config import OPENAI_API_KEY, config
from backend.db import USE_DB, get_conn, dict_row, Tables
from backend.middleware import with_session
from backend.services.async_dispatch import get_executor, _dispatch_openai_image_async, dispatch_gemini_image_async, update_job_status_ready, update_job_status_failed
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
from backend.services.history_service import get_canonical_image_row, save_image_to_normalized_db
from backend.services.identity_service import require_identity
from backend.services.job_service import create_internal_job_row, load_store, save_store
from backend.services.s3_service import is_s3_url, parse_s3_key, presign_s3_url
from backend.utils.helpers import now_s, log_event

bp = Blueprint("image_gen", __name__)

# OpenAI blob hosts (from monolith)
ALLOWED_IMAGE_HOSTS = {
    "oaidalleapiprodscus.blob.core.windows.net",
    "oaidalleapiprodscus.bblob.core.windows.net",
    "oaidalleapiprodscus.blob.core.windows.net:443",
}


@bp.route("/image/generate", methods=["POST", "OPTIONS"])
@with_session
def image_generate_unified():
    """
    Unified image generation endpoint that routes based on provider.

    Request body:
    {
        "provider": "google" | "openai",  # Default: "openai"
        "prompt": "A beautiful sunset...",
        "aspect_ratio": "16:9",           # For Google: "1:1", "3:4", "4:3", "9:16", "16:9"
        "image_size": "1K",               # For Google: "1K" or "2K"
        "size": "1024x1024",              # For OpenAI
        "model": "gpt-image-1",           # For OpenAI
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

    if provider == "google":
        # Route to Gemini Imagen
        return _handle_gemini_image_generate(body)
    elif provider == "openai":
        # Route to OpenAI (via existing endpoint logic)
        return _handle_openai_image_generate(body)
    else:
        return jsonify({
            "error": "invalid_provider",
            "message": f"Unknown image provider: {provider}",
            "allowed": ["google", "openai"]
        }), 400


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

    # Reserve credits (creates held reservation in DB)
    action_key = "image_generate"  # Canonical key -> OPENAI_IMAGE (10 credits)
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

    # Register active job for concurrent limit tracking
    ExpenseGuard.register_active_job(internal_job_id)

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

    model = (body.get("model") or os.getenv("OPENAI_IMAGE_MODEL") or "gpt-image-1").strip()
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
    action_key = "image_generate"  # Canonical key -> OPENAI_IMAGE (10 credits)

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

    # Register active job for concurrent limit tracking
    ExpenseGuard.register_active_job(internal_job_id)

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

    # Reserve credits (creates held reservation in DB)
    action_key = "image_generate"  # Canonical key -> OPENAI_IMAGE (10 credits)
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

    model = (body.get("model") or os.getenv("OPENAI_IMAGE_MODEL") or "gpt-image-1").strip()
    n = int(body.get("n") or 1)
    response_format = (body.get("response_format") or "url").strip()

    internal_job_id = str(uuid.uuid4())
    # Use credits helper action key mapping in routes
    action_key = "image_generate"  # Canonical key -> OPENAI_IMAGE (10 credits)

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


@bp.route("/image/openai/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def openai_image_status_mod(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    store = load_store()
    meta = store.get(job_id) or {}

    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, error_message, meta
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

                if job["status"] == "queued":
                    return jsonify({"ok": True, "status": "queued", "job_id": job_id, "message": "Generating image..."})

                if job["status"] == "failed":
                    return jsonify({"ok": False, "status": "failed", "job_id": job_id, "error": job.get("error_message", "Image generation failed")})

                if job["status"] == "ready":
                    image_url = meta.get("image_url") or job_meta.get("image_url")
                    image_urls = meta.get("image_urls") or job_meta.get("image_urls") or ([] if not image_url else [image_url])

                    canonical = get_canonical_image_row(
                        identity_id,
                        upstream_id=job_id,
                        alt_upstream_id=job_meta.get("image_id") or meta.get("image_id"),
                    )
                    if canonical:
                        if canonical.get("image_url"):
                            image_url = canonical["image_url"]
                            image_urls = [image_url]
                        if canonical.get("thumbnail_url"):
                            meta["thumbnail_url"] = canonical["thumbnail_url"]
                    return jsonify({
                        "ok": True,
                        "status": "done",
                        "job_id": job_id,
                        "image_id": job_id,
                        "image_url": image_url,
                        "image_urls": image_urls,
                        "image_base64": meta.get("image_base64") or job_meta.get("image_base64"),
                        "model": meta.get("model") or job_meta.get("model"),
                        "size": meta.get("size") or job_meta.get("size"),
                    })
        except Exception as e:
            print(f"[STATUS][mod] Error checking OpenAI job {job_id}: {e}")

    if meta.get("status") == "done":
        canonical = get_canonical_image_row(
            identity_id,
            upstream_id=job_id,
            alt_upstream_id=meta.get("image_id"),
        )
        if canonical:
            if canonical.get("image_url"):
                meta["image_url"] = canonical["image_url"]
                meta["image_urls"] = [canonical["image_url"]]
            if canonical.get("thumbnail_url"):
                meta["thumbnail_url"] = canonical["thumbnail_url"]
        return jsonify({
            "ok": True,
            "status": "done",
            "job_id": job_id,
            "image_id": job_id,
            "image_url": meta.get("image_url"),
            "image_urls": meta.get("image_urls", []),
            "image_base64": meta.get("image_base64"),
            "model": meta.get("model"),
            "size": meta.get("size"),
        })

    return jsonify({"error": "Job not found"}), 404


@bp.route("/image/gemini/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session
def gemini_image_status_mod(job_id: str):
    """
    Poll status of a Gemini image generation job.

    Returns:
    - status: "queued" | "done" | "failed"
    - On done: image_url, image_urls, image_base64
    - On failed: error message
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    store = load_store()
    meta = store.get(job_id) or {}

    if USE_DB:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, status, error_message, meta
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

                if job["status"] == "queued":
                    return jsonify({"ok": True, "status": "queued", "job_id": job_id, "message": "Generating image..."})

                if job["status"] == "failed":
                    return jsonify({"ok": False, "status": "failed", "job_id": job_id, "error": job.get("error_message", "Image generation failed")})

                if job["status"] == "ready":
                    image_url = meta.get("image_url") or job_meta.get("image_url")
                    image_urls = meta.get("image_urls") or job_meta.get("image_urls") or ([] if not image_url else [image_url])

                    canonical = get_canonical_image_row(
                        identity_id,
                        upstream_id=job_id,
                        alt_upstream_id=job_meta.get("image_id") or meta.get("image_id"),
                    )
                    if canonical:
                        if canonical.get("image_url"):
                            image_url = canonical["image_url"]
                            image_urls = [image_url]
                        if canonical.get("thumbnail_url"):
                            meta["thumbnail_url"] = canonical["thumbnail_url"]

                    # Get updated balance to return to frontend
                    balance_info = get_current_balance(identity_id) if identity_id else None

                    return jsonify({
                        "ok": True,
                        "status": "done",
                        "job_id": job_id,
                        "image_id": job_id,
                        "image_url": image_url,
                        "image_urls": image_urls,
                        "image_base64": meta.get("image_base64") or job_meta.get("image_base64"),
                        "model": meta.get("model") or job_meta.get("model") or "imagen-4.0",
                        "aspect_ratio": meta.get("aspect_ratio") or job_meta.get("aspect_ratio"),
                        "image_size": meta.get("image_size") or job_meta.get("image_size"),
                        "provider": "google",
                        "new_balance": balance_info["available"] if balance_info else None,
                    })
        except Exception as e:
            print(f"[STATUS][mod] Error checking Gemini job {job_id}: {e}")

    # Fallback to in-memory store
    if meta.get("status") == "done":
        canonical = get_canonical_image_row(
            identity_id,
            upstream_id=job_id,
            alt_upstream_id=meta.get("image_id"),
        )
        if canonical:
            if canonical.get("image_url"):
                meta["image_url"] = canonical["image_url"]
                meta["image_urls"] = [canonical["image_url"]]
            if canonical.get("thumbnail_url"):
                meta["thumbnail_url"] = canonical["thumbnail_url"]

        balance_info = get_current_balance(identity_id) if identity_id else None

        return jsonify({
            "ok": True,
            "status": "done",
            "job_id": job_id,
            "image_id": job_id,
            "image_url": meta.get("image_url"),
            "image_urls": meta.get("image_urls", []),
            "image_base64": meta.get("image_base64"),
            "model": meta.get("model") or "imagen-4.0",
            "aspect_ratio": meta.get("aspect_ratio"),
            "image_size": meta.get("image_size"),
            "provider": "google",
            "new_balance": balance_info["available"] if balance_info else None,
        })

    return jsonify({"error": "Job not found"}), 404


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
        return jsonify({"error": f"Fetch failed: {e}"}), 502

    if not r.ok:
        return jsonify({"error": f"Upstream {r.status_code}"}), r.status_code

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
        return jsonify({"error": f"Failed to decode data URL: {e}"}), 400

    return jsonify({"url": f"/api/cache-image/{file_path.name}", "mime": mime})


@bp.route("/cache-image/<path:filename>", methods=["GET"])
def cache_image_get_mod(filename: str):
    target = config.CACHE_DIR / filename
    if not target.exists():
        return jsonify({"error": "Not found"}), 404
    return Response(target.read_bytes(), mimetype="image/png")
