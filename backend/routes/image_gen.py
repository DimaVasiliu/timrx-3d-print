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
from backend.db import USE_DB, get_conn
from backend.middleware import with_session
from backend.services.async_dispatch import get_executor, _dispatch_openai_image_async
from backend.services.credits_helper import get_current_balance, start_paid_job
from backend.services.identity_service import require_identity
from backend.services.job_service import create_internal_job_row, load_store, save_store
from backend.utils.helpers import now_s, log_event

bp = Blueprint("image_gen", __name__)

# OpenAI blob hosts (from monolith)
ALLOWED_IMAGE_HOSTS = {
    "oaidalleapiprodscus.blob.core.windows.net",
    "oaidalleapiprodscus.bblob.core.windows.net",
    "oaidalleapiprodscus.blob.core.windows.net:443",
}


@bp.route("/nano/image", methods=["POST", "OPTIONS"])
def nano_image_mod():
    return jsonify({"error": "NanoBanana disabled"}), 410


@bp.route("/nano/image/<job_id>", methods=["GET", "OPTIONS"])
def nano_image_status_mod(job_id: str):
    return jsonify({"error": "NanoBanana disabled"}), 410


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
    size_map = {
        "256x256": "256x256",
        "512x512": "512x512",
        "1024x1024": "1024x1024",
        "1024x1792": "1024x1792",
        "1792x1024": "1792x1024",
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
    action_key = "image-studio"

    reservation_id, credit_error = start_paid_job(
        identity_id,
        action_key,
        internal_job_id,
        {"prompt": prompt[:100], "n": n, "model": model, "size": size},
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


@bp.route("/proxy-image")
def proxy_image_mod():
    url = request.args.get("u") or ""
    if not url:
        return jsonify({"error": "Missing url"}), 400

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return jsonify({"error": "Invalid scheme"}), 400
    host = parsed.hostname or ""
    if host not in ALLOWED_IMAGE_HOSTS:
        return jsonify({"error": "Host not allowed"}), 400

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
