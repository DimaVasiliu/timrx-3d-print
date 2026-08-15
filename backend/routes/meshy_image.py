"""
Meshy-native Image Generation Routes (Modular)
-----------------------------------------------
Registered under /api/_mod.

    POST /api/_mod/meshy-image/text-to-image        -> POST /openapi/v1/text-to-image
    POST /api/_mod/meshy-image/image-to-image       -> POST /openapi/v1/image-to-image
    GET  /api/_mod/meshy-image/<kind>/status/<id>   -> GET  /openapi/v1/<kind>/:id

TimrX already generates images through several providers. The reason these
exist is the one the parity plan names: a Meshy-native task id that can be
handed straight to Image-to-3D / Multi-Image-to-3D as `input_task_id`, with no
S3 round trip. The status response therefore surfaces `input_task_id`
explicitly alongside the image URLs.

Docs checked 2026-08-15:
  https://docs.meshy.ai/en/api/text-to-image   (nano-banana 3 / -2 6 / -pro 9 / gpt-image-2 9)
  https://docs.meshy.ai/en/api/image-to-image  (… gpt-image-2 12)
"""

from __future__ import annotations

import uuid

from flask import Blueprint, g, jsonify, request

from backend.config import ACTION_KEYS, MESHY_API_KEY
from backend.middleware import with_session, with_session_readonly
from backend.services.credits_helper import (
    finalize_job_credits,
    get_current_balance,
    release_job_credits,
    start_paid_job,
)
from backend.services.identity_service import require_identity
from backend.services.job_service import (
    _update_job_status_failed,
    _update_job_status_ready,
    create_internal_job_row,
    get_job_by_idempotency_key,
    get_job_metadata,
    load_store,
    save_store,
    verify_job_ownership,
)
from backend.services.meshy_service import (
    MeshyTaskNotFoundError,
    mesh_get,
    mesh_post,
    normalize_meshy_task,
    terminalize_expired_meshy_job,
)
from backend.services.s3_service import ensure_s3_url_for_data_uri
from backend.services.status_cache import cache_status, get_cached_status
from backend.utils.helpers import log_event, now_s

bp = Blueprint("meshy_image", __name__)

MESHY_IMAGE_MODELS = {"nano-banana", "nano-banana-2", "nano-banana-pro", "gpt-image-2"}
ASPECT_RATIOS = {"1:1", "16:9", "9:16", "4:3", "3:4", "3:2", "2:3"}

# kind -> (provider path, action key, internal stage)
_IMAGE_OPS = {
    "text-to-image": ("text-to-image", ACTION_KEYS["meshy-text-to-image"], "meshy_text_to_image"),
    "image-to-image": ("image-to-image", ACTION_KEYS["meshy-image-to-image"], "meshy_image_to_image"),
}


def _start(kind: str, body: dict, identity_id: str, payload: dict):
    provider_path, action_key, stage = _IMAGE_OPS[kind]

    idempotency_key = (
        request.headers.get("Idempotency-Key") or body.get("idempotency_key") or ""
    ).strip() or None
    if idempotency_key:
        existing_job = get_job_by_idempotency_key(identity_id, idempotency_key)
        if existing_job and existing_job.get("upstream_job_id"):
            balance_info = get_current_balance(identity_id)
            return jsonify({
                "ok": True,
                "job_id": existing_job["upstream_job_id"],
                "reservation_id": existing_job.get("reservation_id"),
                "new_balance": balance_info["available"] if balance_info else None,
                "kind": kind,
                "source": "modular",
                "was_existing": True,
            })
        if existing_job:
            return jsonify({
                "ok": False,
                "error": "JOB_ALREADY_STARTING",
                "message": "This image is already being generated. Please wait a moment.",
            }), 409

    internal_job_id = str(uuid.uuid4())
    prompt = payload.get("prompt") or ""
    job_meta = {
        "prompt": prompt,
        "root_prompt": prompt,
        "title": prompt[:50] if prompt else "Meshy image",
        "stage": stage,
        "provider": "meshy",
        "ai_model": payload.get("ai_model"),
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=prompt,
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
        idempotency_key=idempotency_key,
    )

    try:
        resp = mesh_post(f"/openapi/v1/{provider_path}", payload)
        log_event(f"meshy-image/{kind}:meshy-resp[mod]", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            _update_job_status_failed(internal_job_id, "Meshy image generation failed to start")
            return jsonify({
                "ok": False,
                "error": "IMAGE_GENERATION_FAILED",
                "message": "Image generation could not be started. Please try again.",
            }), 502
    except Exception as exc:
        if reservation_id:
            release_job_credits(reservation_id, "meshy_api_error", internal_job_id)
        _update_job_status_failed(internal_job_id, str(exc))
        from backend.services.error_sanitizer import MODEL_GENERATION_FAILED, sanitize_provider_error

        return jsonify(sanitize_provider_error(
            provider="meshy", error=exc, job_id=internal_job_id, code=MODEL_GENERATION_FAILED,
        )), 502

    from backend.services.async_dispatch import update_job_with_upstream_id

    update_job_with_upstream_id(internal_job_id, meshy_task_id)
    store = load_store()
    store[meshy_task_id] = {
        **job_meta,
        "created_at": now_s() * 1000,
        "user_id": identity_id,
        "identity_id": identity_id,
        "reservation_id": reservation_id,
        "internal_job_id": internal_job_id,
    }
    save_store(store)

    balance_info = get_current_balance(identity_id)
    return jsonify({
        "ok": True,
        "job_id": meshy_task_id,
        "reservation_id": reservation_id,
        "new_balance": balance_info["available"] if balance_info else None,
        "kind": kind,
        "source": "modular",
    })


def _common_options(body: dict, payload: dict):
    """Validate the options both endpoints share. Returns an error response or None."""
    ai_model = (body.get("ai_model") or body.get("model") or "nano-banana").strip().lower()
    if ai_model not in MESHY_IMAGE_MODELS:
        return jsonify({
            "ok": False,
            "error": f"ai_model must be one of: {', '.join(sorted(MESHY_IMAGE_MODELS))}",
        }), 400
    payload["ai_model"] = ai_model

    if body.get("generate_multi_view") is not None:
        payload["generate_multi_view"] = bool(body.get("generate_multi_view"))

    aspect_ratio = (body.get("aspect_ratio") or "").strip()
    if aspect_ratio:
        if aspect_ratio not in ASPECT_RATIOS:
            return jsonify({"ok": False, "error": f"aspect_ratio must be one of: {', '.join(sorted(ASPECT_RATIOS))}"}), 400
        payload["aspect_ratio"] = aspect_ratio
    return None


@bp.route("/meshy-image/text-to-image", methods=["POST", "OPTIONS"])
@with_session
def meshy_text_to_image():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("meshy-image/text-to-image:incoming[mod]", body)

    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"ok": False, "error": "prompt required"}), 400

    payload = {"prompt": prompt}
    invalid = _common_options(body, payload)
    if invalid:
        return invalid

    pose_mode = (body.get("pose_mode") or "").strip().lower()
    if pose_mode in {"a-pose", "t-pose"}:
        payload["pose_mode"] = pose_mode

    return _start("text-to-image", body, identity_id, payload)


@bp.route("/meshy-image/image-to-image", methods=["POST", "OPTIONS"])
@with_session
def meshy_image_to_image():
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event("meshy-image/image-to-image:incoming[mod]", body)

    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"ok": False, "error": "prompt required"}), 400

    raw_refs = body.get("reference_image_urls")
    if isinstance(raw_refs, str):
        raw_refs = [raw_refs]
    if not isinstance(raw_refs, list):
        return jsonify({"ok": False, "error": "reference_image_urls must be an array of 1-5 image URLs"}), 400

    reference_image_urls = []
    for url in raw_refs:
        url = str(url or "").strip()
        if not url:
            continue
        if url.startswith("data:"):
            try:
                url = ensure_s3_url_for_data_uri(
                    url, "images", f"meshy-image/{identity_id}/reference", user_id=identity_id,
                ) or url
            except Exception as exc:
                print(f"[meshy-image] reference data URI upload failed: {exc}")
                return jsonify({
                    "ok": False,
                    "error": "IMAGE_UPLOAD_FAILED",
                    "message": "Could not stage a reference image. Try a smaller file.",
                }), 400
        reference_image_urls.append(url)

    if not (1 <= len(reference_image_urls) <= 5):
        return jsonify({"ok": False, "error": "reference_image_urls must contain 1-5 image URLs"}), 400

    payload = {"prompt": prompt, "reference_image_urls": reference_image_urls}
    invalid = _common_options(body, payload)
    if invalid:
        return invalid

    return _start("image-to-image", body, identity_id, payload)


@bp.route("/meshy-image/<kind>/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def meshy_image_status(kind: str, job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503
    if kind not in _IMAGE_OPS:
        return jsonify({"error": "kind must be text-to-image or image-to-image"}), 400

    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    identity_id = g.identity_id
    if not verify_job_ownership(job_id, identity_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    provider_path, _action_key, stage = _IMAGE_OPS[kind]
    try:
        ms = mesh_get(f"/openapi/v1/{provider_path}/{job_id}")
        log_event(f"meshy-image/{kind}/status:meshy-resp[mod]", ms)
    except MeshyTaskNotFoundError:
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({
            "status": "failed",
            "error": "TASK_EXPIRED",
            "message": "This image task has expired on the provider.",
        }), 200
    except Exception as exc:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} kind={kind} error={exc}")
        return jsonify({
            "error": "IMAGE_STATUS_FAILED",
            "message": "Failed to fetch image status. Please try again.",
        }), 502

    out = normalize_meshy_task(ms, stage=stage)
    image_urls = ms.get("image_urls") if isinstance(ms, dict) else None
    if isinstance(image_urls, list) and image_urls:
        out["image_urls"] = image_urls
        out["image_url"] = image_urls[0]
        if not out.get("thumbnail_url"):
            out["thumbnail_url"] = image_urls[0]

    # The whole point of this route family: hand the id straight to
    # Image-to-3D / Multi-Image-to-3D without an S3 round trip.
    if out["status"] == "done":
        out["input_task_id"] = job_id

    store = load_store()
    meta = get_job_metadata(job_id, store) or {}
    reservation_id = meta.get("reservation_id")
    internal_job_id = meta.get("internal_job_id")

    if out["status"] == "failed":
        error_msg = out.get("message") or out.get("error") or "Image generation failed"
        if reservation_id:
            release_job_credits(reservation_id, "provider_job_failed", internal_job_id or job_id)
        if internal_job_id:
            _update_job_status_failed(internal_job_id, error_msg)
    elif out["status"] == "done":
        if reservation_id:
            finalize_job_credits(reservation_id, internal_job_id or job_id, meta.get("identity_id") or identity_id)
        if internal_job_id:
            try:
                _update_job_status_ready(internal_job_id, upstream_job_id=job_id)
            except Exception as exc:
                print(f"[meshy-image/{kind}] job status→ready failed: {exc}")

    cache_status(job_id, out, is_terminal=(out["status"] in ("done", "failed")))
    return jsonify(out)
