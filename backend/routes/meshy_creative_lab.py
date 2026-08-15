"""
Meshy Creative Lab Routes Blueprint (Modular)
----------------------------------------------
Registered under /api/_mod.

Meshy's Creative Lab products are two-stage: a cheap *prototype* turns a photo
into a styled concept image, then a *build* turns that prototype into printable
3D geometry. Both stages are per-product and versioned separately by Meshy:

    POST /openapi/creative-lab/<product>/v1/prototype     {image_url}
    POST /openapi/creative-lab/<product>/v1/build         {input_task_id}
    GET  /openapi/creative-lab/<product>/v1/<stage>/:id

TimrX exposes them through one validated route family rather than 14 routes:

    POST /api/_mod/creative-lab/<product>/prototype
    POST /api/_mod/creative-lab/<product>/build
    GET  /api/_mod/creative-lab/<product>/<stage>/status/<job_id>

Docs checked 2026-08-15:
  https://docs.meshy.ai/en/api/creative-lab-keychain
  https://docs.meshy.ai/en/api/creative-lab-figure
  https://docs.meshy.ai/en/api/pricing
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
from backend.services.s3_service import ensure_s3_url_for_data_uri, save_finished_job_to_normalized_db
from backend.services.status_cache import cache_status, get_cached_status
from backend.utils.helpers import log_event, log_status_summary, now_s

bp = Blueprint("meshy_creative_lab", __name__)

# Meshy product slug -> display label. Every product uses the same two-stage
# shape; only the slug and the price differ.
CREATIVE_LAB_PRODUCTS = {
    "keychain": "Keychain",
    "fridge-magnet": "Fridge Magnet",
    "figure": "Figure",
    "vinyl-figure": "Vinyl Figure",
    "brick-figure": "Brick Figure",
    "lamp": "Lamp",
    "keycap": "Keycap",
}

STAGES = ("prototype", "build")


def _action_key(product: str, stage: str) -> str:
    """Route-level action key, e.g. creative-lab-keychain-build."""
    return f"creative-lab-{product}-{stage}"


def _job_stage(product: str, stage: str) -> str:
    """Internal stage stored on the job, e.g. creative_lab_keychain_build."""
    return f"creative_lab_{product.replace('-', '_')}_{stage}"


def _validate(product: str, stage: str):
    if product not in CREATIVE_LAB_PRODUCTS:
        return jsonify({
            "ok": False,
            "error": "UNKNOWN_CREATIVE_LAB_PRODUCT",
            "message": f"Unknown Creative Lab product '{product}'.",
            "supported": sorted(CREATIVE_LAB_PRODUCTS),
        }), 400
    if stage not in STAGES:
        return jsonify({"ok": False, "error": "stage must be prototype or build"}), 400
    return None


def _start(product: str, stage: str, body: dict, identity_id: str, payload: dict):
    """Reserve credits, register the job, and dispatch to Meshy."""
    label = f"{CREATIVE_LAB_PRODUCTS[product]} {stage}"
    action_key = _action_key(product, stage)
    job_stage = _job_stage(product, stage)

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
                "product": product,
                "stage": stage,
                "source": "modular",
                "was_existing": True,
            })
        if existing_job:
            return jsonify({
                "ok": False,
                "error": "JOB_ALREADY_STARTING",
                "message": f"This {label} is already being started. Please wait a moment.",
            }), 409

    internal_job_id = str(uuid.uuid4())
    store = load_store()
    source_task_id = payload.get("input_task_id")
    source_meta = get_job_metadata(source_task_id, store) or {}
    title = (body.get("name") or body.get("title") or "").strip() \
        or source_meta.get("title") \
        or f"{CREATIVE_LAB_PRODUCTS[product]}"

    job_meta = {
        "prompt": title,
        "root_prompt": source_meta.get("root_prompt") or title,
        "title": title,
        "stage": job_stage,
        "creative_lab_product": product,
        "creative_lab_stage": stage,
        "source_task_id": source_task_id,
    }

    reservation_id, credit_error = start_paid_job(identity_id, action_key, internal_job_id, job_meta)
    if credit_error:
        return credit_error

    create_internal_job_row(
        internal_job_id=internal_job_id,
        identity_id=identity_id,
        provider="meshy",
        action_key=action_key,
        prompt=title,
        meta=job_meta,
        reservation_id=reservation_id,
        status="queued",
        idempotency_key=idempotency_key,
    )

    try:
        resp = mesh_post(f"/openapi/creative-lab/{product}/v1/{stage}", payload)
        log_event(f"creative-lab/{product}/{stage}:meshy-resp[mod]", resp)
        meshy_task_id = resp.get("result") or resp.get("id")
        if not meshy_task_id:
            if reservation_id:
                release_job_credits(reservation_id, "meshy_no_job_id", internal_job_id)
            _update_job_status_failed(internal_job_id, f"{label} failed to start")
            return jsonify({
                "ok": False,
                "error": "CREATIVE_LAB_FAILED",
                "message": f"{label} could not be started. Please try again.",
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
        "product": product,
        "stage": stage,
        "source": "modular",
    })


@bp.route("/creative-lab/products", methods=["GET", "OPTIONS"])
def creative_lab_products():
    """Products this deployment exposes, so the UI is not hard-coded."""
    if request.method == "OPTIONS":
        return ("", 204)
    from backend.services.pricing_service import PricingService

    products = []
    for slug, label in CREATIVE_LAB_PRODUCTS.items():
        entry = {"product": slug, "label": label}
        for stage in STAGES:
            try:
                entry[f"{stage}_credits"] = PricingService.get_action_cost(_action_key(slug, stage))
            except Exception:
                entry[f"{stage}_credits"] = None
        products.append(entry)
    return jsonify({"ok": True, "products": products})


@bp.route("/creative-lab/<product>/prototype", methods=["POST", "OPTIONS"])
@with_session
def creative_lab_prototype(product: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    invalid = _validate(product, "prototype")
    if invalid:
        return invalid

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event(f"creative-lab/{product}/prototype:incoming[mod]", body)

    image_url = (body.get("image_url") or "").strip()
    if not image_url:
        return jsonify({"ok": False, "error": "image_url required"}), 400
    if image_url.startswith("data:"):
        # Creative Lab wants a fetchable URL; park data URIs in S3 first.
        try:
            image_url = ensure_s3_url_for_data_uri(
                image_url, "images", f"creative-lab/{identity_id}/source", user_id=identity_id,
            ) or image_url
        except Exception as exc:
            print(f"[creative-lab] data URI upload failed: {exc}")
            return jsonify({
                "ok": False,
                "error": "IMAGE_UPLOAD_FAILED",
                "message": "Could not stage that image. Try a smaller file.",
            }), 400

    payload = {"image_url": image_url}
    name = (body.get("name") or "").strip()
    if name:
        payload["name"] = name[:100]

    return _start(product, "prototype", body, identity_id, payload)


@bp.route("/creative-lab/<product>/build", methods=["POST", "OPTIONS"])
@with_session
def creative_lab_build(product: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"ok": False, "error": "MESHY_API_KEY not configured"}), 503

    invalid = _validate(product, "build")
    if invalid:
        return invalid

    identity_id, auth_error = require_identity()
    if auth_error:
        return auth_error

    body = request.get_json(silent=True) or {}
    log_event(f"creative-lab/{product}/build:incoming[mod]", body)

    input_task_id = (body.get("input_task_id") or body.get("prototype_task_id") or "").strip()
    if not input_task_id:
        return jsonify({
            "ok": False,
            "error": "input_task_id required",
            "message": "Run the prototype stage first, then build from its task id.",
        }), 400

    payload = {"input_task_id": input_task_id}
    name = (body.get("name") or "").strip()
    if name:
        payload["name"] = name[:100]
    # Meshy validates the per-product option set; pass it through untouched.
    if isinstance(body.get("options"), dict):
        payload["options"] = body["options"]
    output_format = (body.get("output_format") or "").strip().lower()
    if output_format in {"glb", "obj", "zip"}:
        payload["output"] = {"format": output_format}

    return _start(product, "build", body, identity_id, payload)


@bp.route("/creative-lab/<product>/<stage>/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def creative_lab_status(product: str, stage: str, job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    if not MESHY_API_KEY:
        return jsonify({"error": "MESHY_API_KEY not configured"}), 503

    invalid = _validate(product, stage)
    if invalid:
        return invalid

    cached = get_cached_status(job_id)
    if cached is not None:
        return jsonify(cached)

    identity_id = g.identity_id
    if not verify_job_ownership(job_id, identity_id):
        return jsonify({"error": "Job not found or access denied"}), 404

    try:
        ms = mesh_get(f"/openapi/creative-lab/{product}/v1/{stage}/{job_id}")
        log_event(f"creative-lab/{product}/{stage}/status:meshy-resp[mod]", ms)
    except MeshyTaskNotFoundError:
        terminalize_expired_meshy_job(job_id, identity_id)
        return jsonify({
            "status": "failed",
            "error": "TASK_EXPIRED",
            "message": "This Creative Lab task has expired on the provider.",
        }), 200
    except Exception as exc:
        print(f"[PROVIDER_ERROR] provider=meshy job_id={job_id} creative_lab={product}/{stage} error={exc}")
        return jsonify({
            "error": "CREATIVE_LAB_STATUS_FAILED",
            "message": "Failed to fetch Creative Lab status. Please try again.",
        }), 502

    job_stage = _job_stage(product, stage)
    out = normalize_meshy_task(ms, stage=job_stage)
    out["product"] = product
    out["creative_lab_stage"] = stage

    # Prototype returns concept images, not a model.
    image_urls = ms.get("image_urls") if isinstance(ms, dict) else None
    if isinstance(image_urls, list) and image_urls:
        out["image_urls"] = image_urls
        if not out.get("thumbnail_url"):
            out["thumbnail_url"] = image_urls[0]

    log_status_summary(f"creative-lab/{product}/{stage}[mod]", job_id, out)

    store = load_store()
    meta = get_job_metadata(job_id, store) or {}
    reservation_id = meta.get("reservation_id")
    internal_job_id = meta.get("internal_job_id")

    if out["status"] == "failed":
        error_msg = out.get("message") or out.get("error") or f"{product} {stage} failed"
        if reservation_id:
            release_job_credits(reservation_id, "provider_job_failed", internal_job_id or job_id)
        if internal_job_id:
            _update_job_status_failed(internal_job_id, error_msg)

    elif out["status"] == "done":
        if reservation_id:
            finalize_job_credits(reservation_id, internal_job_id or job_id, meta.get("identity_id") or identity_id)

        # Only the build stage yields a model worth storing in history.
        if stage == "build" and (out.get("glb_url") or out.get("model_urls")):
            if identity_id and not meta.get("identity_id"):
                meta["identity_id"] = identity_id
                meta["user_id"] = identity_id
            try:
                s3_result = save_finished_job_to_normalized_db(
                    job_id, out, meta, job_type=f"creative_lab_{product.replace('-', '_')}",
                    user_id=meta.get("identity_id") or identity_id,
                )
                if s3_result and s3_result.get("success"):
                    if s3_result.get("glb_url"):
                        out["glb_url"] = s3_result["glb_url"]
                    if s3_result.get("thumbnail_url"):
                        out["thumbnail_url"] = s3_result["thumbnail_url"]
                    if s3_result.get("model_urls"):
                        out["model_urls"] = s3_result["model_urls"]
                    if internal_job_id:
                        _update_job_status_ready(
                            internal_job_id,
                            upstream_job_id=job_id,
                            model_id=s3_result.get("model_id"),
                            glb_url=s3_result.get("glb_url"),
                        )
            except Exception as exc:
                print(f"[creative-lab/{product}] saving build output failed: {exc}")
        elif internal_job_id:
            try:
                _update_job_status_ready(internal_job_id, upstream_job_id=job_id)
            except Exception as exc:
                print(f"[creative-lab/{product}] job status→ready failed: {exc}")

    cache_status(job_id, out, is_terminal=(out["status"] in ("done", "failed")))
    return jsonify(out)
