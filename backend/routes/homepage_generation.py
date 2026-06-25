"""
Homepage generation gateway.

This endpoint turns the public homepage prompt bar into a real, guided
generation starter while delegating actual generation to the existing TimrX
pipelines. It intentionally lives under /api/_mod so apex /api/* can remain
reserved for Worker/blog routes in production.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Tuple

from flask import Blueprint, g, jsonify, request

from backend.config import config
from backend.middleware import with_session, with_session_readonly
from backend.services.free_generation_service import (
    get_current_trial_state,
    get_trial_for_job,
    has_paid_balance,
    mark_completed,
    mark_failed,
    mark_trial_failed,
    mark_started,
    reserve_trial,
)
from backend.services.pricing_service import (
    CanonicalActions,
    PricingService,
    get_video_action_code,
    get_video_credit_cost,
)

bp = Blueprint("homepage_generation", __name__)


_VIDEO_WORDS = re.compile(r"\b(video|animate|animation|cinematic|clip|short|reel|movie|motion)\b", re.I)
_THREE_D_WORDS = re.compile(
    r"\b(3d|three[-\s]?d|model|stl|obj|glb|3mf|print|printable|figurine|miniature|keychain|collectible|toy|mesh)\b",
    re.I,
)


def _env_bool(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _free_generation_enabled() -> bool:
    return bool(getattr(config, "HOMEPAGE_FREE_ENABLED", True))


def _free_type_allowed(generation_type: str) -> bool:
    if generation_type == "video":
        return bool(getattr(config, "HOMEPAGE_FREE_ALLOW_VIDEO", False))
    if generation_type == "3d":
        return bool(getattr(config, "HOMEPAGE_FREE_ALLOW_3D", False))
    return bool(getattr(config, "HOMEPAGE_FREE_ALLOW_IMAGE", True))


def _detect_generation_type(prompt: str, requested_type: str | None = None) -> str:
    requested = (requested_type or "auto").strip().lower()
    if requested in {"image", "video", "3d"}:
        return requested
    if _THREE_D_WORDS.search(prompt or ""):
        return "3d"
    if _VIDEO_WORDS.search(prompt or ""):
        return "video"
    return "image"


def _response_json(result) -> Tuple[Dict[str, Any], int]:
    status_code = 200
    response = result
    if isinstance(result, tuple):
        response = result[0]
        if len(result) > 1 and isinstance(result[1], int):
            status_code = result[1]
    if hasattr(response, "status_code"):
        status_code = int(response.status_code)
    data = None
    if hasattr(response, "get_json"):
        data = response.get_json(silent=True)
    if not isinstance(data, dict):
        data = {}
    return data, status_code


def _homepage_blocked_response(reason: str = "free_trial_used", status_code: int = 402):
    return jsonify(
        {
            "ok": False,
            "error": reason,
            "message": "Your free TimrX generation has been used. Sign up or buy credits to keep creating.",
            "trust_message": "Your first generation is saved. Sign up to continue creating and keep your results.",
            "free_trial_remaining": False,
            "actions": {
                "signup": "/3dprint",
                "buy_credits": "/hub#pricing",
                "workspace": "/3dprint",
            },
        }
    ), status_code


def _choose_image_provider() -> Tuple[str, str, int]:
    from backend.services.image_provider_registry import (
        get_enabled_image_providers,
        get_image_action_key,
    )

    preferred = (getattr(config, "HOMEPAGE_FREE_IMAGE_PROVIDER", "") or os.getenv("HOMEPAGE_FREE_IMAGE_PROVIDER") or "").strip().lower()
    enabled = list(get_enabled_image_providers())
    if not enabled:
        enabled = ["openai"]
    if preferred and preferred in enabled:
        enabled = [preferred]

    candidates = []
    for provider in enabled:
        action_key = get_image_action_key(provider=provider, image_size="1K")
        cost = PricingService.get_action_cost(action_key)
        if cost > 0:
            candidates.append((cost, provider, action_key))
    if not candidates:
        action_key = CanonicalActions.IMAGE_GENERATE
        return "openai", action_key, PricingService.get_action_cost(action_key)
    cost, provider, action_key = sorted(candidates, key=lambda item: item[0])[0]
    return provider, action_key, cost


def _choose_video_provider() -> Tuple[str, str, int, Dict[str, Any]]:
    from backend.services.video_router import video_router
    from backend.services.video_providers.fal_seedance_provider import normalize_fal_seedance_params
    from backend.services.video_providers.seedance_provider import normalize_seedance_params
    from backend.services.video_providers.vertex_provider import normalize_vertex_params

    preferred = (getattr(config, "HOMEPAGE_FREE_VIDEO_PROVIDER", "") or os.getenv("HOMEPAGE_FREE_VIDEO_PROVIDER") or "").strip().lower()
    available = {provider.name for provider in video_router.get_available_providers()}
    preference = [preferred] if preferred in available else ["fal_seedance", "seedance", "vertex"]
    provider = next((name for name in preference if name in available), "vertex")

    if provider == "fal_seedance":
        params = normalize_fal_seedance_params(duration_seconds=5, aspect_ratio="16:9", resolution="720p")
        seedance_tier = "fast"
    elif provider == "seedance":
        params = normalize_seedance_params(
            duration_seconds=5,
            aspect_ratio="16:9",
            tier="fast",
            seedance_variant=None,
            resolution="480p",
        )
        seedance_tier = params.get("tier") or "fast"
    else:
        params = normalize_vertex_params(duration_seconds=4, aspect_ratio="16:9", resolution="720p")
        seedance_tier = "fast"

    action_key = get_video_action_code(
        "text2video",
        int(params["duration_seconds"]),
        params["resolution"],
        provider=provider,
        seedance_tier=seedance_tier,
    )
    cost = get_video_credit_cost(
        int(params["duration_seconds"]),
        params["resolution"],
        provider=provider,
        seedance_tier=seedance_tier,
        task="text2video",
    )
    params["seedance_tier"] = seedance_tier
    return provider, action_key, cost, params


def _action_for_generation_type(generation_type: str) -> Tuple[str, int, Dict[str, Any]]:
    if generation_type == "video":
        provider, action_key, cost, params = _choose_video_provider()
        params["provider"] = provider
        return action_key, cost, params
    if generation_type == "3d":
        provider = (getattr(config, "HOMEPAGE_FREE_3D_PROVIDER", "") or os.getenv("HOMEPAGE_FREE_3D_PROVIDER") or "meshy").strip().lower()
        if provider != "meshy":
            print(f"[HOMEPAGE_FREE] unsupported 3d provider override={provider}; falling back to meshy")
        action_key = CanonicalActions.TEXT_TO_3D_GENERATE
        return action_key, PricingService.get_action_cost(action_key), {}
    provider, action_key, cost = _choose_image_provider()
    return action_key, cost, {"provider": provider}


def _dispatch_image(prompt: str, params: Dict[str, Any]):
    from backend.routes.image_gen import (
        _handle_flux_pro_image_generate,
        _handle_gemini_image_generate,
        _handle_google_nano_image_generate,
        _handle_ideogram_v3_image_generate,
        _handle_nano_banana_image_generate,
        _handle_openai_image_generate,
        _handle_recraft_v4_image_generate,
    )

    provider = params.get("provider") or "openai"
    body = {
        **(request.get_json(silent=True) or {}),
        "prompt": prompt,
        "provider": provider,
        "image_size": "1K",
        "aspect_ratio": (request.get_json(silent=True) or {}).get("aspect_ratio") or "1:1",
        "source": "homepage_chat",
    }
    handlers = {
        "nano_banana": _handle_nano_banana_image_generate,
        "google": _handle_gemini_image_generate,
        "google_nano": _handle_google_nano_image_generate,
        "flux_pro": _handle_flux_pro_image_generate,
        "ideogram_v3": _handle_ideogram_v3_image_generate,
        "recraft_v4": _handle_recraft_v4_image_generate,
        "openai": _handle_openai_image_generate,
    }
    return handlers.get(provider, _handle_openai_image_generate)(body)


def _dispatch_video(prompt: str, params: Dict[str, Any]):
    from backend.routes.video import _dispatch_video_job
    from backend.services.video_prompts import sanitize_prompt

    provider = params.get("provider") or "vertex"
    clean_prompt = sanitize_prompt(prompt, provider=provider)
    return _dispatch_video_job(
        identity_id=getattr(g, "identity_id", "") or "",
        task="text2video",
        prompt=clean_prompt,
        image_data=None,
        aspect_ratio=params.get("aspect_ratio") or "16:9",
        resolution=params.get("resolution") or "720p",
        duration_seconds=int(params.get("duration_seconds") or 5),
        motion="",
        negative_prompt="",
        seed=None,
        provider=provider,
        seedance_variant=params.get("task_type"),
        seedance_tier=params.get("seedance_tier") or "fast",
    )


def _dispatch_3d():
    from backend.routes.text_to_3d import text_to_3d_start_mod

    return text_to_3d_start_mod()


def _dispatch_generation(generation_type: str, prompt: str, params: Dict[str, Any]):
    if generation_type == "video":
        return _dispatch_video(prompt, params)
    if generation_type == "3d":
        return _dispatch_3d()
    return _dispatch_image(prompt, params)


def _first_string(data: Dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, list) and value and isinstance(value[0], str):
            return value[0]
    return None


def _download_urls(data: Dict[str, Any], generation_type: str) -> Dict[str, str]:
    downloads: Dict[str, str] = {}
    if generation_type == "image":
        image_url = _first_string(data, ("download_url", "image_url", "thumbnail_url"))
        if image_url:
            downloads["image"] = image_url
        return downloads
    if generation_type == "video":
        video_url = _first_string(data, ("download_url", "video_url", "output_url", "url"))
        if video_url:
            downloads["video"] = video_url
        return downloads

    key_map = {
        "glb": ("glb_url", "model_url"),
        "stl": ("stl_url",),
        "obj": ("obj_url",),
        "3mf": ("three_mf_url", "threeMF_url", "mf3_url"),
        "fbx": ("fbx_url",),
        "usdz": ("usdz_url",),
    }
    for label, keys in key_map.items():
        value = _first_string(data, keys)
        if value:
            downloads[label] = value
    exports = data.get("exports") or data.get("download_urls") or data.get("urls")
    if isinstance(exports, dict):
        for key, value in exports.items():
            if isinstance(value, str) and value:
                downloads[str(key).lower()] = value
            elif isinstance(value, dict) and isinstance(value.get("url"), str):
                downloads[str(key).lower()] = value["url"]
    return downloads


def _normalize_homepage_status(data: Dict[str, Any], job_id: str, generation_type: str) -> Dict[str, Any]:
    raw_status = (data.get("status") or "").lower()
    if raw_status in {"ready", "succeeded", "success", "complete", "completed"}:
        status = "done"
    elif raw_status in {"failed", "error", "cancelled", "canceled", "provider_stalled"}:
        status = "failed"
    elif raw_status in {"queued", "pending", "provider_pending"}:
        status = "queued"
    else:
        status = raw_status or "processing"

    progress = data.get("progress", data.get("pct"))
    try:
        progress = int(progress)
    except (TypeError, ValueError):
        progress = 100 if status == "done" else (0 if status == "queued" else 42)
    progress = max(0, min(100, progress))

    downloads = _download_urls(data, generation_type)
    out = {
        **data,
        "ok": status != "failed",
        "job_id": str(job_id),
        "status": status,
        "generation_type": generation_type,
        "progress": progress,
        "message": data.get("message") or _status_message(generation_type, status),
        "download_urls": downloads,
        "download_url": data.get("download_url") or next(iter(downloads.values()), None),
        "free_trial_remaining": False,
        "actions": {
            "signup": "/3dprint",
            "buy_credits": "/hub#pricing",
            "workspace": "/3dprint",
        },
    }
    if generation_type == "image":
        out["image_url"] = _first_string(data, ("image_url", "thumbnail_url", "url")) or out.get("download_url")
    elif generation_type == "video":
        out["video_url"] = _first_string(data, ("video_url", "output_url", "url")) or out.get("download_url")
        out["thumbnail_url"] = _first_string(data, ("thumbnail_url", "preview_url", "image_url"))
    else:
        out["model_url"] = _first_string(data, ("model_url", "glb_url"))
        out["glb_url"] = _first_string(data, ("glb_url", "model_url"))
        out["thumbnail_url"] = _first_string(data, ("thumbnail_url", "preview_url", "image_url"))
        if status != "done" and not downloads:
            out["downloads_message"] = "Preparing downloads..."
    return out


def _status_message(generation_type: str, status: str) -> str:
    if status == "done":
        return f"Your {_asset_label(generation_type)} is ready."
    if status == "failed":
        return "Generation failed. You can open the workspace or try again with credits."
    if status == "queued":
        return f"Queued. Preparing your {_asset_label(generation_type)}."
    return f"Generating your {_asset_label(generation_type)}..."


def _asset_label(generation_type: str) -> str:
    if generation_type == "video":
        return "video"
    if generation_type == "3d":
        return "3D model"
    return "image"


@bp.route("/homepage/trial", methods=["GET", "OPTIONS"])
@with_session_readonly
def homepage_trial_state():
    if request.method == "OPTIONS":
        return ("", 204)
    return jsonify(get_current_trial_state())


@bp.route("/homepage/generate", methods=["POST", "OPTIONS"])
@with_session
def homepage_generate():
    if request.method == "OPTIONS":
        return ("", 204)

    body = request.get_json(silent=True) or {}
    prompt = (body.get("prompt") or "").strip()
    if len(prompt) < 3:
        return jsonify({"ok": False, "error": "invalid_prompt", "message": "Describe what you want to create."}), 400
    if len(prompt) > 1200:
        return jsonify({"ok": False, "error": "prompt_too_long", "message": "Keep homepage prompts under 1,200 characters."}), 400

    generation_type = _detect_generation_type(prompt, body.get("requested_type"))
    action_key, required_credits, route_params = _action_for_generation_type(generation_type)
    identity_id = getattr(g, "identity_id", None)
    paid_mode = bool(identity_id and has_paid_balance(identity_id, action_key, required_credits))
    idempotency_key = request.headers.get("Idempotency-Key") or body.get("idempotency_key") or ""

    trial = None
    if not paid_mode:
        if not _free_generation_enabled():
            return jsonify({
                "ok": False,
                "error": "homepage_free_disabled",
                "message": "Homepage free generation is temporarily unavailable. Create an account or add credits to continue.",
                "free_trial_remaining": False,
                "actions": {"signup": "/3dprint", "buy_credits": "/hub#pricing", "workspace": "/3dprint"},
            }), 503
        if not _free_type_allowed(generation_type):
            return jsonify({
                "ok": False,
                "error": "homepage_free_type_disabled",
                "message": "The homepage free generation currently starts with image creation. Open the workspace to create videos or 3D models with credits.",
                "free_trial_remaining": False,
                "actions": {"signup": "/3dprint", "buy_credits": "/hub#pricing", "workspace": "/3dprint"},
            }), 402
        max_trial_credits = int(getattr(config, "HOMEPAGE_FREE_MAX_CREDITS", _env_int("HOMEPAGE_FREE_MAX_CREDITS", 6)) or 0)
        if max_trial_credits > 0 and int(required_credits or 0) > max_trial_credits:
            return jsonify({
                "ok": False,
                "error": "homepage_free_cost_limit",
                "message": "This request is above the homepage free generation limit. Open the workspace to continue with credits.",
                "free_trial_remaining": False,
                "actions": {"signup": "/3dprint", "buy_credits": "/hub#pricing", "workspace": "/3dprint"},
            }), 402
        decision = reserve_trial(
            prompt,
            generation_type,
            idempotency_key=idempotency_key,
            max_daily_total=int(getattr(config, "HOMEPAGE_FREE_MAX_DAILY_TOTAL", _env_int("HOMEPAGE_FREE_MAX_DAILY_TOTAL", 250))),
            max_per_ip_per_day=int(getattr(config, "HOMEPAGE_FREE_MAX_PER_IP_PER_DAY", _env_int("HOMEPAGE_FREE_MAX_PER_IP_PER_DAY", 3))),
        )
        if not decision.allowed:
            if decision.active_job:
                return jsonify(
                    {
                        "ok": True,
                        "status": "queued",
                        "job_id": decision.active_job["job_id"],
                        "generation_type": decision.active_job["generation_type"],
                        "polling_url": f"/api/_mod/homepage/status/{decision.active_job['job_id']}",
                        "free_trial_remaining": False,
                        "message": "Your free generation is already running.",
                    }
                ), 202
            if decision.blocked_reason in {"homepage_free_daily_limit", "homepage_free_ip_limit"}:
                print(f"[HOMEPAGE_FREE] rate_limit reason={decision.blocked_reason}")
                return jsonify({
                    "ok": False,
                    "error": decision.blocked_reason,
                    "message": "Free homepage generation is busy right now. Create an account or add credits to continue.",
                    "free_trial_remaining": False,
                    "actions": {"signup": "/3dprint", "buy_credits": "/hub#pricing", "workspace": "/3dprint"},
                }), 429
            return _homepage_blocked_response(decision.blocked_reason or "free_trial_used")
        trial = decision.trial
        g.homepage_free_trial_id = str(trial["id"])

    result = _dispatch_generation(generation_type, prompt, route_params)
    data, status_code = _response_json(result)
    print(
        f"[HOMEPAGE_GENERATION] start type={generation_type} paid={paid_mode} "
        f"action={action_key} credits={required_credits} status={status_code}"
    )
    job_id = data.get("job_id") or data.get("video_id")

    if status_code >= 400 or not job_id:
        if trial:
            mark_trial_failed(str(trial["id"]), data.get("error") or "dispatch_failed")
        return jsonify(data or {"ok": False, "error": "dispatch_failed"}), status_code

    if trial:
        mark_started(
            str(trial["id"]),
            str(job_id),
            generation_type,
            {
                "action_key": action_key,
                "required_credits": required_credits,
                "paid_mode": False,
            },
        )

    return jsonify(
        {
            **data,
            "ok": True,
            "generation_type": generation_type,
            "action_key": action_key,
            "polling_url": f"/api/_mod/homepage/status/{job_id}",
            "free_trial_remaining": False if trial else None,
            "paid_mode": paid_mode,
            "estimated_message": _estimated_message(generation_type),
        }
    ), 200 if status_code < 300 else status_code


@bp.route("/homepage/status/<job_id>", methods=["GET", "OPTIONS"])
@with_session_readonly
def homepage_generation_status(job_id: str):
    if request.method == "OPTIONS":
        return ("", 204)
    trial = get_trial_for_job(job_id)
    generation_type = (trial or {}).get("generation_type") or request.args.get("type") or "image"

    if generation_type == "video":
        from backend.routes.video import video_status_canonical

        result = video_status_canonical(job_id)
    elif generation_type == "3d":
        from backend.routes.text_to_3d import text_to_3d_status_mod

        result = text_to_3d_status_mod(job_id)
    else:
        from backend.routes.image_gen import image_status_unified

        result = image_status_unified(job_id)

    data, status_code = _response_json(result)
    normalized = _normalize_homepage_status(data, job_id, generation_type)
    status = (normalized.get("status") or "").lower()
    if status == "done":
        mark_completed(job_id)
    elif status == "failed":
        mark_failed(job_id, normalized.get("error") or data.get("error") or "generation_failed")

    return jsonify(normalized), status_code


def _estimated_message(generation_type: str) -> str:
    if generation_type == "video":
        return "Creating a short video. This can take a few minutes."
    if generation_type == "3d":
        return "Creating a printable 3D model preview. This can take a few minutes."
    return "Creating your image. This usually finishes quickly."
