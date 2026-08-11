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
from backend.middleware import with_optional_session, with_session, with_session_readonly
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
from backend.services.turnstile_service import is_turnstile_enabled, verify_turnstile_token

bp = Blueprint("homepage_generation", __name__)


_IMAGE_WORDS = re.compile(
    r"\b(image|picture|photo|photograph|poster|render|visual|mockup|logo|illustration|artwork|wallpaper|cover|thumbnail)\b",
    re.I,
)
_VIDEO_WORDS = re.compile(
    r"\b(video|animate|animation|cinematic|clip|short|reel|movie|motion|text[-\s]?to[-\s]?video|veo|seedance)\b",
    re.I,
)
_THREE_D_WORDS = re.compile(
    r"\b(3d\s+model|three[-\s]?d\s+model|text[-\s]?to[-\s]?3d|stl|obj|glb|3mf|printable|print[-\s]?ready|figurine|miniature|mesh|remesh|low[-\s]?poly)\b",
    re.I,
)
_THREE_D_OBJECT_HINTS = re.compile(r"\b(keychain|collectible|toy|product|shoe|bottle|chair|robot)\b", re.I)
_IMAGE_PROVIDER_WORDS = re.compile(
    r"\b(nano\s?banana|nanobanana|piapi|gemini|google\s+nano|google|openai|gpt[-\s]?image|gpt|dall[-\s]?e|dalle|flux|bfl|ideogram|recraft)\b",
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
    return bool(
        getattr(config, "HOMEPAGE_FREE_ENABLED", False)
        and getattr(config, "TURNSTILE_ENABLED", False)
        and str(getattr(config, "TURNSTILE_SECRET_KEY", "") or "").strip()
        and os.getenv("FREE_GENERATION_HASH_SALT", "").strip()
    )


def _free_type_allowed(generation_type: str) -> bool:
    if generation_type == "video":
        return bool(getattr(config, "HOMEPAGE_FREE_ALLOW_VIDEO", False))
    if generation_type == "3d":
        return bool(getattr(config, "HOMEPAGE_FREE_ALLOW_3D", False))
    return bool(getattr(config, "HOMEPAGE_FREE_ALLOW_IMAGE", True))


def _free_cost_limit(generation_type: str) -> int:
    limits = {
        "image": getattr(config, "HOMEPAGE_FREE_IMAGE_MAX_CREDITS", 12),
        "video": getattr(config, "HOMEPAGE_FREE_VIDEO_MAX_CREDITS", 80),
        "3d": getattr(config, "HOMEPAGE_FREE_3D_MAX_CREDITS", 20),
    }
    return int(limits.get(generation_type, getattr(config, "HOMEPAGE_FREE_MAX_CREDITS", 120)) or 0)


def _free_provider_ready(generation_type: str, route_params: Dict[str, Any]) -> bool:
    if generation_type == "image":
        expected = (getattr(config, "HOMEPAGE_FREE_IMAGE_PROVIDER", "") or "nano_banana").strip().lower()
        return route_params.get("provider") == expected
    if generation_type == "video":
        expected = (getattr(config, "HOMEPAGE_FREE_VIDEO_PROVIDER", "") or "seedance").strip().lower()
        return route_params.get("provider") == expected
    return bool(getattr(config, "MESHY_API_KEY", ""))


def _turnstile_remote_ip() -> str:
    if getattr(config, "HOMEPAGE_FREE_TRUST_PROXY_HEADERS", False):
        forwarded = (request.headers.get("CF-Connecting-IP") or "").split(",", 1)[0].strip()
        if forwarded:
            return forwarded
    return request.remote_addr or ""


def _normalize_requested_type(raw: str | None) -> str:
    requested = (raw or "auto").strip().lower()
    if requested in {"image", "img", "photo", "picture"}:
        return "image"
    if requested in {"video", "movie", "clip"}:
        return "video"
    if requested in {"3d", "model", "mesh", "stl"}:
        return "3d"
    return "auto"


def _detect_generation_type(prompt: str, requested_type: str | None = None) -> str:
    text = prompt or ""
    requested = (requested_type or "auto").strip().lower()
    explicit_image = bool(_IMAGE_WORDS.search(text) or _IMAGE_PROVIDER_WORDS.search(text))
    explicit_video = bool(_VIDEO_WORDS.search(text))
    direct_video = bool(re.search(r"\b(video|animate|animation|clip|reel|movie|text[-\s]?to[-\s]?video|image[-\s]?to[-\s]?video|veo|seedance)\b", text, re.I))
    explicit_3d = bool(_THREE_D_WORDS.search(text))

    # Explicit output medium wins over object nouns. A "robot keychain" can be an
    # image subject, while "printable STL keychain" is a 3D request.
    if direct_video:
        return "video"
    if explicit_image:
        return "image"
    if explicit_video:
        return "video"
    if explicit_3d:
        return "3d"

    requested = _normalize_requested_type(requested)
    if requested in {"image", "video", "3d"}:
        return requested
    if _THREE_D_OBJECT_HINTS.search(text) and re.search(r"\b(3d|model|mesh|print|stl|obj|glb|3mf)\b", text, re.I):
        return "3d"
    return "image"


def _normalize_image_provider(raw: str | None) -> str:
    name = re.sub(r"[^a-z0-9]+", "_", (raw or "").strip().lower()).strip("_")
    aliases = {
        "nano_banana": "nano_banana",
        "nanobanana": "nano_banana",
        "piapi": "nano_banana",
        "google_nano": "google_nano",
        "gemini_nano": "google_nano",
        "gemini": "google",
        "google": "google",
        "openai": "openai",
        "gpt": "openai",
        "gpt_image": "openai",
        "dall_e": "openai",
        "dalle": "openai",
        "flux": "flux_pro",
        "bfl": "flux_pro",
        "flux_pro": "flux_pro",
        "ideogram": "ideogram_v3",
        "ideogram_v3": "ideogram_v3",
        "recraft": "recraft_v4",
        "recraft_v4": "recraft_v4",
    }
    return aliases.get(name, "")


def _detect_image_provider(prompt: str, raw_provider: str | None = None) -> str:
    explicit = _normalize_image_provider(raw_provider)
    if explicit:
        return explicit
    text = prompt or ""
    checks = [
        (r"\b(nano\s?banana|nanobanana|piapi)\b", "nano_banana"),
        (r"\b(google\s+nano|gemini\s+nano)\b", "google_nano"),
        (r"\b(gemini|google)\b", "google"),
        (r"\b(openai|gpt[-\s]?image|gpt|dall[-\s]?e|dalle)\b", "openai"),
        (r"\b(flux|bfl)\b", "flux_pro"),
        (r"\bideogram\b", "ideogram_v3"),
        (r"\b(recraft|svg|vector)\b", "recraft_v4"),
    ]
    for pattern, provider in checks:
        if re.search(pattern, text, re.I):
            return provider
    return ""


def _detect_image_size(prompt: str, raw: str | None = None) -> str:
    source = f"{raw or ''} {prompt or ''}".lower()
    if re.search(r"\b(4k|4096|uhd)\b", source):
        return "4K"
    if re.search(r"\b(2k|2048|quad\s?hd|qhd)\b", source):
        return "2K"
    if re.search(r"\b(1k|1024|standard)\b", source):
        return "1K"
    return ""


def _detect_aspect_ratio(prompt: str, raw: str | None = None) -> str:
    source = f"{raw or ''} {prompt or ''}".lower()
    ratio = re.search(r"\b(21:9|16:9|9:16|4:3|3:4|1:1)\b", source)
    if ratio:
        return ratio.group(1)
    if re.search(r"\b(square|avatar|profile)\b", source):
        return "1:1"
    if re.search(r"\b(vertical|portrait|story|shorts|tiktok|reel)\b", source):
        return "9:16"
    if re.search(r"\b(landscape|wide|cinematic|youtube|banner)\b", source):
        return "16:9"
    return ""


def _detect_duration_seconds(prompt: str, raw: Any = None) -> int | None:
    if raw not in (None, ""):
        try:
            return max(1, min(30, int(str(raw).replace("seconds", "").replace("second", "").replace("secs", "").replace("sec", "").replace("s", "").strip())))
        except (TypeError, ValueError):
            pass
    match = re.search(r"\b(\d{1,2})\s*(?:s|sec|secs|second|seconds)\b", prompt or "", re.I)
    if not match:
        return None
    return max(1, min(30, int(match.group(1))))


def _detect_seedance_tier(prompt: str, raw: str | None = None) -> str:
    source = f"{raw or ''} {prompt or ''}".lower()
    if re.search(r"\b(seedance\s*2\.?5|2\.5|v25|unlimited)\b", source):
        return "v25"
    if re.search(r"\b(quality|pro|best|cinematic)\b", source):
        return "quality"
    if re.search(r"\b(mini|cheap|cheapest|draft)\b", source):
        return "mini"
    if re.search(r"\b(fast|quick)\b", source):
        return "fast"
    return ""


def _detect_video_provider(prompt: str, raw_provider: str | None = None) -> tuple[str, bool]:
    from backend.services.video_router import normalize_provider_name

    source = f"{raw_provider or ''} {prompt or ''}"
    explicit = bool(raw_provider)
    if re.search(r"\b(veo|vertex|google\s+video|google\s+veo)\b", source, re.I):
        return "vertex", True
    if re.search(r"\b(fal\s+seedance|seedance\s*1\.?5|fal)\b", source, re.I):
        return "fal_seedance", True
    if re.search(r"\b(seedance|seedance\s*2|seedance\s*2\.?5)\b", source, re.I):
        return "seedance", True
    if raw_provider:
        return normalize_provider_name(raw_provider), explicit
    return "", False


def _parse_homepage_intent(prompt: str, source: Any | None = None) -> Dict[str, Any]:
    source = source or {}
    get = source.get if hasattr(source, "get") else (lambda key, default=None: default)
    requested_type = get("requested_type") or get("type")
    provider_hint = get("provider") or get("requested_provider")
    generation_type = _detect_generation_type(prompt, requested_type)
    intent: Dict[str, Any] = {"generation_type": generation_type}
    aspect_ratio = _detect_aspect_ratio(prompt, get("aspect_ratio") or get("aspectRatio"))
    if aspect_ratio:
        intent["aspect_ratio"] = aspect_ratio
    if generation_type == "image":
        provider = _detect_image_provider(prompt, provider_hint)
        if provider:
            intent["provider"] = provider
            intent["explicit_provider"] = True
        size = _detect_image_size(prompt, get("image_size") or get("imageSize") or get("resolution"))
        if size:
            intent["image_size"] = size
    elif generation_type == "video":
        provider, explicit = _detect_video_provider(prompt, provider_hint)
        if provider:
            intent["provider"] = provider
            intent["strict_provider"] = explicit
        resolution = _detect_image_size(prompt, get("resolution"))
        if resolution:
            intent["resolution"] = resolution.lower()
        duration = _detect_duration_seconds(prompt, get("duration_seconds") or get("duration_sec") or get("seconds"))
        if duration:
            intent["duration_seconds"] = duration
        tier = _detect_seedance_tier(prompt, get("seedance_tier") or get("tier"))
        if tier:
            intent["seedance_tier"] = tier
    return intent


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


def _turnstile_required_response(reason: str = "turnstile_required"):
    return jsonify(
        {
            "ok": False,
            "error": "turnstile_required",
            "reason": reason,
            "message": "Please verify you are human to use your free generation.",
            "free_trial_remaining": None,
        }
    ), 403


def _coerce_image_size(provider: str, requested_size: str | None = None) -> str:
    from backend.services.image_provider_registry import get_image_provider_spec

    spec = get_image_provider_spec(provider)
    if not spec:
        return "1K"
    requested = (requested_size or "").strip().upper()
    if requested in spec.image_sizes:
        return requested
    if requested:
        wanted_rank = {"1K": 1, "2K": 2, "4K": 4}.get(requested, 1)
        for size in ("4K", "2K", "1K"):
            if size in spec.image_sizes and {"1K": 1, "2K": 2, "4K": 4}[size] <= wanted_rank:
                return size
    return "2K" if "2K" in spec.image_sizes else spec.default_image_size


def _choose_image_provider(intent: Dict[str, Any] | None = None) -> Tuple[str, str, int, Dict[str, Any]]:
    from backend.services.image_provider_registry import (
        get_enabled_image_providers,
        get_image_action_key,
    )

    intent = intent or {}
    requested_provider = _normalize_image_provider(intent.get("provider"))
    preferred = (getattr(config, "HOMEPAGE_FREE_IMAGE_PROVIDER", "") or os.getenv("HOMEPAGE_FREE_IMAGE_PROVIDER") or "nano_banana").strip().lower()
    enabled = list(get_enabled_image_providers())
    if not enabled:
        enabled = ["openai"]
    if requested_provider and requested_provider in enabled:
        enabled = [requested_provider]
    elif preferred and preferred in enabled:
        enabled = [preferred]

    candidates = []
    for provider in enabled:
        image_size = _coerce_image_size(provider, intent.get("image_size"))
        action_key = get_image_action_key(provider=provider, image_size=image_size)
        cost = PricingService.get_action_cost(action_key)
        if cost > 0:
            candidates.append((cost, provider, action_key, image_size))
    if not candidates:
        action_key = CanonicalActions.IMAGE_GENERATE
        image_size = _coerce_image_size("openai", intent.get("image_size"))
        return "openai", action_key, PricingService.get_action_cost(action_key), {"provider": "openai", "image_size": image_size}
    cost, provider, action_key, image_size = sorted(candidates, key=lambda item: item[0])[0]
    return provider, action_key, cost, {"provider": provider, "image_size": image_size}


def _choose_video_provider(intent: Dict[str, Any] | None = None) -> Tuple[str, str, int, Dict[str, Any]]:
    from backend.services.video_router import normalize_provider_name
    from backend.services.video_router import video_router
    from backend.services.video_providers.fal_seedance_provider import normalize_fal_seedance_params
    from backend.services.video_providers.seedance_provider import normalize_seedance_params
    from backend.services.video_providers.vertex_provider import normalize_vertex_params

    intent = intent or {}
    requested_provider = normalize_provider_name(intent.get("provider")) if intent.get("provider") else ""
    preferred = (getattr(config, "HOMEPAGE_FREE_VIDEO_PROVIDER", "") or os.getenv("HOMEPAGE_FREE_VIDEO_PROVIDER") or "seedance").strip().lower()
    available = {provider.name for provider in video_router.get_available_providers()}
    if requested_provider and requested_provider in available:
        preference = [requested_provider]
    elif preferred in available:
        preference = [preferred]
    else:
        preference = ["fal_seedance", "seedance", "vertex"]
    provider = next((name for name in preference if name in available), "vertex")

    duration = intent.get("duration_seconds")
    aspect_ratio = intent.get("aspect_ratio") or "16:9"
    resolution = intent.get("resolution")
    requested_tier = intent.get("seedance_tier")
    if provider == "fal_seedance":
        params = normalize_fal_seedance_params(duration_seconds=duration or 5, aspect_ratio=aspect_ratio, resolution=resolution or "720p")
        seedance_tier = "fast"
    elif provider == "seedance":
        params = normalize_seedance_params(
            duration_seconds=duration or 5,
            aspect_ratio=aspect_ratio,
            tier=requested_tier or "fast",
            seedance_variant=None,
            resolution=resolution or None,
        )
        seedance_tier = params.get("tier") or "fast"
    else:
        params = normalize_vertex_params(duration_seconds=duration or 4, aspect_ratio=aspect_ratio, resolution=resolution or "720p")
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
    if requested_provider == provider and intent.get("strict_provider"):
        params["strict_provider"] = True
    return provider, action_key, cost, params


def _action_for_generation_type(generation_type: str, intent: Dict[str, Any] | None = None) -> Tuple[str, int, Dict[str, Any]]:
    intent = intent or {}
    if generation_type == "video":
        provider, action_key, cost, params = _choose_video_provider(intent)
        params["provider"] = provider
        return action_key, cost, params
    if generation_type == "3d":
        provider = (getattr(config, "HOMEPAGE_FREE_3D_PROVIDER", "") or os.getenv("HOMEPAGE_FREE_3D_PROVIDER") or "meshy").strip().lower()
        if provider != "meshy":
            print(f"[HOMEPAGE_FREE] unsupported 3d provider override={provider}; falling back to meshy")
        action_key = CanonicalActions.TEXT_TO_3D_GENERATE
        return action_key, PricingService.get_action_cost(action_key), {"provider": "meshy"}
    provider, action_key, cost, params = _choose_image_provider(intent)
    params["provider"] = provider
    if intent.get("aspect_ratio"):
        params["aspect_ratio"] = intent["aspect_ratio"]
    return action_key, cost, params


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
    incoming = request.get_json(silent=True) or {}
    body = {
        **incoming,
        "prompt": prompt,
        "provider": provider,
        "image_size": params.get("image_size") or incoming.get("image_size") or "2K",
        "aspect_ratio": params.get("aspect_ratio") or incoming.get("aspect_ratio") or "1:1",
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
        strict_provider=bool(params.get("strict_provider")),
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
    settings = {
        key: data.get(key)
        for key in ("provider", "image_size", "resolution", "aspect_ratio", "duration_seconds", "seedance_tier")
        if data.get(key) not in (None, "")
    }
    if settings:
        out["provider"] = settings.get("provider")
        out["settings"] = settings
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


def _provider_label(provider: str | None) -> str:
    return {
        "nano_banana": "Nano Banana",
        "google_nano": "Google Nano",
        "google": "Gemini",
        "openai": "GPT Image",
        "flux_pro": "FLUX",
        "ideogram_v3": "Ideogram",
        "recraft_v4": "Recraft",
        "vertex": "Veo",
        "seedance": "Seedance",
        "fal_seedance": "Seedance 1.5",
        "meshy": "Meshy",
    }.get(provider or "", "TimrX")


@bp.route("/homepage/trial", methods=["GET", "OPTIONS"])
@with_optional_session
def homepage_trial_state():
    if request.method == "OPTIONS":
        return ("", 204)
    return jsonify(get_current_trial_state(request.args.get("type")))


@bp.route("/homepage/preflight", methods=["GET", "OPTIONS"])
@with_optional_session
def homepage_generation_preflight():
    """Resolve entitlement and credit mode before asking for human verification."""
    if request.method == "OPTIONS":
        return ("", 204)
    intent = _parse_homepage_intent("", request.args)
    generation_type = intent["generation_type"]
    action_key, required_credits, route_params = _action_for_generation_type(generation_type, intent)
    state = get_current_trial_state(generation_type)
    entitlement = state["entitlements"][generation_type]
    identity_id = getattr(g, "identity_id", None)
    has_credits = bool(identity_id and has_paid_balance(identity_id, action_key, required_credits))
    free_available = bool(
        _free_generation_enabled()
        and _free_type_allowed(generation_type)
        and _free_provider_ready(generation_type, route_params)
        and entitlement.get("remaining")
        and (_free_cost_limit(generation_type) <= 0 or required_credits <= _free_cost_limit(generation_type))
    )
    return jsonify({
        "ok": True,
        "generation_type": generation_type,
        "action_key": action_key,
        "required_credits": required_credits,
        "provider": route_params.get("provider"),
        "settings": route_params,
        "mode": "free" if free_available else ("paid" if has_credits else "blocked"),
        "challenge_required": bool(free_available and is_turnstile_enabled()),
        "entitlement": entitlement,
        "entitlements": state["entitlements"],
        "has_credits": has_credits,
        "free_offer": {
            "image": f"{_provider_label(route_params.get('provider'))} {route_params.get('image_size', '2K')} image",
            "video": f"{_provider_label(route_params.get('provider'))} video, {route_params.get('duration_seconds', 5)} seconds",
            "3d": "Meshy 3D model",
        }.get(generation_type),
    })


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

    intent = _parse_homepage_intent(prompt, body)
    generation_type = intent["generation_type"]
    action_key, required_credits, route_params = _action_for_generation_type(generation_type, intent)
    identity_id = getattr(g, "identity_id", None)
    idempotency_key = request.headers.get("Idempotency-Key") or body.get("idempotency_key") or ""

    state = get_current_trial_state(generation_type)
    entitlement = state["entitlements"][generation_type]
    if entitlement.get("active_job_id"):
        active_job_id = entitlement["active_job_id"]
        return jsonify({
            "ok": True,
            "status": "queued",
            "job_id": active_job_id,
            "generation_type": generation_type,
            "polling_url": f"/api/_mod/homepage/status/{active_job_id}",
            "free_trial_remaining": False,
            "message": "Your free generation is already running.",
        }), 202

    free_mode = bool(
        _free_generation_enabled()
        and _free_type_allowed(generation_type)
        and _free_provider_ready(generation_type, route_params)
        and entitlement.get("remaining")
        and (_free_cost_limit(generation_type) <= 0 or required_credits <= _free_cost_limit(generation_type))
    )
    paid_mode = bool(not free_mode and identity_id and has_paid_balance(identity_id, action_key, required_credits))

    trial = None
    if free_mode:
        # Anonymous/free homepage generation is abuse-sensitive and must pass
        # Cloudflare Turnstile before any free trial row, credit reservation, or
        # upstream provider job can be created. Paid users with sufficient
        # credits skip this branch and use the normal paid reservation flow.
        if is_turnstile_enabled():
            turnstile_result = verify_turnstile_token(
                body.get("turnstile_token"),
                remote_ip=_turnstile_remote_ip(),
                expected_action="free_generation",
            )
            if not turnstile_result.ok:
                print(
                    f"[HOMEPAGE_GENERATION] turnstile_blocked "
                    f"reason={turnstile_result.reason} errors={turnstile_result.errors}"
                )
                return _turnstile_required_response(turnstile_result.reason)

        decision = reserve_trial(
            prompt,
            generation_type,
            idempotency_key=idempotency_key,
            max_daily_total=int(getattr(config, "HOMEPAGE_FREE_MAX_DAILY_TOTAL", _env_int("HOMEPAGE_FREE_MAX_DAILY_TOTAL", 50))),
            max_per_ip_per_day=int(getattr(config, "HOMEPAGE_FREE_MAX_PER_IP_PER_DAY", _env_int("HOMEPAGE_FREE_MAX_PER_IP_PER_DAY", 6))),
            max_attempts_per_type_per_day=int(getattr(config, "HOMEPAGE_FREE_MAX_ATTEMPTS_PER_TYPE_PER_DAY", 2)),
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
            if decision.blocked_reason in {"homepage_free_daily_limit", "homepage_free_ip_limit", "free_attempt_limit"}:
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
        g.homepage_free_generation_type = generation_type
    elif not paid_mode:
        return _homepage_blocked_response("free_trial_used")

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
                "provider": route_params.get("provider"),
                "settings": route_params,
            },
        )

    return jsonify(
        {
            **data,
            "ok": True,
            "generation_type": generation_type,
            "action_key": action_key,
            "provider": route_params.get("provider"),
            "settings": route_params,
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
