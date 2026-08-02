"""Canonical planning and quoting for the natural-language command bar.

The language model may suggest intent and settings, but it is never trusted
with provider ids, supported values, or prices. Those decisions stay here and
are shared by plan, quote, and execute.
"""

from __future__ import annotations

import re
from typing import Any

from backend.config import MESHY_API_KEY, config
from backend.services.image_provider_registry import (
    get_image_action_key,
    get_image_provider_spec,
    is_image_provider_enabled,
)
from backend.services.pricing_service import (
    PricingService,
    get_video_action_code,
    get_video_credit_cost,
)
from backend.services.video_providers.fal_seedance_provider import normalize_fal_seedance_params
from backend.services.video_providers.seedance_provider import normalize_seedance_params
from backend.services.video_providers.vertex_provider import normalize_vertex_params
from backend.services.video_router import normalize_provider_name


class CommandPlanError(ValueError):
    """A user-facing, non-provider error in a command plan."""


_IMAGE_PROVIDERS = {
    "openai": "openai",
    "gpt image": "openai",
    "google": "google",
    "gemini": "google",
    "imagen": "google",
    "nano banana": "nano_banana",
    "nano-banana": "nano_banana",
    "nanobanana": "nano_banana",
    "nano_banana": "nano_banana",
}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _coerce_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    return _clean(value).lower() in {"1", "true", "yes", "on", "enabled"}


def _flatten_settings(raw: dict) -> dict:
    settings = raw.get("settings") if isinstance(raw.get("settings"), dict) else {}
    merged = dict(settings)
    for key in (
        "provider", "model", "image_size", "imageSize", "size", "aspect_ratio",
        "aspectRatio", "duration", "duration_seconds", "video_duration_seconds",
        "resolution", "video_resolution", "tier", "video_tier", "audio",
        "video_mode", "mode", "style_preset", "motion",
    ):
        if key in raw and key not in merged:
            merged[key] = raw[key]
    return merged


def _infer_intent(text: str, raw: dict) -> str:
    intent = _clean(raw.get("intent")).lower().replace("3d", "model")
    if intent in {"image", "model", "video"}:
        return intent
    lower = text.lower()
    if re.search(r"\b(video|clip|veo|seedance|animate)\b", lower):
        return "video"
    if re.search(r"\b(3d|model|mesh|meshy|stl|glb)\b", lower):
        return "model"
    if re.search(r"\b(image|picture|illustration|photo|nano\s*banana)\b", lower):
        return "image"
    raise CommandPlanError("Tell me whether you want an image, video, or 3D model.")


def _infer_image_provider(text: str, requested: str) -> str:
    value = requested.lower().replace("_", " ").replace("-", " ").strip()
    if value and not any(value == alias or alias in value for alias in _IMAGE_PROVIDERS):
        raise CommandPlanError(f"Image provider '{requested}' is not supported.")
    for alias, provider in _IMAGE_PROVIDERS.items():
        if value == alias or alias in value:
            return provider
    lower = text.lower()
    for alias, provider in _IMAGE_PROVIDERS.items():
        if alias in lower:
            return provider
    return "openai"


def _infer_video_provider(text: str, requested: str) -> tuple[str, str]:
    lower = f"{requested} {text}".lower()
    requested_value = requested.lower().replace("_", "-").strip()
    known_requested = {
        "vertex", "veo", "google", "aistudio", "video", "seedance", "seedance-2",
        "seedance-1.5", "seedance-2.5", "fal", "fal-seedance", "fal-seedance-pro",
    }
    if requested_value and requested_value not in known_requested and not requested_value.startswith(("seedance", "veo", "google")):
        raise CommandPlanError(f"Video provider '{requested}' is not supported.")
    if re.search(r"\b(seedance[-\s]*1\.5|seedance[-\s]*1-5|fal[-\s]*seedance)\b", lower):
        return "fal_seedance", "fast"
    if re.search(r"\b(seedance[-\s]*2\.5|seedance[-\s]*2-5|seedance[-\s]*v25)\b", lower):
        return "seedance", "v25"
    if re.search(r"\b(seedance|seedance\s*2)\b", lower):
        return "seedance", "fast"
    return normalize_provider_name(requested or "vertex"), "fast"


def _infer_ratio(text: str, value: Any, default: str = "1:1") -> str:
    raw = _clean(value).lower()
    if raw in {"portrait", "vertical", "9x16"}:
        return "9:16"
    if raw in {"landscape", "horizontal", "16x9"}:
        return "16:9"
    match = re.search(r"\b(\d+)\s*[:x]\s*(\d+)\b", raw)
    if match:
        return f"{match.group(1)}:{match.group(2)}"
    match = re.search(r"\b(\d+)\s*[:x]\s*(\d+)\b", text.lower())
    return f"{match.group(1)}:{match.group(2)}" if match else default


def _infer_number(text: str, values: set[int], default: int) -> int:
    matches = re.findall(r"\b(\d+)\s*(?:s|sec|secs|second|seconds)?\b", text.lower())
    for value in matches:
        number = int(value)
        if number in values:
            return number
    return default


def _infer_image_size(text: str, value: Any, default: str) -> str:
    raw = _clean(value).upper().replace(" ", "")
    if raw in {"1K", "2K", "4K"}:
        return raw
    match = re.search(r"\b([124])\s*K\b", text, re.IGNORECASE)
    return f"{match.group(1)}K" if match else default


def _infer_resolution(text: str, value: Any, default: str) -> str:
    raw = _clean(value).lower().replace(" ", "")
    if raw in {"480p", "720p", "1080p", "4k"}:
        return raw
    match = re.search(r"\b(480p|720p|1080p|4k)\b", text, re.IGNORECASE)
    return match.group(1).lower() if match else default


def normalize_plan(text: str, raw: dict | None = None) -> dict:
    """Return a strict, provider-ready plan from model output and user text."""
    raw = raw if isinstance(raw, dict) else {}
    prompt = _clean(raw.get("prompt")) or text.strip()
    if not prompt:
        raise CommandPlanError("Describe what you want to create.")

    intent = _infer_intent(text, raw)
    settings = _flatten_settings(raw)

    if intent == "image":
        provider = _infer_image_provider(text, _clean(settings.get("provider")))
        spec = get_image_provider_spec(provider)
        if not spec:
            raise CommandPlanError(f"Image provider '{provider}' is not supported.")
        image_size = _infer_image_size(text, settings.get("image_size") or settings.get("imageSize") or settings.get("resolution"), spec.default_image_size)
        if image_size not in spec.image_sizes:
            raise CommandPlanError(f"{spec.display_name} supports image sizes: {', '.join(spec.image_sizes)}.")
        aspect_ratio = _infer_ratio(text, settings.get("aspect_ratio") or settings.get("aspectRatio"))
        allowed_ratios = {"1:1", "3:4", "4:3", "9:16", "16:9"}
        if aspect_ratio not in allowed_ratios:
            raise CommandPlanError("Use a supported image aspect ratio such as 1:1, 9:16, or 16:9.")
        model = spec.model
        size = "1024x1024"
        if aspect_ratio in {"9:16", "3:4"}:
            size = "1024x1536"
        elif aspect_ratio in {"16:9", "4:3"}:
            size = "1536x1024"
        return {
            "intent": "image", "prompt": prompt, "provider": provider,
            "model": model, "image_size": image_size,
            "aspect_ratio": aspect_ratio, "size": size,
        }

    if intent == "model":
        requested = _clean(settings.get("model") or settings.get("model_version") or text).lower()
        model = "meshy-5" if re.search(r"\b(meshy\s*5|meshy-5)\b", requested) else "latest"
        if re.search(r"\b(meshy\s*6|meshy-6|latest)\b", requested):
            model = "latest"
        return {
            "intent": "model", "prompt": prompt, "provider": "meshy",
            "model": model,
            "pose_mode": _clean(settings.get("pose_mode")) if _clean(settings.get("pose_mode")) in {"a-pose", "t-pose"} else "",
            "symmetry_mode": _clean(settings.get("symmetry_mode")) if _clean(settings.get("symmetry_mode")) in {"off", "auto", "on"} else "",
            "model_type": _clean(settings.get("model_type")) if _clean(settings.get("model_type")) in {"standard", "lowpoly"} else "standard",
        }

    provider, inferred_tier = _infer_video_provider(text, _clean(settings.get("provider")))
    tier = _clean(settings.get("video_tier") or settings.get("tier")) or inferred_tier
    if re.search(r"\b(seedance[-\s]*2\.5|seedance[-\s]*v25)\b", f"{tier} {text}", re.IGNORECASE):
        tier = "v25"
    aspect_ratio = _infer_ratio(text, settings.get("aspect_ratio") or settings.get("aspectRatio"), "16:9")
    raw_duration = settings.get("duration_seconds") or settings.get("video_duration_seconds") or settings.get("duration")
    if provider == "vertex":
        clean = normalize_vertex_params(raw_duration or _infer_number(text, {4, 6, 8}, 6), aspect_ratio, _infer_resolution(text, settings.get("resolution") or settings.get("video_resolution"), "720p"))
    elif provider == "fal_seedance":
        clean = normalize_fal_seedance_params(raw_duration or _infer_number(text, {5, 10, 12}, 5), aspect_ratio, _infer_resolution(text, settings.get("resolution") or settings.get("video_resolution"), "720p"))
    else:
        clean = normalize_seedance_params(raw_duration or _infer_number(text, {5, 10, 15}, 5), aspect_ratio, tier=tier, resolution=_infer_resolution(text, settings.get("resolution") or settings.get("video_resolution"), "480p"), audio=_coerce_bool(settings.get("audio")))
    return {
        "intent": "video", "prompt": prompt, "provider": provider,
        "model": "veo" if provider == "vertex" else ("seedance-1.5-pro" if provider == "fal_seedance" else "seedance-2"),
        "video_tier": clean.get("tier", tier),
        "video_duration_seconds": clean["duration_seconds"],
        "video_resolution": clean["resolution"],
        "aspect_ratio": clean["aspect_ratio"],
        "seedance_variant": clean.get("task_type"),
        "seedance_less_restriction": bool(clean.get("less_restriction", False)),
        "video_mode": _clean(settings.get("video_mode") or settings.get("mode")) or "text2video",
        "style_preset": _clean(settings.get("style_preset")),
        "motion": _clean(settings.get("motion")),
        "audio": clean.get("audio"),
    }


def quote_plan(plan: dict) -> dict:
    """Calculate the authoritative quote for a normalized plan."""
    intent = plan["intent"]
    if intent == "image":
        action_key = get_image_action_key(plan["provider"], plan["image_size"])
        credits = PricingService.get_action_cost(action_key)
        return {"credits": credits, "credit_type": "general", "action_key": action_key, "provider": plan["provider"], "model": plan["model"]}
    if intent == "model":
        action_key = "text_to_3d_generate"
        return {"credits": PricingService.get_action_cost(action_key), "credit_type": "general", "action_key": action_key, "provider": "meshy", "model": plan["model"]}
    task = "image2video" if plan.get("video_mode") in {"image2video", "animate_image"} else "text2video"
    action_key = get_video_action_code(task, plan["video_duration_seconds"], plan["video_resolution"], provider=plan["provider"], seedance_tier=plan.get("video_tier", "fast"))
    credits = get_video_credit_cost(plan["video_duration_seconds"], plan["video_resolution"], provider=plan["provider"], seedance_tier=plan.get("video_tier", "fast"), task=task)
    return {"credits": credits, "credit_type": "video", "action_key": action_key, "provider": plan["provider"], "model": plan["model"]}


def provider_availability(plan: dict) -> tuple[bool, str | None]:
    """Check configuration without selecting a fallback provider."""
    if plan["intent"] == "model":
        return (bool(MESHY_API_KEY), None if MESHY_API_KEY else "Meshy is not configured.")
    if plan["intent"] == "image":
        if not is_image_provider_enabled(plan["provider"]):
            return False, f"{plan['provider']} is not enabled on this workspace."
        spec = get_image_provider_spec(plan["provider"])
        if spec and spec.api_key_attr and not getattr(config, spec.api_key_attr, None):
            return False, f"{spec.display_name} is not configured on this workspace."
        return True, None
    from backend.services.video_router import resolve_video_provider
    provider = resolve_video_provider(plan["provider"])
    if not provider:
        return False, f"{plan['provider']} is not supported."
    configured, error = provider.is_configured()
    return bool(configured), None if configured else (error or f"{plan['provider']} is not configured.")
