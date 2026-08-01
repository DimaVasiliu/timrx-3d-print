"""
Seedance Video Generation Service (via PiAPI).

Wraps the PiAPI unified API for Seedance 2.0 video generation.
Endpoints:
  - POST https://api.piapi.ai/api/v1/task  (create task)
  - GET  https://api.piapi.ai/api/v1/task/{task_id}  (poll status)

Task types (PiAPI, Aug 2026).

Seedance 2.0 — three tiers, each with a permissive-review twin:
  seedance-2       / seedance-2-less-restriction        480p / 720p / 1080p
  seedance-2-fast  / seedance-2-fast-less-restriction   480p / 720p
  seedance-2-mini  / seedance-2-mini-less-restriction   480p / 720p (defaults 720p)

Seedance 2.5 — a separate, newer model rather than another 2.0 tier:
  seedance-2.5                                          480p / 720p (defaults 720p)
    • single model: no fast/mini split and no -less-restriction twin
    • 12 combined references (9 images / 3 videos / 3 audio) vs 2.0's flat 9
    • no `auto` aspect ratio; first_last_frames takes its shape from the frames
    • `asset://` references are not supported
    • $0.30/s at 480p, $0.60/s at 720p — pricier per second than 2.0 at 1080p
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import requests

from backend.config import config
from backend.services.video_errors import (
    PIAPI_STATUS_MAP,
    PIAPI_ZERO_TIMESTAMPS,
    ErrorCategory,
    classify_error,
)


# ── Constants ────────────────────────────────────────────────
PIAPI_BASE = "https://api.piapi.ai/api/v1"
PIAPI_TIMEOUT = 30  # seconds

# ── Provider capability note ─────────────────────────────────
# Seedance via PiAPI is a POLL-FIRST provider. Observed behavior (March 2025):
#   - We send webhook_config in the create-task request body
#   - PiAPI returns webhook_config with empty endpoint/secret in the response
#   - No webhook deliveries are received for seedance-2-fast-preview or
#     seedance-2-preview task types
# Conclusion: PiAPI ignores or strips webhook_config for Seedance models.
# Polling via the durable job worker is the primary and required completion
# mechanism. Webhook config is still sent best-effort for future compatibility
# but all Seedance logic (completion, failure, timeout, credits) must work
# correctly without webhook delivery.

# Startup diagnostic (safe — never logs the key itself)
_api_key_present = bool(getattr(config, "PIAPI_API_KEY", ""))
_webhook_enabled = getattr(config, "PIAPI_WEBHOOK_ENABLED", False)
_webhook_url = getattr(config, "PIAPI_WEBHOOK_URL", "")
print(f"[SEEDANCE] api_key configured={_api_key_present}")
print(f"[SEEDANCE] webhook enabled={_webhook_enabled} url={_webhook_url or 'NONE'}")
print("[SEEDANCE] mode=poll-first (webhook optional, PiAPI strips webhook_config for seedance models)")
if not _api_key_present:
    print("[SEEDANCE] WARNING: PIAPI_API_KEY is not set — Seedance video generation will fail")


# ── Errors ───────────────────────────────────────────────────
class SeedanceConfigError(Exception):
    """Raised when PIAPI_API_KEY is not configured."""
    pass


class SeedanceAuthError(Exception):
    """Raised on 401/403 from PiAPI."""
    pass


class SeedanceQuotaError(Exception):
    """Raised on 429 / quota exhaustion from PiAPI."""
    pass


# ── Configuration check ─────────────────────────────────────
def check_seedance_configured() -> Tuple[bool, Optional[str]]:
    """Check if Seedance (PiAPI) is configured."""
    api_key = getattr(config, "PIAPI_API_KEY", "")
    if not api_key:
        return False, "PIAPI_API_KEY not set"
    return True, None


def _get_headers() -> Dict[str, str]:
    """Build PiAPI request headers."""
    api_key = getattr(config, "PIAPI_API_KEY", "")
    if not api_key:
        raise SeedanceConfigError("PIAPI_API_KEY not set")
    return {
        "X-API-Key": api_key,
        "Content-Type": "application/json",
    }


# ── Task-type compatibility ─────────────────────────────────
# GA task types accept a `mode` field and resolution; legacy preview-era types
# do not. We keep both so:
#   • new code targets GA (`seedance-2-fast` / `seedance-2` / `seedance-2-mini`)
#   • legacy in-flight jobs polling against this service keep working
#   • frontends pinned to old `*-preview` task types still produce a job
#
# PiAPI (Aug 2026) exposes six task types: three strict-moderation tiers and a
# `-less-restriction` counterpart for each. The LR variants apply a more
# permissive content review, cost +10% upstream, and are the only variants that
# accept `asset://<id>` references from the Private Asset Library. A rejection on
# an LR variant is final (no alternate review path).
GA_TASK_TYPES = frozenset({
    "seedance-2",
    "seedance-2-fast",
    "seedance-2-mini",
    "seedance-2-less-restriction",
    "seedance-2-fast-less-restriction",
    "seedance-2-mini-less-restriction",
    # Seedance 2.5 — separate model, single variant (no fast/mini/LR split).
    "seedance-2.5",
})

# Seedance 2.5 task types. Kept as a set so future point releases slot in cleanly.
V25_TASK_TYPES = frozenset({"seedance-2.5"})


def is_v25(task_type: str) -> bool:
    """True for Seedance 2.5 task types (different limits and contract from 2.0)."""
    return (task_type or "").strip().lower() in V25_TASK_TYPES

# Strict tier ↔ its `-less-restriction` counterpart (both directions).
LESS_RESTRICTION_TASK_TYPES = frozenset({
    "seedance-2-less-restriction",
    "seedance-2-fast-less-restriction",
    "seedance-2-mini-less-restriction",
})
_STRICT_TO_LR = {
    "seedance-2":      "seedance-2-less-restriction",
    "seedance-2-fast": "seedance-2-fast-less-restriction",
    "seedance-2-mini": "seedance-2-mini-less-restriction",
}
_LR_TO_STRICT = {v: k for k, v in _STRICT_TO_LR.items()}

LEGACY_TASK_TYPES = frozenset({
    "seedance-2-preview",
    "seedance-2-fast-preview",
    "seedance-2-preview-vip",
    "seedance-2-fast-preview-vip",
})

# When a caller passes a legacy alias from the frontend, transparently upgrade to GA.
# PiAPI no longer lists these in the task_type enum (it auto-redirects), so we
# translate before sending rather than relying on upstream compatibility.
_LEGACY_TO_GA = {
    "seedance-2-preview":          "seedance-2",
    "seedance-2-fast-preview":     "seedance-2-fast",
    "seedance-2-preview-vip":      "seedance-2",
    "seedance-2-fast-preview-vip": "seedance-2-fast",
}

# Modes are required on GA task types only.
VALID_MODES = frozenset({"text_to_video", "first_last_frames", "omni_reference"})

# PiAPI input contract. Shared across models.
MAX_PROMPT_CHARS = 4000
MIN_DURATION_SECONDS = 4
VALID_ASPECT_RATIOS = frozenset({"21:9", "16:9", "4:3", "1:1", "3:4", "9:16"})
# Ephemeral-asset retention window accepted by PiAPI (`-less-restriction` only).
ASSET_RETENTION_HOURS_RANGE = (3, 8)

# ── Seedance 2.0 limits ──
MAX_TOTAL_REFERENCES = 9          # image_urls + video_urls + audio_urls combined
MAX_IMAGE_REFERENCES = 9
MAX_VIDEO_REFERENCES = 9
MAX_AUDIO_REFERENCES = 9
MAX_DURATION_SECONDS = 15
# On 2.0, `auto` is accepted in first_last_frames mode only; elsewhere it 400s.
AUTO_ASPECT_MODES = frozenset({"first_last_frames"})

# ── Seedance 2.5 limits ──
# 2.5 splits its reference budget by kind instead of using one flat ceiling, and
# drops `auto` entirely (first_last_frames takes its shape from the frames, so we
# omit aspect_ratio for that mode rather than sending a value PiAPI ignores).
V25_MAX_TOTAL_REFERENCES = 12
V25_MAX_IMAGE_REFERENCES = 9
V25_MAX_VIDEO_REFERENCES = 3
V25_MAX_AUDIO_REFERENCES = 3
# PiAPI's docs disagree on the 2.5 ceiling: the product page and model blog both say
# "4 to 15 seconds, with 15 seconds as the generation cap", while the API-specs blog
# says "1 to 30 seconds". The model's headline is 30s single-shot output, but two of
# three sources call 15 a hard cap, so we enforce 15 and make it overridable — set
# SEEDANCE_25_MAX_DURATION=30 once verified against a live key, no deploy needed.
# Pricing needs no new rows if raised: the per-second fallback covers any duration.
V25_MAX_DURATION_SECONDS = int(os.getenv("SEEDANCE_25_MAX_DURATION", "15"))

# Per-task-type resolution support and defaults (PiAPI: Mini and 2.5 default to
# 720p; `seedance-2` is the only task type offering 1080p).
TASK_TYPE_RESOLUTIONS: Dict[str, frozenset] = {
    "seedance-2":      frozenset({"480p", "720p", "1080p"}),
    "seedance-2-fast": frozenset({"480p", "720p"}),
    "seedance-2-mini": frozenset({"480p", "720p"}),
    "seedance-2.5":    frozenset({"480p", "720p"}),
}
TASK_TYPE_DEFAULT_RESOLUTION: Dict[str, str] = {
    "seedance-2":      "480p",
    "seedance-2-fast": "480p",
    "seedance-2-mini": "720p",
    "seedance-2.5":    "720p",
}


def reference_limits_for(task_type: str) -> Dict[str, int]:
    """Per-model reference ceilings. 2.5 budgets by kind; 2.0 uses one flat cap."""
    if is_v25(task_type):
        return {
            "total":  V25_MAX_TOTAL_REFERENCES,
            "images": V25_MAX_IMAGE_REFERENCES,
            "videos": V25_MAX_VIDEO_REFERENCES,
            "audios": V25_MAX_AUDIO_REFERENCES,
        }
    return {
        "total":  MAX_TOTAL_REFERENCES,
        "images": MAX_IMAGE_REFERENCES,
        "videos": MAX_VIDEO_REFERENCES,
        "audios": MAX_AUDIO_REFERENCES,
    }


def max_duration_for(task_type: str) -> int:
    """Maximum output duration PiAPI accepts for this task type."""
    return V25_MAX_DURATION_SECONDS if is_v25(task_type) else MAX_DURATION_SECONDS


def strip_less_restriction(task_type: str) -> str:
    """Return the strict-moderation equivalent of a task type."""
    t = (task_type or "").strip().lower()
    return _LR_TO_STRICT.get(t, t)


def apply_less_restriction(task_type: str) -> str:
    """Return the `-less-restriction` counterpart of a strict task type."""
    t = (task_type or "").strip().lower()
    if t in LESS_RESTRICTION_TASK_TYPES:
        return t
    return _STRICT_TO_LR.get(t, t)


def is_less_restriction(task_type: str) -> bool:
    return (task_type or "").strip().lower() in LESS_RESTRICTION_TASK_TYPES


def _resolve_task_type(task_type: str) -> str:
    """Coerce any provided task_type to a canonical PiAPI string. Defaults to GA fast."""
    t = (task_type or "").strip().lower()
    if t in GA_TASK_TYPES or t in LEGACY_TASK_TYPES:
        return t
    return "seedance-2-fast"


# ── Create task ──────────────────────────────────────────────
def create_seedance_task(
    prompt: str,
    duration: int = 5,
    aspect_ratio: str = "16:9",
    image_urls: Optional[List[str]] = None,
    video_urls: Optional[List[str]] = None,
    audio_urls: Optional[List[str]] = None,
    task_type: str = "seedance-2-fast",
    mode: Optional[str] = None,
    resolution: Optional[str] = None,
    audio: Optional[bool] = None,
    auto_upload_assets: Optional[bool] = None,
    asset_retention_hours: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Create a Seedance video generation task via PiAPI.

    Args:
        prompt:       Text prompt for video generation (max 4000 chars — truncated if longer).
        duration:     Video duration in seconds (4–15 for GA; 5/10/15 for legacy preview).
        aspect_ratio: Output aspect ratio (GA: 21:9 / 16:9 / 4:3 / 1:1 / 3:4 / 9:16, plus
                      `auto` in first_last_frames mode only; legacy: 16:9 / 9:16 / 4:3 / 3:4).
        image_urls:   Optional image URLs (or `asset://<id>` on `-less-restriction` types).
                       • text_to_video mode MUST omit image_urls.
                       • first_last_frames mode accepts 1 image (animate from single ref)
                         or 2 images (transition from first → last frame).
                       • omni_reference accepts up to 9 on 2.0, or up to 9 images
                         within a 12-reference budget on 2.5.
        video_urls:   Optional video reference URLs (omni_reference only; mp4/mov).
                      Max 9 on 2.0, 3 on 2.5.
        audio_urls:   Optional audio reference URLs (omni_reference only; mp3/wav, ≤15s total).
                      Max 9 on 2.0, 3 on 2.5.
        task_type:    PiAPI task type. Defaults to GA `seedance-2-fast`. Either a 2.0
                      type (seedance-2 / -fast / -mini, each with an optional
                      `-less-restriction` suffix) or `seedance-2.5`. Legacy preview
                      types are accepted (and silently upgraded to their GA equivalent).
        mode:         "text_to_video" | "first_last_frames" | "omni_reference"
                      Required for GA task types; ignored for legacy preview.
        resolution:   "480p" | "720p" | "1080p" (GA only; 1080p is `seedance-2` only —
                      2.5, Fast and Mini all cap at 720p).
        audio:        Generate a soundtrack. PiAPI defaults to True; pass False to
                      suppress. Omitted from the request body when None.
        auto_upload_assets:    Ingest raw reference URLs into PiAPI's Private Asset
                      Library before running. `-less-restriction` task types only.
        asset_retention_hours: Ephemeral asset lifetime, 3–8 hours.
                      `-less-restriction` task types only.

    Returns:
        {"task_id": "...", "status": "processing"}

    Raises:
        SeedanceConfigError: API key not set.
        SeedanceAuthError:   Authentication failed.
        SeedanceQuotaError:  Quota exhausted or rate limited.
        RuntimeError:        Other API errors.
    """
    headers = _get_headers()

    # Resolve task type (may upgrade legacy → GA so we can use mode/resolution).
    resolved_task_type = _resolve_task_type(task_type)
    is_ga = resolved_task_type in GA_TASK_TYPES

    # Some frontends still pass `seedance-2-fast-preview` etc. Upgrade them so we
    # can send `mode` + `resolution` on the modern endpoint.
    if resolved_task_type in _LEGACY_TO_GA:
        upgraded = _LEGACY_TO_GA[resolved_task_type]
        print(f"[Seedance] upgrading legacy task_type {resolved_task_type!r} → {upgraded!r} (GA)")
        resolved_task_type = upgraded
        is_ga = True

    # PiAPI enforces prompt maxLength=4000 — truncate rather than 400 on the user.
    safe_prompt = prompt or ""
    if len(safe_prompt) > MAX_PROMPT_CHARS:
        print(
            f"[Seedance] prompt exceeds {MAX_PROMPT_CHARS} chars "
            f"({len(safe_prompt)}) — truncating per PiAPI limit"
        )
        safe_prompt = safe_prompt[:MAX_PROMPT_CHARS]

    # Clamp to the range this task type accepts instead of letting PiAPI 400.
    duration_ceiling = max_duration_for(resolved_task_type)
    safe_duration = int(duration)
    if safe_duration < MIN_DURATION_SECONDS or safe_duration > duration_ceiling:
        clamped = max(MIN_DURATION_SECONDS, min(duration_ceiling, safe_duration))
        print(
            f"[Seedance] duration {safe_duration}s out of range for {resolved_task_type} "
            f"(max {duration_ceiling}s) — clamping to {clamped}s"
        )
        safe_duration = clamped

    input_obj: Dict[str, Any] = {
        "prompt": safe_prompt,
        "duration": safe_duration,
        "aspect_ratio": aspect_ratio,
    }

    has_av_refs = bool(video_urls or audio_urls)

    if is_ga:
        # mode is REQUIRED on GA. Default to text_to_video if caller didn't say.
        # PiAPI auto-infers mode from inputs; we mirror that here:
        #   video/audio refs (any combo) → omni_reference
        #   1–2 images only              → first_last_frames
        #   no refs                      → text_to_video
        if mode and mode in VALID_MODES:
            resolved_mode = mode
        elif has_av_refs:
            resolved_mode = "omni_reference"
        elif image_urls:
            resolved_mode = "first_last_frames"
        else:
            resolved_mode = "text_to_video"

        # If A/V refs are present the mode MUST be omni_reference (first_last_frames
        # and text_to_video both reject video/audio per the PiAPI spec).
        if has_av_refs and resolved_mode != "omni_reference":
            print("[Seedance] video/audio refs present — forcing mode=omni_reference per PiAPI spec")
            resolved_mode = "omni_reference"

        # GA contract: text_to_video does NOT accept any references. Strip them to avoid 400.
        if resolved_mode == "text_to_video" and image_urls:
            print("[Seedance] WARNING: text_to_video mode rejects image_urls — stripping per PiAPI spec")
            image_urls = None

        input_obj["mode"] = resolved_mode

        # Aspect ratio rules differ by model.
        #   2.0: `auto` is valid in first_last_frames only; PiAPI 400s on it elsewhere.
        #   2.5: no `auto` at all, and first_last_frames derives its shape from the
        #        supplied frames — so we omit aspect_ratio entirely for that mode.
        ar = (aspect_ratio or "16:9").strip().lower()
        if is_v25(resolved_task_type):
            if resolved_mode == "first_last_frames":
                input_obj.pop("aspect_ratio", None)
                print("[Seedance] 2.5 first_last_frames — omitting aspect_ratio (frames define the shape)")
            else:
                if ar not in VALID_ASPECT_RATIOS:
                    print(
                        f"[Seedance] aspect_ratio {ar!r} unsupported on {resolved_task_type} "
                        "(2.5 has no `auto`) — falling back to 16:9"
                    )
                    ar = "16:9"
                input_obj["aspect_ratio"] = ar
        else:
            if ar == "auto" and resolved_mode not in AUTO_ASPECT_MODES:
                print(
                    f"[Seedance] aspect_ratio=auto is only valid in first_last_frames "
                    f"(mode={resolved_mode}) — falling back to 16:9 per PiAPI spec"
                )
                ar = "16:9"
            elif ar != "auto" and ar not in VALID_ASPECT_RATIOS:
                print(f"[Seedance] unsupported aspect_ratio {ar!r} — falling back to 16:9")
                ar = "16:9"
            input_obj["aspect_ratio"] = ar

        # Resolution: snap to what this task type actually offers (Mini/Fast have no
        # 1080p; Mini defaults to 720p rather than 480p).
        strict_type = strip_less_restriction(resolved_task_type)
        allowed_res = TASK_TYPE_RESOLUTIONS.get(strict_type)
        default_res = TASK_TYPE_DEFAULT_RESOLUTION.get(strict_type, "480p")
        res = (resolution or "").strip().lower()
        if allowed_res is not None:
            if res not in allowed_res:
                if res:
                    print(
                        f"[Seedance] resolution {res!r} unsupported on {resolved_task_type} "
                        f"— using {default_res}"
                    )
                res = default_res
            input_obj["resolution"] = res
        elif res:
            input_obj["resolution"] = res

        # Soundtrack toggle. PiAPI defaults to true; only send when explicitly set
        # so we never fight a future default change.
        if audio is not None:
            input_obj["audio"] = bool(audio)

        # Private Asset Library controls — accepted by `-less-restriction` types only.
        # Strict task types reject `asset://` references (and these fields) with 422.
        if is_less_restriction(resolved_task_type):
            if auto_upload_assets is not None:
                input_obj["auto_upload_assets"] = bool(auto_upload_assets)
            if asset_retention_hours is not None:
                lo, hi = ASSET_RETENTION_HOURS_RANGE
                input_obj["asset_retention_hours"] = max(lo, min(hi, int(asset_retention_hours)))
        elif auto_upload_assets is not None or asset_retention_hours is not None:
            print(
                f"[Seedance] asset options ignored — {resolved_task_type} is a strict "
                "task type (PiAPI accepts them on -less-restriction variants only)"
            )
    else:
        # Legacy preview types never carried video/audio refs.
        has_av_refs = False
        video_urls = None
        audio_urls = None

    # Reference ceilings. 2.0 allows 9 combined; 2.5 allows 12 combined but budgets
    # them by kind (9 images / 3 videos / 3 audio), so per-kind caps are enforced too.
    ref_caps = reference_limits_for(resolved_task_type)
    if image_urls and len(image_urls) > ref_caps["images"]:
        print(f"[Seedance] image_urls trimmed {len(image_urls)} → {ref_caps['images']} (PiAPI cap)")
        image_urls = image_urls[:ref_caps["images"]]
    if video_urls and len(video_urls) > ref_caps["videos"]:
        print(f"[Seedance] video_urls trimmed {len(video_urls)} → {ref_caps['videos']} (PiAPI cap)")
        video_urls = video_urls[:ref_caps["videos"]]
    if audio_urls and len(audio_urls) > ref_caps["audios"]:
        print(f"[Seedance] audio_urls trimmed {len(audio_urls)} → {ref_caps['audios']} (PiAPI cap)")
        audio_urls = audio_urls[:ref_caps["audios"]]
    total_refs = len(image_urls or []) + len(video_urls or []) + len(audio_urls or [])
    if total_refs > ref_caps["total"]:
        raise RuntimeError(
            f"seedance_too_many_references: {resolved_task_type} accepts at most "
            f"{ref_caps['total']} combined references (got {total_refs})"
        )

    body = {
        "model": "seedance",
        "task_type": resolved_task_type,
        "input": input_obj,
    }

    if image_urls:
        body["input"]["image_urls"] = image_urls
    # Video / audio references — omni_reference mode only (guaranteed by the logic above).
    if video_urls:
        body["input"]["video_urls"] = video_urls
    if audio_urls:
        body["input"]["audio_urls"] = audio_urls

    # Best-effort webhook config: PiAPI currently strips this for Seedance models,
    # but we include it for forward compatibility. Polling is the primary path.
    webhook_url = getattr(config, "PIAPI_WEBHOOK_URL", "")
    webhook_secret = getattr(config, "PIAPI_WEBHOOK_SECRET", "")

    if webhook_url:
        wh_cfg: Dict[str, str] = {"endpoint": webhook_url}
        if webhook_secret:
            wh_cfg["secret"] = webhook_secret
        body["config"] = {"webhook_config": wh_cfg}

    print(f"[Seedance] operating in poll-first mode (webhook optional, sent={bool(webhook_url)})")

    # Log the exact request body (redact API key from headers, keep webhook visible)
    _prompt_preview = (body.get("input", {}).get("prompt") or "")[:60]
    _input_view = body.get("input", {})
    _log_body_safe = {
        "model": body.get("model"),
        "task_type": body.get("task_type"),
        "mode": _input_view.get("mode"),
        "resolution": _input_view.get("resolution"),
        "duration": _input_view.get("duration"),
        "aspect_ratio": _input_view.get("aspect_ratio"),
        "audio": _input_view.get("audio", "DEFAULT(true)"),
        "less_restriction": is_less_restriction(body.get("task_type", "")),
        "model_family": "2.5" if is_v25(body.get("task_type", "")) else "2.0",
        "auto_upload_assets": _input_view.get("auto_upload_assets", "NOT_SET"),
        "asset_retention_hours": _input_view.get("asset_retention_hours", "NOT_SET"),
        "prompt_chars": len(_input_view.get("prompt") or ""),
        "image_url_count": len(_input_view.get("image_urls") or []),
        "video_url_count": len(_input_view.get("video_urls") or []),
        "audio_url_count": len(_input_view.get("audio_urls") or []),
        "input_keys": sorted(_input_view.keys()),
        "prompt_preview": _prompt_preview,
        "webhook_config": (body.get("config") or {}).get("webhook_config", "NOT_SET"),
    }
    print(f"[Seedance] REQUEST body: {_log_body_safe}")

    try:
        resp = requests.post(
            f"{PIAPI_BASE}/task",
            json=body,
            headers=headers,
            timeout=PIAPI_TIMEOUT,
        )
    except requests.RequestException as e:
        raise RuntimeError(f"seedance_network_error: {e}")

    # Log response immediately (safe: no secrets in response)
    print(
        f"[Seedance] RESPONSE status={resp.status_code} "
        f"body={resp.text[:500]}"
    )

    if resp.status_code == 401 or resp.status_code == 403:
        raise SeedanceAuthError(f"PiAPI auth failed: {resp.status_code} {resp.text[:200]}")

    if resp.status_code == 429:
        raise SeedanceQuotaError(f"PiAPI rate limited: {resp.text[:200]}")

    if resp.status_code >= 400:
        raise RuntimeError(f"seedance_api_error: {resp.status_code} {resp.text[:300]}")

    data = resp.json()

    # PiAPI returns {"code": 200, "data": {"task_id": "...", ...}}
    task_data = data.get("data", data)
    task_id = task_data.get("task_id")

    if not task_id:
        raise RuntimeError(f"seedance_no_task_id: {data}")

    # Log whether provider echoed back our webhook_config.
    # PiAPI Seedance (seedance-2-fast-preview, seedance-2-preview) appears to
    # ignore webhook_config — treat as poll-driven unless provider behavior changes.
    provider_webhook = task_data.get("webhook_config")
    wh_endpoint = (provider_webhook or {}).get("endpoint", "") if isinstance(provider_webhook, dict) else ""
    if wh_endpoint:
        print(f"[Seedance] Task created: {task_id} webhook_config echoed (endpoint={wh_endpoint[:60]})")
    else:
        print(f"[Seedance] Task created: {task_id} — provider ignored webhook_config; polling lifecycle active")

    return {
        "task_id": task_id,
        "status": "processing",
    }


# ── Check status ─────────────────────────────────────────────
# PiAPI status → internal status (shared source of truth in video_errors.py)
_STATUS_MAP = PIAPI_STATUS_MAP
_ZERO_TIMESTAMPS = PIAPI_ZERO_TIMESTAMPS


def check_seedance_status(task_id: str) -> Dict[str, Any]:
    """
    Check the status of a Seedance task via PiAPI.

    Returns a rich dict:
        status:          done | processing | pending | failed | error
        provider_status: raw PiAPI status string
        started_at:      ISO timestamp or None
        ended_at:        ISO timestamp or None
        progress:        0-100
        video_url:       (only when done)
        error/message:   (only when failed)
    """
    headers = _get_headers()

    try:
        resp = requests.get(
            f"{PIAPI_BASE}/task/{task_id}",
            headers=headers,
            timeout=PIAPI_TIMEOUT,
        )
    except requests.RequestException as e:
        return {"status": "error", "message": f"Network error: {e}"}

    if resp.status_code == 401 or resp.status_code == 403:
        return {"status": "failed", "error": ErrorCategory.AUTH, "message": "PiAPI auth failed"}

    if resp.status_code >= 400:
        return {"status": "error", "error": ErrorCategory.NETWORK, "message": f"PiAPI error: {resp.status_code}"}

    data = resp.json()
    task_data = data.get("data", data)

    piapi_status = task_data.get("status", "unknown")
    internal_status = _STATUS_MAP.get(piapi_status, "pending")

    # Determine if the job is queued upstream (staged) vs actively pending/processing.
    # PiAPI "Staged"/"staged" means the task is queued at the provider but NOT yet
    # actively running. This distinction matters for timeout behavior.
    is_queued_upstream = piapi_status.lower() == "staged"

    # Extract timing metadata
    started_at = task_data.get("started_at") or task_data.get("start_time")
    ended_at = task_data.get("ended_at") or task_data.get("end_time")
    has_start_timestamp = started_at not in _ZERO_TIMESTAMPS

    # PiAPI often returns zero/missing timestamps even when genuinely
    # processing. Trust the explicit status from PiAPI over timestamps.
    # "started" here means: provider says processing OR we have a real timestamp.
    started = has_start_timestamp or internal_status == "processing"

    # Debug: log raw PiAPI status for rescue diagnostics
    queue_label = " (QUEUED_UPSTREAM)" if is_queued_upstream else ""
    print(f"[Seedance] status check task={task_id[:12]}... "
          f"raw_status={piapi_status!r} -> {internal_status}{queue_label} "
          f"started={started} (has_ts={has_start_timestamp}) "
          f"keys={list(task_data.keys())[:8]}")

    result: Dict[str, Any] = {
        "status": internal_status,
        "provider_status": piapi_status,
        "queued_upstream": is_queued_upstream,
        "started_at": started_at if has_start_timestamp else None,
        "ended_at": ended_at if ended_at not in _ZERO_TIMESTAMPS else None,
        "progress": task_data.get("progress", 0),
    }

    if internal_status == "done":
        # Extract video URL from output
        output = task_data.get("output") or {}
        video_url = (
            output.get("video")
            or output.get("video_url")
            or output.get("video_urls", [None])[0]
        )

        if not video_url:
            video_url = task_data.get("video_url") or task_data.get("video")

        if video_url:
            result["video_url"] = video_url
        else:
            result["status"] = "failed"
            result["error"] = ErrorCategory.NO_OUTPUT
            result["message"] = "Task completed but no video URL found"

    elif internal_status == "failed":
        error = task_data.get("error", {})
        result["error"] = ErrorCategory.INTERNAL
        if isinstance(error, dict):
            result["message"] = error.get("message", "") or "Seedance generation failed"
            result["provider_error_code"] = error.get("code")
        else:
            result["message"] = str(error) or "Seedance generation failed"
        # PiAPI content review rejects (final on `-less-restriction` variants) are the
        # caller's fault, not ours — classify so the job fails terminally with a clear
        # message instead of being retried as an internal error.
        _classified = classify_error(
            str(result.get("provider_error_code") or ""), str(result.get("message") or "")
        )
        if _classified == ErrorCategory.VALIDATION:
            result["error"] = ErrorCategory.VALIDATION
            result["content_rejected"] = True
        # Capture provider logs if present (PiAPI includes these for debugging)
        provider_logs = task_data.get("logs") or task_data.get("log")
        if provider_logs:
            result["provider_logs"] = provider_logs

    return result


# ── Download video ───────────────────────────────────────────
def _validate_video_bytes(data: bytes, content_type: str, video_url: str) -> None:
    """
    Validate that downloaded bytes are actually an MP4 video.

    PiAPI serves completed videos from ephemeral URLs (img.theapi.app).
    If the URL expired or an error page was returned, we may receive HTML
    or an empty body instead of video data. Uploading garbage to S3 would
    corrupt the user's result, so we validate before returning.

    MP4 files contain an 'ftyp' box: bytes 4-8 == b'ftyp'.
    """
    if len(data) < 8:
        print(f"[SEEDANCE_OBS] event=download_rejected reason=too_small bytes={len(data)} content_type={content_type}")
        raise RuntimeError(
            f"seedance_download_corrupt: response too small "
            f"({len(data)} bytes, content_type={content_type}, url={video_url[:80]})"
        )

    # MP4 'ftyp' box signature check
    if data[4:8] == b"ftyp":
        return  # valid MP4

    # Content-type says video but bytes don't match — warn but allow
    # (some providers may use non-ftyp container formats)
    if content_type.startswith("video/"):
        print(
            f"[Seedance] WARNING: downloaded bytes lack ftyp signature "
            f"but content_type={content_type} — allowing"
        )
        return

    # Non-video content-type AND no ftyp → definitely not a video
    print(f"[SEEDANCE_OBS] event=download_rejected reason=not_video content_type={content_type} bytes={len(data)}")
    raise RuntimeError(
        f"seedance_download_not_video: expected video, got content_type={content_type}, "
        f"first_bytes={data[:16]!r}, url={video_url[:80]}"
    )


def download_seedance_video(video_url: str) -> Tuple[bytes, str]:
    """
    Download video bytes from a Seedance result URL.

    Validates the response is actually an MP4 before returning.
    Raises RuntimeError on download failure or corrupt/expired URLs.

    Returns:
        (video_bytes, content_type)
    """
    try:
        resp = requests.get(video_url, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"seedance_download_error: {e}")

    content_type = resp.headers.get("Content-Type", "video/mp4")

    _validate_video_bytes(resp.content, content_type, video_url)

    return resp.content, content_type
