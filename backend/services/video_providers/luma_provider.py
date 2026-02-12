"""
Luma Dream Machine Video Provider — wraps the Luma API for the VideoRouter.

Implements the VideoProvider interface defined in video_router.py.

Supported tasks:
  - text_to_video  (models: ray-2, ray-flash-2)
  - image_to_video (models: ray-2, ray-flash-2 with keyframes)

Quality tiers:
  - FAST_PREVIEW:   ray-flash-2 @ 720p (cheapest, fastest)
  - STUDIO_HD:      ray-2 @ 720p (balanced)
  - PRO_FULL_HD:    ray-2 @ 1080p (highest quality)

Luma API docs: https://docs.lumalabs.ai/docs/video-generation
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from backend.services.luma_service import (
    LumaAuthError,
    LumaConfigError,
    LumaError,
    LumaQuotaError,
    check_luma_configured,
    luma_create_generation,
    luma_download,
    luma_get_generation,
    normalize_luma_status,
)


# ── Aspect-ratio mapping ─────────────────────────────────────
# Luma accepts friendly ratios directly
_RATIO_MAP: Dict[str, str] = {
    "16:9": "16:9",
    "9:16": "9:16",
    "1:1": "1:1",
    "4:3": "4:3",
    "3:4": "3:4",
    "21:9": "21:9",
    "9:21": "9:21",
    # Pixel-based fallbacks
    "1280:720": "16:9",
    "720:1280": "9:16",
    "1920:1080": "16:9",
    "1080:1920": "9:16",
}

# Quality tier to model mapping
QUALITY_TIER_MAP: Dict[str, Dict[str, Any]] = {
    "fast_preview": {
        "model": "ray-flash-2",
        "resolution": "720p",
        "display_name": "Fast Preview",
    },
    "studio_hd": {
        "model": "ray-2",
        "resolution": "720p",
        "display_name": "Studio HD",
    },
    "pro_full_hd": {
        "model": "ray-2",
        "resolution": "1080p",
        "display_name": "Pro Full HD",
    },
}

# Default model settings
DEFAULT_MODEL = "ray-2"
DEFAULT_RESOLUTION = "720p"

# Valid durations: Luma natively supports 5s and 10s
# We map UI durations (4s, 6s, 8s) to nearest Luma duration
_DURATION_MAP = {
    4: 5,   # 4s -> 5s
    5: 5,
    6: 5,   # 6s -> 5s (closer to 5 than 10)
    7: 10,  # 7s -> 10s (closer to 10)
    8: 10,  # 8s -> 10s
    10: 10,
}


# ── Helpers ──────────────────────────────────────────────────
def _map_ratio(ratio: str) -> str:
    """Convert UI ratio to Luma-accepted ratio."""
    mapped = _RATIO_MAP.get(ratio, ratio)
    # Validate - Luma supports these ratios
    valid_ratios = {"16:9", "9:16", "1:1", "4:3", "3:4", "21:9", "9:21"}
    if mapped not in valid_ratios:
        return "16:9"  # Default to landscape
    return mapped


def _map_duration(seconds: int) -> int:
    """Map UI duration to Luma-supported duration (5 or 10 seconds)."""
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        seconds = 6

    return _DURATION_MAP.get(seconds, 5)


def _get_model_settings(quality_tier: str, resolution: str) -> Tuple[str, str]:
    """
    Get model and resolution based on quality tier.

    Args:
        quality_tier: "fast_preview", "studio_hd", "pro_full_hd"
        resolution: Fallback resolution if tier not specified

    Returns:
        (model_name, resolution)
    """
    tier_key = quality_tier.lower().replace(" ", "_").replace("-", "_") if quality_tier else ""

    if tier_key in QUALITY_TIER_MAP:
        settings = QUALITY_TIER_MAP[tier_key]
        return settings["model"], settings["resolution"]

    # Fallback: infer from resolution
    if resolution == "1080p":
        return "ray-2", "1080p"
    elif resolution == "720p":
        return "ray-2", "720p"

    return DEFAULT_MODEL, DEFAULT_RESOLUTION


# ── Provider class ───────────────────────────────────────────
class LumaProvider:
    """
    Luma Dream Machine video generation provider for the VideoRouter.

    Conforms to the VideoProvider interface (start_text_to_video,
    start_image_to_video, check_status, download_video, extract_thumbnail).
    """

    name = "luma"

    def is_configured(self) -> Tuple[bool, Optional[str]]:
        return check_luma_configured()

    # ── submit ───────────────────────────────────────────────
    def start_text_to_video(self, prompt: str, **params) -> Dict[str, Any]:
        """
        Submit a text-to-video task to Luma.

        Params:
            prompt: Text description
            aspect_ratio: "16:9", "9:16", "1:1", etc.
            duration_seconds: 4, 6, 8 (mapped to Luma's 5 or 10)
            quality_tier: "fast_preview", "studio_hd", "pro_full_hd"
            resolution: Fallback if quality_tier not specified
            loop: Whether video should loop

        Returns:
            {"generation_id": "<uuid>"}  — used as upstream identifier for polling.
        """
        ratio = _map_ratio(params.get("aspect_ratio", "16:9"))
        duration = _map_duration(params.get("duration_seconds", 6))
        quality_tier = params.get("quality_tier", "")
        resolution = params.get("resolution", "720p")
        loop = params.get("loop", False)

        model, final_resolution = _get_model_settings(quality_tier, resolution)

        try:
            resp = luma_create_generation(
                prompt=prompt,
                model=model,
                aspect_ratio=ratio,
                duration_seconds=duration,
                loop=loop,
                resolution=final_resolution,
            )
        except LumaQuotaError:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, "Luma rate limit / quota exhausted")
        except LumaAuthError as e:
            raise RuntimeError(f"luma_auth_failed: {e.message}") from e
        except LumaConfigError as e:
            raise RuntimeError(f"luma_not_configured: {e.message}") from e

        generation_id = resp.get("id")
        if not generation_id:
            raise RuntimeError(f"luma_submit_failed: No generation id in response: {resp}")

        print(f"[Luma] text_to_video submitted -> generation_id={generation_id}, model={model}, resolution={final_resolution}")
        return {"generation_id": generation_id, "task_id": generation_id}

    def start_image_to_video(self, image_data: str, prompt: str, **params) -> Dict[str, Any]:
        """
        Submit an image-to-video task to Luma.

        ``image_data`` must be an HTTPS URL or data URI.

        Returns:
            {"generation_id": "<uuid>"}
        """
        ratio = _map_ratio(params.get("aspect_ratio", "16:9"))
        duration = _map_duration(params.get("duration_seconds", 6))
        quality_tier = params.get("quality_tier", "")
        resolution = params.get("resolution", "720p")
        loop = params.get("loop", False)

        model, final_resolution = _get_model_settings(quality_tier, resolution)

        # Build keyframes for image-to-video
        # Luma expects: { "frame0": { "type": "image", "url": "..." } }
        keyframes = {
            "frame0": {
                "type": "image",
                "url": image_data,
            }
        }

        try:
            resp = luma_create_generation(
                prompt=prompt if prompt else "Animate this image with natural, smooth motion",
                model=model,
                aspect_ratio=ratio,
                duration_seconds=duration,
                loop=loop,
                keyframes=keyframes,
                resolution=final_resolution,
            )
        except LumaQuotaError:
            from backend.services.video_router import QuotaExhaustedError
            raise QuotaExhaustedError(self.name, "Luma rate limit / quota exhausted")
        except LumaAuthError as e:
            raise RuntimeError(f"luma_auth_failed: {e.message}") from e
        except LumaConfigError as e:
            raise RuntimeError(f"luma_not_configured: {e.message}") from e

        generation_id = resp.get("id")
        if not generation_id:
            raise RuntimeError(f"luma_submit_failed: No generation id in response: {resp}")

        print(f"[Luma] image_to_video submitted -> generation_id={generation_id}, model={model}")
        return {"generation_id": generation_id, "task_id": generation_id}

    # ── poll ─────────────────────────────────────────────────
    def check_status(self, generation_id: str) -> Dict[str, Any]:
        """
        Poll Luma generation status.

        Returns a normalized dict matching the format expected by
        the async_dispatch polling loop:
            status:    "processing" | "done" | "failed" | "error"
            progress:  int (0-100, estimated)
            video_url: str (on done — video asset URL)
            error:     str (on failed)
            message:   str (human-readable)
        """
        try:
            generation = luma_get_generation(generation_id)
            return normalize_luma_status(generation)
        except LumaError as e:
            if e.retryable:
                return {"status": "error", "error": "luma_server_error", "message": e.message}
            return {"status": "failed", "error": "luma_api_error", "message": e.message}

    # ── download ─────────────────────────────────────────────
    def download_video(self, video_url: str) -> Tuple[bytes, str]:
        """Download video from a Luma output URL."""
        return luma_download(video_url)

    def extract_thumbnail(self, video_bytes: bytes, timestamp_sec: float = 1.0) -> Optional[bytes]:
        """Reuse the ffmpeg-based thumbnail extractor from Gemini service."""
        from backend.services.gemini_video_service import extract_video_thumbnail
        return extract_video_thumbnail(video_bytes, timestamp_sec)


# ── Luma Credit Cost Calculation ─────────────────────────────
# Based on Luma's published credit system:
# - Ray2 Flash 720p: 55 credits for 5s, 110 for 10s (11 credits/sec)
# - Ray2 720p: 160 credits for 5s, 320 for 10s (32 credits/sec)
# - Ray2 1080p: 170 credits for 5s, 340 for 10s (34 credits/sec)
#
# TimrX pricing formula:
#   user_cost = ceil(luma_cost(duration, tier) * margin_multiplier) + platform_buffer
#   margin_multiplier = 2.5
#   platform_buffer = +5 credits

LUMA_CREDITS_PER_SECOND = {
    "fast_preview": 11,    # ray-flash-2 @ 720p
    "studio_hd": 32,       # ray-2 @ 720p
    "pro_full_hd": 34,     # ray-2 @ 1080p
}

MARGIN_MULTIPLIER = 2.5
PLATFORM_BUFFER = 5


def calculate_luma_user_cost(duration_seconds: int, quality_tier: str) -> int:
    """
    Calculate TimrX credit cost for a Luma video generation.

    Args:
        duration_seconds: 4, 6, or 8 (UI duration)
        quality_tier: "fast_preview", "studio_hd", "pro_full_hd"

    Returns:
        TimrX credits to charge the user
    """
    tier_key = quality_tier.lower().replace(" ", "_").replace("-", "_") if quality_tier else "studio_hd"

    credits_per_sec = LUMA_CREDITS_PER_SECOND.get(tier_key, LUMA_CREDITS_PER_SECOND["studio_hd"])

    # Calculate raw Luma cost
    import math
    raw_cost = credits_per_sec * duration_seconds

    # Apply margin and buffer
    user_cost = math.ceil(raw_cost * MARGIN_MULTIPLIER) + PLATFORM_BUFFER

    return user_cost


# Pre-computed cost table for quick lookup
LUMA_COST_TABLE = {
    ("fast_preview", 4): 115,   # ceil(11*4*2.5)+5 = ceil(110)+5 = 115
    ("fast_preview", 6): 170,   # ceil(11*6*2.5)+5 = ceil(165)+5 = 170
    ("fast_preview", 8): 225,   # ceil(11*8*2.5)+5 = ceil(220)+5 = 225
    ("studio_hd", 4): 325,      # ceil(32*4*2.5)+5 = ceil(320)+5 = 325
    ("studio_hd", 6): 485,      # ceil(32*6*2.5)+5 = ceil(480)+5 = 485
    ("studio_hd", 8): 645,      # ceil(32*8*2.5)+5 = ceil(640)+5 = 645
    ("pro_full_hd", 4): 345,    # ceil(34*4*2.5)+5 = ceil(340)+5 = 345
    ("pro_full_hd", 6): 515,    # ceil(34*6*2.5)+5 = ceil(510)+5 = 515
    ("pro_full_hd", 8): 685,    # ceil(34*8*2.5)+5 = ceil(680)+5 = 685
}


def get_luma_credit_cost(duration_seconds: int, quality_tier: str) -> int:
    """
    Get pre-computed TimrX credit cost for Luma generation.

    Args:
        duration_seconds: 4, 6, or 8
        quality_tier: "fast_preview", "studio_hd", "pro_full_hd"

    Returns:
        TimrX credits to charge
    """
    tier_key = quality_tier.lower().replace(" ", "_").replace("-", "_") if quality_tier else "studio_hd"

    # Normalize duration
    if duration_seconds <= 4:
        duration = 4
    elif duration_seconds <= 6:
        duration = 6
    else:
        duration = 8

    return LUMA_COST_TABLE.get((tier_key, duration), calculate_luma_user_cost(duration, tier_key))
