"""
Pricing Service - Manages plans and action costs.

Responsibilities:
- Get available purchase plans
- Get action costs for credit checks
- Validate plan codes
- Normalize action keys to canonical form

CANONICAL ACTION KEYS (use these in new code):
- image_generate          (4c)  - OpenAI standard image (1K)
- image_generate_2k       (8c)  - OpenAI 2K image
- gemini_image_generate   (4c)  - Gemini standard image (1K)
- gemini_image_generate_2k(8c)  - Gemini 2K image
- piapi_image_generate    (7c)  - Nano Banana standard image (1K) — PREMIUM
- piapi_image_generate_2k (12c) - Nano Banana 2K image — PREMIUM
- piapi_image_generate_4k (18c) - Nano Banana 4K image — PREMIUM EXCLUSIVE
- text_to_3d_generate  (20c) - Text to 3D preview generation
- image_to_3d_generate (30c) - Image to 3D conversion
- refine               (6c)  - Refine/upscale 3D model
- remesh               (6c)  - Remesh 3D model (same cost as refine)
- retexture            (5c)  - Apply new texture to 3D model
- rigging              (5c)  - Rig a 3D model for animation
- animation            (3c)  - Animate a rigged 3D model
- video_generate       (96c) - Generic video generation (Vertex 8s 720p base)
- video_text_generate  (96c) - Text-to-video generation
- video_image_animate  (96c) - Image-to-video (equalized with text-to-video)

LEGACY ALIASES (backwards compatibility only):
- preview, text-to-3d, text-to-3d-preview -> text_to_3d_generate
- image-to-3d -> image_to_3d_generate
- text-to-3d-refine, upscale -> refine
- texture -> retexture
- image_studio_generate, openai-image, text-to-image -> image_generate
- video, video-generate -> video_generate
- text2video, video-text-generate -> video_text_generate
- image2video, video-image-animate -> video_image_animate
"""

from typing import Optional, Dict, Any, List

from backend.db import query_one, query_all, execute, Tables


# ─────────────────────────────────────────────────────────────────────────────
# CANONICAL ACTION KEYS - Single source of truth
# ─────────────────────────────────────────────────────────────────────────────

# Canonical action keys (use these in all new code)
class CanonicalActions:
    """Canonical action key constants."""
    # Image generation — OpenAI (standard tier, no 4K)
    IMAGE_GENERATE = "image_generate"                      # OpenAI standard 1K (4c)
    IMAGE_GENERATE_2K = "image_generate_2k"                # OpenAI 2K (8c)
    # Image generation — Gemini (same price as OpenAI)
    GEMINI_IMAGE_GENERATE = "gemini_image_generate"        # Gemini standard 1K (4c)
    GEMINI_IMAGE_GENERATE_2K = "gemini_image_generate_2k"  # Gemini 2K (8c)
    # Image generation — Nano Banana PREMIUM
    PIAPI_IMAGE_GENERATE = "piapi_image_generate"          # Nano Banana standard 1K (7c)
    PIAPI_IMAGE_GENERATE_2K = "piapi_image_generate_2k"    # Nano Banana 2K (12c)
    PIAPI_IMAGE_GENERATE_4K = "piapi_image_generate_4k"    # Nano Banana 4K (18c) — EXCLUSIVE
    # 3D generation
    TEXT_TO_3D_GENERATE = "text_to_3d_generate"
    IMAGE_TO_3D_GENERATE = "image_to_3d_generate"
    REFINE = "refine"
    REMESH = "remesh"
    RETEXTURE = "retexture"
    # Video generation (uses general credits under unified model)
    VIDEO_GENERATE = "video_generate"
    VIDEO_TEXT_GENERATE = "video_text_generate"
    VIDEO_IMAGE_ANIMATE = "video_image_animate"
    RIGGING = "rigging"
    ANIMATION = "animation"
    GEMINI_VIDEO = "gemini_video"  # Legacy alias for Vertex video — retained for DB compat


# Canonical key -> DB action code mapping
CANONICAL_TO_DB = {
    # Image generation — OpenAI (4c / 8c — no 4K)
    CanonicalActions.IMAGE_GENERATE: "OPENAI_IMAGE",
    CanonicalActions.IMAGE_GENERATE_2K: "OPENAI_IMAGE_2K",
    # Image generation — Gemini (4c / 8c — no 4K)
    CanonicalActions.GEMINI_IMAGE_GENERATE: "GEMINI_IMAGE",
    CanonicalActions.GEMINI_IMAGE_GENERATE_2K: "GEMINI_IMAGE_2K",
    # Image generation — Nano Banana PREMIUM (15c / 20c / 30c)
    CanonicalActions.PIAPI_IMAGE_GENERATE: "PIAPI_IMAGE",
    CanonicalActions.PIAPI_IMAGE_GENERATE_2K: "PIAPI_IMAGE_2K",
    CanonicalActions.PIAPI_IMAGE_GENERATE_4K: "PIAPI_IMAGE_4K",
    # 3D generation
    CanonicalActions.TEXT_TO_3D_GENERATE: "MESHY_TEXT_TO_3D",
    CanonicalActions.IMAGE_TO_3D_GENERATE: "MESHY_IMAGE_TO_3D",
    CanonicalActions.REFINE: "MESHY_REFINE",
    CanonicalActions.REMESH: "MESHY_REFINE",  # Remesh uses same cost as refine
    CanonicalActions.RETEXTURE: "MESHY_RETEXTURE",
    CanonicalActions.RIGGING: "MESHY_RIGGING",
    CanonicalActions.ANIMATION: "MESHY_ANIMATION",
    # Video generation
    CanonicalActions.VIDEO_GENERATE: "VIDEO_GENERATE",
    CanonicalActions.VIDEO_TEXT_GENERATE: "VIDEO_TEXT_GENERATE",
    CanonicalActions.VIDEO_IMAGE_ANIMATE: "VIDEO_IMAGE_ANIMATE",
    CanonicalActions.GEMINI_VIDEO: "GEMINI_VIDEO",
}

# Alias -> Canonical key mapping (for backwards compatibility)
# All variations map to canonical keys
ALIAS_TO_CANONICAL = {
    # Image generation aliases (standard resolution)
    "image_studio_generate": CanonicalActions.IMAGE_GENERATE,
    "openai-image": CanonicalActions.IMAGE_GENERATE,
    "text-to-image": CanonicalActions.IMAGE_GENERATE,
    "image-studio": CanonicalActions.IMAGE_GENERATE,
    "nano-image": CanonicalActions.IMAGE_GENERATE,
    # Tiered image aliases
    "image-2k": CanonicalActions.IMAGE_GENERATE_2K,
    "image-4k": CanonicalActions.PIAPI_IMAGE_GENERATE_4K,  # 4K is Nano Banana exclusive

    # Text-to-3D aliases
    "preview": CanonicalActions.TEXT_TO_3D_GENERATE,
    "text-to-3d": CanonicalActions.TEXT_TO_3D_GENERATE,
    "text-to-3d-preview": CanonicalActions.TEXT_TO_3D_GENERATE,

    # Image-to-3D aliases
    "image-to-3d": CanonicalActions.IMAGE_TO_3D_GENERATE,

    # Refine aliases
    "text-to-3d-refine": CanonicalActions.REFINE,
    "upscale": CanonicalActions.REFINE,

    # Remesh aliases
    "text-to-3d-remesh": CanonicalActions.REMESH,

    # Retexture aliases
    "texture": CanonicalActions.RETEXTURE,

    # Rigging / Animation aliases
    "rig": CanonicalActions.RIGGING,
    "animate": CanonicalActions.ANIMATION,

    # Video aliases
    "video": CanonicalActions.VIDEO_GENERATE,
    "video-generate": CanonicalActions.VIDEO_GENERATE,
    "text2video": CanonicalActions.VIDEO_TEXT_GENERATE,
    "video-text-generate": CanonicalActions.VIDEO_TEXT_GENERATE,
    "image2video": CanonicalActions.VIDEO_IMAGE_ANIMATE,
    "video-image-animate": CanonicalActions.VIDEO_IMAGE_ANIMATE,
}


def normalize_action_key(action_key: str) -> str:
    """
    Normalize any action key to its canonical form.

    Args:
        action_key: Any action key (canonical, alias, or legacy)

    Returns:
        Canonical action key

    Example:
        normalize_action_key("openai-image") -> "image_generate"
        normalize_action_key("image_generate") -> "image_generate"
        normalize_action_key("text-to-3d-preview") -> "text_to_3d_generate"
        normalize_action_key("video_image_animate_8s_1080p") -> "video_image_animate_8s_1080p"
    """
    # Already canonical?
    if action_key in CANONICAL_TO_DB:
        return action_key

    # Check alias mapping
    if action_key in ALIAS_TO_CANONICAL:
        return ALIAS_TO_CANONICAL[action_key]

    # Normalize hyphens to underscores and try again
    normalized = action_key.replace("-", "_").lower()
    if normalized in CANONICAL_TO_DB:
        return normalized
    if normalized in ALIAS_TO_CANONICAL:
        return ALIAS_TO_CANONICAL[normalized]

    # Video variant codes are canonical by design
    # Veo: video_text_generate_4s_720p  |  Seedance: seedance_fast_text_generate_5s
    # fal Seedance: fal_seedance_text_generate_5s
    if _is_video_variant_code(normalized) or _is_seedance_variant_code(normalized) or _is_fal_seedance_variant_code(normalized):
        return normalized

    # Unknown - log warning and return as-is
    print(f"[PRICING] WARNING: Unknown action key '{action_key}', cannot normalize")
    return action_key


def _is_video_variant_code(action_key: str) -> bool:
    """
    Check if an action key is a Veo video variant code.

    Pattern: video_{text_generate|image_animate|image_transition}_{duration}s_{resolution}
    """
    if not action_key.startswith("video_"):
        return False

    valid_prefixes = ("video_text_generate_", "video_image_animate_", "video_image_transition_")
    for prefix in valid_prefixes:
        if action_key.startswith(prefix):
            suffix = action_key[len(prefix):]
            valid_suffixes = {"4s_720p", "6s_720p", "8s_720p", "8s_1080p", "8s_4k"}
            return suffix in valid_suffixes

    return False


def _is_seedance_variant_code(action_key: str) -> bool:
    """
    Check if an action key is a Seedance variant code.

    Pattern: seedance_{fast|preview}_{text_generate|image_animate}_{duration}s
    """
    if not action_key.startswith("seedance_"):
        return False

    valid_prefixes = (
        "seedance_fast_text_generate_",
        "seedance_fast_image_animate_",
        "seedance_preview_text_generate_",
        "seedance_preview_image_animate_",
    )
    for prefix in valid_prefixes:
        if action_key.startswith(prefix):
            suffix = action_key[len(prefix):]
            return suffix in {"5s", "10s", "15s"}

    return False


def _is_fal_seedance_variant_code(action_key: str) -> bool:
    """
    Check if an action key is a fal Seedance variant code.

    Pattern: fal_seedance_{text_generate|image_animate}_{duration}s
    """
    if not action_key.startswith("fal_seedance_"):
        return False

    valid_prefixes = (
        "fal_seedance_text_generate_",
        "fal_seedance_image_animate_",
        "fal_seedance_image_transition_",
    )
    for prefix in valid_prefixes:
        if action_key.startswith(prefix):
            suffix = action_key[len(prefix):]
            return suffix in {"5s", "10s", "12s"}

    return False


def get_db_action_code_from_canonical(canonical_key: str) -> Optional[str]:
    """
    Get DB action code from canonical key.

    Args:
        canonical_key: A canonical action key

    Returns:
        DB action code (e.g., "OPENAI_IMAGE") or None if not found
    """
    return CANONICAL_TO_DB.get(canonical_key)


# Default action costs to seed into the database
DEFAULT_ACTION_COSTS = [
    # ── 3D Generation (target ~3–5x markup) ──
    {"action_code": "MESHY_TEXT_TO_3D", "cost_credits": 20, "provider": "meshy"},
    {"action_code": "MESHY_IMAGE_TO_3D", "cost_credits": 30, "provider": "meshy"},
    {"action_code": "MESHY_REFINE", "cost_credits": 6, "provider": "meshy"},
    {"action_code": "MESHY_RETEXTURE", "cost_credits": 5, "provider": "meshy"},
    {"action_code": "MESHY_RIGGING", "cost_credits": 5, "provider": "meshy"},
    {"action_code": "MESHY_ANIMATION", "cost_credits": 3, "provider": "meshy"},
    # ── Image Generation — OpenAI (4c / 8c / 12c) ──
    {"action_code": "OPENAI_IMAGE", "cost_credits": 4, "provider": "openai"},        # Standard 1K
    {"action_code": "OPENAI_IMAGE_2K", "cost_credits": 8, "provider": "openai"},     # 2K
    # OPENAI_IMAGE_4K removed — OpenAI does not support 4K in this product
    # ── Image Generation — Google Imagen (4c / 8c / 12c — same as OpenAI) ──
    {"action_code": "GEMINI_IMAGE", "cost_credits": 4, "provider": "google"},        # Standard 1K
    {"action_code": "GEMINI_IMAGE_2K", "cost_credits": 8, "provider": "google"},     # 2K
    # GEMINI_IMAGE_4K removed — Gemini does not support 4K in this product
    # ── Image Generation — PiAPI Nano Banana PREMIUM (7c / 12c / 18c) ──
    {"action_code": "PIAPI_IMAGE", "cost_credits": 7, "provider": "nano_banana"},        # Standard 1K
    {"action_code": "PIAPI_IMAGE_2K", "cost_credits": 12, "provider": "nano_banana"},    # 2K
    {"action_code": "PIAPI_IMAGE_4K", "cost_credits": 18, "provider": "nano_banana"},    # 4K — EXCLUSIVE
    # ── Video — Vertex (Veo 3.1) — 12 credits/sec (margin-stabilized) ──
    # All modes equalized: text→video == image→video == transition (API cost identical)
    # Text-to-Video
    {"action_code": "video_text_generate_4s_720p", "cost_credits": 48, "provider": "vertex"},
    {"action_code": "video_text_generate_6s_720p", "cost_credits": 72, "provider": "vertex"},
    {"action_code": "video_text_generate_8s_720p", "cost_credits": 96, "provider": "vertex"},
    {"action_code": "video_text_generate_8s_1080p", "cost_credits": 120, "provider": "vertex"},
    {"action_code": "video_text_generate_8s_4k", "cost_credits": 156, "provider": "vertex"},
    # Image-to-Video (equalized — same cost as text-to-video)
    {"action_code": "video_image_animate_4s_720p", "cost_credits": 48, "provider": "vertex"},
    {"action_code": "video_image_animate_6s_720p", "cost_credits": 72, "provider": "vertex"},
    {"action_code": "video_image_animate_8s_720p", "cost_credits": 96, "provider": "vertex"},
    {"action_code": "video_image_animate_8s_1080p", "cost_credits": 120, "provider": "vertex"},
    {"action_code": "video_image_animate_8s_4k", "cost_credits": 156, "provider": "vertex"},
    # Image Transition (equalized — same cost as text-to-video)
    {"action_code": "video_image_transition_4s_720p", "cost_credits": 48, "provider": "vertex"},
    {"action_code": "video_image_transition_6s_720p", "cost_credits": 72, "provider": "vertex"},
    {"action_code": "video_image_transition_8s_720p", "cost_credits": 96, "provider": "vertex"},
    {"action_code": "video_image_transition_8s_1080p", "cost_credits": 120, "provider": "vertex"},
    {"action_code": "video_image_transition_8s_4k", "cost_credits": 156, "provider": "vertex"},
    # ── Seedance 2.0 Fast — STANDARD tier (10 credits/sec) ──
    {"action_code": "seedance_fast_text_generate_5s", "cost_credits": 50, "provider": "seedance"},
    {"action_code": "seedance_fast_text_generate_10s", "cost_credits": 100, "provider": "seedance"},
    {"action_code": "seedance_fast_text_generate_15s", "cost_credits": 150, "provider": "seedance"},
    # ── Seedance 2.0 Preview — PREMIUM tier (16 credits/sec) ──
    {"action_code": "seedance_preview_text_generate_5s", "cost_credits": 80, "provider": "seedance"},
    {"action_code": "seedance_preview_text_generate_10s", "cost_credits": 160, "provider": "seedance"},
    {"action_code": "seedance_preview_text_generate_15s", "cost_credits": 240, "provider": "seedance"},
    # ── Seedance 2.0 — Image-to-Video (equalized with text-to-video per tier) ──
    {"action_code": "seedance_fast_image_animate_5s", "cost_credits": 50, "provider": "seedance"},
    {"action_code": "seedance_fast_image_animate_10s", "cost_credits": 100, "provider": "seedance"},
    {"action_code": "seedance_fast_image_animate_15s", "cost_credits": 150, "provider": "seedance"},
    {"action_code": "seedance_preview_image_animate_5s", "cost_credits": 80, "provider": "seedance"},
    {"action_code": "seedance_preview_image_animate_10s", "cost_credits": 160, "provider": "seedance"},
    {"action_code": "seedance_preview_image_animate_15s", "cost_credits": 240, "provider": "seedance"},
    # ── fal Seedance 1.5 Pro — BUDGET tier (8–9 credits/sec) ──
    {"action_code": "fal_seedance_text_generate_5s", "cost_credits": 45, "provider": "fal_seedance"},
    {"action_code": "fal_seedance_text_generate_10s", "cost_credits": 80, "provider": "fal_seedance"},
    {"action_code": "fal_seedance_text_generate_12s", "cost_credits": 95, "provider": "fal_seedance"},
    # ── fal Seedance 1.5 Pro — Image-to-Video (equalized) ──
    {"action_code": "fal_seedance_image_animate_5s", "cost_credits": 45, "provider": "fal_seedance"},
    {"action_code": "fal_seedance_image_animate_10s", "cost_credits": 80, "provider": "fal_seedance"},
    {"action_code": "fal_seedance_image_animate_12s", "cost_credits": 95, "provider": "fal_seedance"},
    # ── fal Seedance 1.5 Pro — Image Transition (equalized) ──
    {"action_code": "fal_seedance_image_transition_5s", "cost_credits": 45, "provider": "fal_seedance"},
    {"action_code": "fal_seedance_image_transition_10s", "cost_credits": 80, "provider": "fal_seedance"},
    {"action_code": "fal_seedance_image_transition_12s", "cost_credits": 95, "provider": "fal_seedance"},
    # ── Legacy fallback codes (backward compat — Vertex 8s 720p base = 96) ──
    {"action_code": "VIDEO_GENERATE", "cost_credits": 96, "provider": "vertex"},
    {"action_code": "VIDEO_TEXT_GENERATE", "cost_credits": 96, "provider": "vertex"},
    {"action_code": "VIDEO_IMAGE_ANIMATE", "cost_credits": 96, "provider": "vertex"},
    {"action_code": "GEMINI_VIDEO", "cost_credits": 96, "provider": "vertex"},
    # Lowercase legacy codes
    {"action_code": "video_generate", "cost_credits": 96, "provider": "vertex"},
    {"action_code": "video_text_generate", "cost_credits": 96, "provider": "vertex"},
    {"action_code": "video_image_animate", "cost_credits": 96, "provider": "vertex"},
]


# ─────────────────────────────────────────────────────────────────────────────
# VIDEO VARIANT COST MAPPING
# ─────────────────────────────────────────────────────────────────────────────

# Video credit costs by resolution and duration (must match frontend)
# Vertex Veo 3.1: 12 credits/sec (margin-stabilized). All modes equalized.
# 4K requires VERTEX_ALLOW_4K env var — backend rejects if not allowlisted.
VIDEO_CREDIT_COSTS = {
    "720p": {4: 48, 6: 72, 8: 96},
    "1080p": {8: 120},
    "4k": {8: 156},
}

# Image-to-Video / Image Transition costs — EQUALIZED with text-to-video
# (API cost is identical across modes for Vertex)
VIDEO_IMAGE_CREDIT_COSTS = {
    "720p": {4: 48, 6: 72, 8: 96},
    "1080p": {8: 120},
    "4k": {8: 156},
}

# Seedance 2.0 credit costs by tier and duration (explicit lookup, DB is authoritative)
# Fast = STANDARD tier (10 credits/sec), Preview = PREMIUM tier (16 credits/sec)
SEEDANCE_CREDIT_COSTS = {
    "fast": {5: 50, 10: 100, 15: 150},
    "preview": {5: 80, 10: 160, 15: 240},
}
# Approximate CPS for fallback only (DB values are authoritative)
SEEDANCE_CREDITS_PER_SEC = {
    "fast": 10,
    "preview": 16,
}
SEEDANCE_VALID_DURATIONS = [5, 10, 15]

# fal Seedance 1.5 Pro credit costs by duration (explicit lookup, DB is authoritative)
# BUDGET tier: 8–9 credits/sec
FAL_SEEDANCE_CREDIT_COSTS = {5: 45, 10: 80, 12: 95}
# Approximate CPS for fallback only (DB values are authoritative)
FAL_SEEDANCE_CREDITS_PER_SEC = 8
FAL_SEEDANCE_VALID_DURATIONS = [5, 10, 12]

# Valid durations per resolution (Vertex/Veo constraints)
VIDEO_VALID_DURATIONS = {
    "720p": [4, 6, 8],
    "1080p": [8],
    "4k": [8],
}


def get_video_action_code(
    task: str,
    duration_seconds: int,
    resolution: str,
    provider: str = "vertex",
    seedance_tier: str = "fast",
) -> str:
    """
    Build the video action code for a specific variant.

    Args:
        task: "text2video", "image2video", or "image_transition"
        duration_seconds: Duration in seconds
        resolution: "720p" or "1080p"
        provider: "vertex" or "seedance"
        seedance_tier: "fast" or "preview" (only for seedance)

    Returns:
        Action code like "video_text_generate_4s_720p" or "seedance_fast_text_generate_5s"
    """
    if task.lower() in ("text2video", "text_to_video", "text"):
        task_part = "text_generate"
    elif task.lower() in ("image_transition",):
        task_part = "image_transition"
    else:
        task_part = "image_animate"

    if provider == "fal_seedance":
        return f"fal_seedance_{task_part}_{duration_seconds}s"

    if provider == "seedance":
        tier = seedance_tier if seedance_tier in ("fast", "preview") else "fast"
        return f"seedance_{tier}_{task_part}_{duration_seconds}s"

    duration_part = f"{duration_seconds}s"
    resolution_part = resolution.lower()
    return f"video_{task_part}_{duration_part}_{resolution_part}"


def get_video_credit_cost(
    duration_seconds: int,
    resolution: str,
    provider: str = "vertex",
    seedance_tier: str = "fast",
    task: str = "text2video",
) -> int:
    """
    Get the credit cost for a video variant.

    Args:
        duration_seconds: Duration in seconds
        resolution: "720p" or "1080p"
        provider: "vertex" or "seedance"
        seedance_tier: "fast" or "preview" (only for seedance)
        task: "text2video", "image2video", or "image_transition"

    Returns:
        Credit cost
    """
    if provider == "fal_seedance":
        duration = int(duration_seconds)
        # Prefer explicit lookup; fall back to CPS approximation
        if duration in FAL_SEEDANCE_CREDIT_COSTS:
            return FAL_SEEDANCE_CREDIT_COSTS[duration]
        return FAL_SEEDANCE_CREDITS_PER_SEC * duration

    if provider == "seedance":
        tier = seedance_tier if seedance_tier in ("fast", "preview") else "fast"
        duration = int(duration_seconds)
        # Prefer explicit lookup; fall back to CPS approximation
        tier_costs = SEEDANCE_CREDIT_COSTS.get(tier, {})
        if duration in tier_costs:
            return tier_costs[duration]
        cps = SEEDANCE_CREDITS_PER_SEC.get(tier, 16)
        return cps * duration

    resolution = resolution.lower()
    duration = int(duration_seconds)
    is_image_task = task.lower() not in ("text2video", "text_to_video", "text")
    cost_table = VIDEO_IMAGE_CREDIT_COSTS if is_image_task else VIDEO_CREDIT_COSTS
    resolution_costs = cost_table.get(resolution, {})
    return resolution_costs.get(duration, 96)


DEFAULT_PLANS = [
    # ── General credit packs (one-time) ──
    {
        "code": "starter_250",
        "name": "Starter",
        "description": "Perfect for exploring AI-powered 3D creation.",
        "price_gbp": 7.99,
        "credit_grant": 350,          # Pricing refactor Mar 2026 (was 250)
        "includes_priority": False,
    },
    {
        "code": "creator_900",
        "name": "Creator",
        "description": "For serious creators building their portfolio.",
        "price_gbp": 19.99,
        "credit_grant": 1100,         # Pricing refactor Mar 2026 (was 900)
        "includes_priority": False,
    },
    {
        "code": "studio_2200",
        "name": "Studio",
        "description": "Maximum value for professional workflows.",
        "price_gbp": 37.99,
        "credit_grant": 2400,         # Margin stabilization Mar 2026 (was 2600)
        "includes_priority": True,
    },
    # ── Video credit packs (one-time, separate pool) — Pricing refactor Mar 2026 ──
    {
        "code": "video_starter_300",
        "name": "Video Starter",
        "description": "Get started with AI video generation.",
        "price_gbp": 9.99,
        "credit_grant": 550,          # Pricing refactor Mar 2026 (was 300)
        "includes_priority": False,
    },
    {
        "code": "video_creator_900",
        "name": "Video Creator",
        "description": "Consistent workflow for content creators.",
        "price_gbp": 29.99,
        "credit_grant": 1800,         # Pricing refactor Mar 2026 (was 900)
        "includes_priority": False,
    },
    {
        "code": "video_studio_2000",
        "name": "Video Studio",
        "description": "High-volume production. Priority queue.",
        "price_gbp": 59.99,
        "credit_grant": 4000,         # Pricing refactor Mar 2026 (was 2000)
        "includes_priority": True,
    },
]


class PricingService:
    """Service for managing pricing plans and action costs."""

    # Cache for action costs (refreshed on startup or manually)
    _action_costs_cache: Dict[str, int] = {}

    # DB action code -> canonical key mapping (reverse of CANONICAL_TO_DB)
    DB_TO_CANONICAL = {v: k for k, v in CANONICAL_TO_DB.items()}

    # Legacy mapping for backwards compatibility (deprecated - use normalize_action_key)
    ACTION_CODE_MAP = {
        "MESHY_TEXT_TO_3D": CanonicalActions.TEXT_TO_3D_GENERATE,
        "MESHY_IMAGE_TO_3D": CanonicalActions.IMAGE_TO_3D_GENERATE,
        "MESHY_REFINE": CanonicalActions.REFINE,
        "MESHY_RETEXTURE": CanonicalActions.RETEXTURE,
        "OPENAI_IMAGE": CanonicalActions.IMAGE_GENERATE,
        "VIDEO_GENERATE": CanonicalActions.VIDEO_GENERATE,
        "VIDEO_TEXT_GENERATE": CanonicalActions.VIDEO_TEXT_GENERATE,
        "VIDEO_IMAGE_ANIMATE": CanonicalActions.VIDEO_IMAGE_ANIMATE,
    }

    # Legacy aliases (deprecated - use ALIAS_TO_CANONICAL)
    FRONTEND_ALIASES = {alias: CANONICAL_TO_DB.get(canonical, canonical)
                        for alias, canonical in ALIAS_TO_CANONICAL.items()}

    @staticmethod
    def get_plans(active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get available credit plans.
        Returns list of plans with code, name, price, credits.
        """
        if active_only:
            plans = query_all(
                f"""
                SELECT id, code, name, description, price_gbp, currency,
                       credit_grant, includes_priority, meta, created_at
                FROM {Tables.PLANS}
                WHERE is_active = TRUE
                ORDER BY price_gbp ASC
                """
            )
        else:
            plans = query_all(
                f"""
                SELECT id, code, name, description, price_gbp, currency,
                       credit_grant, includes_priority, is_active, meta, created_at
                FROM {Tables.PLANS}
                ORDER BY price_gbp ASC
                """
            )

        # Format for frontend
        return [
            {
                "id": str(plan["id"]),
                "code": plan["code"],
                "name": plan["name"],
                "description": plan.get("description"),
                "price": float(plan["price_gbp"]),
                "currency": plan.get("currency", "GBP"),
                "credits": plan["credit_grant"],
                "includes_priority": plan.get("includes_priority", False),
            }
            for plan in plans
        ]

    @staticmethod
    def get_plan_by_code(code: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific plan by its code.
        Returns None if not found or inactive.
        """
        plan = query_one(
            f"""
            SELECT id, code, name, description, price_gbp, currency,
                   credit_grant, includes_priority, meta, created_at
            FROM {Tables.PLANS}
            WHERE code = %s AND is_active = TRUE
            """,
            (code,),
        )

        if not plan:
            return None

        return {
            "id": str(plan["id"]),
            "code": plan["code"],
            "name": plan["name"],
            "description": plan.get("description"),
            "price": float(plan["price_gbp"]),
            "currency": plan.get("currency", "GBP"),
            "credits": plan["credit_grant"],
            "includes_priority": plan.get("includes_priority", False),
        }

    @staticmethod
    def get_plans_with_perks(active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get available credit plans with perks object.
        Returns list of plans in frontend-friendly format.

        Response format:
        [
            {
                "id": "uuid",
                "code": "starter_250",
                "name": "Starter",
                "price_gbp": 7.99,
                "credits": 250,
                "perks": {
                    "priority": false,
                    "retention_days": 30
                }
            },
            ...
        ]
        """
        if active_only:
            plans = query_all(
                f"""
                SELECT id, code, name, description, price_gbp, currency,
                       credit_grant, includes_priority, meta, created_at
                FROM {Tables.PLANS}
                WHERE is_active = TRUE
                ORDER BY price_gbp ASC
                """
            )
        else:
            plans = query_all(
                f"""
                SELECT id, code, name, description, price_gbp, currency,
                       credit_grant, includes_priority, is_active, meta, created_at
                FROM {Tables.PLANS}
                ORDER BY price_gbp ASC
                """
            )

        # Default retention days (can be overridden via meta)
        DEFAULT_RETENTION_DAYS = 30

        # Format for frontend with perks
        return [
            {
                "id": str(plan["id"]),
                "code": plan["code"],
                "name": plan["name"],
                "price_gbp": float(plan["price_gbp"]),
                "credits": plan["credit_grant"],
                "perks": {
                    "priority": plan.get("includes_priority", False),
                    "retention_days": (plan.get("meta") or {}).get("retention_days", DEFAULT_RETENTION_DAYS),
                },
            }
            for plan in plans
        ]

    @staticmethod
    def get_plans_with_estimates(active_only: bool = True) -> List[Dict[str, Any]]:
        """
        Get available credit plans with estimated outputs based on current action costs.
        Returns plans with credits and example outputs for UI display.

        Response format:
        [
            {
                "id": "uuid",
                "code": "starter_250",
                "name": "Starter",
                "price_gbp": 7.99,
                "credits": 250,
                "perks": {"priority": false, "retention_days": 30},
                "estimates": {
                    "ai_images": 40,      # 400 credits / 10 (OpenAI tier)
                    "text_to_3d": 12,     # credits / 20
                    "image_to_3d": 8      # credits / 30
                }
            },
            ...
        ]
        """
        # Get base plans with perks
        plans = PricingService.get_plans_with_perks(active_only)

        # Get action costs for estimates
        costs = PricingService.get_action_costs()
        image_cost = costs.get("image_generate", 4)           # Standard image (1K, OpenAI tier)
        text_to_3d_cost = costs.get("text_to_3d_generate", 20)
        image_to_3d_cost = costs.get("image_to_3d_generate", 30)

        # Add estimates to each plan
        for plan in plans:
            plan_credits = plan.get("credits", 0)
            plan["estimates"] = {
                "ai_images": plan_credits // image_cost if image_cost > 0 else 0,
                "text_to_3d": plan_credits // text_to_3d_cost if text_to_3d_cost > 0 else 0,
                "image_to_3d": plan_credits // image_to_3d_cost if image_to_3d_cost > 0 else 0,
            }

        return plans

    @staticmethod
    def get_action_costs() -> Dict[str, int]:
        """
        Get all action costs as a dict with canonical keys.
        Returns {canonical_key: cost_credits}.

        Example response:
        {
            "image_generate": 4,
            "text_to_3d_generate": 20,
            "image_to_3d_generate": 30,
            "refine": 6,
            "remesh": 6,
            "retexture": 5,
            "video_generate": 96,
            "video_text_generate": 96,
            "video_image_animate": 96
        }
        """
        # Use cache if available
        if PricingService._action_costs_cache:
            return PricingService._action_costs_cache.copy()

        # Fetch from DB
        rows = query_all(
            f"""
            SELECT action_code, cost_credits
            FROM {Tables.ACTION_COSTS}
            """
        )

        # Build cost lookup by DB code
        db_costs: Dict[str, int] = {}
        for row in rows:
            db_costs[row["action_code"]] = row["cost_credits"]

        # Map DB codes to canonical keys
        result: Dict[str, int] = {}
        for canonical_key, db_code in CANONICAL_TO_DB.items():
            if db_code in db_costs:
                result[canonical_key] = db_costs[db_code]

        # Also include all aliases for backwards compatibility
        for alias, canonical_key in ALIAS_TO_CANONICAL.items():
            if canonical_key in result:
                result[alias] = result[canonical_key]

        # IMPORTANT: Include ALL DB codes directly (for video variants like VIDEO_TEXT_GENERATE_4S_720P)
        # This allows direct lookup by DB action_code without requiring a canonical mapping
        for db_code, cost in db_costs.items():
            # Add DB code as-is (uppercase)
            result[db_code] = cost
            # Also add lowercase version for flexibility
            result[db_code.lower()] = cost

        # Log what we're returning for debugging
        if not db_costs:
            print("[PRICING] WARNING: No action costs found in database!")

        # Cache the result
        PricingService._action_costs_cache = result.copy()

        return result

    @staticmethod
    def get_action_costs_list() -> List[Dict[str, Any]]:
        """
        Get all action costs as a list of {action_key, credits} objects.
        Suitable for frontend caching.

        Response format:
        [
            {"action_key": "text_to_3d_generate", "credits": 20},
            {"action_key": "image_to_3d_generate", "credits": 30},
            ...
        ]
        """
        costs_dict = PricingService.get_action_costs()
        return [
            {"action_key": key, "credits": credits}
            for key, credits in costs_dict.items()
        ]

    @staticmethod
    def get_action_cost(action_key: str) -> int:
        """
        Get cost in credits for a specific action.
        Accepts any action key (canonical, alias, or DB code).

        Args:
            action_key: Any action key

        Returns:
            Cost in credits, or 0 if not found
        """
        costs = PricingService.get_action_costs()

        # Direct lookup (handles canonical keys and cached aliases)
        if action_key in costs:
            return costs[action_key]

        # Try normalizing to canonical
        canonical = normalize_action_key(action_key)
        if canonical in costs:
            return costs[canonical]

        # Try as DB code (e.g., "MESHY_TEXT_TO_3D")
        canonical_from_db = PricingService.DB_TO_CANONICAL.get(action_key)
        if canonical_from_db and canonical_from_db in costs:
            return costs[canonical_from_db]

        # fal Seedance variant fallback: use explicit lookup, then CPS approximation
        # Pattern: fal_seedance_{text_generate|image_animate}_{duration}s
        if _is_fal_seedance_variant_code(canonical):
            suffix = canonical.rsplit("_", 1)[-1]  # e.g. "10s"
            try:
                duration = int(suffix.rstrip("s"))
            except ValueError:
                duration = 5
            if duration in FAL_SEEDANCE_CREDIT_COSTS:
                computed = FAL_SEEDANCE_CREDIT_COSTS[duration]
            else:
                computed = FAL_SEEDANCE_CREDITS_PER_SEC * duration
            print(f"[PRICING] fal Seedance fallback cost: {canonical} -> {computed} credits")
            return computed

        # Seedance variant fallback: use explicit lookup, then CPS approximation
        # Pattern: seedance_{fast|preview}_{text_generate|image_animate}_{duration}s
        if _is_seedance_variant_code(canonical):
            tier = "preview" if canonical.startswith("seedance_preview_") else "fast"
            suffix = canonical.rsplit("_", 1)[-1]  # e.g. "15s"
            try:
                duration = int(suffix.rstrip("s"))
            except ValueError:
                duration = 5
            tier_costs = SEEDANCE_CREDIT_COSTS.get(tier, {})
            if duration in tier_costs:
                computed = tier_costs[duration]
            else:
                cps = SEEDANCE_CREDITS_PER_SEC.get(tier, 16)
                computed = cps * duration
            print(f"[PRICING] Seedance fallback cost: {canonical} -> {computed} credits")
            return computed

        print(f"[PRICING] Warning: Unknown action key '{action_key}', returning 0 cost")
        return 0

    @staticmethod
    def get_db_action_code(action_key: str) -> Optional[str]:
        """
        Convert any action key to DB action_code.

        Args:
            action_key: Any action key (canonical, alias, or legacy)

        Returns:
            DB action code (e.g., "OPENAI_IMAGE") or None if not found

        Example:
            get_db_action_code('image_generate') -> 'OPENAI_IMAGE'
            get_db_action_code('openai-image') -> 'OPENAI_IMAGE'
            get_db_action_code('text-to-3d-preview') -> 'MESHY_TEXT_TO_3D'
        """
        # Normalize to canonical first
        canonical = normalize_action_key(action_key)

        # Get DB code from canonical
        db_code = CANONICAL_TO_DB.get(canonical)
        if db_code:
            return db_code

        # Video/Seedance variant codes ARE their own DB action codes
        if _is_video_variant_code(canonical) or _is_seedance_variant_code(canonical) or _is_fal_seedance_variant_code(canonical):
            return canonical

        # Already a DB code?
        if action_key in PricingService.DB_TO_CANONICAL:
            return action_key

        return None

    @staticmethod
    def normalize_and_get_cost(action_key: str) -> tuple[str, str, int]:
        """
        Normalize action key and get its cost.
        Returns (canonical_key, db_code, cost).

        Useful for logging both requested and canonical action codes.

        Example:
            normalize_and_get_cost('openai-image') -> ('image_generate', 'OPENAI_IMAGE', 10)  # cost from DB
        """
        canonical = normalize_action_key(action_key)
        db_code = CANONICAL_TO_DB.get(canonical, "UNKNOWN")
        cost = PricingService.get_action_cost(canonical)
        return (canonical, db_code, cost)

    @staticmethod
    def refresh_costs_cache() -> None:
        """
        Refresh the action costs cache from database.
        Called on startup and can be called to refresh.
        """
        PricingService._action_costs_cache = {}
        PricingService.get_action_costs()  # This repopulates the cache
        # print(f"[PRICING] Refreshed action costs cache: {PricingService._action_costs_cache}")

    @staticmethod
    def map_job_type_to_action(job_type: str) -> str:
        """
        Map any job type string to DB action code.
        Uses canonical normalization under the hood.

        Args:
            job_type: Any job type string (legacy or current)

        Returns:
            DB action code (e.g., 'MESHY_TEXT_TO_3D' or 'video_image_animate_8s_1080p')

        Example:
            map_job_type_to_action('text-to-3d') -> 'MESHY_TEXT_TO_3D'
            map_job_type_to_action('openai-image') -> 'OPENAI_IMAGE'
            map_job_type_to_action('video_image_animate_8s_1080p') -> 'video_image_animate_8s_1080p'
        """
        if not job_type:
            return "MESHY_TEXT_TO_3D"  # Default

        # Normalize the action key
        canonical = normalize_action_key(job_type)

        # Video variant codes ARE the DB action codes (lowercase canonical format)
        # e.g., video_text_generate_4s_720p, seedance_fast_text_generate_5s
        if _is_video_variant_code(canonical) or _is_seedance_variant_code(canonical) or _is_fal_seedance_variant_code(canonical):
            return canonical

        # For other actions, look up the DB code mapping
        db_code = CANONICAL_TO_DB.get(canonical)
        if db_code:
            return db_code

        # Fallback for unknown types
        print(f"[PRICING] Warning: Unknown job type '{job_type}', defaulting to MESHY_TEXT_TO_3D")
        return "MESHY_TEXT_TO_3D"

    @staticmethod
    def seed_plans() -> int:
        """
        Seed the default plans into the database.
        Uses INSERT ... ON CONFLICT DO UPDATE to ensure plans exist and are active.
        Safe to call multiple times (idempotent).

        Plans use the credit_grant column to store the number of credits to grant.

        Returns:
            Number of plans seeded/updated
        """
        from backend.db import is_available

        # print("[PRICING] Starting plan seed...")

        if not is_available():
            # print("[PRICING] Database not available, skipping plan seed")
            return 0

        # First verify the table exists
        try:
            result = query_one(
                f"SELECT COUNT(*) as cnt FROM {Tables.PLANS}"
            )
            existing_count = result["cnt"] if result else 0
            # print(f"[PRICING] Plans table exists, current count: {existing_count}")
        except Exception as e:
            print(f"[PRICING] Plans table check failed: {e}")
            print("[PRICING] Table may not exist - ensure migrations have run")
            return 0

        seeded = 0
        for plan in DEFAULT_PLANS:
            try:
                execute(
                    f"""
                    INSERT INTO {Tables.PLANS}
                        (code, name, description, price_gbp, currency, credit_grant, includes_priority, is_active, created_at)
                    VALUES
                        (%s, %s, %s, %s, 'GBP', %s, %s, TRUE, NOW())
                    ON CONFLICT (code) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        price_gbp = EXCLUDED.price_gbp,
                        credit_grant = EXCLUDED.credit_grant,
                        includes_priority = EXCLUDED.includes_priority,
                        is_active = TRUE
                    """,
                    (
                        plan["code"],
                        plan["name"],
                        plan["description"],
                        plan["price_gbp"],
                        plan["credit_grant"],
                        plan["includes_priority"],
                    ),
                )
                seeded += 1
                # print(f"[PRICING] Seeded plan: {plan['code']} ({plan['credit_grant']} credits @ £{plan['price_gbp']})")
            except Exception as e:
                print(f"[PRICING] Error seeding plan {plan['code']}: {e}")
                import traceback
                traceback.print_exc()

        # print(f"[PRICING] Plans seed complete: {seeded}/{len(DEFAULT_PLANS)}")
        return seeded

    @staticmethod
    def seed_action_costs() -> int:
        """
        Seed default action costs into the database.
        Uses INSERT ... ON CONFLICT DO NOTHING (idempotent).
        Safe to call multiple times — existing rows are never overwritten.

        Returns:
            Number of action costs seeded (new rows only)
        """
        from backend.db import is_available

        # print("[PRICING] Starting action_costs seed...")

        if not is_available():
            # print("[PRICING] Database not available, skipping action_costs seed")
            return 0

        try:
            result = query_one(
                f"SELECT COUNT(*) as cnt FROM {Tables.ACTION_COSTS}"
            )
            existing_count = result["cnt"] if result else 0
            # print(f"[PRICING] action_costs table exists, current count: {existing_count}")
        except Exception as e:
            print(f"[PRICING] action_costs table check failed: {e}")
            return 0

        seeded = 0
        for ac in DEFAULT_ACTION_COSTS:
            try:
                execute(
                    f"""
                    INSERT INTO {Tables.ACTION_COSTS}
                        (action_code, cost_credits, provider, updated_at)
                    VALUES
                        (%s, %s, %s, NOW())
                    ON CONFLICT (action_code) DO NOTHING
                    """,
                    (ac["action_code"], ac["cost_credits"], ac["provider"]),
                )
                seeded += 1
            except Exception as e:
                print(f"[PRICING] Error seeding action_cost {ac['action_code']}: {e}")

        # Clear cache so next lookup fetches fresh data
        PricingService._action_costs_cache = {}

        # print(f"[PRICING] Action costs seed complete: {seeded} checked, {existing_count} pre-existing")
        return seeded
