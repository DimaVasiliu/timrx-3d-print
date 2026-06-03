"""
Unified provider cost estimation registry.

Single source of truth for estimated upstream provider costs (GBP) across
all generation providers: 3D, image, and video.

These are *estimates* used for spend visibility and dashboard metrics.
They are NOT used for billing, credit reservation, or wallet logic.
"""

from __future__ import annotations

import json
from typing import Dict, Optional, Tuple

from backend.db import USE_DB, get_conn, Tables, query_one


# ─────────────────────────────────────────────────────────────────────────────
# NON-VIDEO COSTS — keyed by action_code
# ─────────────────────────────────────────────────────────────────────────────

ACTION_CODE_COST_GBP: Dict[str, float] = {
    # Meshy 3D generation
    "MESHY_TEXT_TO_3D":   0.05,
    "MESHY_IMAGE_TO_3D":  0.06,
    "MESHY_REFINE":       0.03,
    "MESHY_RETEXTURE":    0.02,

    # OpenAI image generation (gpt-image-1)
    "OPENAI_IMAGE":       0.04,
    "OPENAI_IMAGE_2K":    0.08,
    "OPENAI_IMAGE_4K":    0.12,
    # Long-form codes (match action keys stored in jobs table by image_gen.py):
    "IMAGE_GENERATE":     0.04,
    "IMAGE_GENERATE_2K":  0.08,

    # Gemini / Google image generation (Imagen 4.0)
    "GEMINI_IMAGE":       0.035,
    "GEMINI_IMAGE_2K":    0.07,
    "GEMINI_IMAGE_4K":    0.10,
    # Long-form codes (match action keys stored in jobs table by image_gen.py):
    "GEMINI_IMAGE_GENERATE":     0.035,
    "GEMINI_IMAGE_GENERATE_2K":  0.07,

    # PiAPI Nano Banana 2 image generation
    # Source: PiAPI pricing page — $0.06/0.08/0.12 per image (USD)
    # Converted at USD/GBP ≈ 0.80 (conservative, update if rate shifts >5%)
    "PIAPI_IMAGE":              0.048,
    "PIAPI_IMAGE_2K":           0.064,
    "PIAPI_IMAGE_4K":           0.096,
    # Long-form codes (match action keys stored in jobs table by image_gen.py):
    "PIAPI_IMAGE_GENERATE":     0.048,
    "PIAPI_IMAGE_GENERATE_2K":  0.064,
    "PIAPI_IMAGE_GENERATE_4K":  0.096,
    # Direct Google Nano (Gemini 2.5 Flash Image)
    "GOOGLE_NANO_IMAGE":              0.031,
    "GOOGLE_NANO_IMAGE_GENERATE":     0.031,
    # BFL FLUX.2 Pro
    "FLUX_PRO_IMAGE":                 0.044,
    "FLUX_PRO_IMAGE_GENERATE":        0.044,
    # Ideogram V3
    "IDEOGRAM_V3_IMAGE":              0.032,
    "IDEOGRAM_V3_IMAGE_GENERATE":     0.032,
    # Recraft V4 raster / vector
    "RECRAFT_V4_IMAGE":               0.064,
    "RECRAFT_V4_IMAGE_GENERATE":      0.064,
    "RECRAFT_V4_VECTOR":              0.064,
    "RECRAFT_V4_VECTOR_GENERATE":     0.064,
}


# ─────────────────────────────────────────────────────────────────────────────
# VIDEO COSTS — keyed by (provider_variant, duration_seconds)
#
# Migrated from video_limits.py PROVIDER_COST_GBP.  video_limits.py now
# imports from here so there is a single source of truth.
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# PiAPI Seedance 2.0 pricing (USD per second), May 2026:
#   seedance-2-fast       480p $0.08   720p $0.16     (no 1080p)
#   seedance-2            480p $0.10   720p $0.20   1080p $0.50
#   Legacy preview/VIP variants share the same per-second rates at matching res.
# Converted at USD/GBP ≈ 0.80. Update if exchange rate moves >5%.
# ─────────────────────────────────────────────────────────────────────────────

# Per-second GBP rates by (variant, resolution). Used as both an explicit
# lookup and the source for legacy duration-keyed tables.
_SEEDANCE_RATE_GBP: Dict[Tuple[str, str], float] = {
    ("seedance_fast",    "480p"): 0.064,  # $0.08 × 0.80
    ("seedance_fast",    "720p"): 0.128,  # $0.16 × 0.80
    ("seedance_quality", "480p"): 0.080,  # $0.10 × 0.80
    ("seedance_quality", "720p"): 0.160,  # $0.20 × 0.80
    ("seedance_quality", "1080p"): 0.400, # $0.50 × 0.80
}

def estimate_seedance_provider_cost(
    tier: str,
    duration_seconds: int,
    resolution: str = "480p",
    input_video_seconds: float = 0.0,
) -> float:
    """Estimate the GBP cost charged by PiAPI for a Seedance job.

    PiAPI's Seedance 2 billing formula:
        cost = unit_price × output_duration
             + (unit_price / 2) × total_input_video_duration

    ``input_video_seconds`` is the summed duration of any reference videos
    (omni_reference / Reference Video mode). It is billed at half the per-second
    rate. Pass 0 (default) for text/image/first-last jobs.
    """
    from backend.services.pricing_service import normalize_seedance_tier  # late import to avoid cycle
    canon_tier = normalize_seedance_tier(tier)
    variant = f"seedance_{canon_tier}"
    res = (resolution or "480p").lower()
    rate = _SEEDANCE_RATE_GBP.get((variant, res))
    if rate is None:
        # Fast 1080p doesn't exist; fall back to 720p rate so reports don't crash.
        rate = _SEEDANCE_RATE_GBP.get((variant, "720p"))
    if rate is None:
        rate = 0.08  # generic Seedance fallback
    base = rate * int(duration_seconds)
    surcharge = (rate / 2.0) * max(0.0, float(input_video_seconds or 0.0))
    return round(base + surcharge, 4)


VIDEO_COST_GBP: Dict[Tuple[str, int], float] = {
    # Vertex (Veo)
    ("vertex", 4):            0.30,
    ("vertex", 6):            0.45,
    ("vertex", 8):            0.60,
    # Seedance 2.0 Fast (480p baseline — PiAPI $0.08/s)
    ("seedance_fast", 5):     round(0.064 * 5,  4),
    ("seedance_fast", 10):    round(0.064 * 10, 4),
    ("seedance_fast", 15):    round(0.064 * 15, 4),
    # Seedance 2.0 Quality (480p baseline — PiAPI $0.10/s) — replaces legacy "preview" rate.
    ("seedance_quality", 5):  round(0.080 * 5,  4),
    ("seedance_quality", 10): round(0.080 * 10, 4),
    ("seedance_quality", 15): round(0.080 * 15, 4),
    # Legacy "preview" key — pre-GA preview-only PiAPI charge was $0.10/s, not the $0.15 we previously stored.
    ("seedance_preview", 5):  round(0.080 * 5,  4),
    ("seedance_preview", 10): round(0.080 * 10, 4),
    ("seedance_preview", 15): round(0.080 * 15, 4),
    # fal Seedance 1.5 Pro
    ("fal_seedance", 5):      0.25,
    ("fal_seedance", 10):     0.50,
}

# Fallback per-second rates for unknown durations (GBP)
_VIDEO_FALLBACK_RATE: Dict[str, float] = {
    "vertex":           0.075,
    "seedance_fast":    0.064,   # 480p baseline; 720p costs ~2× this
    "seedance_quality": 0.080,   # 480p baseline; 720p ~2×, 1080p ~5×
    "seedance_preview": 0.080,   # legacy alias of quality at 480p
    "fal_seedance":     0.05,
}


# ─────────────────────────────────────────────────────────────────────────────
# VIDEO COST HELPER (same logic as the original estimate_video_provider_cost)
# ─────────────────────────────────────────────────────────────────────────────

def estimate_video_cost(
    provider: str,
    duration_seconds: int,
    seedance_tier: str = "fast",
    resolution: str | None = None,
    input_video_seconds: float = 0.0,
) -> float:
    """
    Estimate the real GBP cost of a video job to the provider.

    Args:
        provider:            "vertex" | "seedance" | "fal_seedance"
        duration_seconds:    Duration in seconds
        seedance_tier:       "fast" | "quality" (legacy "preview" → quality). Ignored for non-seedance.
        resolution:          "480p" / "720p" / "1080p" — Seedance only; defaults to 480p.
        input_video_seconds: Total reference-video duration (Seedance omni_reference). Billed
                             by PiAPI at half-rate. Ignored for non-seedance providers.

    This is the canonical implementation; video_limits.py delegates here.
    """
    if provider == "fal_seedance":
        key = ("fal_seedance", int(duration_seconds))
        cost = VIDEO_COST_GBP.get(key)
        if cost is not None:
            return cost
        rate = _VIDEO_FALLBACK_RATE.get("fal_seedance", 0.05)
        return round(rate * int(duration_seconds), 2)

    if provider == "seedance":
        # Prefer the explicit (tier, resolution) lookup — it reflects the real PiAPI rate.
        from backend.services.pricing_service import normalize_seedance_tier  # late import
        canon_tier = normalize_seedance_tier(seedance_tier)
        return estimate_seedance_provider_cost(
            canon_tier, int(duration_seconds), resolution or "480p",
            input_video_seconds=float(input_video_seconds or 0.0),
        )

    # Vertex
    key = ("vertex", int(duration_seconds))
    cost = VIDEO_COST_GBP.get(key)
    if cost is not None:
        return cost
    rate = _VIDEO_FALLBACK_RATE.get("vertex", 0.075)
    return round(rate * int(duration_seconds), 2)


# ─────────────────────────────────────────────────────────────────────────────
# UNIFIED COST ESTIMATOR
# ─────────────────────────────────────────────────────────────────────────────

def estimate_provider_cost(
    provider: str,
    action_code: str,
    meta: Optional[dict] = None,
) -> float:
    """
    Estimate upstream provider cost in GBP for any job.

    Args:
        provider:    Provider name (meshy, openai, google, google_nano, flux_pro, ideogram_v3, recraft_v4, vertex, seedance, fal_seedance)
        action_code: Action code from the jobs table
        meta:        Job metadata (used for video duration/tier)

    Returns:
        Estimated GBP cost.  Returns 0.0 if unknown.
    """
    # 1. Try direct action_code lookup (covers 3D + image)
    upper_code = (action_code or "").upper()
    if upper_code in ACTION_CODE_COST_GBP:
        return ACTION_CODE_COST_GBP[upper_code]

    # 2. For video providers, use duration-based lookup
    if provider in ("vertex", "seedance", "fal_seedance"):
        meta = meta or {}
        duration = meta.get("duration_seconds", 6)
        tier = meta.get("seedance_tier", "fast")
        resolution = meta.get("resolution")
        input_video_seconds = meta.get("input_video_seconds", 0.0)
        return estimate_video_cost(provider, int(duration), tier, resolution, input_video_seconds)

    # 3. Legacy / video action_code fallback
    # Some older jobs stored VIDEO_GENERATE, GEMINI_VIDEO, etc.
    if upper_code in (
        "VIDEO_GENERATE", "VIDEO_TEXT_GENERATE", "VIDEO_IMAGE_ANIMATE",
        "GEMINI_VIDEO",
    ):
        meta = meta or {}
        prov = provider or "vertex"
        duration = meta.get("duration_seconds", 6)
        tier = meta.get("seedance_tier", "fast")
        resolution = meta.get("resolution")
        input_video_seconds = meta.get("input_video_seconds", 0.0)
        return estimate_video_cost(prov, int(duration), tier, resolution, input_video_seconds)

    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# STAMP COST ON JOB RECORD
# ─────────────────────────────────────────────────────────────────────────────

def stamp_estimated_cost(job_id: str) -> Optional[float]:
    """
    Compute and persist estimated_provider_cost_gbp on a job row.

    Reads the job's provider, action_code, and meta, computes the estimate,
    and writes it to the column.  Returns the cost or None on error.

    Safe to call multiple times (idempotent write).
    """
    if not USE_DB:
        return None
    try:
        row = query_one(
            f"""
            SELECT provider, action_code, meta
            FROM {Tables.JOBS}
            WHERE id::text = %s
            """,
            (str(job_id),),
        )
        if not row:
            return None

        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}

        cost = estimate_provider_cost(
            provider=row.get("provider", ""),
            action_code=row.get("action_code", ""),
            meta=meta,
        )

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.JOBS}
                    SET estimated_provider_cost_gbp = %s
                    WHERE id::text = %s
                    """,
                    (cost, str(job_id)),
                )
            conn.commit()

        return cost
    except Exception as e:
        print(f"[PROVIDER_COST] Error stamping cost for job {job_id}: {e}")
        return None
