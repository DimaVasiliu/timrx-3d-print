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

VIDEO_COST_GBP: Dict[Tuple[str, int], float] = {
    # Vertex (Veo)
    ("vertex", 4):            0.30,
    ("vertex", 6):            0.45,
    ("vertex", 8):            0.60,
    # Seedance 2.0 Fast (PiAPI) — $0.08/sec USD, converted at USD/GBP ≈ 0.80
    ("seedance_fast", 5):     0.32,
    ("seedance_fast", 10):    0.64,
    ("seedance_fast", 15):    0.96,
    # Seedance 2.0 Preview (PiAPI) — $0.15/sec USD, converted at USD/GBP ≈ 0.80
    ("seedance_preview", 5):  0.60,
    ("seedance_preview", 10): 1.20,
    ("seedance_preview", 15): 1.80,
    # fal Seedance 1.5 Pro
    ("fal_seedance", 5):      0.25,
    ("fal_seedance", 10):     0.50,
}

# Fallback per-second rates for unknown durations (GBP)
_VIDEO_FALLBACK_RATE: Dict[str, float] = {
    "vertex":           0.075,
    "seedance_fast":    0.064,   # $0.08/sec × 0.80
    "seedance_preview": 0.12,    # $0.15/sec × 0.80
    "fal_seedance":     0.05,
}


# ─────────────────────────────────────────────────────────────────────────────
# VIDEO COST HELPER (same logic as the original estimate_video_provider_cost)
# ─────────────────────────────────────────────────────────────────────────────

def estimate_video_cost(provider: str, duration_seconds: int, seedance_tier: str = "fast") -> float:
    """
    Estimate the real GBP cost of a video job to the provider.

    This is the canonical implementation; video_limits.py delegates here.
    """
    if provider == "fal_seedance":
        key = ("fal_seedance", int(duration_seconds))
    elif provider == "seedance":
        key = (f"seedance_{seedance_tier}", int(duration_seconds))
    else:
        key = ("vertex", int(duration_seconds))

    cost = VIDEO_COST_GBP.get(key)
    if cost is not None:
        return cost

    # Fallback: linear estimate
    variant = key[0]
    rate = _VIDEO_FALLBACK_RATE.get(variant, 0.075)
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
        return estimate_video_cost(provider, int(duration), tier)

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
        return estimate_video_cost(prov, int(duration), tier)

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
