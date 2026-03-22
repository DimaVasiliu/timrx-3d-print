"""
Video Daily Quota Service — per-user, per-provider daily generation limits.

Provider buckets:
  veo             — Vertex Veo 3.1 (all resolutions/durations)
  seedance_fast   — Seedance 2.0 Fast tier
  seedance_preview — Seedance 2.0 Preview tier
  fal_seedance    — fal Seedance 1.5 Pro

Quota enforcement:
  1. check_quota()    — returns (allowed, usage, limit, resets_at)
  2. increment_quota() — atomically increments after job creation
  3. get_user_quota_summary() — admin view

Day boundary: UTC midnight (00:00 UTC).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple

from backend.db import get_conn, Tables


# ── Configurable daily limits ───────────────────────────────────────────────
# Override via environment variables. Defaults are conservative starting points.

DAILY_LIMITS: Dict[str, int] = {
    "veo":              int(os.getenv("VIDEO_QUOTA_VEO", "6")),
    "seedance_fast":    int(os.getenv("VIDEO_QUOTA_SEEDANCE_FAST", "20")),
    "seedance_preview": int(os.getenv("VIDEO_QUOTA_SEEDANCE_PREVIEW", "10")),
    "fal_seedance":     int(os.getenv("VIDEO_QUOTA_FAL_SEEDANCE", "20")),
}

# Human-readable provider names for user-facing messages
PROVIDER_DISPLAY_NAMES: Dict[str, str] = {
    "veo":              "Veo 3.1",
    "seedance_fast":    "Seedance 2.0 Fast",
    "seedance_preview": "Seedance 2.0 Preview",
    "fal_seedance":     "Seedance 1.5",
}


# ── Provider key resolution ─────────────────────────────────────────────────

def resolve_quota_key(provider: str, seedance_tier: Optional[str] = None) -> str:
    """
    Map a provider + tier into a quota bucket key.

    Args:
        provider: normalized provider name ('vertex', 'seedance', 'fal_seedance')
        seedance_tier: 'fast' or 'preview' (only for seedance provider)

    Returns:
        Quota bucket key: 'veo', 'seedance_fast', 'seedance_preview', 'fal_seedance'
    """
    p = (provider or "").lower().strip()

    if p in ("vertex", "veo", "google", "aistudio"):
        return "veo"
    elif p == "fal_seedance":
        return "fal_seedance"
    elif p == "seedance":
        tier = (seedance_tier or "fast").lower()
        return "seedance_preview" if tier == "preview" else "seedance_fast"

    # Fallback — treat unknown as veo (most restrictive)
    return "veo"


def resolve_quota_key_from_action_code(action_code: str) -> str:
    """
    Derive quota bucket from an action_code string.

    Examples:
        video_text_generate_8s_720p  → veo
        seedance_fast_text_generate_5s → seedance_fast
        seedance_preview_image_animate_10s → seedance_preview
        fal_seedance_text_generate_5s → fal_seedance
    """
    ac = (action_code or "").lower()

    if ac.startswith("fal_seedance"):
        return "fal_seedance"
    elif ac.startswith("seedance_preview"):
        return "seedance_preview"
    elif ac.startswith("seedance_fast") or ac.startswith("seedance_"):
        return "seedance_fast"
    else:
        return "veo"


# ── Core quota operations ───────────────────────────────────────────────────

def _utc_today() -> datetime:
    """Current UTC date as a date object."""
    return datetime.now(timezone.utc).date()


def _next_reset_iso() -> str:
    """ISO timestamp for the next UTC midnight reset."""
    tomorrow = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(days=1)
    return tomorrow.isoformat()


def check_quota(
    identity_id: str,
    provider_key: str,
) -> Dict[str, Any]:
    """
    Check if user has remaining daily quota for this provider.

    Returns:
        {
            "allowed": True/False,
            "provider_key": "veo",
            "provider_name": "Veo 3.1",
            "used_today": 3,
            "limit": 6,
            "remaining": 3,
            "resets_at": "2026-03-23T00:00:00+00:00",
        }
    """
    limit = DAILY_LIMITS.get(provider_key, 6)
    today = _utc_today()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT job_count
                    FROM {Tables.VIDEO_DAILY_USAGE}
                    WHERE identity_id = %s
                      AND provider_key = %s
                      AND usage_date = %s
                    """,
                    (identity_id, provider_key, today),
                )
                row = cur.fetchone()
                used = row["job_count"] if row else 0
    except Exception as e:
        print(f"[QUOTA] Check failed (allowing): {e}")
        # Fail open — don't block users if DB is down
        return {
            "allowed": True,
            "provider_key": provider_key,
            "provider_name": PROVIDER_DISPLAY_NAMES.get(provider_key, provider_key),
            "used_today": 0,
            "limit": limit,
            "remaining": limit,
            "resets_at": _next_reset_iso(),
        }

    remaining = max(0, limit - used)
    return {
        "allowed": used < limit,
        "provider_key": provider_key,
        "provider_name": PROVIDER_DISPLAY_NAMES.get(provider_key, provider_key),
        "used_today": used,
        "limit": limit,
        "remaining": remaining,
        "resets_at": _next_reset_iso(),
    }


def increment_quota(
    identity_id: str,
    provider_key: str,
) -> int:
    """
    Atomically increment the daily usage counter.

    Call this AFTER a job is successfully created (not before).
    Uses INSERT ... ON CONFLICT UPDATE for concurrency safety.

    Returns:
        New job_count after increment.
    """
    today = _utc_today()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {Tables.VIDEO_DAILY_USAGE}
                        (identity_id, provider_key, usage_date, job_count, updated_at)
                    VALUES (%s, %s, %s, 1, NOW())
                    ON CONFLICT (identity_id, provider_key, usage_date)
                    DO UPDATE SET
                        job_count = {Tables.VIDEO_DAILY_USAGE}.job_count + 1,
                        updated_at = NOW()
                    RETURNING job_count
                    """,
                    (identity_id, provider_key, today),
                )
                row = cur.fetchone()
            conn.commit()
            new_count = row["job_count"] if row else 1
            print(f"[QUOTA] {provider_key}: {identity_id[:8]}… now at {new_count}/{DAILY_LIMITS.get(provider_key, '?')}")
            return new_count
    except Exception as e:
        print(f"[QUOTA] Increment failed (non-blocking): {e}")
        return 0


# ── Admin / observability ───────────────────────────────────────────────────

def get_user_quota_summary(
    identity_id: str,
) -> Dict[str, Any]:
    """Get all quota usage for a user today."""
    today = _utc_today()
    result = {}

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT provider_key, job_count
                    FROM {Tables.VIDEO_DAILY_USAGE}
                    WHERE identity_id = %s AND usage_date = %s
                    """,
                    (identity_id, today),
                )
                rows = cur.fetchall()

        for row in rows:
            pk = row["provider_key"]
            result[pk] = {
                "used": row["job_count"],
                "limit": DAILY_LIMITS.get(pk, 0),
                "remaining": max(0, DAILY_LIMITS.get(pk, 0) - row["job_count"]),
            }
    except Exception as e:
        print(f"[QUOTA] Summary failed: {e}")

    # Fill in providers not yet used today
    for pk, limit in DAILY_LIMITS.items():
        if pk not in result:
            result[pk] = {"used": 0, "limit": limit, "remaining": limit}

    return result


def get_daily_quota_report(
    usage_date: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Admin report: top users by quota usage on a given date.

    Args:
        usage_date: ISO date string (default: today UTC)
        limit: max users to return
    """
    if usage_date:
        try:
            target_date = datetime.strptime(usage_date, "%Y-%m-%d").date()
        except ValueError:
            target_date = _utc_today()
    else:
        target_date = _utc_today()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                        u.identity_id,
                        i.email,
                        u.provider_key,
                        u.job_count,
                        u.updated_at
                    FROM {Tables.VIDEO_DAILY_USAGE} u
                    LEFT JOIN {Tables.IDENTITIES} i ON i.id = u.identity_id
                    WHERE u.usage_date = %s
                    ORDER BY u.job_count DESC
                    LIMIT %s
                    """,
                    (target_date, limit),
                )
                rows = cur.fetchall()

        users = {}
        for row in rows:
            uid = str(row["identity_id"])
            if uid not in users:
                users[uid] = {
                    "identity_id": uid,
                    "email": row["email"] or uid[:8],
                    "providers": {},
                    "total_jobs": 0,
                }
            pk = row["provider_key"]
            count = row["job_count"]
            lim = DAILY_LIMITS.get(pk, 0)
            users[uid]["providers"][pk] = {
                "used": count,
                "limit": lim,
                "at_limit": count >= lim,
            }
            users[uid]["total_jobs"] += count

        return {
            "date": str(target_date),
            "limits": DAILY_LIMITS,
            "users": sorted(users.values(), key=lambda u: u["total_jobs"], reverse=True),
        }
    except Exception as e:
        print(f"[QUOTA] Report failed: {e}")
        return {"date": str(target_date), "limits": DAILY_LIMITS, "users": []}
