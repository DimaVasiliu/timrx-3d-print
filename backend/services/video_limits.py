"""
Video Usage Limits — per-user rate limits, concurrency caps, budget guardrails,
queue inspection, abuse detection, and priority management.

Enforced in video.py _dispatch_video_job() BEFORE credit reservation.
Fail-closed: any check that cannot determine limits blocks the request.

Tier derivation priority:
  1. Active subscription (starter/creator/studio)
  2. Most recent video pack purchase (video_starter_300 → starter, etc.)
  3. Free (most restrictive — still allows generation if user has credits)

All numeric limits are config-driven via environment variables.
"""

from __future__ import annotations

import json
import time
import threading
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from flask import jsonify

from backend.db import USE_DB, query_one, query_all, Tables


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — all limits tuneable via env vars loaded in config.py
# ─────────────────────────────────────────────────────────────────────────────

def _cfg():
    """Lazy import to avoid circular dependency."""
    from backend.config import config
    return config


# ─────────────────────────────────────────────────────────────────────────────
# PLAN-TIER LIMITS (video-specific)
# ─────────────────────────────────────────────────────────────────────────────

VIDEO_PLAN_LIMITS: Dict[str, Dict[str, int]] = {
    "free": {
        "max_concurrent_video": 1,
        "max_video_per_hour": 10,
        "daily_provider_spend_gbp": 5,
    },
    "starter": {
        "max_concurrent_video": 1,
        "max_video_per_hour": 20,
        "daily_provider_spend_gbp": 10,
    },
    "creator": {
        "max_concurrent_video": 2,
        "max_video_per_hour": 60,
        "daily_provider_spend_gbp": 40,
    },
    "studio": {
        "max_concurrent_video": 4,
        "max_video_per_hour": 150,
        "daily_provider_spend_gbp": 120,
    },
}

# Map one-time video pack codes to tiers
_VIDEO_PACK_TIER_MAP = {
    "video_starter_300": "starter",
    "video_creator_900": "creator",
    "video_studio_2000": "studio",
}

# Estimated provider cost per video job in GBP (used for daily spend guardrails)
# Keyed by (provider, duration_seconds). Approximate real API costs.
PROVIDER_COST_GBP: Dict[Tuple[str, int], float] = {
    # Veo (Google Vertex)
    ("veo", 4): 0.30,
    ("veo", 6): 0.45,
    ("veo", 8): 0.60,
    # Seedance Fast (PiAPI)
    ("seedance_fast", 5): 0.25,
    ("seedance_fast", 10): 0.50,
    ("seedance_fast", 15): 0.75,
    # Seedance Preview (PiAPI)
    ("seedance_preview", 5): 0.45,
    ("seedance_preview", 10): 0.90,
    ("seedance_preview", 15): 1.35,
}

# Cooldown between video starts (seconds)
VIDEO_COOLDOWN_SECONDS = 10


# ─────────────────────────────────────────────────────────────────────────────
# TIER RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

def _get_video_tier(identity_id: str) -> str:
    """
    Resolve the user's video tier.

    Priority:
      1. Active subscription tier (starter/creator/studio)
      2. Highest video pack ever purchased
      3. "free"
    """
    # 1. Check subscription
    try:
        from backend.services.subscription_service import SubscriptionService
        perks = SubscriptionService.get_tier_perks(identity_id)
        tier = perks.get("tier", "free")
        if tier != "free":
            return tier
    except Exception as e:
        print(f"[VIDEO_LIMITS] Subscription lookup error: {e}")

    # 2. Check most recent video pack purchase
    if USE_DB:
        try:
            row = query_one(
                f"""
                SELECT p.plan_code
                FROM {Tables.PURCHASES} p
                WHERE p.identity_id = %s
                  AND p.plan_code LIKE 'video_%%'
                  AND p.status = 'completed'
                ORDER BY p.created_at DESC
                LIMIT 1
                """,
                (identity_id,),
            )
            if row:
                return _VIDEO_PACK_TIER_MAP.get(row["plan_code"], "starter")
        except Exception as e:
            print(f"[VIDEO_LIMITS] Purchase lookup error: {e}")

    return "free"


def get_video_plan_limits(identity_id: str) -> Dict[str, Any]:
    """
    Return the video-specific limits for a user.

    Returns:
        {
            "tier": "starter",
            "max_concurrent_video": 1,
            "max_video_per_hour": 20,
            "daily_provider_spend_gbp": 10,
            "cooldown_seconds": 10,
        }
    """
    tier = _get_video_tier(identity_id)
    limits = VIDEO_PLAN_LIMITS.get(tier, VIDEO_PLAN_LIMITS["free"]).copy()
    limits["tier"] = tier
    limits["cooldown_seconds"] = VIDEO_COOLDOWN_SECONDS
    return limits


# ─────────────────────────────────────────────────────────────────────────────
# DB QUERIES — job counting
# ─────────────────────────────────────────────────────────────────────────────

def count_active_video_jobs(identity_id: str) -> int:
    """Count video jobs in processing/queued state for this user."""
    if not USE_DB:
        return 0
    try:
        row = query_one(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {Tables.JOBS}
            WHERE identity_id = %s
              AND status IN ('queued', 'processing')
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
            """,
            (identity_id,),
        )
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[VIDEO_LIMITS] count_active error: {e}")
        return 0


def count_video_jobs_last_hour(identity_id: str) -> int:
    """Count video jobs started in the last rolling hour."""
    if not USE_DB:
        return 0
    try:
        row = query_one(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {Tables.JOBS}
            WHERE identity_id = %s
              AND created_at >= NOW() - INTERVAL '1 hour'
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
            """,
            (identity_id,),
        )
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[VIDEO_LIMITS] count_hourly error: {e}")
        return 0


def get_last_video_job_started_at(identity_id: str) -> Optional[datetime]:
    """Return the created_at of the user's most recent video job, or None."""
    if not USE_DB:
        return None
    try:
        row = query_one(
            f"""
            SELECT created_at
            FROM {Tables.JOBS}
            WHERE identity_id = %s
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (identity_id,),
        )
        return row["created_at"] if row else None
    except Exception as e:
        print(f"[VIDEO_LIMITS] last_started error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# PROVIDER SPEND TRACKING
# ─────────────────────────────────────────────────────────────────────────────

def estimate_video_provider_cost(provider: str, duration_seconds: int, seedance_tier: str = "fast") -> float:
    """
    Estimate the real GBP cost of a video job to the provider.

    Returns approximate cost in GBP.
    """
    if provider == "seedance":
        key = (f"seedance_{seedance_tier}", int(duration_seconds))
    else:
        key = ("veo", int(duration_seconds))

    cost = PROVIDER_COST_GBP.get(key)
    if cost is not None:
        return cost

    # Fallback: linear estimate based on closest known cost
    if provider == "seedance":
        rate = 0.05 if seedance_tier == "fast" else 0.09
    else:
        rate = 0.075
    return round(rate * int(duration_seconds), 2)


def get_daily_user_video_provider_spend(identity_id: str) -> float:
    """
    Sum estimated provider GBP spend for this user today (UTC midnight to now).

    Uses the meta->provider and meta->duration_seconds stored on each job row,
    falling back to a flat estimate if meta is missing.
    """
    if not USE_DB:
        return 0.0
    try:
        rows = query_all(
            f"""
            SELECT provider, meta
            FROM {Tables.JOBS}
            WHERE identity_id = %s
              AND created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
              AND status NOT IN ('failed', 'refunded', 'abandoned_legacy', 'recovery_blocked')
            """,
            (identity_id,),
        )
        total = 0.0
        for r in rows:
            meta = r.get("meta") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            prov = r.get("provider") or meta.get("provider", "veo")
            dur = meta.get("duration_seconds", 6)
            tier = meta.get("seedance_tier", "fast")
            total += estimate_video_provider_cost(prov, dur, tier)
        return round(total, 2)
    except Exception as e:
        print(f"[VIDEO_LIMITS] daily_user_spend error: {e}")
        return 0.0


def get_daily_global_video_provider_spend() -> float:
    """
    Sum estimated provider GBP spend across ALL users today.
    Used for the global daily budget cap.
    """
    if not USE_DB:
        return 0.0
    try:
        rows = query_all(
            f"""
            SELECT provider, meta
            FROM {Tables.JOBS}
            WHERE created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
              AND status NOT IN ('failed', 'refunded', 'abandoned_legacy', 'recovery_blocked')
            """,
        )
        total = 0.0
        for r in rows:
            meta = r.get("meta") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            prov = r.get("provider") or meta.get("provider", "veo")
            dur = meta.get("duration_seconds", 6)
            tier = meta.get("seedance_tier", "fast")
            total += estimate_video_provider_cost(prov, dur, tier)
        return round(total, 2)
    except Exception as e:
        print(f"[VIDEO_LIMITS] daily_global_spend error: {e}")
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# MAIN VALIDATION — called before credit reservation
# ─────────────────────────────────────────────────────────────────────────────

def validate_video_rate_limits(
    identity_id: str,
    provider: str = "veo",
    duration_seconds: int = 6,
    seedance_tier: str = "fast",
) -> Optional[Tuple]:
    """
    Run all video rate-limit checks for a user. Returns None if OK,
    or a Flask (jsonify(...), status_code) tuple to return directly.

    Check order (fail-closed):
      1. Enforce limits enabled?
      2. Cooldown (10s between starts)
      3. Concurrency (active jobs per tier)
      4. Hourly generation cap
      5. Daily per-user provider spend
      6. Daily global provider budget
    """
    cfg = _cfg()

    # Master kill-switch
    if not getattr(cfg, "VIDEO_ENFORCE_LIMITS", True):
        return None

    limits = get_video_plan_limits(identity_id)
    tier = limits["tier"]
    now = datetime.now(timezone.utc)

    # ── 1. Cooldown ──────────────────────────────────────────────────────
    last_started = get_last_video_job_started_at(identity_id)
    if last_started is not None:
        # Ensure timezone-aware comparison
        if last_started.tzinfo is None:
            last_started = last_started.replace(tzinfo=timezone.utc)
        elapsed = (now - last_started).total_seconds()
        cooldown = limits["cooldown_seconds"]
        if elapsed < cooldown:
            wait = int(cooldown - elapsed) + 1
            print(f"[VIDEO_LIMITS] COOLDOWN identity={identity_id} elapsed={elapsed:.1f}s wait={wait}s")
            return jsonify({
                "ok": False,
                "error": "video_cooldown",
                "message": f"Please wait {wait}s before starting another video.",
                "retry_after": wait,
            }), 429

    # ── 2. Concurrency ───────────────────────────────────────────────────
    active = count_active_video_jobs(identity_id)
    max_conc = limits["max_concurrent_video"]
    if active >= max_conc:
        print(f"[VIDEO_LIMITS] CONCURRENCY identity={identity_id} active={active} max={max_conc} tier={tier}")
        return jsonify({
            "ok": False,
            "error": "video_concurrency_limit",
            "message": f"You already have {active} video(s) processing. Your plan allows {max_conc} at a time.",
            "active": active,
            "limit": max_conc,
            "tier": tier,
        }), 429

    # ── 3. Hourly cap ────────────────────────────────────────────────────
    hourly = count_video_jobs_last_hour(identity_id)
    max_hourly = limits["max_video_per_hour"]
    if hourly >= max_hourly:
        print(f"[VIDEO_LIMITS] HOURLY_CAP identity={identity_id} count={hourly} max={max_hourly} tier={tier}")
        return jsonify({
            "ok": False,
            "error": "video_hourly_limit",
            "message": f"Hourly video limit reached ({max_hourly}/hr for {tier.title()} plan). Please try again later.",
            "count": hourly,
            "limit": max_hourly,
            "tier": tier,
        }), 429

    # ── 4. Daily per-user provider spend ─────────────────────────────────
    est_cost = estimate_video_provider_cost_safe(provider, duration_seconds, seedance_tier)
    user_spend = get_daily_user_video_provider_spend(identity_id)
    max_user_daily = limits["daily_provider_spend_gbp"]
    if user_spend + est_cost > max_user_daily:
        print(f"[VIDEO_LIMITS] USER_DAILY_SPEND identity={identity_id} spend={user_spend} +{est_cost} max={max_user_daily} tier={tier}")
        return jsonify({
            "ok": False,
            "error": "video_daily_spend_limit",
            "message": f"Daily video spend limit reached for your {tier.title()} plan. Resets at midnight UTC.",
            "spend_gbp": user_spend,
            "limit_gbp": max_user_daily,
            "tier": tier,
        }), 429

    # ── 5. Global daily provider budget ──────────────────────────────────
    global_budget = getattr(cfg, "VIDEO_DAILY_PROVIDER_BUDGET_GBP", 500)
    global_spend = get_daily_global_video_provider_spend()
    if global_spend + est_cost > global_budget:
        print(f"[VIDEO_LIMITS] GLOBAL_BUDGET global_spend={global_spend} +{est_cost} budget={global_budget}")
        return jsonify({
            "ok": False,
            "error": "video_global_budget",
            "message": "Video generation is temporarily paused due to high demand. Please try again later.",
        }), 503

    # ── 6. Abuse detection ─────────────────────────────────────────────
    abuse = check_abuse_signals(identity_id)
    if abuse:
        print(f"[VIDEO_ABUSE_DETECTED] identity={identity_id} reason={abuse}")
        return jsonify({
            "ok": False,
            "error": "video_abuse_detected",
            "message": "Unusual activity detected. Please slow down and try again in a few minutes.",
        }), 429

    # All checks passed
    print(f"[VIDEO_LIMITS] OK identity={identity_id} tier={tier} active={active}/{max_conc} hourly={hourly}/{max_hourly} spend={user_spend}/{max_user_daily}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# QUEUE POSITION & ESTIMATED RENDER TIME  (Parts 1 & 2)
# ─────────────────────────────────────────────────────────────────────────────

# Average generation time per provider (seconds)
# Observed PiAPI timings (2026-03-10):
#   fast:    7-8 min total (queue + render)
#   preview: 20-100+ min (highly variable queue)
AVERAGE_GENERATION_TIME = {
    "veo": 80,         # 45–120s
    "seedance": 480,   # ~8 min (fast tier typical)
}

# Estimated render time ranges shown to user
RENDER_TIME_RANGE = {
    "veo": (45, 120),
    "seedance": (300, 600),  # 5-10 min (fast tier)
}


def get_queue_position(job_id: str) -> Dict[str, Any]:
    """
    Return the queue position and estimated start time for a queued job.

    Returns:
        {"queue_position": 3, "estimated_start_seconds": 25, "estimated_duration_seconds": 80}
    """
    if not USE_DB:
        return {"queue_position": 0, "estimated_start_seconds": 0}
    try:
        # Count queued video jobs created before this one
        row = query_one(
            f"""
            SELECT COUNT(*) AS pos
            FROM {Tables.JOBS}
            WHERE status IN ('queued', 'processing')
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
              AND created_at < (
                  SELECT created_at FROM {Tables.JOBS} WHERE id::text = %s LIMIT 1
              )
            """,
            (job_id,),
        )
        position = (row["pos"] if row else 0) + 1  # 1-based

        # Estimate start time: position * avg_time / max_workers
        max_workers = getattr(_cfg(), "MAX_VIDEO_WORKERS", 8)
        avg_time = 60  # blended average
        estimated_start = max(0, int((position - 1) * avg_time / max(max_workers, 1)))

        return {
            "queue_position": position,
            "estimated_start_seconds": estimated_start,
        }
    except Exception as e:
        print(f"[VIDEO_LIMITS] queue_position error: {e}")
        return {"queue_position": 0, "estimated_start_seconds": 0}


def get_estimated_render_time(provider: str = "veo") -> Dict[str, int]:
    """
    Return estimated render time range for a provider.

    Returns:
        {"estimated_min_seconds": 45, "estimated_max_seconds": 120, "estimated_duration_seconds": 80}
    """
    low, high = RENDER_TIME_RANGE.get(provider, (30, 90))
    avg = AVERAGE_GENERATION_TIME.get(provider, 60)
    return {
        "estimated_min_seconds": low,
        "estimated_max_seconds": high,
        "estimated_duration_seconds": avg,
    }


def get_total_queued_video_jobs() -> int:
    """Count all queued video jobs system-wide."""
    if not USE_DB:
        return 0
    try:
        row = query_one(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {Tables.JOBS}
            WHERE status IN ('queued', 'processing')
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
            """,
        )
        return row["cnt"] if row else 0
    except Exception as e:
        print(f"[VIDEO_LIMITS] queued_count error: {e}")
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL WORKER LIMIT  (Part 3)
# ─────────────────────────────────────────────────────────────────────────────

# In-memory active worker counter (thread-safe)
_active_video_workers = 0
_worker_lock = threading.Lock()


def acquire_video_worker() -> bool:
    """
    Try to acquire a video worker slot. Returns True if acquired, False if at capacity.
    Called at the START of dispatch_gemini_video_async.
    """
    global _active_video_workers
    max_workers = getattr(_cfg(), "MAX_VIDEO_WORKERS", 8)
    with _worker_lock:
        if _active_video_workers >= max_workers:
            return False
        _active_video_workers += 1
        return True


def release_video_worker():
    """Release a video worker slot. Called when a video job completes or fails."""
    global _active_video_workers
    with _worker_lock:
        _active_video_workers = max(0, _active_video_workers - 1)


def get_active_video_worker_count() -> int:
    """Return current active video worker count."""
    return _active_video_workers


# ─────────────────────────────────────────────────────────────────────────────
# DYNAMIC PROVIDER ROUTING  (Part 4)
# ─────────────────────────────────────────────────────────────────────────────

QUEUE_THRESHOLD_FOR_CHEAPER_PROVIDER = 20


def select_video_provider(requested_provider: str = "veo") -> str:
    """
    Select the optimal video provider based on queue conditions.
    Only overrides when user did NOT explicitly select a provider.

    If queue is long (>20 queued jobs), prefer Seedance (cheaper/faster).
    """
    # If user explicitly selected a provider, respect that
    if requested_provider in ("seedance",):
        return requested_provider

    # Only apply dynamic routing when user selected "veo" (default)
    if requested_provider != "veo":
        return requested_provider

    queue_len = get_total_queued_video_jobs()
    threshold = QUEUE_THRESHOLD_FOR_CHEAPER_PROVIDER

    if queue_len > threshold:
        # Check if Seedance is configured before routing to it
        try:
            from backend.services.video_router import resolve_video_provider
            seedance = resolve_video_provider("seedance")
            if seedance:
                configured, _ = seedance.is_configured()
                if configured:
                    print(f"[VIDEO_LIMITS] DYNAMIC_ROUTE queue={queue_len} > {threshold}, routing to seedance")
                    return "seedance"
        except Exception:
            pass

    return requested_provider


# ────────────────────────────────────────────────────────────��────────────────
# ABUSE PATTERN DETECTION  (Part 7)
# ─────────────────────────────────────────────────────────────────────────────

# In-memory sliding window: identity_id -> list of (timestamp, prompt_hash)
_request_log: Dict[str, List[Tuple[float, str]]] = defaultdict(list)
_request_log_lock = threading.Lock()

ABUSE_WINDOW_SECONDS = 120  # 2-minute window
ABUSE_MAX_REQUESTS = 10     # Max requests in window
ABUSE_MAX_IDENTICAL = 5     # Max identical prompts in window


def record_video_request(identity_id: str, prompt: str = ""):
    """Record a video request for abuse tracking."""
    import hashlib
    prompt_hash = hashlib.md5((prompt or "").encode()).hexdigest()[:8]
    now = time.time()
    with _request_log_lock:
        log = _request_log[identity_id]
        # Prune old entries
        cutoff = now - ABUSE_WINDOW_SECONDS
        _request_log[identity_id] = [(ts, ph) for ts, ph in log if ts > cutoff]
        _request_log[identity_id].append((now, prompt_hash))


def check_abuse_signals(identity_id: str) -> Optional[str]:
    """
    Check for abuse patterns. Returns a reason string if abuse detected, None if OK.
    """
    now = time.time()
    cutoff = now - ABUSE_WINDOW_SECONDS
    with _request_log_lock:
        log = [(ts, ph) for ts, ph in _request_log.get(identity_id, []) if ts > cutoff]

    if not log:
        return None

    # Check 1: Too many requests in window
    if len(log) > ABUSE_MAX_REQUESTS:
        return f"rapid_requests:{len(log)}_in_{ABUSE_WINDOW_SECONDS}s"

    # Check 2: Too many identical prompts
    prompt_counts: Dict[str, int] = defaultdict(int)
    for _, ph in log:
        prompt_counts[ph] += 1
    for ph, count in prompt_counts.items():
        if count > ABUSE_MAX_IDENTICAL:
            return f"repeated_prompt:{count}_identical"

    return None


# ─────────────────────────────────────────────────────────────────────────────
# PRIORITY QUEUE  (Part 8)
# ─────────────────────────────────────────────────────────────────────────────

TIER_PRIORITY = {
    "studio": 1,   # Highest
    "creator": 2,
    "starter": 3,
    "free": 4,      # Lowest
}


def get_job_priority(identity_id: str) -> int:
    """Return priority value for a user (lower = higher priority)."""
    tier = _get_video_tier(identity_id)
    return TIER_PRIORITY.get(tier, 4)


# ─────────────────────────────────────────────────────────────────────────────
# SAFETY COST MULTIPLIER  (Part 12)
# ─────────────────────────────────────────────────────────────────────────────

def get_safety_multiplier() -> float:
    """Return the provider cost safety multiplier from config."""
    return getattr(_cfg(), "VIDEO_COST_SAFETY_MULTIPLIER", 1.25)


def estimate_video_provider_cost_safe(provider: str, duration_seconds: int, seedance_tier: str = "fast") -> float:
    """Estimate provider cost with safety multiplier applied."""
    base = estimate_video_provider_cost(provider, duration_seconds, seedance_tier)
    return round(base * get_safety_multiplier(), 2)


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN METRICS  (Part 10)
# ─────────────────────────────────────────────────────────────────────────────

def get_video_metrics() -> Dict[str, Any]:
    """
    Return operational video metrics for the admin endpoint.

    Returns:
        {
            "active_jobs": int,
            "queue_length": int,
            "videos_generated_today": int,
            "provider_spend_today": float,
            "average_generation_time": float,
            "active_workers": int,
            "max_workers": int,
        }
    """
    metrics: Dict[str, Any] = {
        "active_jobs": 0,
        "queue_length": 0,
        "videos_generated_today": 0,
        "provider_spend_today": 0.0,
        "average_generation_time": 0.0,
        "active_workers": get_active_video_worker_count(),
        "max_workers": getattr(_cfg(), "MAX_VIDEO_WORKERS", 8),
    }

    if not USE_DB:
        return metrics

    try:
        # Active jobs (queued + processing)
        row = query_one(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {Tables.JOBS}
            WHERE status IN ('queued', 'processing')
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
            """,
        )
        metrics["active_jobs"] = row["cnt"] if row else 0

        # Queue length (queued only)
        row = query_one(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {Tables.JOBS}
            WHERE status = 'queued'
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
            """,
        )
        metrics["queue_length"] = row["cnt"] if row else 0

        # Videos generated today
        row = query_one(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {Tables.JOBS}
            WHERE created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
            """,
        )
        metrics["videos_generated_today"] = row["cnt"] if row else 0

        # Provider spend today
        metrics["provider_spend_today"] = get_daily_global_video_provider_spend()

        # Average generation time (from completed jobs today)
        row = query_one(
            f"""
            SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) AS avg_sec
            FROM {Tables.JOBS}
            WHERE status = 'ready'
              AND created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
              AND action_code LIKE ANY(ARRAY['video_%%', 'seedance_%%'])
              AND updated_at IS NOT NULL
            """,
        )
        if row and row.get("avg_sec"):
            metrics["average_generation_time"] = round(float(row["avg_sec"]), 1)

    except Exception as e:
        print(f"[VIDEO_LIMITS] metrics error: {e}")

    return metrics
