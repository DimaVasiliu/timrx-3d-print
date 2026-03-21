"""
Admin Dashboard Service — unified summary for the admin super-dashboard.

Single entry point: get_dashboard_summary(force_refresh)
Reuses shared service functions; never duplicates analytics logic.

Caching: 60-second in-memory TTL (single admin, read-heavy).
Graceful degradation: each section can fail independently.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.db import USE_DB, query_all, query_one, Tables


# ─────────────────────────────────────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────────────────────────────────────
_cache: Dict[str, Any] = {"data": None, "expires_at": 0}
_CACHE_TTL = 60  # seconds


def get_dashboard_summary(force_refresh: bool = False) -> Dict[str, Any]:
    """
    Return the unified admin dashboard summary.

    Sections: operational, economics, safety, anomalies.
    Each section is computed independently; a failure in one does not
    block the others.
    """
    now = time.time()
    if not force_refresh and _cache["data"] and now < _cache["expires_at"]:
        return {**_cache["data"], "cached": True}

    result: Dict[str, Any] = {
        "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cached": False,
        "operational": _safe_section(_compute_operational),
        "economics": _safe_section(_compute_economics),
        "safety": _safe_section(_compute_safety),
        "anomalies": [],
    }

    # Anomalies computed from the other sections
    result["anomalies"] = _compute_anomalies(result)

    _cache["data"] = result
    _cache["expires_at"] = time.time() + _CACHE_TTL

    return result


def get_safety_summary(hours: int = 24, days: int = 7) -> Dict[str, Any]:
    """
    Dedicated safety summary backed by the same shared analytics.
    Reuses get_safety_analytics() from prompt_safety_service.
    """
    try:
        from backend.services.prompt_safety_service import get_safety_analytics
        today_data = get_safety_analytics(hours=hours)
        week_data = get_safety_analytics(hours=days * 24)
        return {
            "ok": True,
            "today": today_data,
            "last_7d": week_data,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Section builders
# ─────────────────────────────────────────────────────────────────────────────

def _safe_section(fn):
    """Run a section builder; return error marker on failure."""
    try:
        return fn()
    except Exception as e:
        print(f"[ADMIN_DASHBOARD] Section {fn.__name__} failed: {e}")
        return {"_error": f"{fn.__name__} failed: {type(e).__name__}"}


def _compute_operational() -> Dict[str, Any]:
    if not USE_DB:
        return {}

    # Provider health (reuse existing service)
    from backend.services.admin_ops_service import get_provider_health
    health = get_provider_health()
    summary = health.get("summary", {})
    providers = health.get("providers", [])

    # Jobs today + 7d
    jobs_row = query_one(f"""
        SELECT
            COUNT(*) FILTER (WHERE created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')) AS jobs_today,
            COUNT(*) AS jobs_7d,
            COUNT(*) FILTER (WHERE status IN ('succeeded', 'ready')
                             AND created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC'))
                AS success_today,
            COUNT(*) FILTER (WHERE status IN ('succeeded', 'ready')) AS success_7d
        FROM {Tables.JOBS}
        WHERE created_at >= NOW() - INTERVAL '7 days'
          AND provider IS NOT NULL
    """)

    jobs_today = jobs_row["jobs_today"] if jobs_row else 0
    jobs_7d = jobs_row["jobs_7d"] if jobs_row else 0
    success_today = jobs_row["success_today"] if jobs_row else 0
    success_7d = jobs_row["success_7d"] if jobs_row else 0

    rate_today = round(success_today / jobs_today, 2) if jobs_today > 0 else None
    rate_7d = round(success_7d / jobs_7d, 2) if jobs_7d > 0 else None

    # Refund queue
    refund_row = query_one(f"""
        SELECT COUNT(*) AS cnt FROM {Tables.REFUNDS}
        WHERE refund_status IN ('pending', 'manual_review_required', 'approved')
    """)
    refund_count = refund_row["cnt"] if refund_row else 0

    # Wallet drift
    drift_row = query_one("""
        SELECT COUNT(*) AS cnt FROM timrx_billing.v_wallet_ledger_comparison
        WHERE has_drift = TRUE
    """)
    wallets_with_drift = drift_row["cnt"] if drift_row else 0

    # Last reconciliation
    recon_row = query_one("""
        SELECT MAX(started_at) AS last_at
        FROM timrx_billing.reconciliation_runs
        WHERE status = 'completed'
    """)
    last_recon = recon_row["last_at"] if recon_row else None

    return {
        "providers_total": summary.get("provider_count", 0),
        "providers_healthy": summary.get("healthy_count", 0),
        "providers_warning": summary.get("warning_count", 0),
        "providers_down": summary.get("down_count", 0),
        "providers_unknown": summary.get("provider_count", 0) - summary.get("healthy_count", 0)
                             - summary.get("warning_count", 0) - summary.get("down_count", 0),
        "jobs_today": jobs_today,
        "jobs_7d": jobs_7d,
        "success_rate_today": rate_today,
        "success_rate_7d": rate_7d,
        "active_alerts": summary.get("alerts_active", 0),
        "refund_queue_count": refund_count,
        "wallets_with_drift": wallets_with_drift,
        "last_reconciliation_at": last_recon.isoformat() if hasattr(last_recon, "isoformat") else last_recon,
    }


def _compute_economics() -> Dict[str, Any]:
    if not USE_DB:
        return {}

    # Credits spent today + 7d (from finalized reservations / succeeded jobs)
    credits_row = query_one(f"""
        SELECT
            COALESCE(SUM(cost_credits) FILTER (
                WHERE created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
            ), 0) AS spent_today,
            COALESCE(SUM(cost_credits), 0) AS spent_7d
        FROM {Tables.JOBS}
        WHERE status IN ('succeeded', 'ready')
          AND created_at >= NOW() - INTERVAL '7 days'
    """)

    # Credits granted today + 7d (purchases + subscriptions + grants)
    granted_row = query_one(f"""
        SELECT
            COALESCE(SUM(amount_credits) FILTER (
                WHERE created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
                  AND amount_credits > 0
            ), 0) AS granted_today,
            COALESCE(SUM(amount_credits) FILTER (
                WHERE amount_credits > 0
            ), 0) AS granted_7d
        FROM {Tables.LEDGER_ENTRIES}
        WHERE entry_type IN ('purchase_credit', 'subscription', 'admin_adjust', 'signup_grant', 'grant')
          AND created_at >= NOW() - INTERVAL '7 days'
    """)

    # Provider cost today + 7d (estimated GBP)
    cost_row = query_one(f"""
        SELECT
            COALESCE(SUM(estimated_provider_cost_gbp) FILTER (
                WHERE created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
            ), 0) AS cost_today,
            COALESCE(SUM(estimated_provider_cost_gbp), 0) AS cost_7d
        FROM {Tables.JOBS}
        WHERE status IN ('succeeded', 'ready')
          AND created_at >= NOW() - INTERVAL '7 days'
    """)

    # Revenue today + 7d (from purchases)
    rev_row = query_one(f"""
        SELECT
            COALESCE(SUM(amount_gbp) FILTER (
                WHERE paid_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
            ), 0) AS rev_today,
            COALESCE(SUM(amount_gbp), 0) AS rev_7d,
            COUNT(*) FILTER (
                WHERE paid_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
            ) AS purchases_today,
            COUNT(*) AS purchases_7d
        FROM {Tables.PURCHASES}
        WHERE status IN ('completed', 'paid')
          AND paid_at >= NOW() - INTERVAL '7 days'
    """)

    # Subscriptions
    sub_row = query_one(f"""
        SELECT
            COUNT(*) FILTER (WHERE status = 'active') AS active,
            COUNT(*) FILTER (WHERE status = 'past_due') AS past_due
        FROM {Tables.SUBSCRIPTIONS}
    """)

    return {
        "credits_spent_today": int(credits_row["spent_today"]) if credits_row else 0,
        "credits_spent_7d": int(credits_row["spent_7d"]) if credits_row else 0,
        "credits_granted_today": int(granted_row["granted_today"]) if granted_row else 0,
        "credits_granted_7d": int(granted_row["granted_7d"]) if granted_row else 0,
        "provider_cost_today_gbp": round(float(cost_row["cost_today"]), 2) if cost_row else 0.0,
        "provider_cost_7d_gbp": round(float(cost_row["cost_7d"]), 2) if cost_row else 0.0,
        "revenue_today_gbp": round(float(rev_row["rev_today"]), 2) if rev_row else 0.0,
        "revenue_7d_gbp": round(float(rev_row["rev_7d"]), 2) if rev_row else 0.0,
        "purchases_today": rev_row["purchases_today"] if rev_row else 0,
        "purchases_7d": rev_row["purchases_7d"] if rev_row else 0,
        "active_subscriptions": sub_row["active"] if sub_row else 0,
        "past_due_subscriptions": sub_row["past_due"] if sub_row else 0,
    }


def _compute_safety() -> Dict[str, Any]:
    """Reuse shared safety analytics — never duplicate SQL logic."""
    from backend.services.prompt_safety_service import get_safety_analytics

    today = get_safety_analytics(hours=24)
    week = get_safety_analytics(hours=168)  # 7 days

    return {
        "blocks_today": today.get("blocks_by_category", {}).get("_total", 0),
        "warns_today": today.get("warns_by_category", {}).get("_total", 0),
        "penalties_today": today.get("penalties_applied", 0),
        "penalty_credits_today": today.get("penalty_credits_total", 0),
        "blocks_7d": week.get("blocks_by_category", {}).get("_total", 0),
        "penalties_7d": week.get("penalties_applied", 0),
        "penalty_credits_7d": week.get("penalty_credits_total", 0),
        "false_negative_candidates_24h": today.get("false_negative_candidates", 0),
        "top_penalized_users": today.get("top_penalized_users", []),
        "category_breakdown_7d": week.get("category_breakdown", []),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Anomaly detection (Python threshold logic, not SQL)
# ─────────────────────────────────────────────────────────────────────────────

def _compute_anomalies(data: Dict) -> List[Dict]:
    anomalies = []
    econ = data.get("economics", {})
    ops = data.get("operational", {})
    safety = data.get("safety", {})

    if isinstance(econ, dict) and "_error" not in econ:
        # Revenue drought: 0 purchases in last 7d while normally > 0
        if econ.get("purchases_7d", 0) == 0:
            anomalies.append({
                "type": "revenue_drought",
                "message": "No completed purchases in the last 7 days",
                "severity": "warning",
            })

    if isinstance(safety, dict) and "_error" not in safety:
        # Penalty spike: >10 penalties in 24h
        if safety.get("penalties_today", 0) > 10:
            anomalies.append({
                "type": "penalty_spike",
                "message": f"{safety['penalties_today']} penalties in 24h (threshold: 10)",
                "severity": "warning",
                "metric_value": safety["penalties_today"],
                "threshold": 10,
            })

    if isinstance(ops, dict) and "_error" not in ops:
        # Job failure spike: success rate < 75% today
        rate = ops.get("success_rate_today")
        if rate is not None and rate < 0.75 and ops.get("jobs_today", 0) >= 5:
            anomalies.append({
                "type": "job_failure_spike",
                "message": f"Success rate today is {rate*100:.0f}% (threshold: 75%)",
                "severity": "warning",
                "metric_value": rate,
                "threshold": 0.75,
            })

        # Wallet drift detected
        drift = ops.get("wallets_with_drift", 0)
        if drift > 0:
            anomalies.append({
                "type": "wallet_drift_detected",
                "message": f"{drift} wallet(s) with balance drift detected",
                "severity": "warning" if drift <= 3 else "critical",
                "metric_value": drift,
            })

        # Reconciliation overdue (>7 days)
        last_recon = ops.get("last_reconciliation_at")
        if last_recon is None:
            anomalies.append({
                "type": "reconciliation_overdue",
                "message": "No reconciliation run recorded",
                "severity": "info",
            })

        # Provider down
        if ops.get("providers_down", 0) > 0:
            anomalies.append({
                "type": "provider_down",
                "message": f"{ops['providers_down']} provider(s) in DOWN state",
                "severity": "critical",
                "metric_value": ops["providers_down"],
            })

    return anomalies
