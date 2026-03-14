"""
Admin Operations Service — provider health, alerts, refund review.

Powers the admin operations dashboard with aggregated views.
Read-only queries; does not modify business data.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.db import USE_DB, query_all, query_one, Tables


# Known providers and their feature areas
_PROVIDER_FEATURES: Dict[str, str] = {
    "meshy":        "3d",
    "openai":       "image",
    "google":       "image",
    "vertex":       "video",
    "seedance":     "video",
    "fal_seedance": "video",
}

_ALERTS_TABLE = "timrx_billing.provider_alerts"


def _iso(val) -> Optional[str]:
    """Safely convert a datetime to ISO string."""
    if val and hasattr(val, "isoformat"):
        return val.isoformat()
    return val


# ─────────────────────────────────────────────────────────────────────────────
# PART A — Provider Health
# ─────────────────────────────────────────────────────────────────────────────

def get_provider_health() -> Dict[str, Any]:
    """
    Return per-provider operational summary.

    Status rules (documented):
      healthy  — success_rate_24h >= 0.80 AND no active critical alerts
      warning  — success_rate_24h >= 0.50 OR has active warning/critical alerts
      down     — success_rate_24h < 0.50 AND has recent failures (or only failures)
      unknown  — zero jobs in last 24h
    """
    # 1. Job counts by provider (last 24h)
    jobs_24h = query_all(f"""
        SELECT
            provider,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status IN ('succeeded', 'ready')) AS successes,
            COUNT(*) FILTER (WHERE status = 'failed') AS failures,
            MAX(CASE WHEN status IN ('succeeded', 'ready') THEN updated_at END) AS last_success_at,
            MAX(CASE WHEN status = 'failed' THEN updated_at END) AS last_failure_at
        FROM {Tables.JOBS}
        WHERE created_at > NOW() - INTERVAL '24 hours'
          AND provider IS NOT NULL
        GROUP BY provider
    """)

    # 2. Job counts (last 1h)
    jobs_1h = query_all(f"""
        SELECT provider, COUNT(*) AS total
        FROM {Tables.JOBS}
        WHERE created_at > NOW() - INTERVAL '1 hour'
          AND provider IS NOT NULL
        GROUP BY provider
    """)
    jobs_1h_map = {r["provider"]: r["total"] for r in jobs_1h}

    # 3. Top error codes per provider (last 24h, failed only)
    error_codes = query_all(f"""
        SELECT provider, last_error_code AS code, COUNT(*) AS cnt
        FROM {Tables.JOBS}
        WHERE created_at > NOW() - INTERVAL '24 hours'
          AND status = 'failed'
          AND last_error_code IS NOT NULL
        GROUP BY provider, last_error_code
        ORDER BY provider, cnt DESC
    """)
    # Group by provider
    errors_by_provider: Dict[str, List[Dict]] = {}
    for r in error_codes:
        p = r["provider"]
        if p not in errors_by_provider:
            errors_by_provider[p] = []
        if len(errors_by_provider[p]) < 5:
            errors_by_provider[p].append({"code": r["code"], "count": r["cnt"]})

    # 4. Active alerts from provider_alerts
    active_alerts = query_all(f"""
        SELECT provider, alert_type, severity, COUNT(*) AS cnt
        FROM {_ALERTS_TABLE}
        WHERE is_active = TRUE AND provider IS NOT NULL
        GROUP BY provider, alert_type, severity
    """)
    alerts_by_provider: Dict[str, Dict] = {}
    for r in active_alerts:
        p = r["provider"]
        if p not in alerts_by_provider:
            alerts_by_provider[p] = {"count": 0, "has_critical": False, "wallet_24h": 0}
        alerts_by_provider[p]["count"] += r["cnt"]
        if r["severity"] == "critical":
            alerts_by_provider[p]["has_critical"] = True

    # 5. Wallet alerts in last 24h
    wallet_alerts_24h = query_all(f"""
        SELECT provider, COUNT(*) AS cnt
        FROM {_ALERTS_TABLE}
        WHERE alert_type = 'wallet_depleted'
          AND last_seen_at > NOW() - INTERVAL '24 hours'
          AND provider IS NOT NULL
        GROUP BY provider
    """)
    wallet_24h_map = {r["provider"]: r["cnt"] for r in wallet_alerts_24h}

    # 6. Estimated spend today + this month (from estimated_provider_cost_gbp)
    spend_today = query_all(f"""
        SELECT provider,
               COALESCE(SUM(estimated_provider_cost_gbp), 0) AS spend
        FROM {Tables.JOBS}
        WHERE created_at >= DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')
          AND status IN ('succeeded', 'ready')
          AND provider IS NOT NULL
        GROUP BY provider
    """)
    spend_today_map = {r["provider"]: float(r["spend"]) for r in spend_today}

    spend_month = query_all(f"""
        SELECT provider,
               COALESCE(SUM(estimated_provider_cost_gbp), 0) AS spend
        FROM {Tables.JOBS}
        WHERE created_at >= DATE_TRUNC('month', NOW() AT TIME ZONE 'UTC')
          AND status IN ('succeeded', 'ready')
          AND provider IS NOT NULL
        GROUP BY provider
    """)
    spend_month_map = {r["provider"]: float(r["spend"]) for r in spend_month}

    # 7. Provider configuration checks
    config_status = _check_provider_configs()

    # 8. Build per-provider result
    # Collect all providers seen in data or config
    all_providers = set(_PROVIDER_FEATURES.keys())
    for r in jobs_24h:
        all_providers.add(r["provider"])

    jobs_24h_map = {r["provider"]: r for r in jobs_24h}

    providers = []
    summary_healthy = 0
    summary_warning = 0
    summary_down = 0

    for prov in sorted(all_providers):
        row = jobs_24h_map.get(prov, {})
        total = row.get("total", 0) or 0
        successes = row.get("successes", 0) or 0
        failures = row.get("failures", 0) or 0
        rate = round(successes / total, 2) if total > 0 else None
        alert_info = alerts_by_provider.get(prov, {})
        has_critical = alert_info.get("has_critical", False)

        # Status determination
        if total == 0:
            status = "unknown"
        elif rate is not None and rate < 0.50:
            status = "down"
            summary_down += 1
        elif has_critical or (rate is not None and rate < 0.80):
            status = "warning"
            summary_warning += 1
        else:
            status = "healthy"
            summary_healthy += 1

        providers.append({
            "provider": prov,
            "configured": config_status.get(prov, {}).get("configured", False),
            "feature_area": _PROVIDER_FEATURES.get(prov, "unknown"),
            "status": status,
            "jobs_1h": jobs_1h_map.get(prov, 0),
            "jobs_24h": total,
            "successes_24h": successes,
            "failures_24h": failures,
            "success_rate_24h": rate,
            "last_success_at": _iso(row.get("last_success_at")),
            "last_failure_at": _iso(row.get("last_failure_at")),
            "top_error_codes": errors_by_provider.get(prov, []),
            "wallet_alert_count_24h": wallet_24h_map.get(prov, 0),
            "active_alerts": alert_info.get("count", 0),
            "estimated_spend_today_gbp": round(spend_today_map.get(prov, 0), 2),
            "estimated_spend_month_gbp": round(spend_month_map.get(prov, 0), 2),
        })

    total_spend_today = round(sum(p["estimated_spend_today_gbp"] for p in providers), 2)
    total_spend_month = round(sum(p["estimated_spend_month_gbp"] for p in providers), 2)
    total_active_alerts = sum(p["active_alerts"] for p in providers)

    return {
        "providers": providers,
        "summary": {
            "provider_count": len(providers),
            "healthy_count": summary_healthy,
            "warning_count": summary_warning,
            "down_count": summary_down,
            "alerts_active": total_active_alerts,
            "estimated_spend_today_gbp": total_spend_today,
            "estimated_spend_month_gbp": total_spend_month,
        },
    }


def _check_provider_configs() -> Dict[str, Dict]:
    """Check which providers are configured. Returns {provider: {configured, error}}."""
    results = {}

    # Meshy — configured if MESHY_API_KEY is set
    try:
        import os
        results["meshy"] = {
            "configured": bool(os.getenv("MESHY_API_KEY")),
            "error": None if os.getenv("MESHY_API_KEY") else "MESHY_API_KEY not set",
        }
    except Exception:
        results["meshy"] = {"configured": False, "error": "check failed"}

    # OpenAI — configured if OPENAI_API_KEY is set
    try:
        import os
        results["openai"] = {
            "configured": bool(os.getenv("OPENAI_API_KEY")),
            "error": None if os.getenv("OPENAI_API_KEY") else "OPENAI_API_KEY not set",
        }
    except Exception:
        results["openai"] = {"configured": False, "error": "check failed"}

    # Google/Gemini — configured if GEMINI_API_KEY or GOOGLE_API_KEY is set
    try:
        import os
        key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        results["google"] = {
            "configured": bool(key),
            "error": None if key else "GEMINI_API_KEY not set",
        }
    except Exception:
        results["google"] = {"configured": False, "error": "check failed"}

    # Vertex
    try:
        from backend.services.vertex_video_service import check_vertex_configured
        ok, err = check_vertex_configured()
        results["vertex"] = {"configured": ok, "error": err}
    except Exception as e:
        results["vertex"] = {"configured": False, "error": str(e)}

    # Seedance
    try:
        from backend.services.seedance_service import check_seedance_configured
        ok, err = check_seedance_configured()
        results["seedance"] = {"configured": ok, "error": err}
    except Exception as e:
        results["seedance"] = {"configured": False, "error": str(e)}

    # fal Seedance
    try:
        from backend.services.fal_seedance_service import check_fal_seedance_configured
        ok, err = check_fal_seedance_configured()
        results["fal_seedance"] = {"configured": ok, "error": err}
    except Exception as e:
        results["fal_seedance"] = {"configured": False, "error": str(e)}

    return results


# ─────────────────────────────────────────────────────────────────────────────
# PART B — Alerts
# ─────────────────────────────────────────────────────────────────────────────

def list_alerts(
    *,
    active_only: bool = False,
    severity: Optional[str] = None,
    provider: Optional[str] = None,
    alert_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    """List alerts with optional filters."""
    conditions = []
    params: list = []

    if active_only:
        conditions.append("is_active = TRUE")
    if severity:
        conditions.append("severity = %s")
        params.append(severity)
    if provider:
        conditions.append("provider = %s")
        params.append(provider)
    if alert_type:
        conditions.append("alert_type = %s")
        params.append(alert_type)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Count
    count_row = query_one(
        f"SELECT COUNT(*) AS total FROM {_ALERTS_TABLE} {where}",
        tuple(params),
    )
    total = count_row["total"] if count_row else 0

    # Rows
    params.extend([limit, offset])
    rows = query_all(
        f"""
        SELECT id, alert_key, alert_type, provider, severity, subject,
               message, occurrence_count, first_seen_at, last_seen_at,
               last_sent_at, is_active, metadata,
               related_job_id, related_subscription_id
        FROM {_ALERTS_TABLE}
        {where}
        ORDER BY last_seen_at DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params),
    )

    alerts = []
    for r in rows:
        alerts.append({
            "id": str(r["id"]),
            "alert_key": r["alert_key"],
            "alert_type": r["alert_type"],
            "provider": r["provider"],
            "severity": r["severity"],
            "subject": r["subject"],
            "message": r["message"],
            "occurrence_count": r["occurrence_count"],
            "first_seen_at": _iso(r["first_seen_at"]),
            "last_seen_at": _iso(r["last_seen_at"]),
            "last_sent_at": _iso(r["last_sent_at"]),
            "is_active": r["is_active"],
            "metadata": r["metadata"],
            "related_job_id": str(r["related_job_id"]) if r["related_job_id"] else None,
            "related_subscription_id": str(r["related_subscription_id"]) if r["related_subscription_id"] else None,
        })

    return {"alerts": alerts, "total": total}


def resolve_alert(alert_id: str) -> bool:
    """Resolve an alert by ID (set is_active = false)."""
    from backend.db import get_conn
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {_ALERTS_TABLE}
                    SET is_active = FALSE
                    WHERE id = %s::uuid AND is_active = TRUE
                    RETURNING id
                    """,
                    (alert_id,),
                )
                row = cur.fetchone()
            conn.commit()
        return row is not None
    except Exception as e:
        print(f"[ADMIN_OPS] Failed to resolve alert {alert_id}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# PART C — Refund Review
# ─────────────────────────────────────────────────────────────────────────────

def get_refund_review(
    *,
    days: int = 30,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Refund review / decision-support endpoint.

    NOTE: TimrX does not currently have a formal "refund requests" table.
    This endpoint surfaces purchase + service-failure signals that an admin
    should review before deciding on refunds.

    It finds:
      1. Purchases where the user experienced high job failure rates
      2. Purchases in suspicious states (failed, pending for too long)
      3. Recent refunded purchases (for awareness)

    Returns enriched records with credits_used, credits_remaining,
    failure counts, and a conservative recommendation.
    """

    # 1. Find purchases with associated job failures (high-failure users)
    #    Join purchases → identities → jobs to find users who paid
    #    but had a bad experience.
    failure_candidates = query_all(f"""
        WITH recent_purchases AS (
            SELECT
                p.id AS purchase_id,
                p.identity_id,
                p.amount_gbp,
                p.credits_granted,
                p.status AS purchase_status,
                p.provider AS payment_provider,
                p.created_at AS purchase_date,
                i.email
            FROM {Tables.PURCHASES} p
            LEFT JOIN {Tables.IDENTITIES} i ON i.id = p.identity_id
            WHERE p.status = 'completed'
              AND p.created_at > NOW() - INTERVAL '%s days'
            ORDER BY p.created_at DESC
            LIMIT 200
        ),
        user_job_stats AS (
            SELECT
                rp.purchase_id,
                rp.identity_id,
                rp.email,
                rp.amount_gbp,
                rp.credits_granted,
                rp.purchase_status,
                rp.payment_provider,
                rp.purchase_date,
                COUNT(*) FILTER (WHERE j.status = 'failed')    AS failed_jobs_7d,
                COUNT(*) FILTER (WHERE j.status IN ('succeeded', 'ready')) AS success_jobs_7d,
                COUNT(*)                                        AS total_jobs_7d
            FROM recent_purchases rp
            LEFT JOIN {Tables.JOBS} j
                ON j.identity_id = rp.identity_id
               AND j.created_at > rp.purchase_date
               AND j.created_at < rp.purchase_date + INTERVAL '7 days'
            GROUP BY rp.purchase_id, rp.identity_id, rp.email,
                     rp.amount_gbp, rp.credits_granted,
                     rp.purchase_status, rp.payment_provider, rp.purchase_date
        )
        SELECT *
        FROM user_job_stats
        WHERE failed_jobs_7d >= 3
           OR (total_jobs_7d > 0 AND failed_jobs_7d::float / NULLIF(total_jobs_7d, 0) > 0.5)
        ORDER BY failed_jobs_7d DESC
        LIMIT %s
    """, (days, limit))

    # 2. Suspicious purchases (stuck pending > 1 hour, or failed)
    suspicious = query_all(f"""
        SELECT
            p.id AS purchase_id,
            p.identity_id,
            p.amount_gbp,
            p.credits_granted,
            p.status AS purchase_status,
            p.provider AS payment_provider,
            p.created_at AS purchase_date,
            i.email
        FROM {Tables.PURCHASES} p
        LEFT JOIN {Tables.IDENTITIES} i ON i.id = p.identity_id
        WHERE (
            (p.status = 'pending' AND p.created_at < NOW() - INTERVAL '1 hour')
            OR p.status = 'failed'
        )
        AND p.created_at > NOW() - INTERVAL '%s days'
        ORDER BY p.created_at DESC
        LIMIT %s
    """, (days, limit))

    # 3. Recent refunds (for awareness)
    recent_refunds = query_all(f"""
        SELECT
            p.id AS purchase_id,
            p.identity_id,
            p.amount_gbp,
            p.credits_granted,
            p.status AS purchase_status,
            p.provider AS payment_provider,
            p.created_at AS purchase_date,
            i.email
        FROM {Tables.PURCHASES} p
        LEFT JOIN {Tables.IDENTITIES} i ON i.id = p.identity_id
        WHERE p.status = 'refunded'
          AND p.created_at > NOW() - INTERVAL '%s days'
        ORDER BY p.created_at DESC
        LIMIT 20
    """, (days,))

    # 4. Enrich with wallet / credit usage data
    items = []
    seen_purchases = set()

    for row in failure_candidates:
        pid = str(row["purchase_id"])
        if pid in seen_purchases:
            continue
        seen_purchases.add(pid)
        items.append(_build_review_item(row, "high_failure_rate", "review"))

    for row in suspicious:
        pid = str(row["purchase_id"])
        if pid in seen_purchases:
            continue
        seen_purchases.add(pid)
        reason = "purchase_stuck_pending" if row["purchase_status"] == "pending" else "purchase_failed"
        items.append(_build_review_item(row, reason, "manual_only"))

    for row in recent_refunds:
        pid = str(row["purchase_id"])
        if pid in seen_purchases:
            continue
        seen_purchases.add(pid)
        items.append(_build_review_item(row, "already_refunded", "likely_not_refundable"))

    # 5. Enrich with subscription context
    _enrich_subscription_context(items)

    review_count = len([i for i in items if i["refund_recommendation"] == "review"])
    manual_count = len([i for i in items if i["refund_recommendation"] == "manual_only"])

    return {
        "items": items,
        "summary": {
            "review_count": len(items),
            "recommended_review_count": review_count,
            "recommended_manual_only_count": manual_count,
        },
    }


def _build_review_item(row: Dict, reason: str, recommendation: str) -> Dict:
    """Build a single refund review item with wallet enrichment."""
    identity_id = str(row["identity_id"])

    # Get wallet balance
    credits_used = 0
    credits_remaining = 0
    try:
        from backend.services.wallet_service import WalletService
        balance = WalletService.get_all_balances(identity_id)
        credits_remaining = balance.get("general", 0) + balance.get("video", 0)

        # Estimate credits used: granted - remaining (simplified)
        granted = row.get("credits_granted", 0) or 0
        credits_used = max(0, granted - credits_remaining)
    except Exception:
        pass

    return {
        "type": "purchase_review",
        "purchase_id": str(row["purchase_id"]),
        "identity_id": identity_id,
        "email": row.get("email"),
        "amount_gbp": float(row.get("amount_gbp", 0)),
        "credits_granted": row.get("credits_granted", 0),
        "credits_used": credits_used,
        "credits_remaining": credits_remaining,
        "purchase_status": row.get("purchase_status"),
        "is_subscription": False,  # Enriched later by _enrich_subscription_context
        "payment_provider": row.get("payment_provider"),
        "created_at": _iso(row.get("purchase_date")),
        "reason": reason,
        "refund_recommendation": recommendation,
        "related_failed_jobs_7d": row.get("failed_jobs_7d", 0) or 0,
        "related_success_jobs_7d": row.get("success_jobs_7d", 0) or 0,
    }


def _enrich_subscription_context(items: List[Dict]) -> None:
    """Check if any review items belong to subscription users."""
    if not items:
        return
    identity_ids = list({i["identity_id"] for i in items})
    if not identity_ids:
        return

    try:
        placeholders = ",".join(["%s"] * len(identity_ids))
        subs = query_all(
            f"""
            SELECT identity_id, id AS subscription_id, plan_code, status
            FROM {Tables.SUBSCRIPTIONS}
            WHERE identity_id::text IN ({placeholders})
              AND status IN ('active', 'cancelled')
            ORDER BY created_at DESC
            """,
            tuple(identity_ids),
        )
        sub_map = {}
        for s in subs:
            iid = str(s["identity_id"])
            if iid not in sub_map:
                sub_map[iid] = s

        for item in items:
            sub = sub_map.get(item["identity_id"])
            if sub:
                item["is_subscription"] = True
                item["subscription_id"] = str(sub["subscription_id"])
                item["subscription_plan"] = sub["plan_code"]
                item["subscription_status"] = sub["status"]
    except Exception as e:
        print(f"[ADMIN_OPS] Subscription enrichment error: {e}")
