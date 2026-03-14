"""
Provider Ledger Service — manual provider finance tracking + monthly spend reporting.

Manages the provider_ledger table for recording:
  - Top-ups (prepaid wallet credits)
  - Invoices (monthly charges from providers)
  - Balance snapshots (manual current-balance records)
  - Adjustments (corrections, credits, refunds from providers)
  - Notes (free-text operational notes)

Also provides monthly spend reporting that combines:
  - Estimated usage cost from jobs.estimated_provider_cost_gbp
  - Manual ledger entries (top-ups, invoices, etc.)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from backend.db import USE_DB, get_conn, query_all, query_one, Tables

_TABLE = Tables.PROVIDER_LEDGER
_VALID_ENTRY_TYPES = {"topup", "invoice", "balance_snapshot", "adjustment", "note"}
_AMOUNT_REQUIRED_TYPES = {"topup", "invoice", "adjustment"}


def _iso(val) -> Optional[str]:
    if val and hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val) if val is not None else None


# ─────────────────────────────────────────────────────────────────────────────
# LIST
# ─────────────────────────────────────────────────────────────────────────────

def list_ledger_entries(
    *,
    provider: Optional[str] = None,
    entry_type: Optional[str] = None,
    month: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    """List provider ledger entries with optional filters."""
    conditions: list = []
    params: list = []

    if provider:
        conditions.append("provider = %s")
        params.append(provider)
    if entry_type:
        conditions.append("entry_type = %s")
        params.append(entry_type)
    if month:
        # Accept YYYY-MM or YYYY-MM-DD, normalize to first of month
        try:
            parts = month.strip().split("-")
            m_date = date(int(parts[0]), int(parts[1]), 1)
            conditions.append("period_month = %s")
            params.append(m_date)
        except (ValueError, IndexError):
            pass  # ignore bad month format

    # Date range filters on created_at (inclusive end date)
    if date_from:
        try:
            d = date.fromisoformat(date_from.strip())
            conditions.append("created_at >= %s")
            params.append(d)
        except ValueError:
            pass
    if date_to:
        try:
            d = date.fromisoformat(date_to.strip())
            conditions.append("created_at < %s + INTERVAL '1 day'")
            params.append(d)
        except ValueError:
            pass

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    count_row = query_one(
        f"SELECT COUNT(*) AS total FROM {_TABLE} {where}",
        tuple(params),
    )
    total = count_row["total"] if count_row else 0

    params.extend([limit, offset])
    rows = query_all(
        f"""
        SELECT id, provider, entry_type, amount_gbp, currency,
               balance_snapshot_gbp, description, reference,
               period_month, metadata, recorded_by, created_at
        FROM {_TABLE}
        {where}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params),
    )

    entries = []
    for r in rows:
        entries.append({
            "id": str(r["id"]),
            "provider": r["provider"],
            "entry_type": r["entry_type"],
            "amount_gbp": float(r["amount_gbp"]) if r["amount_gbp"] is not None else None,
            "currency": r["currency"],
            "balance_snapshot_gbp": float(r["balance_snapshot_gbp"]) if r["balance_snapshot_gbp"] is not None else None,
            "description": r["description"],
            "reference": r["reference"],
            "period_month": _iso(r["period_month"]),
            "metadata": r["metadata"],
            "recorded_by": r["recorded_by"],
            "created_at": _iso(r["created_at"]),
        })

    print(f"[ADMIN_PROVIDER_LEDGER] listed entries={len(entries)} total={total}")
    return {"entries": entries, "total": total}


# ─────────────────────────────────────────────────────────────────────────────
# CREATE
# ─────────────────────────────────────────────────────────────────────────────

def create_ledger_entry(
    *,
    provider: str,
    entry_type: str,
    amount_gbp: Optional[float] = None,
    currency: str = "GBP",
    balance_snapshot_gbp: Optional[float] = None,
    description: Optional[str] = None,
    reference: Optional[str] = None,
    period_month: Optional[str] = None,
    metadata: Optional[dict] = None,
    recorded_by: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a provider ledger entry.

    Validates:
      - provider is required
      - entry_type must be one of: topup, invoice, balance_snapshot, adjustment, note
      - amount_gbp is required for topup, invoice, adjustment
      - balance_snapshot_gbp is only meaningful for balance_snapshot type
      - period_month is normalized to first day of month
    """
    # Validate
    if not provider or not provider.strip():
        raise ValueError("provider is required")
    provider = provider.strip().lower()

    if entry_type not in _VALID_ENTRY_TYPES:
        raise ValueError(f"entry_type must be one of: {', '.join(sorted(_VALID_ENTRY_TYPES))}")

    if entry_type in _AMOUNT_REQUIRED_TYPES and amount_gbp is None:
        raise ValueError(f"amount_gbp is required for entry_type={entry_type}")

    # Normalize period_month
    period_date = None
    if period_month:
        try:
            parts = str(period_month).strip().split("-")
            period_date = date(int(parts[0]), int(parts[1]), 1)
        except (ValueError, IndexError):
            raise ValueError("period_month must be YYYY-MM or YYYY-MM-DD format")

    meta_json = json.dumps(metadata, default=str) if metadata else None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_TABLE}
                    (provider, entry_type, amount_gbp, currency,
                     balance_snapshot_gbp, description, reference,
                     period_month, metadata, recorded_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                RETURNING id, created_at
                """,
                (
                    provider,
                    entry_type,
                    amount_gbp,
                    currency,
                    balance_snapshot_gbp,
                    (description or "")[:2000] if description else None,
                    (reference or "")[:500] if reference else None,
                    period_date,
                    meta_json,
                    recorded_by,
                ),
            )
            row = cur.fetchone()
        conn.commit()

    entry_id = str(row["id"])
    created_at = _iso(row["created_at"])

    amt_str = f"{amount_gbp:.2f}" if amount_gbp is not None else "null"
    print(f"[ADMIN_PROVIDER_LEDGER] created provider={provider} type={entry_type} amount={amt_str}")

    return {
        "id": entry_id,
        "provider": provider,
        "entry_type": entry_type,
        "amount_gbp": float(amount_gbp) if amount_gbp is not None else None,
        "currency": currency,
        "balance_snapshot_gbp": float(balance_snapshot_gbp) if balance_snapshot_gbp is not None else None,
        "description": description,
        "reference": reference,
        "period_month": _iso(period_date),
        "metadata": metadata,
        "recorded_by": recorded_by,
        "created_at": created_at,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MONTHLY SPEND REPORT
# ─────────────────────────────────────────────────────────────────────────────

def get_monthly_spend_report(
    *,
    months: int = 3,
    provider: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Monthly provider spend report combining estimated job costs + manual ledger.

    Returns per-provider, per-month breakdown with:
      - estimated_usage_gbp: sum of jobs.estimated_provider_cost_gbp
      - job_count: number of succeeded/ready jobs
      - ledger totals by entry_type (topups, invoices, adjustments)
      - latest balance snapshot if available
    """
    months = min(max(months, 1), 24)

    prov_filter = ""
    prov_params: tuple = ()
    if provider:
        prov_filter = "AND provider = %s"
        prov_params = (provider,)

    # 1. Estimated usage from jobs (grouped by provider + month)
    usage_rows = query_all(
        f"""
        SELECT
            provider,
            DATE_TRUNC('month', created_at)::date AS month,
            COALESCE(SUM(estimated_provider_cost_gbp), 0) AS estimated_usage_gbp,
            COUNT(*) AS job_count
        FROM {Tables.JOBS}
        WHERE status IN ('succeeded', 'ready')
          AND created_at >= DATE_TRUNC('month', NOW()) - INTERVAL '%s months'
          AND provider IS NOT NULL
          {prov_filter}
        GROUP BY provider, DATE_TRUNC('month', created_at)::date
        ORDER BY month DESC, provider
        """,
        (months, *prov_params),
    )

    # 2. Ledger entries aggregated by provider + month + entry_type
    ledger_rows = query_all(
        f"""
        SELECT
            provider,
            COALESCE(period_month, DATE_TRUNC('month', created_at)::date) AS month,
            entry_type,
            COALESCE(SUM(amount_gbp), 0) AS total_gbp,
            COUNT(*) AS entry_count
        FROM {_TABLE}
        WHERE created_at >= DATE_TRUNC('month', NOW()) - INTERVAL '%s months'
          {prov_filter}
        GROUP BY provider,
                 COALESCE(period_month, DATE_TRUNC('month', created_at)::date),
                 entry_type
        ORDER BY month DESC, provider
        """,
        (months, *prov_params),
    )

    # 3. Latest balance snapshots per provider
    snapshot_rows = query_all(
        f"""
        SELECT DISTINCT ON (provider)
            provider,
            balance_snapshot_gbp,
            created_at
        FROM {_TABLE}
        WHERE entry_type = 'balance_snapshot'
          AND balance_snapshot_gbp IS NOT NULL
          {prov_filter}
        ORDER BY provider, created_at DESC
        """,
        prov_params if prov_params else (),
    )
    snapshots = {
        r["provider"]: {
            "balance_gbp": float(r["balance_snapshot_gbp"]),
            "recorded_at": _iso(r["created_at"]),
        }
        for r in snapshot_rows
    }

    # 4. Build nested structure: { provider -> { month -> data } }
    data: Dict[str, Dict[str, Dict]] = {}

    for r in usage_rows:
        prov = r["provider"]
        month_str = _iso(r["month"])
        if prov not in data:
            data[prov] = {}
        if month_str not in data[prov]:
            data[prov][month_str] = _empty_month()
        data[prov][month_str]["estimated_usage_gbp"] = round(float(r["estimated_usage_gbp"]), 2)
        data[prov][month_str]["job_count"] = r["job_count"]

    for r in ledger_rows:
        prov = r["provider"]
        month_str = _iso(r["month"])
        if prov not in data:
            data[prov] = {}
        if month_str not in data[prov]:
            data[prov][month_str] = _empty_month()

        et = r["entry_type"]
        total = round(float(r["total_gbp"]), 2)
        count = r["entry_count"]

        if et == "topup":
            data[prov][month_str]["topups_gbp"] = total
            data[prov][month_str]["topup_count"] = count
        elif et == "invoice":
            data[prov][month_str]["invoices_gbp"] = total
            data[prov][month_str]["invoice_count"] = count
        elif et == "adjustment":
            data[prov][month_str]["adjustments_gbp"] = total
            data[prov][month_str]["adjustment_count"] = count

    # 5. Flatten to list format
    report: List[Dict] = []
    for prov in sorted(data.keys()):
        for month_str in sorted(data[prov].keys(), reverse=True):
            entry = data[prov][month_str]
            entry["provider"] = prov
            entry["month"] = month_str
            report.append(entry)

    provider_count = len(data)
    print(f"[ADMIN_PROVIDER_SPEND_MONTHLY] months={months} providers={provider_count}")

    return {
        "report": report,
        "latest_snapshots": snapshots,
        "months_requested": months,
        "provider_count": provider_count,
    }


def _empty_month() -> Dict[str, Any]:
    return {
        "estimated_usage_gbp": 0.0,
        "job_count": 0,
        "topups_gbp": 0.0,
        "topup_count": 0,
        "invoices_gbp": 0.0,
        "invoice_count": 0,
        "adjustments_gbp": 0.0,
        "adjustment_count": 0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PROVIDER BALANCE SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

# Known providers — mirrors admin_ops_service._PROVIDER_FEATURES
_KNOWN_PROVIDERS = {"meshy", "openai", "google", "vertex", "seedance", "fal_seedance"}

# Thresholds
_STALE_SNAPSHOT_DAYS = 7          # snapshot older than this → warning
_ACTION_NEEDED_SNAPSHOT_DAYS = 14  # snapshot older than this → action_needed
_LOW_BALANCE_GBP = 10.0           # balance below this → warning
_CRITICAL_BALANCE_GBP = 3.0       # balance below this → action_needed


def get_provider_balances() -> Dict[str, Any]:
    """
    Provider balance summary: latest snapshot, estimated spend since snapshot,
    wallet alerts, and computed balance_status per provider.

    Status logic:
      action_needed — snapshot stale >14d, balance <£3, or active wallet_depleted alert
      warning       — snapshot stale >7d, balance <£10
      ok            — recent snapshot, balance looks fine
      unknown       — no snapshot recorded
    """
    now = datetime.now(timezone.utc)

    # 1. Latest balance snapshot per provider
    snapshot_rows = query_all(f"""
        SELECT DISTINCT ON (provider)
            provider, balance_snapshot_gbp, created_at, description
        FROM {_TABLE}
        WHERE entry_type = 'balance_snapshot'
          AND balance_snapshot_gbp IS NOT NULL
        ORDER BY provider, created_at DESC
    """)
    snapshots = {}
    for r in snapshot_rows:
        snapshots[r["provider"]] = {
            "balance_gbp": float(r["balance_snapshot_gbp"]),
            "recorded_at": r["created_at"],
            "description": r["description"],
        }

    # 2. Estimated spend per provider since their last snapshot
    spend_since = {}
    for prov, snap in snapshots.items():
        row = query_one(
            f"""
            SELECT COALESCE(SUM(estimated_provider_cost_gbp), 0) AS spend,
                   COUNT(*) AS job_count
            FROM {Tables.JOBS}
            WHERE provider = %s
              AND status IN ('succeeded', 'ready')
              AND created_at > %s
            """,
            (prov, snap["recorded_at"]),
        )
        if row:
            spend_since[prov] = {
                "estimated_spend_gbp": round(float(row["spend"]), 2),
                "job_count": row["job_count"],
            }

    # 3. Active wallet_depleted alerts (last 7 days)
    _ALERTS_TABLE = "timrx_billing.provider_alerts"
    wallet_alerts = query_all(f"""
        SELECT provider, COUNT(*) AS cnt,
               MAX(last_seen_at) AS latest
        FROM {_ALERTS_TABLE}
        WHERE alert_type = 'wallet_depleted'
          AND last_seen_at > NOW() - INTERVAL '7 days'
          AND provider IS NOT NULL
        GROUP BY provider
    """)
    wallet_alert_map = {
        r["provider"]: {"count": r["cnt"], "latest": _iso(r["latest"])}
        for r in wallet_alerts
    }

    # 4. Build per-provider result
    all_providers = _KNOWN_PROVIDERS | set(snapshots.keys())
    providers = []
    action_needed_count = 0
    warning_count = 0

    for prov in sorted(all_providers):
        snap = snapshots.get(prov)
        spend = spend_since.get(prov, {"estimated_spend_gbp": 0.0, "job_count": 0})
        wallet = wallet_alert_map.get(prov)

        if snap is None:
            status = "unknown"
            days_since = None
            balance = None
            estimated_remaining = None
        else:
            days_since = (now - snap["recorded_at"].replace(tzinfo=timezone.utc
                          if snap["recorded_at"].tzinfo is None else snap["recorded_at"].tzinfo)).days
            balance = snap["balance_gbp"]
            estimated_remaining = round(balance - spend["estimated_spend_gbp"], 2)

            # Status determination
            has_wallet_alert = wallet is not None
            if (days_since >= _ACTION_NEEDED_SNAPSHOT_DAYS
                    or estimated_remaining <= _CRITICAL_BALANCE_GBP
                    or has_wallet_alert):
                status = "action_needed"
            elif (days_since >= _STALE_SNAPSHOT_DAYS
                  or estimated_remaining <= _LOW_BALANCE_GBP):
                status = "warning"
            else:
                status = "ok"

        if status == "action_needed":
            action_needed_count += 1
        elif status == "warning":
            warning_count += 1

        entry = {
            "provider": prov,
            "balance_status": status,
            "last_snapshot_gbp": balance,
            "last_snapshot_at": _iso(snap["recorded_at"]) if snap else None,
            "last_snapshot_description": snap["description"] if snap else None,
            "days_since_snapshot": days_since,
            "estimated_spend_since_gbp": spend["estimated_spend_gbp"],
            "jobs_since_snapshot": spend["job_count"],
            "estimated_remaining_gbp": estimated_remaining,
            "wallet_alerts_7d": wallet["count"] if wallet else 0,
            "wallet_alert_latest": wallet["latest"] if wallet else None,
        }
        providers.append(entry)

    # Sort: action_needed first, then warning, then ok, then unknown
    _STATUS_ORDER = {"action_needed": 0, "warning": 1, "ok": 2, "unknown": 3}
    providers.sort(key=lambda p: (_STATUS_ORDER.get(p["balance_status"], 9), p["provider"]))

    print(f"[ADMIN_PROVIDER_BALANCES] providers={len(providers)} action_needed={action_needed_count} warning={warning_count}")

    return {
        "providers": providers,
        "summary": {
            "provider_count": len(providers),
            "action_needed_count": action_needed_count,
            "warning_count": warning_count,
        },
    }
