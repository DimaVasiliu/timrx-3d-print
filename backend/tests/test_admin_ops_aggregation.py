from __future__ import annotations

import importlib
import os
import sys
import types
from datetime import date, datetime, timezone

# Ensure backend package is importable
backend_root = os.path.join(os.path.dirname(__file__), "..", "..")
services_root = os.path.join(backend_root, "backend", "services")

if "backend" not in sys.modules:
    backend_pkg = types.ModuleType("backend")
    backend_pkg.__path__ = [os.path.join(backend_root, "backend")]
    sys.modules["backend"] = backend_pkg

if "backend.services" not in sys.modules:
    services_pkg = types.ModuleType("backend.services")
    services_pkg.__path__ = [services_root]
    sys.modules["backend.services"] = services_pkg

sys.path.insert(0, backend_root)

admin_ops_service = importlib.import_module("backend.services.admin_ops_service")
provider_ledger_service = importlib.import_module("backend.services.provider_ledger_service")


def test_monthly_spend_report_rolls_up_shared_provider_accounts(monkeypatch):
    month = date(2026, 4, 1)
    created_at = datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc)

    def fake_query_all(sql, params=()):
        if "GROUP BY provider, DATE_TRUNC('month', created_at)::date" in sql:
            return [
                {"provider": "google", "month": month, "estimated_usage_gbp": 3.0, "job_count": 2},
                {"provider": "google_nano", "month": month, "estimated_usage_gbp": 1.5, "job_count": 1},
                {"provider": "seedance", "month": month, "estimated_usage_gbp": 4.0, "job_count": 3},
                {"provider": "nano_banana", "month": month, "estimated_usage_gbp": 2.0, "job_count": 5},
            ]
        if "COALESCE(period_month, DATE_TRUNC('month', created_at)::date) AS month" in sql:
            return [
                {"provider": "google", "month": month, "entry_type": "invoice", "total_gbp": 10.0, "entry_count": 1},
                {"provider": "piapi", "month": month, "entry_type": "topup", "total_gbp": 25.0, "entry_count": 1},
            ]
        if "WHERE entry_type = 'balance_snapshot'" in sql:
            return [
                {"provider": "google", "balance_snapshot_gbp": 50.0, "created_at": created_at},
                {"provider": "seedance", "balance_snapshot_gbp": 20.0, "created_at": created_at},
            ]
        raise AssertionError(f"Unexpected query: {sql}")

    monkeypatch.setattr(provider_ledger_service, "query_all", fake_query_all)

    result = provider_ledger_service.get_monthly_spend_report(months=3)
    report = {(row["provider"], row["month"]): row for row in result["report"]}

    google = report[("google", "2026-04-01")]
    assert google["estimated_usage_gbp"] == 4.5
    assert google["job_count"] == 3
    assert google["display_label"] == "Google Account"

    piapi = report[("piapi", "2026-04-01")]
    assert piapi["estimated_usage_gbp"] == 6.0
    assert piapi["job_count"] == 8
    assert piapi["display_label"] == "PiAPI Account"

    assert result["latest_snapshots"]["piapi"]["balance_gbp"] == 20.0
    assert result["latest_snapshots"]["piapi"]["display_label"] == "PiAPI Account"


def test_provider_balances_roll_up_wallet_alerts_to_account(monkeypatch):
    snapshot_at = datetime(2026, 4, 5, 12, 0, tzinfo=timezone.utc)
    now = datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)

    def fake_query_all(sql, params=()):
        if "SELECT DISTINCT ON (provider)" in sql:
            return [
                {"provider": "piapi", "balance_snapshot_gbp": 30.0, "created_at": snapshot_at, "description": "wallet"},
                {"provider": "google", "balance_snapshot_gbp": 15.0, "created_at": snapshot_at, "description": "console"},
            ]
        if "MAX(last_seen_at) AS latest" in sql:
            return [
                {"provider": "seedance", "cnt": 2, "latest": snapshot_at},
                {"provider": "google_nano", "cnt": 1, "latest": snapshot_at},
            ]
        if "GROUP BY provider, action_code" in sql:
            return [
                {"provider": "seedance", "action_code": "seedance_fast_text_generate_5s", "job_count": 3, "cost_gbp": 4.0, "credits": 40},
                {"provider": "nano_banana", "action_code": "piapi_image_generate_4k", "job_count": 2, "cost_gbp": 2.0, "credits": 20},
            ]
        raise AssertionError(f"Unexpected query_all: {sql}")

    def fake_query_one(sql, params=()):
        if "COALESCE(SUM(estimated_provider_cost_gbp), 0) AS spend" in sql:
            params = tuple(params)
            if "seedance" in params or "nano_banana" in params:
                return {"spend": 6.0, "credits": 60, "job_count": 5}
            if "google" in params or "google_nano" in params:
                return {"spend": 2.5, "credits": 25, "job_count": 4}
        raise AssertionError(f"Unexpected query_one: {sql} params={params}")

    monkeypatch.setattr(provider_ledger_service, "query_all", fake_query_all)
    monkeypatch.setattr(provider_ledger_service, "query_one", fake_query_one)
    monkeypatch.setattr(provider_ledger_service, "datetime", FixedDateTime)

    result = provider_ledger_service.get_provider_balances()
    providers = {row["provider"]: row for row in result["providers"]}

    assert providers["piapi"]["wallet_alerts_7d"] == 2
    assert providers["piapi"]["balance_status"] == "action_needed"
    assert providers["piapi"]["display_label"] == "PiAPI Account"
    assert providers["piapi"]["breakdown"]["nano_banana_4k_count"] == 2

    assert providers["google"]["wallet_alerts_7d"] == 1
    assert providers["google"]["display_label"] == "Google Account"


def test_provider_health_marks_alert_only_provider_as_warning(monkeypatch):
    def fake_query_all(sql, params=()):
        if "COUNT(*) FILTER (WHERE status IN ('succeeded', 'ready')) AS successes" in sql:
            return []
        if "WHERE created_at > NOW() - INTERVAL '1 hour'" in sql:
            return []
        if "last_error_code AS code" in sql:
            return []
        if "avg_latency_ms" in sql:
            return []
        if "WHERE is_active = TRUE" in sql:
            return [{"provider": "openai", "alert_type": "wallet_depleted", "severity": "critical", "cnt": 1}]
        if "WHERE alert_type = 'wallet_depleted'" in sql:
            return [{"provider": "openai", "cnt": 1}]
        if "SUM(estimated_provider_cost_gbp)" in sql:
            return []
        if "SUM(cost_credits)" in sql:
            return []
        raise AssertionError(f"Unexpected query: {sql}")

    monkeypatch.setattr(admin_ops_service, "query_all", fake_query_all)
    monkeypatch.setattr(
        admin_ops_service,
        "_check_provider_configs",
        lambda: {"openai": {"configured": True, "error": None}},
    )

    result = admin_ops_service.get_provider_health()
    providers = {row["provider"]: row for row in result["providers"]}

    assert providers["openai"]["status"] == "warning"
    assert providers["openai"]["display_label"] == "OpenAI"
    assert providers["openai"]["active_alerts"] == 1
