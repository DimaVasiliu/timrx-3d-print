"""
Health check routes.

Registered under /api/_mod during migration.
"""

from __future__ import annotations

from flask import Blueprint, jsonify

from backend.config import config
from backend.db import USE_DB, get_conn, get_runtime_report, now_utc_iso

bp = Blueprint("health", __name__)


def _service_status_payload(check: str = "liveness"):
    db_report = get_runtime_report()
    readiness_ok = bool(db_report["ready"])
    return {
        "ok": True if check == "liveness" else readiness_ok,
        "service": "timrx-3d-backend",
        "check": check,
        "source": "modular",
        "service_status": "degraded" if db_report["mode"] == "degraded" else "ok",
        "time_utc": now_utc_iso(),
        "environment": "development" if config.IS_DEV else "production",
        "readiness": {
            "ready": readiness_ok,
            "reason": None if readiness_ok else (db_report.get("reason") or "Database-backed mode is unavailable"),
        },
        "dependencies": {
            "database": db_report,
        },
        "database": db_report,
    }


@bp.route("/health", methods=["GET"])
@bp.route("/status", methods=["GET"])
def health():
    return jsonify(_service_status_payload("liveness"))


@bp.route("/ready", methods=["GET"])
def ready():
    payload = _service_status_payload("readiness")
    return jsonify(payload), (200 if payload["ok"] else 503)


@bp.route("/db-check", methods=["GET"])
def db_check():
    db_report = get_runtime_report()
    if not USE_DB:
        return jsonify({
            "ok": False,
            "error": "db_disabled",
            "source": "modular",
            "service_status": "degraded",
            "database": db_report,
        }), 503
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                _ = cur.fetchone()
        return jsonify({
            "ok": True,
            "db": "connected",
            "source": "modular",
            "service_status": "ok",
            "database": db_report,
        })
    except Exception as e:
        print(f"[DB] modular db_check failed: {e}")
        return jsonify({
            "ok": False,
            "error": "db_query_failed",
            "source": "modular",
            "service_status": "degraded",
            "database": db_report,
        }), 503
