"""
Health check routes.

Registered under /api/_mod during migration.
"""

from __future__ import annotations

from flask import Blueprint, jsonify

from backend.db import USE_DB, get_conn

bp = Blueprint("health", __name__)


@bp.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "source": "modular"})


@bp.route("/db-check", methods=["GET"])
def db_check():
    if not USE_DB:
        return jsonify({"ok": False, "error": "db_disabled", "source": "modular"}), 503
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                _ = cur.fetchone()
        return jsonify({"ok": True, "db": "connected", "source": "modular"})
    except Exception as e:
        print(f"[DB] modular db_check failed: {e}")
        return jsonify({"ok": False, "error": "db_query_failed", "source": "modular"}), 503
