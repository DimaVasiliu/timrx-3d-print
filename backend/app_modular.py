"""Canonical 3D backend entrypoint.

Use the same Flask app object everywhere:
- Local dev: ``flask --app app_modular:app run --host 0.0.0.0 --port 5001``
- Deploy: ``gunicorn app_modular:app``

This module builds the Flask app and registers all blueprints (core + migrated).
"""

from __future__ import annotations

import os
import re

from flask import Flask, g, jsonify, request
from flask_cors import CORS

from backend.config import config


def create_app() -> Flask:
    app = Flask(__name__)

    if config.IS_DEV:
        config.print_summary()
    config_warnings = config.validate()
    if config_warnings:
        print("[CONFIG] Startup configuration warnings:")
        for warning in config_warnings:
            print(f"[CONFIG][WARN] {warning}")

    from backend.db import get_runtime_report, init_db

    try:
        init_db()
    except Exception as e:
        print(f"[APP] Fatal database startup error: {e}")
        raise

    app.config["DB_RUNTIME_REPORT"] = get_runtime_report()
    if app.config["DB_RUNTIME_REPORT"]["mode"] == "degraded":
        print("[APP] Starting 3D backend in degraded mode")
        for limitation in app.config["DB_RUNTIME_REPORT"]["disabled_capabilities"]:
            print(f"[APP][DEGRADED] {limitation['id']}: {limitation['summary']}")

    # CORS configuration (mirrors monolith behavior)
    if config.ALLOW_ALL_ORIGINS:
        origins = [re.compile(r".*")]
    else:
        origins = config.ALLOWED_ORIGINS

    CORS(
        app,
        resources={r"/api/*": {"origins": origins}},
        supports_credentials=True,
        allow_headers=["Content-Type", "Authorization", "Idempotency-Key", "X-Requested-With", "X-Admin-Token"],
        expose_headers=["Content-Type"],
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )

    @app.before_request
    def _identity_default():
        g.identity_id = None
        g.user_id = None
        g._identity_resolved = False
        g._identity_source = None
        g._identity_touch_done = False
        g._cached_identity = None

    # ── Lightweight per-request diagnostic (auth efficiency) ──
    _AUTH_DIAG = os.getenv("AUTH_DIAG", "false").lower() in ("true", "1", "yes")

    @app.after_request
    def _auth_diag(response):
        if not _AUTH_DIAG:
            return response
        try:
            source = getattr(g, '_identity_source', None)
            if source:
                sid = getattr(g, 'session_id', None)
                sid_short = sid[:8] + "..." if sid else "none"
                touched = getattr(g, '_identity_touch_done', False)
                print(
                    f"[AUTH_DIAG] {request.method} {request.path} "
                    f"sid={sid_short} source={source} "
                    f"touched={touched} status={response.status_code}"
                )
        except Exception:
            pass
        return response

    # ─────────────────────────────────────────────────────────────
    # Origin validation for state-changing requests.
    #
    # CORS is browser-enforced.  Non-browser clients (curl, scripts)
    # can bypass it.  This server-side check rejects requests with an
    # Origin header that is not in ALLOWED_ORIGINS.
    #
    # Requests WITHOUT an Origin header are ALLOWED — this covers:
    #   - non-browser clients (curl, Postman, server-to-server)
    #   - same-origin requests (some browsers omit Origin)
    #   - webhook callbacks from payment providers
    #
    # Webhook routes are explicitly exempt: Mollie/Stripe call these
    # from their servers, never from a browser, and may or may not
    # send an Origin header.
    # ─────────────────────────────────────────────────────────────
    @app.before_request
    def _check_origin():
        if request.method not in ('POST', 'PUT', 'PATCH', 'DELETE'):
            return  # GET/HEAD/OPTIONS are safe

        # Webhook endpoints are called by payment providers, not browsers
        if request.path.startswith('/api/billing/webhook'):
            return

        origin = request.headers.get('Origin')
        if not origin:
            return  # No Origin header → non-browser client, allow

        # Wildcard CORS config → allow all origins
        if config.ALLOW_ALL_ORIGINS:
            return

        if origin not in config.ALLOWED_ORIGINS:
            print(f"[SECURITY] Origin rejected: origin={origin} path={request.path} method={request.method}")
            return jsonify({"ok": False, "error": "origin_not_allowed"}), 403

    from backend.routes import register_blueprints

    register_blueprints(app)

    # ─────────────────────────────────────────────────────────────
    # Legacy compat: /api/wallet (same as /api/credits/wallet)
    # This matches the legacy app.py.backup route exactly
    # ─────────────────────────────────────────────────────────────
    @app.route("/api/wallet", methods=["GET"])
    def api_wallet_compat():
        """Legacy /api/wallet endpoint for backward compatibility."""
        from backend.services.identity_service import IdentityService
        from backend.services.wallet_service import WalletService

        # Get session from cookie
        from flask import request

        session_id = IdentityService.get_session_id_from_request(request)
        if not session_id:
            return jsonify({
                "error": {
                    "code": "UNAUTHORIZED",
                    "message": "No valid session",
                }
            }), 401

        # Validate session
        identity = IdentityService.validate_session(session_id)
        if not identity:
            return jsonify({
                "error": {
                    "code": "UNAUTHORIZED",
                    "message": "Invalid or expired session",
                }
            }), 401

        identity_id = str(identity["id"])

        # Get balance
        try:
            balance = WalletService.get_balance(identity_id) if WalletService else 0
            return jsonify({
                "identity_id": identity_id,
                "credits_balance": balance,
            })
        except Exception as e:
            print(f"[WALLET] Error fetching balance: {e}")
            return jsonify({
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "Failed to fetch wallet balance",
                }
            }), 500

    # ─────────────────────────────────────────────────────────────
    # Startup seeding: ensure action_costs & plans exist in DB
    # ─────────────────────────────────────────────────────────────
    if app.config["DB_RUNTIME_REPORT"]["ready"]:
        try:
            from backend.services.pricing_service import PricingService

            PricingService.seed_action_costs()
            PricingService.seed_plans()
        except Exception as e:
            print(f"[APP] Warning: Failed to seed pricing data: {e}")

        # ── Recover stale jobs and start durable worker ───────────
        # After a deploy/restart, mark orphaned jobs as stalled so the
        # durable worker loop picks them up. Then start the worker.
        try:
            from backend.services.job_recovery import recover_stale_jobs

            result = recover_stale_jobs(app)
            if result.get("recovered", 0) > 0 or result.get("abandoned", 0) > 0:
                print(f"[APP] Job recovery: {result}")
        except Exception as e:
            print(f"[APP] Warning: Stale job recovery failed: {e}")

        # Start the durable job worker (DB-driven, restart-safe).
        # Each Gunicorn process spawns a worker thread, but only one
        # acquires the PostgreSQL advisory lock (leader election).
        # Non-leaders exit immediately.
        try:
            from backend.services.job_worker import start_worker

            start_worker()
        except Exception as e:
            print(f"[APP] Warning: Failed to start job worker: {e}")

        # Start the operations loop (stall detection + stale sweep + rescue).
        # Runs on all processes since it's idempotent and lightweight.
        # Config-driven intervals from STALE_SWEEP_* and RESCUE_* env vars.
        try:
            from backend.services.job_worker import start_operations_loop

            start_operations_loop()
        except Exception as e:
            print(f"[APP] Warning: Failed to start operations loop: {e}")

    return app


app = create_app()
application = app  # WSGI compatibility alias


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")))
