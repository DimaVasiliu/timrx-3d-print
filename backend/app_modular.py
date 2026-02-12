"""True modular entrypoint.

This builds a new Flask app and registers all blueprints (core + migrated).
"""

from __future__ import annotations

import os
import re

from flask import Flask, g, jsonify
from flask_cors import CORS

from backend.config import config


def create_app() -> Flask:
    app = Flask(__name__)

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

    from backend.routes import register_blueprints

    register_blueprints(app)

    # ─────────────────────────────────────────────────────────────
    # Legacy compat: /api/wallet (same as /api/credits/wallet)
    # This matches the legacy app.py route exactly
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
    from backend.db import USE_DB

    if USE_DB:
        try:
            from backend.services.pricing_service import PricingService

            PricingService.seed_action_costs()
            PricingService.seed_plans()
        except Exception as e:
            print(f"[APP] Warning: Failed to seed pricing data: {e}")

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")))
