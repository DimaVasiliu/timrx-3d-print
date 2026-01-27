"""True modular entrypoint.

This builds a new Flask app and registers all blueprints (core + migrated).
"""

from __future__ import annotations

import os
import re

from flask import Flask, g
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
        allow_headers=["Content-Type", "Authorization"],
        expose_headers=["Content-Type"],
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    )

    @app.before_request
    def _identity_default():
        g.identity_id = None
        g.user_id = None

    from backend.routes import register_blueprints

    register_blueprints(app)
    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")))
