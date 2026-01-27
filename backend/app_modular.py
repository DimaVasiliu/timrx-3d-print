
"""
True modular entrypoint.

This builds a new Flask app and registers the modular blueprints under /api.
It does NOT import the monolith module.
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
        # with_session middleware will overwrite this when present.
        g.identity_id = None
        g.user_id = None

    # Import and register modular blueprints.
    from backend.routes.health import bp as health_bp
    from backend.routes.assets import bp as assets_bp
    from backend.routes.image_gen import bp as image_gen_bp
    from backend.routes.text_to_3d import bp as text_to_3d_bp
    from backend.routes.image_to_3d import bp as image_to_3d_bp
    from backend.routes.mesh_operations import bp as mesh_ops_bp
    from backend.routes.history import bp as history_bp
    from backend.routes.community import bp as community_bp
    from backend.routes import register_blueprints

    # Register under the /api/_mod prefix to keep compatibility with frontend.
    for bp in (
        health_bp,
        assets_bp,
        image_gen_bp,
        text_to_3d_bp,
        image_to_3d_bp,
        mesh_ops_bp,
        history_bp,
        community_bp,
    ):
        app.register_blueprint(bp, url_prefix="/api/_mod")

    # Register core API routes (auth, billing, credits, etc.) under /api.
    register_blueprints(app)

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5001")))
