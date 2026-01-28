"""
Routes package for TimrX Backend.
Contains Flask Blueprints for different API namespaces.
"""

__all__ = [
    "register_blueprints",
]


def register_blueprints(app):
    """Register all blueprints with the Flask app."""
    from .me import bp as me_bp
    from .billing import bp as billing_bp
    from .auth import bp as auth_bp
    from .admin import bp as admin_bp
    from .jobs import bp as jobs_bp
    from .credits import bp as credits_bp

    from .health import bp as health_bp
    from .assets import bp as assets_bp
    from .image_gen import bp as image_gen_bp
    from .text_to_3d import bp as text_to_3d_bp
    from .image_to_3d import bp as image_to_3d_bp
    from .mesh_operations import bp as mesh_ops_bp
    from .history import bp as history_bp
    from .community import bp as community_bp

    app.register_blueprint(me_bp, url_prefix="/api/me")
    app.register_blueprint(billing_bp, url_prefix="/api/billing")
    app.register_blueprint(auth_bp, url_prefix="/api/auth")
    app.register_blueprint(admin_bp, url_prefix="/api/admin")
    app.register_blueprint(jobs_bp, url_prefix="/api/jobs")
    app.register_blueprint(credits_bp, url_prefix="/api/credits")

    app.register_blueprint(health_bp, url_prefix="/api/_mod")
    app.register_blueprint(assets_bp, url_prefix="/api/_mod")
    app.register_blueprint(image_gen_bp, url_prefix="/api/_mod")
    app.register_blueprint(text_to_3d_bp, url_prefix="/api/_mod")
    app.register_blueprint(image_to_3d_bp, url_prefix="/api/_mod")
    app.register_blueprint(mesh_ops_bp, url_prefix="/api/_mod")
    app.register_blueprint(history_bp, url_prefix="/api/_mod")
    app.register_blueprint(community_bp, url_prefix="/api/_mod")

    print(
        "[ROUTES] Registered blueprints: /api/me, /api/billing, /api/auth, /api/admin, /api/jobs, /api/credits, "
        "/api/_mod/*"
    )
