"""
Routes package for TimrX Backend.
Contains Flask Blueprints for different API namespaces.
"""

from .me import bp as me_bp
from .billing import bp as billing_bp
from .auth import bp as auth_bp
from .admin import bp as admin_bp
from .jobs import bp as jobs_bp
from .credits import bp as credits_bp

__all__ = ["me_bp", "billing_bp", "auth_bp", "admin_bp", "jobs_bp", "credits_bp"]


def register_blueprints(app):
    """Register all blueprints with the Flask app."""
    app.register_blueprint(me_bp, url_prefix="/api/me")
    app.register_blueprint(billing_bp, url_prefix="/api/billing")
    app.register_blueprint(auth_bp, url_prefix="/api/auth")
    app.register_blueprint(admin_bp, url_prefix="/api/admin")
    app.register_blueprint(jobs_bp, url_prefix="/api/jobs")
    app.register_blueprint(credits_bp, url_prefix="/api/credits")
    print("[ROUTES] Registered blueprints: /api/me, /api/billing, /api/auth, /api/admin, /api/jobs, /api/credits")
