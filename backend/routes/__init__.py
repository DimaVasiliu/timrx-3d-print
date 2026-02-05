"""
Routes package for TimrX Backend.
Contains Flask Blueprints for different API namespaces.
"""

__all__ = [
    "register_blueprints",
]


def _print_route_map(app):
    """Print all registered /api/* routes at startup for debugging."""
    api_routes = []
    for rule in app.url_map.iter_rules():
        if rule.rule.startswith("/api"):
            methods = ",".join(sorted(m for m in rule.methods if m not in ("HEAD", "OPTIONS")))
            api_routes.append(f"  {methods:8s} {rule.rule}")

    api_routes.sort(key=lambda x: x.split()[-1])  # Sort by path

    print("[ROUTES] Registered API endpoints:")
    for route in api_routes:
        print(route)
    print(f"[ROUTES] Total: {len(api_routes)} endpoints")


def register_blueprints(app):
    """Register all blueprints with the Flask app."""
    from backend.routes.frontend import bp as frontend_bp
    from backend.routes.me import bp as me_bp
    from backend.routes.billing import bp as billing_bp
    from backend.routes.auth import bp as auth_bp
    from backend.routes.admin import bp as admin_bp
    from backend.routes.jobs import bp as jobs_bp
    from backend.routes.credits import bp as credits_bp

    from backend.routes.health import bp as health_bp
    from backend.routes.assets import bp as assets_bp
    from backend.routes.image_gen import bp as image_gen_bp
    from backend.routes.text_to_3d import bp as text_to_3d_bp
    from backend.routes.image_to_3d import bp as image_to_3d_bp
    from backend.routes.mesh_operations import bp as mesh_ops_bp
    from backend.routes.history import bp as history_bp
    from backend.routes.community import bp as community_bp
    from backend.routes.contact import bp as contact_bp
    from backend.routes.video import bp as video_bp

    # Import inspire with explicit error handling for debugging
    try:
        from backend.routes.inspire import bp as inspire_bp
    except Exception as e:
        print(f"[ROUTES] ERROR importing inspire: {e}")
        import traceback
        traceback.print_exc()
        inspire_bp = None

    # Frontend routes (no prefix)
    app.register_blueprint(frontend_bp)

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
    app.register_blueprint(video_bp, url_prefix="/api/_mod")
    if inspire_bp:
        app.register_blueprint(inspire_bp, url_prefix="/api/_mod")
    app.register_blueprint(contact_bp, url_prefix="/api")

    # Also register under /api for backward compatibility (cached frontend)
    # These must match the legacy app.py routes exactly
    app.register_blueprint(history_bp, url_prefix="/api", name="history_compat")
    app.register_blueprint(text_to_3d_bp, url_prefix="/api", name="text_to_3d_compat")
    app.register_blueprint(image_to_3d_bp, url_prefix="/api", name="image_to_3d_compat")
    app.register_blueprint(health_bp, url_prefix="/api", name="health_compat")
    app.register_blueprint(assets_bp, url_prefix="/api", name="assets_compat")
    app.register_blueprint(mesh_ops_bp, url_prefix="/api", name="mesh_ops_compat")
    app.register_blueprint(image_gen_bp, url_prefix="/api", name="image_gen_compat")
    app.register_blueprint(community_bp, url_prefix="/api", name="community_compat")
    app.register_blueprint(video_bp, url_prefix="/api", name="video_compat")
    if inspire_bp:
        app.register_blueprint(inspire_bp, url_prefix="/api", name="inspire_compat")

    # Print route map at startup for debugging
    _print_route_map(app)
