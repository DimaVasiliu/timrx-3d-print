"""
Frontend Routes Blueprint
-------------------------
Migration Status: PENDING

Routes to migrate from app.py:
- GET /                          -> serve_hub()
- GET /3dprint                   -> serve_3dprint()
- GET /3dprint.html              -> serve_3dprint()
- GET /hub.html                  -> serve_hub_html()
- GET /index.html                -> serve_index_html()
- GET /<path:filename>           -> serve_static_file()

Helper functions:
- _redirect_to_frontend(path) -> Response

Source: app.py lines ~910-976
Estimated lines: ~100

Note: These serve static frontend files from the Frontend directory.
Consider using nginx/CDN for production static file serving.

Usage after migration:
    from backend.routes.frontend import bp as frontend_bp
    app.register_blueprint(frontend_bp)  # No prefix - serves at root
"""

from flask import Blueprint

bp = Blueprint("frontend", __name__)

# TODO: Migrate frontend/static routes from app.py
