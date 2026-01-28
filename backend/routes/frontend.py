"""
Frontend Routes Blueprint
-------------------------
Serves static frontend files or redirects to FRONTEND_BASE_URL.
"""

from __future__ import annotations

import os
from pathlib import Path

from flask import Blueprint, abort, jsonify, redirect, request, send_from_directory

from backend.config import config

bp = Blueprint("frontend", __name__)

# Find frontend directory - check multiple locations
FRONTEND_DIR = None
_possible_frontend_paths = [
    config.APP_DIR / "frontend",                    # Deployed: frontend/ in same dir as app.py
    config.APP_DIR / ".." / ".." / "Frontend",      # Local dev: TimrX/Backend/meshy -> TimrX/Frontend
    config.APP_DIR.parent.parent / "Frontend",      # Alternative path
]
for _fp in _possible_frontend_paths:
    if _fp.exists() and _fp.is_dir():
        FRONTEND_DIR = Path(_fp).resolve()
        break

# Get FRONTEND_BASE_URL for redirects when FRONTEND_DIR is not available
_FRONTEND_BASE_URL = config.FRONTEND_BASE_URL.rstrip("/") if config.FRONTEND_BASE_URL else None

if FRONTEND_DIR:
    print(f"[FRONTEND] Serving from: {FRONTEND_DIR}")
elif _FRONTEND_BASE_URL:
    print(f"[FRONTEND] No local frontend dir. Will redirect HTML routes to: {_FRONTEND_BASE_URL}")
else:
    print("[FRONTEND] WARNING: No FRONTEND_DIR or FRONTEND_BASE_URL. HTML routes will 404.")


def _redirect_to_frontend(path: str):
    """
    Redirect to frontend URL, preserving query string.
    Used when FRONTEND_DIR is not available but FRONTEND_BASE_URL is set.
    """
    if not _FRONTEND_BASE_URL:
        return jsonify({"error": "Frontend not configured. Set FRONTEND_BASE_URL env var."}), 404
    query = request.query_string.decode("utf-8")
    target_url = f"{_FRONTEND_BASE_URL}/{path.lstrip('/')}"
    if query:
        target_url = f"{target_url}?{query}"
    return redirect(target_url, code=302)


@bp.route("/")
def serve_hub():
    """Serve hub.html at root."""
    if not FRONTEND_DIR:
        return _redirect_to_frontend("hub.html")
    return send_from_directory(FRONTEND_DIR, "hub.html")


@bp.route("/3dprint")
@bp.route("/3dprint.html")
def serve_3dprint():
    """Serve 3dprint.html."""
    if not FRONTEND_DIR:
        return _redirect_to_frontend("3dprint.html")
    return send_from_directory(FRONTEND_DIR, "3dprint.html")


@bp.route("/hub.html")
def serve_hub_html():
    """Serve hub.html explicitly."""
    if not FRONTEND_DIR:
        return _redirect_to_frontend("hub.html")
    return send_from_directory(FRONTEND_DIR, "hub.html")


@bp.route("/index.html")
def serve_index_html():
    """Serve index.html."""
    if not FRONTEND_DIR:
        return _redirect_to_frontend("index.html")
    return send_from_directory(FRONTEND_DIR, "index.html")


@bp.route("/<path:filename>")
def serve_static_file(filename):
    """Serve static files from frontend directory."""
    if not FRONTEND_DIR:
        if _FRONTEND_BASE_URL and filename.endswith(
            (
                ".css", ".js", ".html", ".png", ".jpg", ".jpeg", ".gif", ".svg",
                ".ico", ".woff", ".woff2", ".ttf", ".eot",
            )
        ):
            return _redirect_to_frontend(filename)
        abort(404)

    allowed_extensions = {
        ".css", ".js", ".html", ".png", ".jpg", ".jpeg", ".gif", ".svg",
        ".ico", ".woff", ".woff2", ".ttf", ".eot",
    }
    ext = os.path.splitext(filename)[1].lower()
    if ext not in allowed_extensions:
        abort(404)
    try:
        return send_from_directory(FRONTEND_DIR, filename)
    except Exception:
        abort(404)
