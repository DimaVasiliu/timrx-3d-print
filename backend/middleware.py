"""
Middleware for TimrX Backend routes.

Provides decorators and helpers for session/identity management in routes.

Usage:
    from backend.middleware import with_session, require_session

    @app.route("/api/me")
    @with_session
    def get_me():
        # g.identity and g.session_id are available
        return jsonify({"identity_id": str(g.identity["id"])})

    @app.route("/api/billing/checkout", methods=["POST"])
    @require_session
    def checkout():
        # Requires valid session, returns 401 if not authenticated
        return jsonify({"ok": True})

Note: All service imports are lazy (inside functions) to avoid circular import issues.
"""

import os
from functools import wraps
from flask import request, g, jsonify, make_response

# Debug flag for verbose session logging (set SESSION_DEBUG=1 to enable)
SESSION_DEBUG = os.getenv("SESSION_DEBUG", "").lower() in ("1", "true", "yes")

# NOTE: Do NOT import IdentityService, config, or db at module level!
# These cause circular imports. Import them lazily inside functions.


def _get_identity_service():
    """Lazy import of IdentityService to avoid circular imports."""
    from backend.services.identity_service import IdentityService
    return IdentityService


def _get_database_error():
    """Lazy import of DatabaseError to avoid circular imports."""
    from backend.db import DatabaseError
    return DatabaseError


# ─────────────────────────────────────────────────────────────
# Core identity resolution (shared by ALL middleware decorators)
# ─────────────────────────────────────────────────────────────

def _resolve_identity():
    """
    Resolve the current request's identity, using request-scoped caching.

    Returns (identity_dict_or_None, session_id_or_None).
    After this call, g.session_id / g.identity_id / g.identity are set.

    This is the ONLY function that should call get_current_identity().
    All middleware decorators delegate here so that within a single HTTP
    request the identity is resolved at most once (0 or 1 DB borrows for
    session validation, 0 or 1 for touch).
    """
    # Fast path: already resolved in this request
    if getattr(g, '_identity_resolved', False):
        return g.identity, g.session_id

    IdentityService = _get_identity_service()

    session_id = IdentityService.get_session_id_from_request(request)
    identity = IdentityService.get_current_identity(request)

    g.session_id = session_id
    g.identity_id = str(identity["id"]) if identity else None
    g.identity = identity
    g._identity_resolved = True

    return identity, session_id


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def no_cache(f):
    """Decorator that adds Cache-Control headers to prevent caching."""
    @wraps(f)
    def decorated(*args, **kwargs):
        result = f(*args, **kwargs)
        if hasattr(result, 'headers'):
            response = result
        elif isinstance(result, tuple):
            response = make_response(result[0], result[1] if len(result) > 1 else 200)
            if len(result) > 2:
                for key, value in result[2].items():
                    response.headers[key] = value
        else:
            response = make_response(result)
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    return decorated


def _ensure_response(result):
    """Convert a route return value (Response, tuple, dict, str) to a Response object."""
    if hasattr(result, 'headers'):
        return result
    if isinstance(result, tuple):
        return make_response(result[0], result[1] if len(result) > 1 else 200)
    return make_response(result)


def _copy_cookies(source_response, target_response):
    """Copy Set-Cookie headers from source to target response."""
    for cookie in source_response.headers.getlist('Set-Cookie'):
        target_response.headers.add('Set-Cookie', cookie)


def _maybe_refresh_cookie(identity, session_id, result):
    """If session was renewed (sliding window), refresh cookie Max-Age."""
    if identity and identity.get("_session_renewed") and session_id:
        IdentityService = _get_identity_service()
        resp = _ensure_response(result)
        IdentityService.set_session_cookie(resp, session_id)
        return resp
    return result


# ─────────────────────────────────────────────────────────────
# Session/Auth Middleware Decorators
# ─────────────────────────────────────────────────────────────

def with_session(f):
    """
    Decorator that ensures a session exists.
    Creates anonymous identity + session if none exists.
    Skips session logic entirely for OPTIONS (CORS preflight).

    Sets on g:
        - g.session_id, g.identity_id, g.identity
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == 'OPTIONS':
            return f(*args, **kwargs)

        IdentityService = _get_identity_service()
        DatabaseError = _get_database_error()

        try:
            identity, session_id = _resolve_identity()

            if identity:
                result = f(*args, **kwargs)
                return _maybe_refresh_cookie(identity, session_id, result)

            # No valid session — create anonymous identity + session
            # Single-flight gate: if 5 concurrent requests arrive without
            # cookies, only one actually bootstraps; the rest wait and reuse.
            from backend.services.identity_service import _bootstrap_single_flight
            cookie_response = make_response()
            session_id, identity_id = _bootstrap_single_flight(
                request, cookie_response, IdentityService.get_or_create_session,
            )

            g.session_id = session_id
            g.identity_id = identity_id
            # Fetch wallet for the new identity — this will be cached in
            # the process session cache for followers to reuse.
            identity_with_wallet = IdentityService.get_identity_with_wallet(identity_id)
            g.identity = identity_with_wallet
            g._identity_resolved = True
            g._identity_source = "bootstrap"
            # Seed the process cache so concurrent followers get a hit
            # on get_current_identity → validate_session for this session
            if identity_with_wallet:
                from backend.services.identity_service import _session_cache_put
                _session_cache_put(session_id, identity_with_wallet)

            result = f(*args, **kwargs)
            resp = _ensure_response(result)
            _copy_cookies(cookie_response, resp)
            return resp

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in with_session: {e}")
            return jsonify({
                "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
            }), 500

    return decorated


def with_optional_session(f):
    """
    Decorator that validates an existing session but NEVER creates one.
    If no valid session, sets g fields to None and lets the handler proceed.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        DatabaseError = _get_database_error()

        try:
            identity, session_id = _resolve_identity()

            if identity:
                result = f(*args, **kwargs)
                return _maybe_refresh_cookie(identity, session_id, result)

            # No valid session — proceed without one
            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in with_optional_session: {e}")
            return jsonify({
                "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
            }), 500

    return decorated


def require_session(f):
    """
    Decorator that requires a valid session.
    Returns 401 if no valid session exists.
    Does NOT create anonymous sessions.
    Skips auth for OPTIONS (CORS preflight).
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == 'OPTIONS':
            return f(*args, **kwargs)

        DatabaseError = _get_database_error()

        try:
            identity, session_id = _resolve_identity()

            if not identity:
                if SESSION_DEBUG or "/subscriptions" in request.path:
                    print(
                        f"[MIDDLEWARE] require_session 401: "
                        f"path={request.path}, "
                        f"cookie_present={bool(request.cookies.get('timrx_sid'))}, "
                        f"session_id={session_id[:16] + '...' if session_id else 'None'}"
                    )
                return jsonify({
                    "error": {"code": "UNAUTHORIZED", "message": "Valid session required"}
                }), 401

            result = f(*args, **kwargs)
            return _maybe_refresh_cookie(identity, session_id, result)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_session: {e}")
            return jsonify({
                "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
            }), 500

    return decorated


def require_email(f):
    """
    Decorator that requires a valid session with an attached email.
    Returns 401 if no session, 403 if no email attached.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == 'OPTIONS':
            return f(*args, **kwargs)

        DatabaseError = _get_database_error()

        try:
            identity, _ = _resolve_identity()

            if not identity:
                return jsonify({
                    "error": {"code": "UNAUTHORIZED", "message": "Valid session required"}
                }), 401

            if not identity.get("email"):
                return jsonify({
                    "error": {"code": "EMAIL_REQUIRED", "message": "Email address required for this action"}
                }), 403

            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_email: {e}")
            return jsonify({
                "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
            }), 500

    return decorated


def require_verified_email(f):
    """
    Decorator that requires a valid session with a VERIFIED email.
    Returns 401 if no session, 403 if no email or email not verified.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == 'OPTIONS':
            return f(*args, **kwargs)

        DatabaseError = _get_database_error()

        try:
            identity, _ = _resolve_identity()

            if not identity:
                return jsonify({
                    "error": {"code": "UNAUTHORIZED", "message": "Valid session required"}
                }), 401

            if not identity.get("email"):
                return jsonify({
                    "error": {"code": "EMAIL_REQUIRED", "message": "Email address required for this action"}
                }), 403

            if not identity.get("email_verified"):
                return jsonify({
                    "error": {"code": "EMAIL_NOT_VERIFIED", "message": "Please verify your email address before making purchases"}
                }), 403

            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_verified_email: {e}")
            return jsonify({
                "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
            }), 500

    return decorated


def get_identity_from_request():
    """Helper function to get identity from current request."""
    identity, _ = _resolve_identity()
    return identity


def get_session_id_from_request():
    """Helper function to get session ID from current request."""
    IdentityService = _get_identity_service()
    return IdentityService.get_session_id_from_request(request)


def require_admin(f):
    """
    Decorator that requires admin authentication.
    Supports token-based (X-Admin-Token) and email-based auth.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        from backend.config import config
        IdentityService = _get_identity_service()
        DatabaseError = _get_database_error()

        if not config.ADMIN_AUTH_CONFIGURED:
            return jsonify({
                "error": {"code": "ADMIN_NOT_CONFIGURED", "message": "Admin authentication is not configured"}
            }), 503

        # Method 1: Token-based authentication
        admin_token = request.headers.get("X-Admin-Token")
        if admin_token:
            if config.ADMIN_TOKEN and admin_token == config.ADMIN_TOKEN:
                g.admin_auth_method = "token"
                g.admin_email = None
                g.identity = None
                return f(*args, **kwargs)
            else:
                return jsonify({
                    "error": {"code": "INVALID_ADMIN_TOKEN", "message": "Invalid admin token"}
                }), 403

        # Method 2: Email-based authentication
        try:
            identity, _ = _resolve_identity()

            if not identity:
                return jsonify({
                    "error": {"code": "UNAUTHORIZED", "message": "Authentication required"}
                }), 401

            email = identity.get("email")
            if not email:
                return jsonify({
                    "error": {"code": "EMAIL_REQUIRED", "message": "Admin access requires verified email"}
                }), 403

            if not config.is_admin_email(email):
                return jsonify({
                    "error": {"code": "NOT_ADMIN", "message": "You do not have admin privileges"}
                }), 403

            g.admin_auth_method = "email"
            g.admin_email = email
            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_admin: {e}")
            return jsonify({
                "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
            }), 500

    return decorated