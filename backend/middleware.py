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

# ─────────────────────────────────────────────────────────────────────────────
# DEBUG: Session Cookie Diagnostics
# ─────────────────────────────────────────────────────────────────────────────

def _log_session_debug(endpoint_name: str):
    """
    Log detailed session/cookie diagnostics for debugging cookie collisions.
    Call this at the start of session-related middleware.
    """
    try:
        from backend.config import config

        # Use module-level SESSION_DEBUG constant
        session_debug = SESSION_DEBUG

        # Only log for specific endpoints to reduce noise
        path = request.path
        if not any(p in path for p in ["/api/me", "/api/billing/checkout", "/api/billing/subscriptions", "/api/auth/"]):
            return

        # Get raw Cookie header
        raw_cookie_header = request.headers.get("Cookie", "(no Cookie header)")

        # Get parsed cookie value (Flask's version)
        flask_parsed_sid = request.cookies.get("timrx_sid", "(not found)")

        # Count how many timrx_sid appear in raw header (detect collision)
        sid_count = raw_cookie_header.count("timrx_sid=")

        # Get resolved session ID using our collision-aware logic
        try:
            from backend.services.identity_service import IdentityService
            resolved_sid, candidates, reason = IdentityService.resolve_session_id(request)
        except Exception:
            resolved_sid, candidates, reason = None, [], "error"

        # Collision detection
        is_collision = sid_count > 1

        # Production: only log a minimal one-liner for collisions (for monitoring)
        # Debug mode (SESSION_DEBUG=1): log verbose block
        if not session_debug:
            if is_collision:
                # Minimal collision log for production monitoring
                print(f"[SESSION] Cookie collision: {len(candidates)} cookies, resolved={reason}")
            return

        # Verbose debug block (only when SESSION_DEBUG=1)
        print("=" * 70)
        print(f"[SESSION DEBUG] {endpoint_name} - {request.method} {path}")
        print("-" * 70)
        print("  Config:")
        print(f"    IS_PROD={config.IS_PROD}, IS_RENDER={config.IS_RENDER}")
        print(f"    SESSION_COOKIE_DOMAIN={config.SESSION_COOKIE_DOMAIN!r}")
        print(f"    SESSION_COOKIE_SAMESITE={config.SESSION_COOKIE_SAMESITE!r}")
        print(f"    SESSION_COOKIE_SECURE={config.SESSION_COOKIE_SECURE}")
        print("  Request:")
        print(f"    Host: {request.host}")
        print(f"    Origin: {request.headers.get('Origin', '(none)')}")
        print(f"    timrx_sid count in Cookie header: {sid_count}")
        if is_collision:
            print("    *** COOKIE COLLISION DETECTED! ***")
            print(f"    Candidates: {[s[:8] + '...' for s in candidates]}")
            print(f"    Resolution: {reason}")
        print(f"    Flask parsed: {flask_parsed_sid[:16] + '...' if flask_parsed_sid != '(not found)' else flask_parsed_sid}")
        print(f"    Our resolved: {resolved_sid[:16] + '...' if resolved_sid else 'None'}")
        print(f"    Raw Cookie: {raw_cookie_header[:300]}{'...' if len(raw_cookie_header) > 300 else ''}")
        print("=" * 70)

    except Exception as e:
        print(f"[SESSION DEBUG] Error in debug logging: {e}")

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


def no_cache(f):
    """
    Decorator that adds Cache-Control headers to prevent caching.

    Adds:
        - Cache-Control: no-store, no-cache, must-revalidate, max-age=0
        - Pragma: no-cache (for HTTP/1.0 compatibility)
        - Expires: 0

    Use for sensitive endpoints like /api/me, /api/credits/*, /api/billing/*.
    This ensures Cloudflare returns CF-Cache-Status: DYNAMIC.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        result = f(*args, **kwargs)

        # Handle different return types
        if hasattr(result, 'headers'):
            # Already a Response object
            response = result
        elif isinstance(result, tuple):
            # Tuple (body, status) or (body, status, headers)
            response = make_response(result[0], result[1] if len(result) > 1 else 200)
            if len(result) > 2:
                for key, value in result[2].items():
                    response.headers[key] = value
        else:
            # Plain return value (dict, string, etc.)
            response = make_response(result)

        # Add no-cache headers
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'

        return response

    return decorated


def with_session(f):
    """
    Decorator that ensures a session exists.
    Creates anonymous identity + session if none exists.

    Sets on g:
        - g.session_id: The session ID
        - g.identity_id: The identity ID (string)
        - g.identity: The full identity dict (with wallet balance)

    The session cookie is automatically set on new sessions.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        # Lazy imports to avoid circular import at module load time
        IdentityService = _get_identity_service()
        DatabaseError = _get_database_error()

        # DEBUG: Log session diagnostics
        _log_session_debug("with_session")

        # Create response wrapper to set cookies
        try:
            # Check for existing valid session first
            identity = IdentityService.get_current_identity(request)

            if identity:
                # Existing valid session
                g.session_id = IdentityService.get_session_id_from_request(request)
                g.identity_id = str(identity["id"])
                g.identity = identity
                return f(*args, **kwargs)

            # No valid session - need to create one
            # We need to wrap the response to set the cookie
            response = make_response()

            session_id, identity_id = IdentityService.get_or_create_session(request, response)

            g.session_id = session_id
            g.identity_id = identity_id
            g.identity = IdentityService.get_identity_with_wallet(identity_id)

            # Call the actual route function
            result = f(*args, **kwargs)

            # If result is a Response, copy cookies to it
            if hasattr(result, 'headers'):
                # Copy the session cookie from our temp response
                for cookie in response.headers.getlist('Set-Cookie'):
                    result.headers.add('Set-Cookie', cookie)
                return result
            else:
                # Result is not a response (e.g., tuple or dict)
                # Convert to response and add cookies
                if isinstance(result, tuple):
                    actual_response = make_response(result[0], result[1] if len(result) > 1 else 200)
                else:
                    actual_response = make_response(result)

                for cookie in response.headers.getlist('Set-Cookie'):
                    actual_response.headers.add('Set-Cookie', cookie)
                return actual_response

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in with_session: {e}")
            return jsonify({
                "error": {
                    "code": "DATABASE_ERROR",
                    "message": "Database error occurred"
                }
            }), 500

    return decorated


def require_session(f):
    """
    Decorator that requires a valid session.
    Returns 401 if no valid session exists.
    Does NOT create anonymous sessions.

    Sets on g:
        - g.session_id: The session ID
        - g.identity_id: The identity ID (string)
        - g.identity: The full identity dict
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        # Lazy imports to avoid circular import at module load time
        IdentityService = _get_identity_service()
        DatabaseError = _get_database_error()

        # DEBUG: Log session diagnostics
        _log_session_debug("require_session")

        try:
            # Get session ID first for logging
            session_id = IdentityService.get_session_id_from_request(request)
            identity = IdentityService.get_current_identity(request)

            if not identity:
                # DEBUG: Log subscription auth failures always (production debugging)
                # For other paths, only log when SESSION_DEBUG=1
                is_subscription_path = "/subscriptions" in request.path
                cookie_present = bool(request.cookies.get("timrx_sid"))
                origin = request.headers.get("Origin", "(no origin)")

                if is_subscription_path or SESSION_DEBUG:
                    print(
                        f"[MIDDLEWARE] require_session 401: "
                        f"path={request.path}, "
                        f"origin={origin}, "
                        f"cookie_present={cookie_present}, "
                        f"session_id={session_id[:16] + '...' if session_id else 'None'}, "
                        f"identity=None (session invalid/expired/not_found)"
                    )
                return jsonify({
                    "error": {
                        "code": "UNAUTHORIZED",
                        "message": "Valid session required"
                    }
                }), 401

            g.session_id = session_id
            g.identity_id = str(identity["id"])
            g.identity = identity

            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_session: {e}")
            return jsonify({
                "error": {
                    "code": "DATABASE_ERROR",
                    "message": "Database error occurred"
                }
            }), 500

    return decorated


def require_email(f):
    """
    Decorator that requires a valid session with an attached email.
    Returns 401 if no session, 403 if no email attached.

    Use for endpoints that require email (e.g., purchases).
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        # Lazy imports to avoid circular import at module load time
        IdentityService = _get_identity_service()
        DatabaseError = _get_database_error()

        try:
            identity = IdentityService.get_current_identity(request)

            if not identity:
                return jsonify({
                    "error": {
                        "code": "UNAUTHORIZED",
                        "message": "Valid session required"
                    }
                }), 401

            if not identity.get("email"):
                return jsonify({
                    "error": {
                        "code": "EMAIL_REQUIRED",
                        "message": "Email address required for this action"
                    }
                }), 403

            g.session_id = IdentityService.get_session_id_from_request(request)
            g.identity_id = str(identity["id"])
            g.identity = identity

            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_email: {e}")
            return jsonify({
                "error": {
                    "code": "DATABASE_ERROR",
                    "message": "Database error occurred"
                }
            }), 500

    return decorated


def require_verified_email(f):
    """
    Decorator that requires a valid session with a VERIFIED email.
    Returns 401 if no session, 403 if no email or email not verified.

    Use for paid actions (checkout, subscriptions) that require verified email.

    Error codes:
        - UNAUTHORIZED: No valid session
        - EMAIL_REQUIRED: No email attached to identity
        - EMAIL_NOT_VERIFIED: Email attached but not yet verified
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        # Lazy imports to avoid circular import at module load time
        IdentityService = _get_identity_service()
        DatabaseError = _get_database_error()

        try:
            identity = IdentityService.get_current_identity(request)

            if not identity:
                return jsonify({
                    "error": {
                        "code": "UNAUTHORIZED",
                        "message": "Valid session required"
                    }
                }), 401

            if not identity.get("email"):
                return jsonify({
                    "error": {
                        "code": "EMAIL_REQUIRED",
                        "message": "Email address required for this action"
                    }
                }), 403

            if not identity.get("email_verified"):
                return jsonify({
                    "error": {
                        "code": "EMAIL_NOT_VERIFIED",
                        "message": "Please verify your email address before making purchases"
                    }
                }), 403

            g.session_id = IdentityService.get_session_id_from_request(request)
            g.identity_id = str(identity["id"])
            g.identity = identity

            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_verified_email: {e}")
            return jsonify({
                "error": {
                    "code": "DATABASE_ERROR",
                    "message": "Database error occurred"
                }
            }), 500

    return decorated


def get_identity_from_request():
    """
    Helper function to get identity from current request.
    Returns None if no valid session.
    Use this when you don't want a decorator.
    """
    IdentityService = _get_identity_service()
    return IdentityService.get_current_identity(request)


def get_session_id_from_request():
    """
    Helper function to get session ID from current request.
    Returns None if no session cookie.
    """
    IdentityService = _get_identity_service()
    return IdentityService.get_session_id_from_request(request)


def require_admin(f):
    """
    Decorator that requires admin authentication.
    Supports two authentication methods:
      1. Token-based: X-Admin-Token header (for scripts/automation)
      2. Email-based: Session with email in ADMIN_EMAILS list (for browser)

    Returns 401 if not authenticated, 403 if not an admin.

    Sets on g:
        - g.admin_auth_method: 'token' or 'email'
        - g.admin_email: The admin email (if email-based auth)
        - g.identity: The identity (if email-based auth)
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        # Lazy imports to avoid circular import at module load time
        from backend.config import config
        IdentityService = _get_identity_service()
        DatabaseError = _get_database_error()

        # Check if admin auth is configured at all
        if not config.ADMIN_AUTH_CONFIGURED:
            return jsonify({
                "error": {
                    "code": "ADMIN_NOT_CONFIGURED",
                    "message": "Admin authentication is not configured"
                }
            }), 503

        # Method 1: Token-based authentication (X-Admin-Token header)
        admin_token = request.headers.get("X-Admin-Token")
        if admin_token:
            if config.ADMIN_TOKEN and admin_token == config.ADMIN_TOKEN:
                g.admin_auth_method = "token"
                g.admin_email = None
                g.identity = None
                return f(*args, **kwargs)
            else:
                return jsonify({
                    "error": {
                        "code": "INVALID_ADMIN_TOKEN",
                        "message": "Invalid admin token"
                    }
                }), 403

        # Method 2: Email-based authentication (session with admin email)
        try:
            identity = IdentityService.get_current_identity(request)

            if not identity:
                return jsonify({
                    "error": {
                        "code": "UNAUTHORIZED",
                        "message": "Authentication required"
                    }
                }), 401

            email = identity.get("email")
            if not email:
                return jsonify({
                    "error": {
                        "code": "EMAIL_REQUIRED",
                        "message": "Admin access requires verified email"
                    }
                }), 403

            if not config.is_admin_email(email):
                return jsonify({
                    "error": {
                        "code": "NOT_ADMIN",
                        "message": "You do not have admin privileges"
                    }
                }), 403

            g.admin_auth_method = "email"
            g.admin_email = email
            g.session_id = IdentityService.get_session_id_from_request(request)
            g.identity_id = str(identity["id"])
            g.identity = identity

            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_admin: {e}")
            return jsonify({
                "error": {
                    "code": "DATABASE_ERROR",
                    "message": "Database error occurred"
                }
            }), 500

    return decorated
