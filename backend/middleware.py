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
import hmac
import time as _time
from functools import wraps
from flask import request, g, jsonify, make_response

# Debug flag for verbose session logging (set SESSION_DEBUG=1 to enable)
SESSION_DEBUG = os.getenv("SESSION_DEBUG", "").lower() in ("1", "true", "yes")

# Bootstrap circuit-breaker: if the pool can't serve a connection within
# this many seconds, return a lightweight "retry later" response instead
# of queueing more bootstrap attempts that pile on the pool.
_BOOTSTRAP_TIMEOUT = float(os.getenv("BOOTSTRAP_TIMEOUT", "5"))

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

def _resolve_identity(readonly=False):
    """
    Resolve the current request's identity, using request-scoped caching.

    Returns (identity_dict_or_None, session_id_or_None).
    After this call, g.session_id / g.identity_id / g.identity are set.

    When readonly=True, the DB validation path skips touch + renewal,
    avoiding DB writes on read-only endpoints like status polling.

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
    identity = IdentityService.get_current_identity(request, readonly=readonly)

    g.session_id = session_id
    g.identity_id = str(identity["id"]) if identity else None
    g.identity = identity
    g._identity_resolved = True

    return identity, session_id


def _resolve_identity_safe():
    """
    Like _resolve_identity(), but fail-open on DB/pool errors.

    Used by with_optional_session for paths like restore/redeem where
    failing to look up the current session must NOT block the operation.
    Returns (None, None) and sets g fields to None on any DB error.
    """
    try:
        return _resolve_identity()
    except Exception as e:
        # Extract session_id from cookie without DB (pure cookie parse)
        IdentityService = _get_identity_service()
        raw_sid = None
        try:
            raw_sid = IdentityService.get_session_id_from_request(request)
        except Exception:
            pass

        print(
            f"[MIDDLEWARE][SAFE-SESSION] session lookup failed-open "
            f"path={request.path} "
            f"sid={'present' if raw_sid else 'none'} "
            f"reason={type(e).__name__}: {e}"
        )

        g.session_id = None
        g.identity_id = None
        g.identity = None
        g._identity_resolved = True
        return None, None


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
# Bootstrap with circuit-breaker + single-flight
# ─────────────────────────────────────────────────────────────

def _bootstrap_retry_response():
    """Build 503 response for when bootstrap can't get a DB connection.
    Must be a function, not a module-level constant, because jsonify()
    requires Flask application context which doesn't exist at import time."""
    return (
        jsonify({
            "ok": False,
            "error": {
                "code": "BOOTSTRAP_BUSY",
                "message": "Server is starting up. Please retry in a moment.",
            },
            "retry_after": 2,
        }),
        503,
        {"Retry-After": "2"},
    )


def _do_bootstrap():
    """
    Run anonymous bootstrap with circuit-breaker and single-flight.
    Returns (session_id, identity_id, cookie_response) on success.
    Raises _BootstrapBusy if the pool can't serve within BOOTSTRAP_TIMEOUT.
    """
    from backend.services.identity_service import _bootstrap_single_flight

    IdentityService = _get_identity_service()
    cookie_response = make_response()

    t0 = _time.monotonic()
    try:
        session_id, identity_id = _bootstrap_single_flight(
            request, cookie_response, IdentityService.get_or_create_session,
        )
    except Exception as e:
        elapsed_ms = int((_time.monotonic() - t0) * 1000)
        # If it looks like a pool timeout, trip the circuit breaker
        if "PoolTimeout" in type(e).__name__ or "timeout" in str(e).lower():
            print(
                f"[BOOTSTRAP][BREAKER] pool unavailable elapsed_ms={elapsed_ms} "
                f"path={request.path} — returning 503"
            )
            raise _BootstrapBusy() from e
        raise

    elapsed_ms = int((_time.monotonic() - t0) * 1000)
    print(f"[BOOTSTRAP] completed elapsed_ms={elapsed_ms} sid={session_id[:8]}... iid={identity_id[:8]}...")

    # Fetch wallet + seed the process cache for concurrent followers
    identity_with_wallet = IdentityService.get_identity_with_wallet(identity_id)
    if identity_with_wallet:
        from backend.services.identity_service import _session_cache_put
        _session_cache_put(session_id, identity_with_wallet)

    return session_id, identity_id, identity_with_wallet, cookie_response


class _BootstrapBusy(Exception):
    """Raised when bootstrap can't get a DB connection in time."""
    pass


# ─────────────────────────────────────────────────────────────
# Session/Auth Middleware Decorators
# ─────────────────────────────────────────────────────────────

def with_session(f):
    """
    Decorator that ensures a session exists.
    Creates anonymous identity + session if none exists.
    Skips session logic entirely for OPTIONS (CORS preflight).

    If bootstrap can't get a DB connection quickly, returns a lightweight
    503 with Retry-After instead of cascading more pool pressure.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == 'OPTIONS':
            return f(*args, **kwargs)

        DatabaseError = _get_database_error()

        try:
            identity, session_id = _resolve_identity()

            if identity:
                result = f(*args, **kwargs)
                return _maybe_refresh_cookie(identity, session_id, result)

            # No valid session — bootstrap anonymous identity
            try:
                session_id, identity_id, identity_with_wallet, cookie_response = _do_bootstrap()
            except _BootstrapBusy:
                return _bootstrap_retry_response()

            g.session_id = session_id
            g.identity_id = identity_id
            g.identity = identity_with_wallet
            g._identity_resolved = True
            g._identity_source = "bootstrap"

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


def with_session_readonly(f):
    """
    Decorator for read-only endpoints (status polling, etc.).

    Same as with_session but when the session cache misses and a DB
    validation is required, it skips:
    - identity touch (last_seen_at UPDATE)
    - session renewal (sliding-window expiry extension)
    - session diagnostic logging

    This eliminates DB writes from status polling paths, reducing
    DB connection usage from 2-3 per poll to 0-1.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == 'OPTIONS':
            return f(*args, **kwargs)

        DatabaseError = _get_database_error()

        try:
            identity, session_id = _resolve_identity(readonly=True)

            if identity:
                # No cookie refresh needed — readonly never renews sessions
                return f(*args, **kwargs)

            # No valid session — bootstrap anonymous identity (still needed
            # for first-visit status checks via shared links, etc.)
            try:
                session_id, identity_id, identity_with_wallet, cookie_response = _do_bootstrap()
            except _BootstrapBusy:
                return _bootstrap_retry_response()

            g.session_id = session_id
            g.identity_id = identity_id
            g.identity = identity_with_wallet
            g._identity_resolved = True
            g._identity_source = "bootstrap"

            result = f(*args, **kwargs)
            resp = _ensure_response(result)
            _copy_cookies(cookie_response, resp)
            return resp

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in with_session_readonly: {e}")
            return jsonify({
                "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
            }), 500

    return decorated


def with_optional_session(f):
    """
    Decorator that validates an existing session but NEVER creates one.
    If no valid session OR if session lookup fails (DB timeout, pool exhaustion,
    SSL error), sets g fields to None and lets the handler proceed.

    This is fail-open by design: used on restore/redeem and similar paths
    where blocking on a DB error would be worse than proceeding without
    a session.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        # Use the safe variant that catches DB errors and returns (None, None)
        # instead of raising. This ensures restore/redeem never blocks on pool.
        identity, session_id = _resolve_identity_safe()

        if identity:
            result = f(*args, **kwargs)
            return _maybe_refresh_cookie(identity, session_id, result)

        # No valid session (or lookup failed) — proceed without one
        return f(*args, **kwargs)

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


def require_session_readonly(f):
    """
    Decorator that requires a valid session (returns 401 if missing).
    Same as require_session but on cache-miss validation:
    - skips identity touch (last_seen_at UPDATE)
    - skips session renewal (sliding-window expiry extension)
    - skips cookie refresh

    Use for read-only GET endpoints (wallet, subscriptions/summary,
    jobs/active) where DB writes from session bookkeeping are wasted.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.method == 'OPTIONS':
            return f(*args, **kwargs)

        DatabaseError = _get_database_error()

        try:
            identity, session_id = _resolve_identity(readonly=True)

            if not identity:
                if SESSION_DEBUG or "/subscriptions" in request.path:
                    print(
                        f"[MIDDLEWARE] require_session_readonly 401: "
                        f"path={request.path}, "
                        f"cookie_present={bool(request.cookies.get('timrx_sid'))}, "
                        f"session_id={session_id[:16] + '...' if session_id else 'None'}"
                    )
                return jsonify({
                    "error": {"code": "UNAUTHORIZED", "message": "Valid session required"}
                }), 401

            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_session_readonly: {e}")
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


# ─────────────────────────────────────────────────────────────
# Admin auth hardening helpers
# ─────────────────────────────────────────────────────────────
_CF_JWKS_CLIENTS = {}  # certs_url -> PyJWKClient (caches JWKS internally)


def _admin_client_ip() -> str:
    """Best client IP behind Cloudflare/Render proxies."""
    ip = request.headers.get("CF-Connecting-IP")
    if ip:
        return ip.strip()
    xff = request.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"


def _record_admin_failure(ip):
    """
    Record a failed admin auth attempt (per-IP) in the shared rate-limit store.
    Returns (is_rate_limited: bool, retry_after_seconds: int).
    Fails open on infra errors so a DB hiccup never locks out a legitimate admin.
    """
    try:
        from backend.config import config
        from backend.services.auth_rate_limit_service import AuthRateLimitService
        res = AuthRateLimitService.hit(
            "admin_auth_fail",
            [ip],
            limit=config.ADMIN_AUTH_MAX_FAILURES,
            window_seconds=config.ADMIN_AUTH_FAIL_WINDOW,
        )
        return (not res.get("ok", True)), int(res.get("retry_after", 0))
    except Exception as e:  # pragma: no cover - defensive
        print(f"[MIDDLEWARE] admin rate-limit check failed (fail-open): {e}")
        return False, 0


def _verify_cf_access_jwt(token):
    """
    Verify a Cloudflare Access (Zero Trust) JWT from the Cf-Access-Jwt-Assertion
    header. Returns the claims dict on success, else None.

    Validates signature (RS256 via Cloudflare JWKS), issuer (your team domain)
    and audience (your Access application AUD tag).
    """
    try:
        from backend.config import config
        import jwt as _jwt
        team = (config.CF_ACCESS_TEAM_DOMAIN or "").strip()
        aud = (config.CF_ACCESS_AUD or "").strip()
        if not team or not aud:
            return None
        if team.startswith("http"):
            issuer = team.rstrip("/")
        else:
            domain = team if team.endswith("cloudflareaccess.com") else f"{team}.cloudflareaccess.com"
            issuer = f"https://{domain}"
        certs_url = f"{issuer}/cdn-cgi/access/certs"
        client = _CF_JWKS_CLIENTS.get(certs_url)
        if client is None:
            client = _jwt.PyJWKClient(certs_url)
            _CF_JWKS_CLIENTS[certs_url] = client
        signing_key = client.get_signing_key_from_jwt(token)
        return _jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=aud,
            issuer=issuer,
        )
    except Exception as e:
        print(f"[MIDDLEWARE] CF Access JWT verification failed: {e}")
        return None


def require_admin(f):
    """
    Decorator that requires admin authentication. In priority order:
      1. X-Admin-Token header (constant-time compare; for scripts/dashboard)
      2. Cloudflare Access JWT (Cf-Access-Jwt-Assertion; Zero Trust identity)
      3. Email-based session (verified admin email)
    Failed attempts are rate-limited per IP to throttle brute force.
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

        ip = _admin_client_ip()

        def _fail(code, message, status=403):
            limited, retry_after = _record_admin_failure(ip)
            if limited:
                resp = jsonify({
                    "error": {"code": "RATE_LIMITED", "message": "Too many failed admin attempts. Try again later."}
                })
                resp.headers["Retry-After"] = str(retry_after or config.ADMIN_AUTH_FAIL_WINDOW)
                return resp, 429
            return jsonify({"error": {"code": code, "message": message}}), status

        # Method 1: Token-based authentication (constant-time compare)
        admin_token = request.headers.get("X-Admin-Token")
        if admin_token:
            if config.ADMIN_TOKEN and hmac.compare_digest(
                admin_token.encode("utf-8"), config.ADMIN_TOKEN.encode("utf-8")
            ):
                g.admin_auth_method = "token"
                g.admin_email = None
                g.identity = None
                return f(*args, **kwargs)
            return _fail("INVALID_ADMIN_TOKEN", "Invalid admin token")

        # Method 2: Cloudflare Access (Zero Trust) JWT
        cf_jwt = request.headers.get("Cf-Access-Jwt-Assertion") or request.headers.get("CF-Access-Jwt-Assertion")
        if cf_jwt and config.CF_ACCESS_CONFIGURED:
            claims = _verify_cf_access_jwt(cf_jwt)
            if claims:
                email = (claims.get("email") or "").lower().strip()
                # If an admin allow-list is configured, enforce it as a second gate.
                if config.ADMIN_EMAILS and not config.is_admin_email(email):
                    return _fail("NOT_ADMIN", "You do not have admin privileges")
                g.admin_auth_method = "cf_access"
                g.admin_email = email or None
                g.identity = None
                return f(*args, **kwargs)
            return _fail("INVALID_CF_ACCESS", "Invalid Cloudflare Access token")

        # Method 3: Email-based authentication (verified session)
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
                return _fail("NOT_ADMIN", "You do not have admin privileges")

            g.admin_auth_method = "email"
            g.admin_email = email
            return f(*args, **kwargs)

        except DatabaseError as e:
            print(f"[MIDDLEWARE] Database error in require_admin: {e}")
            return jsonify({
                "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
            }), 500

    return decorated
