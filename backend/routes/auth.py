"""
/api/auth routes - Authentication and session management.

Handles:
- POST /api/auth/restore/request - Request magic code via email
- POST /api/auth/restore/redeem - Verify magic code and restore session
- POST /api/auth/request-code - Alias for restore/request
- POST /api/auth/verify-code - Alias for restore/redeem
- GET /api/auth/status - Get current auth status
- POST /api/auth/email/attach - Attach email to current identity
- POST /api/auth/email/verify - Verify email on current identity

Note: Logout is handled by /api/me/logout for consistency with other /me endpoints.
"""

import time
import random
from functools import wraps

from flask import Blueprint, request, jsonify, g

from backend.middleware import with_session, with_optional_session, require_session, require_admin, no_cache
from backend.services.magic_code_service import MagicCodeService
from backend.services.identity_service import IdentityService
from backend.services.wallet_service import WalletService

bp = Blueprint("auth", __name__)


# ─────────────────────────────────────────────────────────────
# Per-IP rate limiter for email attach endpoint
# In-process sliding window — no external dependencies.
# Limits each IP to ATTACH_IP_MAX_REQUESTS within ATTACH_IP_WINDOW_SECS.
# ─────────────────────────────────────────────────────────────

ATTACH_IP_WINDOW_SECS = 60
ATTACH_IP_MAX_REQUESTS = 10
_ip_attach_log = {}  # { ip: [timestamp, ...] }
_ip_attach_last_cleanup = 0


def _check_ip_attach_rate(ip: str) -> bool:
    """
    Return True if the IP is within rate limits, False if over.
    Also piggyback-cleans stale entries every 60 seconds.
    """
    global _ip_attach_last_cleanup

    now = time.time()
    cutoff = now - ATTACH_IP_WINDOW_SECS

    # Periodic cleanup — purge IPs with only stale timestamps (max once per window)
    if now - _ip_attach_last_cleanup > ATTACH_IP_WINDOW_SECS:
        stale_ips = [k for k, v in _ip_attach_log.items() if not v or v[-1] < cutoff]
        for k in stale_ips:
            del _ip_attach_log[k]
        _ip_attach_last_cleanup = now

    # Get or create entry for this IP
    timestamps = _ip_attach_log.get(ip)
    if timestamps is None:
        timestamps = []
        _ip_attach_log[ip] = timestamps

    # Trim expired entries for this IP
    while timestamps and timestamps[0] < cutoff:
        timestamps.pop(0)

    # Check limit
    if len(timestamps) >= ATTACH_IP_MAX_REQUESTS:
        return False

    # Record this request
    timestamps.append(now)
    return True


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _normalize_email(raw_email) -> str:
    """
    Safely normalize email: strip whitespace, lowercase, handle None/non-string.
    Used by both attach and verify to ensure consistency.
    """
    if not raw_email:
        return ""
    if not isinstance(raw_email, str):
        return ""
    return raw_email.strip().lower()


def _mask_email(email: str) -> str:
    """Mask email for safe logging: show first 3 chars + ***"""
    if not email or len(email) < 3:
        return "***"
    return f"{email[:3]}***"


def _mask_code(code: str) -> str:
    """Mask code for safe logging: show only last 2 digits"""
    if not code or len(code) < 2:
        return "**"
    return f"****{code[-2:]}"


def _get_redeem_error_response(message: str):
    """Build error response for redeem failures."""
    error_code = "INVALID_CODE"
    status_code = 400

    if "too many" in message.lower():
        error_code = "TOO_MANY_ATTEMPTS"
    elif "expired" in message.lower():
        error_code = "CODE_EXPIRED"
    elif "session" in message.lower():
        error_code = "SESSION_ERROR"
        status_code = 500

    return jsonify({
        "error": {
            "code": error_code,
            "message": message,
        }
    }), status_code


def _get_client_ip() -> str:
    """Get client IP address from request, handling proxies."""
    # Check X-Forwarded-For header (set by proxies/load balancers)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Take the first IP in the chain (original client)
        return forwarded_for.split(",")[0].strip()
    # Fall back to remote_addr
    return request.remote_addr or ""


# ─────────────────────────────────────────────────────────────
# IDENT-3: Anti-enumeration timing equalization
# ─────────────────────────────────────────────────────────────

# Minimum execution time (seconds) for auth endpoints.
# Set to cover the typical slow path (code generation + email send).
_AUTH_MIN_RESPONSE_SECS = 0.35


def anti_enumeration(min_secs=_AUTH_MIN_RESPONSE_SECS):
    """
    IDENT-3: Decorator that equalizes response timing to prevent
    email-existence enumeration via timing side-channels.

    Ensures every request takes at least `min_secs` plus random jitter,
    so fast-path (unknown email) and slow-path (known email, code sent)
    are indistinguishable from the outside.
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start = time.monotonic()
            try:
                return f(*args, **kwargs)
            finally:
                elapsed = time.monotonic() - start
                remaining = min_secs - elapsed
                # Random jitter 0–50ms prevents statistical averaging
                jitter = random.uniform(0, 0.05)
                pad = max(0, remaining) + jitter
                if pad > 0:
                    time.sleep(pad)
        return wrapper
    return decorator


@bp.route("/restore/request", methods=["POST"])
@no_cache
@anti_enumeration()
def request_restore():
    """
    Request a magic code to restore account access.
    Sends a 6-digit code to the provided email if it exists.

    Request body:
    {
        "email": "user@example.com"
    }

    Response (always 200 to prevent email enumeration):
    {
        "ok": true,
        "message": "If this email is registered, a code has been sent"
    }

    Rate limited:
    - Max 3 active codes per email
    - 60 second cooldown between requests
    """
    data = request.get_json() or {}
    email = data.get("email", "").strip()

    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    # Basic email validation
    if "@" not in email or "." not in email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid email format",
            }
        }), 400

    ip_address = _get_client_ip()
    print(f"[RESTORE][REQUEST] email={email} ip={ip_address or 'none'}")

    result = MagicCodeService.request_restore(email, ip_address)

    if not result.get("ok"):
        message = result.get("message", "Request failed")
        print(f"[RESTORE][ROUTE_FAIL] email={email} message={message}")

        # Rate limit errors should return 429
        if "wait" in message.lower() or "too many" in message.lower():
            return jsonify({
                "error": {
                    "code": "RATE_LIMITED",
                    "message": message,
                }
            }), 429

        # Transient service errors → 503 so frontend knows to retry
        if "temporarily unavailable" in message.lower() or "couldn't send" in message.lower():
            return jsonify({
                "error": {
                    "code": "SERVICE_UNAVAILABLE",
                    "message": message,
                }
            }), 503

        return jsonify({
            "error": {
                "code": "REQUEST_FAILED",
                "message": message,
            }
        }), 400

    print(f"[RESTORE][ROUTE_OK] email={email}")
    return jsonify({
        "ok": True,
        "message": result.get("message", "Code sent"),
    })


@bp.route("/restore/redeem", methods=["POST"])
@no_cache
@with_optional_session
@anti_enumeration()
def redeem_restore():
    """
    Verify a magic code and restore session.
    Links the current browser session (timrx_sid) to the identity with that email.

    Request body:
    {
        "email": "user@example.com",
        "code": "123456"
    }

    Response (success - 200):
    {
        "ok": true,
        "message": "Account restored successfully",
        "me": {
            "identity_id": "uuid",
            "email": "user@example.com",
            "email_verified": true
        },
        "wallet": {
            "balance": 80,
            "reserved": 0,
            "available": 80
        }
    }

    Response (failure - 400):
    {
        "error": {
            "code": "INVALID_CODE",
            "message": "Invalid or expired code"
        }
    }

    Requires: Active session (from cookie)
    """
    data = request.get_json() or {}
    email = data.get("email", "").strip()
    code = data.get("code", "").strip()

    # Validation
    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    if not code:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "code is required",
            }
        }), 400

    if len(code) != 6 or not code.isdigit():
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Code must be 6 digits",
            }
        }), 400

    # AUTH-3: Get session ID from middleware — may be None if no cookie existed
    # or if session lookup failed (pool timeout, SSL error — fail-open).
    session_id = g.session_id  # None if no existing session
    print(f"[RESTORE][REDEEM] proceeding session_id={'present' if session_id else 'none'} email={email[:3]}***")

    # Verify the code and link session (handles session_id=None safely)
    success, identity_id, message, _detail = MagicCodeService.redeem_restore(
        email=email,
        code=code,
        session_id=session_id,
    )

    if not success or not identity_id:
        return _get_redeem_error_response(message)

    # AUTH-3: If there was no pre-existing session, create one directly
    # for the restore target identity (no throwaway identity needed).
    resp_needs_cookie = False

    if not session_id:
        new_session_id = IdentityService.create_session_for_identity(
            identity_id,
            ip_address=_get_client_ip(),
            user_agent=request.headers.get("User-Agent", ""),
        )
        if new_session_id:
            session_id = new_session_id
            resp_needs_cookie = True
            print(
                f"[RESTORE] AUTH-3: Created session directly for target "
                f"{identity_id[:8]}... (no throwaway identity)"
            )

    # Invalidate stale session process cache so subsequent /api/me and
    # /api/credits/wallet calls return the NEW identity's data, not the
    # old anonymous identity cached for this session_id.
    if session_id:
        from backend.services.identity_service import _session_cache_invalidate
        _session_cache_invalidate(session_id)

    # Also invalidate response caches for both identities
    from backend.routes.me import _me_cache
    _me_cache.pop(identity_id, None)
    if g.identity_id and g.identity_id != identity_id:
        _me_cache.pop(g.identity_id, None)
    # Invalidate wallet cache so /api/credits/wallet returns fresh data
    from backend.services.wallet_service import invalidate_wallet_cache
    invalidate_wallet_cache(identity_id)

    # Fetch updated identity and wallet data
    identity = IdentityService.get_identity(identity_id)
    wallet = WalletService.get_wallet(identity_id)

    # Seed session cache with the new identity so subsequent GET /api/me
    # and /api/credits/wallet return the correct identity immediately.
    if session_id and identity:
        from backend.services.identity_service import _session_cache_put
        _session_cache_put(session_id, identity)

    balance = wallet.get("balance_credits", 0) if wallet else 0
    reserved = WalletService.get_reserved_credits(identity_id)
    available = max(0, balance - reserved)

    # Format created_at safely
    created_at = None
    if identity and identity.get("created_at"):
        created_at = identity["created_at"].isoformat()

    result = jsonify({
        "ok": True,
        "message": message,
        "switched_account": True,
        "merge_performed": False,
        "me": {
            "identity_id": identity_id,
            "email": identity.get("email") if identity else email,
            "email_verified": identity.get("email_verified", True) if identity else True,
            "created_at": created_at,
        },
        "wallet": {
            "balance": balance,
            "reserved": reserved,
            "available": available,
            "currency": "GBP",
        },
    })

    # Set cookie on the response if a new session was created
    if resp_needs_cookie and session_id:
        IdentityService.set_session_cookie(result, session_id)

    return result


@bp.route("/status", methods=["GET"])
@no_cache
@with_session
def auth_status():
    """
    Get current authentication status.
    Returns session info and whether user has email attached.

    Response:
    {
        "ok": true,
        "has_session": true,
        "identity_id": "uuid",
        "has_email": true,
        "email": "user@example.com",
        "email_verified": true
    }
    """
    identity = g.identity

    return jsonify({
        "ok": True,
        "has_session": bool(g.session_id),
        "identity_id": g.identity_id,
        "has_email": bool(identity.get("email")) if identity else False,
        "email": identity.get("email") if identity else None,
        "email_verified": identity.get("email_verified", False) if identity else False,
    })


@bp.route("/email/attach", methods=["POST"])
@no_cache
@with_session
@anti_enumeration()
def attach_email():
    """
    Attach an email to the current identity and send verification code.

    This endpoint allows users to attach
    an email address. The email is stored but marked unverified until the user
    completes the magic code verification.

    Request body:
    {
        "email": "user@example.com"
    }

    Response (always 200 to prevent email enumeration):
    {
        "ok": true,
        "message": "If valid, a verification code has been sent"
    }

    Behavior:
    - If email is free: attach to current identity, send verification code
    - If email belongs to another VERIFIED identity: return generic success,
      but user must use restore flow to take over that account
    - If email already on current identity: resend verification code

    Rate limited: same as restore/request (60s cooldown, max 3 active codes)
    """
    data = request.get_json() or {}
    email = _normalize_email(data.get("email"))

    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    # Basic email validation
    if "@" not in email or "." not in email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid email format",
            }
        }), 400

    identity_id = g.identity_id
    if not identity_id:
        return jsonify({
            "error": {
                "code": "SESSION_REQUIRED",
                "message": "Active session required",
            }
        }), 401

    ip_address = _get_client_ip()

    # Per-IP rate limit (defense against enumeration across many emails)
    if not _check_ip_attach_rate(ip_address):
        print(f"[AUTH] IP rate limited on email/attach: ip={ip_address}")
        return jsonify({
            "error": {
                "code": "RATE_LIMITED",
                "message": "Too many requests. Please try again later.",
            }
        }), 429

    # Attach email to current identity (unverified)
    try:
        _, _, reason = IdentityService.attach_email(identity_id, email)
    except ValueError as e:
        print(f"[AUTH] Failed to attach email: {e}")
        reason = IdentityService.ATTACH_REASON_BELONGS_TO_OTHER

    if reason == IdentityService.ATTACH_REASON_BELONGS_TO_OTHER:
        # Email belongs to another account. Do NOT send a verification code
        # for this identity — the code would verify the wrong account.
        # Return a structured response so frontend can guide user to
        # restore/switch instead.
        print(f"[ATTACH] Email belongs to another account; restore required: {_mask_email(email)}")
        return jsonify({
            "ok": True,
            "message": "If valid, a verification code has been sent",
            "hint": "account_switch_required",
            "merge_disabled": True,
        })

    # Check rate limits and send verification code
    rate_ok, rate_msg = MagicCodeService._check_rate_limits(email)
    if not rate_ok:
        if "wait" in rate_msg.lower() or "too many" in rate_msg.lower():
            return jsonify({
                "error": {
                    "code": "RATE_LIMITED",
                    "message": rate_msg,
                }
            }), 429
        return jsonify({
            "error": {
                "code": "REQUEST_FAILED",
                "message": rate_msg,
            }
        }), 400

    # Generate and send verification code
    MagicCodeService.request_restore(email, ip_address)

    return jsonify({
        "ok": True,
        "message": "If valid, a verification code has been sent",
        "hint": "code_sent",
    })


def _handle_verify_cross_identity(code_id, email, source_id, target_id, session_id):
    """
    Handle cross-identity verify: the code proves the user controls `email`,
    but that email belongs to a different identity (target_id).

    NEW BEHAVIOR: Do NOT merge accounts. Consume code, switch session to
    the email-owning identity. Each account keeps its own data.

    Returns a Flask response.
    """
    from backend.db import transaction, Tables

    print(
        f"[VERIFY] Cross-identity switch (no merge): "
        f"source={source_id[:8]}... → target={target_id[:8]}..., "
        f"email={_mask_email(email)}"
    )

    with transaction() as cur:
        # Consume code
        cur.execute(
            f"""
            UPDATE {Tables.MAGIC_CODES}
            SET consumed = TRUE, consumed_at = NOW()
            WHERE id = %s
            """,
            (code_id,),
        )

        # Switch session to the email-owning identity (account switch, not merge)
        try:
            cur.execute(
                f"""
                UPDATE {Tables.SESSIONS}
                SET identity_id = %s, updated_at = NOW()
                WHERE id = %s
                RETURNING id
                """,
                (target_id, session_id),
            )
        except Exception as e:
            if "updated_at" in str(e):
                cur.execute(
                    f"""
                    UPDATE {Tables.SESSIONS}
                    SET identity_id = %s
                    WHERE id = %s
                    RETURNING id
                    """,
                    (target_id, session_id),
                )
            else:
                raise

        # Ensure the target identity is marked email_verified.
        # Do NOT overwrite target's email — it already owns `email`.
        cur.execute(
            f"""
            UPDATE {Tables.IDENTITIES}
            SET email_verified = TRUE, last_seen_at = NOW()
            WHERE id = %s
            """,
            (target_id,),
        )

    MagicCodeService.invalidate_codes_for_email(email)

    # Resume subscriptions paused due to email_unverified
    subscriptions_resumed = 0
    try:
        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.SUBSCRIPTIONS}
                SET pause_reason = NULL,
                    paused_at = NULL,
                    updated_at = NOW()
                WHERE identity_id::text = %s
                  AND pause_reason = 'email_unverified'
                RETURNING id
                """,
                (target_id,),
            )
            resumed = cur.fetchall() or []
            subscriptions_resumed = len(resumed)
    except Exception as e:
        print(f"[VERIFY] Error resuming subscriptions for {target_id}: {e}")

    print(
        f"[VERIFY] Switched session to identity {target_id[:8]}... "
        f"(email={_mask_email(email)}, source was {source_id[:8]}...)"
    )

    return jsonify({
        "ok": True,
        "message": "Email verified successfully",
        "email_verified": True,
        "email_attached": True,
        "identity_changed": True,
        "switched_account": True,
        "merge_performed": False,
        "identity_id": target_id,
        "subscriptions_resumed": subscriptions_resumed,
    })


@bp.route("/email/verify", methods=["POST"])
@no_cache
@require_session
@anti_enumeration()
def verify_email():
    """
    Verify and attach email to current identity using magic code.

    Flow:
    1. User calls POST /api/auth/email/attach (sends code to email)
    2. User enters code and calls POST /api/auth/email/verify
    3. Code is verified, email is attached to identity idempotently

    For cross-identity cases (email belongs to another account),
    session is switched to the email-owning account. No merge is performed.

    Request body:
    {
        "email": "user@example.com",
        "code": "123456"
    }

    Response (success - 200):
    {
        "ok": true,
        "message": "Email verified successfully",
        "email_verified": true,
        "email_attached": true,
        "identity_changed": false,
        "switched_account": true/false,
        "merge_performed": false
    }

    Error responses:
    - 400 VALIDATION_ERROR: Missing/invalid email or code
    - 400 INVALID_CODE: Code doesn't match or expired
    - 400 CODE_EXPIRED: Code has expired
    - 400 TOO_MANY_ATTEMPTS: Max attempts exceeded
    - 401 NO_SESSION: No active session
    """
    data = request.get_json() or {}

    # Normalize email (same as attach_email for consistency)
    email = _normalize_email(data.get("email"))

    # Safely normalize code
    raw_code = data.get("code")
    code = (raw_code.strip() if isinstance(raw_code, str) else "") if raw_code else ""

    # Validation
    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    if "@" not in email or "." not in email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid email format",
            }
        }), 400

    if not code:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "code is required",
            }
        }), 400

    if len(code) != 6 or not code.isdigit():
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Code must be 6 digits",
            }
        }), 400

    # Check session - return specific error code for frontend
    identity_id = g.identity_id
    identity = g.identity

    if not identity_id:
        print(f"[AUTH] verify_email failed: no session for email={_mask_email(email)}")
        return jsonify({
            "error": {
                "code": "NO_SESSION",
                "message": "No active session. Please refresh the page.",
            }
        }), 401

    # Log attempt (never log full code, only last 2 digits if needed)
    print(f"[AUTH] verify_email attempt: identity={identity_id}, email={_mask_email(email)}, code={_mask_code(code)}")

    # Verify the code against magic_codes table
    code_hash = MagicCodeService.hash_code(code)
    from backend.db import query_one, transaction, Tables
    from backend.config import config

    # Find matching active code for this email
    code_record = query_one(
        f"""
        SELECT id, attempts
        FROM {Tables.MAGIC_CODES}
        WHERE email = %s
          AND code_hash = %s
          AND consumed = FALSE
          AND expires_at > NOW()
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (email, code_hash),
    )

    if not code_record:
        # Code not found or doesn't match - increment attempts counter
        MagicCodeService._increment_attempts(email)
        print(f"[AUTH] verify_email failed: invalid code for email={_mask_email(email)}")
        return _get_redeem_error_response("Invalid or expired code")

    # Check max attempts
    max_attempts = config.MAGIC_CODE_MAX_ATTEMPTS
    if code_record["attempts"] >= max_attempts:
        print(f"[AUTH] verify_email failed: too many attempts for email={_mask_email(email)}")
        return _get_redeem_error_response("Too many failed attempts. Please request a new code")

    code_id = str(code_record["id"])

    # ── Determine verification target ──
    current_email = None
    if identity and identity.get("email"):
        current_email = identity["email"].lower() if isinstance(identity["email"], str) else None

    # ── Cross-identity check (no merge) ──
    # If this email belongs to another identity, switch session to that
    # exact identity. Do NOT follow merge chains or resolve canonical.
    # Use a direct query instead of IdentityService.get_identity_by_email
    # because that helper auto-resolves merged_into_id.
    if current_email != email:
        existing_owner = query_one(
            f"SELECT id FROM {Tables.IDENTITIES} WHERE email = %s",
            (email,),
        )
        if existing_owner and str(existing_owner["id"]) != identity_id:
            owner_id = str(existing_owner["id"])
            print(
                f"[VERIFY] Email belongs to another account; "
                f"switching session (merge disabled): "
                f"source={identity_id[:8]}..., target={owner_id[:8]}..., "
                f"email={_mask_email(email)}"
            )
            return _handle_verify_cross_identity(
                code_id=code_id,
                email=email,
                source_id=identity_id,
                target_id=owner_id,
                session_id=g.session_id,
            )

    # ── Same-identity case: attach email idempotently ──
    email_attached = False

    if current_email == email:
        # Email already on this identity - just verify it
        pass
    elif current_email is None:
        # Identity has no email, and email is free → attach it
        email_attached = True
    else:
        # Identity has a DIFFERENT email, and new email is free → change it
        email_attached = True
        print(f"[AUTH] Email change requested: identity {identity_id} from {_mask_email(current_email)} to {_mask_email(email)}")

    # ── Commit: mark code consumed & attach/verify email ──
    with transaction() as cur:
        # Mark code as consumed
        cur.execute(
            f"""
            UPDATE {Tables.MAGIC_CODES}
            SET consumed = TRUE, consumed_at = NOW()
            WHERE id = %s
            """,
            (code_id,),
        )

        # Attach email (if needed) and mark verified
        if email_attached:
            cur.execute(
                f"""
                UPDATE {Tables.IDENTITIES}
                SET email = %s, email_verified = TRUE, last_seen_at = NOW()
                WHERE id = %s
                """,
                (email, identity_id),
            )
        else:
            cur.execute(
                f"""
                UPDATE {Tables.IDENTITIES}
                SET email_verified = TRUE, last_seen_at = NOW()
                WHERE id = %s
                """,
                (identity_id,),
            )

    # Invalidate other pending codes for this email
    MagicCodeService.invalidate_codes_for_email(email)

    # Resume any subscriptions paused due to email_unverified
    subscriptions_resumed = 0
    try:
        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.SUBSCRIPTIONS}
                SET pause_reason = NULL,
                    paused_at = NULL,
                    updated_at = NOW()
                WHERE identity_id::text = %s
                  AND pause_reason = 'email_unverified'
                RETURNING id
                """,
                (identity_id,),
            )
            resumed = cur.fetchall() or []
            subscriptions_resumed = len(resumed)
            for row in resumed:
                print(f"[AUTH] Resumed subscription {row['id']} after email verification")
    except Exception as e:
        print(f"[AUTH] Error resuming subscriptions for {identity_id}: {e}")

    print(f"[AUTH] Email verified for identity {identity_id}: {_mask_email(email)} (attached={email_attached}, subs_resumed={subscriptions_resumed})")

    # IDENT-3: Always include identity_changed and identity_id to normalize
    # response shape (same keys as cross-identity path).
    return jsonify({
        "ok": True,
        "message": "Email verified successfully",
        "email_verified": True,
        "email_attached": email_attached,
        "identity_changed": False,
        "switched_account": False,
        "merge_performed": False,
        "identity_id": identity_id,
        "subscriptions_resumed": subscriptions_resumed,
    })


# ─────────────────────────────────────────────────────────────
# Route Aliases (shorter paths for convenience)
# ─────────────────────────────────────────────────────────────

@bp.route("/request-code", methods=["POST"])
def request_code_alias():
    """Alias for /restore/request - Request a magic code via email."""
    return request_restore()


@bp.route("/verify-code", methods=["POST"])
@with_optional_session
def verify_code_alias():
    """Alias for /restore/redeem - Verify a magic code and restore session."""
    return redeem_restore()


# ─────────────────────────────────────────────────────────────
# Admin Debug Endpoint
# ─────────────────────────────────────────────────────────────

@bp.route("/health", methods=["GET"])
@require_admin
def auth_health():
    """
    Admin-only health check for auth system diagnostics.

    Checks:
    - SES/email configuration
    - DB schema (sessions.updated_at column)
    - magic_codes table exists

    Requires: X-Admin-Token header

    Response:
    {
        "ok": true,
        "checks": {
            "email": { "configured": true, "provider": "ses", "region": "eu-west-2", "from": "no-reply@timrx.live" },
            "db_sessions_updated_at": { "exists": true },
            "db_magic_codes": { "exists": true, "active_count": 5 }
        }
    }
    """
    from backend.config import config
    from backend.db import query_one, USE_DB

    checks = {}

    # ── Email configuration ──
    try:
        email_check = {
            "configured": config.EMAIL_CONFIGURED,
            "enabled": config.EMAIL_ENABLED,
            "provider": config.EMAIL_PROVIDER,
        }
        if config.EMAIL_PROVIDER == "ses":
            email_check["region"] = config.AWS_REGION
            email_check["from_email"] = config.SES_FROM_EMAIL or config.EMAIL_FROM_ADDRESS
        else:
            email_check["smtp_host"] = config.SMTP_HOST or "(not set)"
            name, addr = config.SMTP_FROM_PARSED
            email_check["from_email"] = addr
            email_check["from_name"] = name
        checks["email"] = email_check
    except Exception as e:
        checks["email"] = {"error": str(e)}

    # ── DB: sessions.updated_at column ──
    if USE_DB:
        try:
            result = query_one("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'timrx_billing'
                  AND table_name = 'sessions'
                  AND column_name = 'updated_at'
            """)
            checks["db_sessions_updated_at"] = {"exists": result is not None}
        except Exception as e:
            checks["db_sessions_updated_at"] = {"exists": False, "error": str(e)}

        # ── DB: magic_codes table ──
        try:
            result = query_one("""
                SELECT COUNT(*) as cnt
                FROM timrx_billing.magic_codes
                WHERE consumed = FALSE AND expires_at > NOW()
            """)
            checks["db_magic_codes"] = {
                "exists": True,
                "active_codes": result["cnt"] if result else 0
            }
        except Exception as e:
            checks["db_magic_codes"] = {"exists": False, "error": str(e)}
    else:
        checks["db_sessions_updated_at"] = {"exists": False, "reason": "DB not configured"}
        checks["db_magic_codes"] = {"exists": False, "reason": "DB not configured"}

    # Overall status
    all_ok = (
        checks.get("email", {}).get("configured", False) and
        checks.get("db_sessions_updated_at", {}).get("exists", False) and
        checks.get("db_magic_codes", {}).get("exists", False)
    )

    return jsonify({
        "ok": all_ok,
        "checks": checks,
    })
