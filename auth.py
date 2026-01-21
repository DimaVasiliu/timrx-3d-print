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

from flask import Blueprint, request, jsonify, g

from middleware import with_session
from magic_code_service import MagicCodeService
from identity_service import IdentityService
from wallet_service import WalletService

bp = Blueprint("auth", __name__)


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


@bp.route("/restore/request", methods=["POST"])
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

    # Get client IP for rate limiting
    ip_address = _get_client_ip()

    # Request the code
    success, message = MagicCodeService.request_restore(email, ip_address)

    if not success:
        # Rate limit errors should return 429
        if "wait" in message.lower() or "too many" in message.lower():
            return jsonify({
                "error": {
                    "code": "RATE_LIMITED",
                    "message": message,
                }
            }), 429

        return jsonify({
            "error": {
                "code": "REQUEST_FAILED",
                "message": message,
            }
        }), 400

    # Always return success (even if email doesn't exist) to prevent enumeration
    return jsonify({
        "ok": True,
        "message": message,
    })


@bp.route("/restore/redeem", methods=["POST"])
@with_session
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

    # Get session ID from middleware
    session_id = g.session_id
    if not session_id:
        return jsonify({
            "error": {
                "code": "SESSION_REQUIRED",
                "message": "Active session required. Please refresh the page.",
            }
        }), 401

    # Verify the code and link session
    success, identity_id, message = MagicCodeService.redeem_restore(
        email=email,
        code=code,
        session_id=session_id,
    )

    if not success or not identity_id:
        return _get_redeem_error_response(message)

    # Fetch updated identity and wallet data
    identity = IdentityService.get_identity(identity_id)
    wallet = WalletService.get_wallet(identity_id)

    balance = wallet.get("balance_credits", 0) if wallet else 0
    reserved = WalletService.get_reserved_credits(identity_id)
    available = max(0, balance - reserved)

    # Format created_at safely
    created_at = None
    if identity and identity.get("created_at"):
        created_at = identity["created_at"].isoformat()

    return jsonify({
        "ok": True,
        "message": message,
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


@bp.route("/status", methods=["GET"])
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
@with_session
def attach_email():
    """
    Attach an email to the current identity and send verification code.

    This endpoint allows anonymous users to "secure" their credits by attaching
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
    email = data.get("email", "").strip().lower()

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

    # Attach email to current identity (unverified)
    # This method is anti-enumeration safe - it returns ok even if email belongs
    # to another identity (user must use restore flow, but we don't tell them explicitly)
    try:
        _, _, reason = IdentityService.attach_email(identity_id, email)
        # Log the reason internally but NEVER expose to users
        if reason == IdentityService.ATTACH_REASON_BELONGS_TO_OTHER:
            # Email belongs to another identity - silently skip attachment
            # User must use restore/redeem flow to take over that account
            pass  # Already logged in attach_email
    except ValueError as e:
        print(f"[AUTH] Failed to attach email: {e}")
        # Continue anyway to prevent enumeration

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

    # Generate and send verification code (result ignored - always return generic success)
    MagicCodeService.request_restore(email, ip_address)

    # Always return generic success to prevent enumeration
    return jsonify({
        "ok": True,
        "message": "If valid, a verification code has been sent",
    })


@bp.route("/email/verify", methods=["POST"])
@with_session
def verify_email():
    """
    Verify the email attached to current identity using magic code.

    Unlike restore/redeem, this does NOT switch identity - it just marks
    the email as verified on the current identity.

    Request body:
    {
        "email": "user@example.com",
        "code": "123456"
    }

    Response (success - 200):
    {
        "ok": true,
        "message": "Email verified successfully",
        "email_verified": true
    }

    Response (failure - 400):
    {
        "error": {
            "code": "INVALID_CODE",
            "message": "Invalid or expired code"
        }
    }
    """
    data = request.get_json() or {}
    email = data.get("email", "").strip().lower()
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

    identity_id = g.identity_id
    identity = g.identity

    if not identity_id:
        return jsonify({
            "error": {
                "code": "SESSION_REQUIRED",
                "message": "Active session required",
            }
        }), 401

    # Check that email matches what's on the identity
    current_email = identity.get("email", "").lower() if identity else ""
    if current_email != email:
        return jsonify({
            "error": {
                "code": "EMAIL_MISMATCH",
                "message": "Email does not match your account",
            }
        }), 400

    # Verify the code
    code_hash = MagicCodeService.hash_code(code)
    from db import query_one, transaction, fetch_one, execute, Tables

    # Find matching active code
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
        # Code not found - increment attempts
        MagicCodeService._increment_attempts(email)
        return _get_redeem_error_response("Invalid or expired code")

    from config import config
    if code_record["attempts"] >= config.MAGIC_CODE_MAX_ATTEMPTS:
        return _get_redeem_error_response("Too many failed attempts. Please request a new code")

    code_id = str(code_record["id"])

    # Mark code as consumed and set email_verified = TRUE
    with transaction() as cur:
        cur.execute(
            f"""
            UPDATE {Tables.MAGIC_CODES}
            SET consumed = TRUE, consumed_at = NOW()
            WHERE id = %s
            """,
            (code_id,),
        )

        cur.execute(
            f"""
            UPDATE {Tables.IDENTITIES}
            SET email_verified = TRUE, last_seen_at = NOW()
            WHERE id = %s
            """,
            (identity_id,),
        )

    # Invalidate other pending codes
    MagicCodeService.invalidate_codes_for_email(email)

    print(f"[AUTH] Email verified for identity {identity_id}: {email}")

    return jsonify({
        "ok": True,
        "message": "Email verified successfully",
        "email_verified": True,
    })


# ─────────────────────────────────────────────────────────────
# Route Aliases (shorter paths for convenience)
# ─────────────────────────────────────────────────────────────

@bp.route("/request-code", methods=["POST"])
def request_code_alias():
    """Alias for /restore/request - Request a magic code via email."""
    return request_restore()


@bp.route("/verify-code", methods=["POST"])
@with_session
def verify_code_alias():
    """Alias for /restore/redeem - Verify a magic code and restore session."""
    return redeem_restore()
