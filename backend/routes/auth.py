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

from backend.middleware import with_session, require_admin
from backend.magic_code_service import MagicCodeService
from backend.identity_service import IdentityService
from backend.wallet_service import WalletService

bp = Blueprint("auth", __name__)


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
    Verify and attach email to current identity using magic code.

    Flow:
    1. User calls POST /api/auth/email/attach (sends code to email)
    2. User enters code and calls POST /api/auth/email/verify
    3. Code is verified, email is attached to identity idempotently

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
        "email_attached": true
    }

    Error responses:
    - 400 VALIDATION_ERROR: Missing/invalid email or code
    - 400 INVALID_CODE: Code doesn't match or expired
    - 400 CODE_EXPIRED: Code has expired
    - 400 TOO_MANY_ATTEMPTS: Max attempts exceeded
    - 401 NO_SESSION: No active session
    - 409 EMAIL_IN_USE: Email belongs to another verified identity

    Manual test:
    ```bash
    # 1. Attach email (sends code)
    curl -X POST http://localhost:5001/api/auth/email/attach \
      -H "Content-Type: application/json" \
      -b "timrx_sid=YOUR_SESSION_ID" \
      -d '{"email": "test@example.com"}'

    # 2. Verify with code from email
    curl -X POST http://localhost:5001/api/auth/email/verify \
      -H "Content-Type: application/json" \
      -b "timrx_sid=YOUR_SESSION_ID" \
      -d '{"email": "test@example.com", "code": "123456"}'
    ```
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
    from db import query_one, transaction, Tables
    import config as cfg

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
    max_attempts = cfg.config.MAGIC_CODE_MAX_ATTEMPTS if hasattr(cfg, 'config') else 5
    if code_record["attempts"] >= max_attempts:
        print(f"[AUTH] verify_email failed: too many attempts for email={_mask_email(email)}")
        return _get_redeem_error_response("Too many failed attempts. Please request a new code")

    code_id = str(code_record["id"])

    # ── Attach email to identity idempotently ──
    # Get current identity email (safely handle None)
    current_email = None
    if identity and identity.get("email"):
        current_email = identity["email"].lower() if isinstance(identity["email"], str) else None

    email_attached = False

    if current_email == email:
        # Email already on this identity - just verify it
        pass
    elif current_email is None:
        # Identity has no email - check if email belongs to another identity
        existing = IdentityService.get_identity_by_email(email)
        if existing and str(existing["id"]) != identity_id:
            # Email belongs to another identity - cannot attach
            # User must use restore flow to take over that account
            print(f"[AUTH] verify_email failed: email={_mask_email(email)} belongs to another identity")
            return jsonify({
                "error": {
                    "code": "EMAIL_IN_USE",
                    "message": "This email is already associated with another account. Use 'Restore Account' to access it.",
                }
            }), 409
        email_attached = True
    else:
        # Identity has a DIFFERENT email - this is unusual, reject
        print(f"[AUTH] verify_email failed: identity {identity_id} has different email")
        return jsonify({
            "error": {
                "code": "EMAIL_MISMATCH",
                "message": "This account already has a different email attached.",
            }
        }), 400

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

    print(f"[AUTH] Email verified for identity {identity_id}: {_mask_email(email)} (attached={email_attached})")

    return jsonify({
        "ok": True,
        "message": "Email verified successfully",
        "email_verified": True,
        "email_attached": email_attached,
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
    import config as cfg
    from db import query_one, USE_DB

    checks = {}

    # ── Email configuration ──
    try:
        email_check = {
            "configured": cfg.config.EMAIL_CONFIGURED,
            "enabled": cfg.config.EMAIL_ENABLED,
            "provider": cfg.config.EMAIL_PROVIDER,
        }
        if cfg.config.EMAIL_PROVIDER == "ses":
            email_check["region"] = cfg.config.AWS_REGION
            email_check["from_email"] = cfg.config.SES_FROM_EMAIL or cfg.config.EMAIL_FROM_ADDRESS
        else:
            email_check["smtp_host"] = cfg.config.SMTP_HOST or "(not set)"
            name, addr = cfg.config.SMTP_FROM_PARSED
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
