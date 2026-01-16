"""
/api/auth routes - Authentication and session management.

Handles:
- POST /api/auth/restore/request - Request magic code via email
- POST /api/auth/restore/redeem - Verify magic code and restore session
- GET /api/auth/status - Get current auth status

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
