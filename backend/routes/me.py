"""
/api/me routes - Current user/session endpoints.

Handles:
- GET /api/me - Get current session info (identity, wallet balance)
- POST /api/me/email - Attach email to identity
- GET /api/me/wallet - Get wallet details
- GET /api/me/ledger - Get billing ledger entries (credits/purchases)
- POST /api/me/logout - End current session
"""

from flask import Blueprint, request, jsonify, g, make_response

from backend.middleware import with_session, require_session, no_cache
from backend.services.identity_service import IdentityService
from backend.services.wallet_service import WalletService

bp = Blueprint("me", __name__)


@bp.route("", methods=["GET"])
@with_session
@no_cache
def get_me():
    """
    Get current session info.
    Creates anonymous identity if none exists.
    Returns identity_id, email (if set), wallet balances (general + video), etc.
    """
    identity = g.identity

    # Fetch wallet balances from wallets table (both general and video credits)
    balance = 0
    video_balance = 0
    reserved = 0
    video_reserved = 0

    if g.identity_id:
        # Get all balances at once
        balances = WalletService.get_all_balances(g.identity_id)
        balance = balances["general"]
        video_balance = balances["video"]

        # Get all reserved credits at once
        reserved_credits = WalletService.get_all_reserved_credits(g.identity_id)
        reserved = reserved_credits["general"]
        video_reserved = reserved_credits["video"]

    return jsonify({
        "ok": True,
        "identity_id": g.identity_id,
        "email": identity.get("email") if identity else None,
        "email_verified": identity.get("email_verified", False) if identity else False,
        # General credits (3D + images)
        "balance_credits": balance,
        "reserved_credits": reserved,
        "available_credits": max(0, balance - reserved),
        # Video credits (separate balance)
        "balance_video_credits": video_balance,
        "reserved_video_credits": video_reserved,
        "available_video_credits": max(0, video_balance - video_reserved),
        "created_at": identity.get("created_at").isoformat() if identity and identity.get("created_at") else None,
    })


@bp.route("/email", methods=["POST"])
@require_session
def attach_email():
    """
    Attach email to current identity.
    Idempotent: if same email already set, returns OK.
    Does NOT verify email (verification via magic codes separately).
    """
    data = request.get_json() or {}
    email = data.get("email", "").strip()

    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Email is required",
            }
        }), 400

    try:
        # attach_email is anti-enumeration safe - it returns ok even if email
        # belongs to another identity (reason is logged internally only)
        updated, was_changed, _ = IdentityService.attach_email(g.identity_id, email)

        # ANTI-ENUMERATION: Always return ok=true, never reveal if email belongs to another
        # The reason is for internal logging only
        return jsonify({
            "ok": True,
            "identity_id": g.identity_id,
            "email": updated.get("email"),  # Returns current email (may be unchanged)
            "was_changed": was_changed,
        })
    except ValueError as e:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": str(e),
            }
        }), 400


@bp.route("/wallet", methods=["GET"])
@require_session
@no_cache
def get_wallet():
    """
    Get wallet information for Buy modal and action gating.
    Returns balance info for both general and video credits.
    """
    try:
        wallet = WalletService.get_wallet(g.identity_id)
        if wallet:
            balance = wallet.get("balance_credits", 0) or 0
            video_balance = wallet.get("balance_video_credits", 0) or 0
            updated_at = wallet.get("updated_at")
        else:
            balance = 0
            video_balance = 0
            updated_at = None

        # Get reserved credits for both types
        reserved_credits = WalletService.get_all_reserved_credits(g.identity_id)
        reserved = reserved_credits["general"]
        video_reserved = reserved_credits["video"]

        available = max(0, balance - reserved)
        video_available = max(0, video_balance - video_reserved)

        return jsonify({
            "ok": True,
            # General credits (3D + images)
            "balance": balance,
            "reserved": reserved,
            "available": available,
            # Video credits
            "video_balance": video_balance,
            "video_reserved": video_reserved,
            "video_available": video_available,
            "currency": "GBP",
            "updated_at": updated_at.isoformat() if updated_at else None,
        })
    except Exception:
        # Fallback if wallet service not fully implemented
        return jsonify({
            "ok": True,
            "balance": 0,
            "reserved": 0,
            "available": 0,
            "video_balance": 0,
            "video_reserved": 0,
            "video_available": 0,
            "currency": "GBP",
            "updated_at": None,
        })


@bp.route("/ledger", methods=["GET"])
@require_session
@no_cache
def get_ledger():
    """
    Get billing ledger entries (credit grants, purchases, usage).
    Note: This is billing history, NOT asset history (history_items).
    """
    try:
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)

        entries = WalletService.get_ledger_entries(g.identity_id, limit=limit, offset=offset)

        return jsonify({
            "ok": True,
            "entries": entries,
            "limit": limit,
            "offset": offset,
        })
    except Exception:
        # Fallback if wallet service not fully implemented
        return jsonify({
            "ok": True,
            "entries": [],
            "limit": 50,
            "offset": 0,
        })


@bp.route("/logout", methods=["POST"])
def logout():
    """
    End the current session.
    Revokes the session token and clears the cookie.
    """
    session_id = IdentityService.get_session_id_from_request(request)

    if session_id:
        IdentityService.revoke_session(session_id)

    response = make_response(jsonify({
        "ok": True,
        "message": "Logged out successfully",
    }))

    IdentityService.clear_session_cookie(response)

    return response
