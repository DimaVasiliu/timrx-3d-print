"""
/api/me routes - Current user/session endpoints.

Handles:
- GET /api/me - Get current session info (identity, wallet balance)
- POST /api/me/email - Attach email to identity
- GET /api/me/wallet - Get wallet details
- GET /api/me/ledger - Get billing ledger entries (credits/purchases)
- POST /api/me/logout - End current session
"""

import time as _time
from flask import Blueprint, request, jsonify, g, make_response

from backend.middleware import with_session, require_session, no_cache
from backend.services.identity_service import IdentityService
from backend.services.wallet_service import WalletService
from backend.db import is_transient_db_error

bp = Blueprint("me", __name__)

# Short TTL cache for /api/me response — avoids repeated DB hits on page-load burst
_me_cache = {}  # identity_id -> (payload_dict, monotonic_ts)
_ME_CACHE_TTL = 10  # seconds


@bp.route("", methods=["GET"])
@with_session
@no_cache
def get_me():
    """
    Get current session info.
    Creates anonymous identity if none exists.

    Returns identity fields only — NO wallet queries.
    Wallet data comes from /api/credits/wallet (separate, faster endpoint).
    This split means /api/me needs 0 extra DB queries beyond session validation,
    making it fast (~50ms) even under pool pressure.

    For backward compatibility, balance fields are still present in the
    response but set to 0.  Frontend MUST use /api/credits/wallet for
    authoritative wallet state.
    """
    identity_id = g.identity_id

    # Fast path: return cached response if fresh
    cached = _me_cache.get(identity_id)
    if cached:
        payload, ts = cached
        if _time.monotonic() - ts < _ME_CACHE_TTL:
            return jsonify(payload)

    try:
        identity = g.identity

        last_active_at = None
        if identity:
            raw = identity.get("last_seen_at") or identity.get("created_at")
            if raw:
                last_active_at = raw.isoformat() if hasattr(raw, 'isoformat') else str(raw)

        payload = {
            "ok": True,
            "identity_id": identity_id,
            "email": identity.get("email") if identity else None,
            "email_verified": identity.get("email_verified", False) if identity else False,
            # Wallet fields kept for backward compatibility but always 0.
            # Frontend should use /api/credits/wallet for real balances.
            "balance_credits": 0,
            "reserved_credits": 0,
            "available_credits": 0,
            "balance_video_credits": 0,
            "reserved_video_credits": 0,
            "available_video_credits": 0,
            "created_at": identity.get("created_at").isoformat() if identity and identity.get("created_at") else None,
            "last_active_at": last_active_at,
        }

        # Cache successful response
        if identity_id:
            _me_cache[identity_id] = (payload, _time.monotonic())

        return jsonify(payload)

    except Exception as e:
        # On transient DB error, return stale cache if available
        if is_transient_db_error(e) and cached:
            print(f"[ME][STALE_OK] returning cached /api/me: {type(e).__name__}")
            return jsonify(cached[0])
        raise


@bp.route("/email", methods=["POST"])
@no_cache
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
@no_cache
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
