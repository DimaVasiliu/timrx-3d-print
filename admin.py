"""
/api/admin routes - Admin-only endpoints.

Standard admin endpoints require authentication via:
  1. X-Admin-Token header (for scripts/automation)
  2. Session with email in ADMIN_EMAILS list (for browser)

Backdoor admin endpoints (X-Admin-Key) - for emergency/testing:
- GET  /api/admin/ping               - Health check (requires X-Admin-Key)
- POST /api/admin/bootstrap          - Bootstrap admin identity with credits
- POST /api/admin/wallet/grant       - Grant credits to any identity

Standard admin endpoints:
- GET  /api/admin/overview           - System overview (alias for stats)
- GET  /api/admin/stats              - System statistics
- GET  /api/admin/identities         - List identities
- GET  /api/admin/identities/<id>    - Get identity detail
- GET  /api/admin/purchases          - List purchases
- POST /api/admin/credits/grant      - Grant/deduct credits
- POST /api/admin/wallet/adjust      - Adjust wallet (alias for credits/grant)
- GET  /api/admin/reservations       - List credit reservations
- POST /api/admin/reservations/<id>/release - Release a reservation
- GET  /api/admin/jobs               - List jobs

Environment variables:
  ADMIN_TOKEN=your-secret-token      # For token-based auth (X-Admin-Token)
  ADMIN_EMAILS=admin@example.com     # Comma-separated list for email-based auth
  ADMIN_KEY=your-secret-key          # For backdoor endpoints (X-Admin-Key)
  ADMIN_EMAIL=admin@example.com      # Email for bootstrap identity

Example curl commands:
  # Ping (health check)
  curl -H "X-Admin-Key: YOUR_KEY" https://api.example.com/api/admin/ping

  # Bootstrap admin identity with 100 credits
  curl -X POST -H "X-Admin-Key: YOUR_KEY" -H "Content-Type: application/json" \\
       -d '{"credits":100}' https://api.example.com/api/admin/bootstrap

  # Grant 100 credits to specific identity
  curl -X POST -H "X-Admin-Key: YOUR_KEY" -H "Content-Type: application/json" \\
       -d '{"identity_id":"UUID","credits":100,"reason":"test"}' \\
       https://api.example.com/api/admin/wallet/grant
"""

from datetime import datetime
from flask import Blueprint, request, jsonify, g

from ..middleware import require_admin, require_admin_key
from ..services.admin_service import AdminService
from ..services.identity_service import IdentityService
from ..services.wallet_service import WalletService, LedgerEntryType
from ..config import config
from ..db import DatabaseError, query_one, transaction, fetch_one, Tables

admin_bp = Blueprint("admin", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# BACKDOOR ENDPOINTS (X-Admin-Key auth) - for emergency/testing
# ─────────────────────────────────────────────────────────────────────────────

@admin_bp.route("/ping", methods=["GET"])
@require_admin_key
def admin_ping():
    """
    Simple health check that verifies X-Admin-Key is working.
    Returns current server time.
    """
    return jsonify({
        "ok": True,
        "now": datetime.utcnow().isoformat() + "Z"
    })


@admin_bp.route("/bootstrap", methods=["POST"])
@require_admin_key
def admin_bootstrap():
    """
    Bootstrap a dedicated admin identity and grant credits.
    No cookies/session required - creates identity from ADMIN_EMAIL env var.

    Body:
        - credits: Number of credits to grant (default: 100)

    Returns:
        - identity_id: The admin identity UUID
        - wallet: {balance, reserved, available}
        - granted_credits: How many credits were added
    """
    try:
        data = request.get_json() or {}
        credits_to_grant = data.get("credits", 100)

        if not isinstance(credits_to_grant, int) or credits_to_grant < 0:
            return jsonify({
                "error": {"code": "INVALID_CREDITS", "message": "credits must be a non-negative integer"}
            }), 400

        # Use ADMIN_EMAIL or default
        admin_email = (config.ADMIN_EMAIL or "admin@timrx.local").lower().strip()

        # Find or create admin identity
        identity = query_one(
            f"SELECT id, email FROM {Tables.IDENTITIES} WHERE LOWER(email) = %s",
            (admin_email,)
        )

        if identity:
            identity_id = str(identity["id"])
            print(f"[ADMIN] Bootstrap: found existing identity {identity_id} for {admin_email}")
        else:
            # Create new identity with email
            with transaction() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {Tables.IDENTITIES} (email, created_at)
                    VALUES (%s, NOW())
                    RETURNING id
                    """,
                    (admin_email,)
                )
                new_identity = fetch_one(cur)
                identity_id = str(new_identity["id"])
            print(f"[ADMIN] Bootstrap: created new identity {identity_id} for {admin_email}")

        # Ensure wallet exists
        wallet = WalletService.get_or_create_wallet(identity_id)
        if not wallet:
            return jsonify({
                "error": {"code": "WALLET_ERROR", "message": "Failed to create wallet"}
            }), 500

        # Grant credits if requested
        if credits_to_grant > 0:
            WalletService.add_credits(
                identity_id=identity_id,
                amount=credits_to_grant,
                entry_type=LedgerEntryType.ADMIN_GRANT,
                meta={"reason": "admin_bootstrap", "admin_email": admin_email}
            )
            print(f"[ADMIN] Bootstrap: granted {credits_to_grant} credits to {identity_id}")

        # Get updated wallet state
        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)

        return jsonify({
            "ok": True,
            "identity_id": identity_id,
            "email": admin_email,
            "wallet": {
                "balance": balance,
                "reserved": reserved,
                "available": balance - reserved
            },
            "granted_credits": credits_to_grant
        })

    except DatabaseError as e:
        print(f"[ADMIN] Bootstrap error: {e}")
        return jsonify({
            "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
        }), 500


@admin_bp.route("/wallet/grant", methods=["POST"])
@require_admin_key
def admin_wallet_grant():
    """
    Grant credits to any identity (by ID or current session).
    Uses X-Admin-Key auth, works without cookies if identity_id provided.

    Body:
        - credits: Number of credits to grant (required, must be positive)
        - identity_id: Target identity UUID (optional - uses session if not provided)
        - reason: Reason for grant (optional, default: "admin_grant")

    Returns:
        - identity_id: The identity that received credits
        - wallet: {balance, reserved, available}
    """
    try:
        data = request.get_json() or {}
        credits_to_grant = data.get("credits")
        identity_id = data.get("identity_id")
        reason = data.get("reason", "admin_grant").strip() or "admin_grant"

        # Validate credits
        if credits_to_grant is None:
            return jsonify({
                "error": {"code": "MISSING_CREDITS", "message": "credits is required"}
            }), 400

        if not isinstance(credits_to_grant, int) or credits_to_grant <= 0:
            return jsonify({
                "error": {"code": "INVALID_CREDITS", "message": "credits must be a positive integer"}
            }), 400

        # Get identity_id from param or session
        if not identity_id:
            # Try to get from current session cookie
            identity = IdentityService.get_current_identity(request)
            if identity:
                identity_id = str(identity["id"])
            else:
                return jsonify({
                    "error": {
                        "code": "MISSING_IDENTITY",
                        "message": "identity_id required (no valid session found)"
                    }
                }), 400

        # Verify identity exists
        existing = query_one(
            f"SELECT id FROM {Tables.IDENTITIES} WHERE id = %s",
            (identity_id,)
        )
        if not existing:
            return jsonify({
                "error": {"code": "IDENTITY_NOT_FOUND", "message": "Identity not found"}
            }), 404

        # Ensure wallet exists
        wallet = WalletService.get_or_create_wallet(identity_id)
        if not wallet:
            return jsonify({
                "error": {"code": "WALLET_ERROR", "message": "Failed to get or create wallet"}
            }), 500

        # Grant credits
        WalletService.add_credits(
            identity_id=identity_id,
            amount=credits_to_grant,
            entry_type=LedgerEntryType.ADMIN_GRANT,
            meta={"reason": reason}
        )
        print(f"[ADMIN] Granted {credits_to_grant} credits to {identity_id} - {reason}")

        # Get updated wallet state
        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)

        return jsonify({
            "ok": True,
            "identity_id": identity_id,
            "wallet": {
                "balance": balance,
                "reserved": reserved,
                "available": balance - reserved
            }
        })

    except DatabaseError as e:
        print(f"[ADMIN] Wallet grant error: {e}")
        return jsonify({
            "error": {"code": "DATABASE_ERROR", "message": "Database error occurred"}
        }), 500


# ─────────────────────────────────────────────────────────────────────────────
# STANDARD ADMIN ENDPOINTS (X-Admin-Token or email-based auth)
# ─────────────────────────────────────────────────────────────────────────────

@admin_bp.route("/overview", methods=["GET"])
@admin_bp.route("/stats", methods=["GET"])
@require_admin
def get_stats():
    """
    Get system statistics / overview.

    Returns:
        - total_identities: Total user count
        - identities_with_email: Users with email attached
        - total_purchases: Completed purchase count
        - total_credits_purchased: Sum of credits bought
        - total_credits_spent: Sum of credits used
        - active_reservations: Currently held reservations
        - jobs_by_status: Job counts grouped by status
        - total_jobs: Total job count
        - job_success_rate: Percentage of completed vs failed jobs
        - total_revenue_gbp: Total revenue in GBP
    """
    try:
        stats = AdminService.get_stats()
        return jsonify({"ok": True, **stats})
    except DatabaseError as e:
        print(f"[ADMIN] Stats error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500


@admin_bp.route("/identities", methods=["GET"])
@require_admin
def list_identities():
    """
    List identities with pagination.

    Query params:
        - limit: Max results (default 50, max 100)
        - offset: Pagination offset (default 0)
        - email: Filter by email contains
        - has_email: 'true' or 'false' to filter by email presence
    """
    try:
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)
        email_filter = request.args.get("email")
        has_email_str = request.args.get("has_email")

        has_email = None
        if has_email_str == "true":
            has_email = True
        elif has_email_str == "false":
            has_email = False

        result = AdminService.list_identities(
            limit=limit,
            offset=offset,
            email_filter=email_filter,
            has_email=has_email
        )
        return jsonify({"ok": True, **result})
    except DatabaseError as e:
        print(f"[ADMIN] List identities error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500


@admin_bp.route("/identities/<identity_id>", methods=["GET"])
@require_admin
def get_identity_detail(identity_id):
    """
    Get detailed information about a specific identity.

    Returns identity with wallet, recent purchases, recent jobs.
    """
    try:
        detail = AdminService.get_identity_detail(identity_id)
        if not detail:
            return jsonify({"ok": False, "error": "Identity not found"}), 404
        return jsonify({"ok": True, "identity": detail})
    except DatabaseError as e:
        print(f"[ADMIN] Identity detail error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500


@admin_bp.route("/purchases", methods=["GET"])
@require_admin
def list_purchases():
    """
    List purchases with filtering.

    Query params:
        - status: Filter by status (pending, completed, failed, refunded)
        - identity_id: Filter by identity
        - limit: Max results (default 50)
        - offset: Pagination offset
    """
    try:
        status = request.args.get("status")
        identity_id = request.args.get("identity_id")
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)

        result = AdminService.list_purchases(
            status=status,
            identity_id=identity_id,
            limit=limit,
            offset=offset
        )
        return jsonify({"ok": True, **result})
    except DatabaseError as e:
        print(f"[ADMIN] List purchases error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500


@admin_bp.route("/wallet/adjust", methods=["POST"])
@admin_bp.route("/credits/grant", methods=["POST"])
@require_admin
def grant_credits():
    """
    Grant or deduct credits from an identity.

    Body:
        - identity_id: Target user UUID (required)
        - amount or delta: Credits to add (positive) or remove (negative) (required)
        - reason: Reason for adjustment (required)

    Returns updated wallet state.
    """
    try:
        data = request.get_json() or {}

        identity_id = data.get("identity_id")
        # Accept both 'amount' and 'delta' for flexibility
        amount = data.get("amount") or data.get("delta")
        reason = data.get("reason", "").strip()

        # Validation
        if not identity_id:
            return jsonify({"ok": False, "error": "identity_id is required"}), 400

        if amount is None or not isinstance(amount, int):
            return jsonify({"ok": False, "error": "amount must be an integer"}), 400

        if amount == 0:
            return jsonify({"ok": False, "error": "amount cannot be zero"}), 400

        if not reason:
            return jsonify({"ok": False, "error": "reason is required"}), 400

        # Get admin email from session (if email-based auth)
        admin_email = getattr(g, "admin_email", None)

        result = AdminService.grant_credits(
            identity_id=identity_id,
            amount=amount,
            reason=reason,
            admin_email=admin_email
        )

        action = "granted" if amount > 0 else "deducted"
        print(f"[ADMIN] Credits {action}: {abs(amount)} to {identity_id} by {admin_email or 'token'} - {reason}")

        return jsonify({"ok": True, **result})

    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except DatabaseError as e:
        print(f"[ADMIN] Grant credits error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500


@admin_bp.route("/reservations", methods=["GET"])
@require_admin
def list_reservations():
    """
    List credit reservations.

    Query params:
        - status: 'held', 'released', or 'all' (default 'held')
        - limit: Max results (default 50)
        - offset: Pagination offset
    """
    try:
        status = request.args.get("status", "held")
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)

        result = AdminService.list_reservations(
            status=status,
            limit=limit,
            offset=offset
        )
        return jsonify({"ok": True, **result})
    except DatabaseError as e:
        print(f"[ADMIN] List reservations error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500


@admin_bp.route("/reservations/<reservation_id>/release", methods=["POST"])
@require_admin
def release_reservation(reservation_id):
    """
    Manually release a held credit reservation.
    Returns the credits to the user's wallet.

    Use this for stuck/orphaned reservations.
    """
    try:
        data = request.get_json() or {}
        reason = data.get("reason", "admin_release")

        success = AdminService.release_reservation(reservation_id, reason)

        if not success:
            return jsonify({
                "ok": False,
                "error": "Reservation not found or already released"
            }), 404

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Reservation released: {reservation_id} by {admin_email or 'token'}")

        return jsonify({"ok": True, "reservation_id": reservation_id})
    except DatabaseError as e:
        print(f"[ADMIN] Release reservation error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500


@admin_bp.route("/jobs", methods=["GET"])
@require_admin
def list_jobs():
    """
    List jobs with filtering.

    Query params:
        - status: Filter by status (queued, pending, completed, failed)
        - identity_id: Filter by identity
        - limit: Max results (default 50)
        - offset: Pagination offset
    """
    try:
        status = request.args.get("status")
        identity_id = request.args.get("identity_id")
        limit = request.args.get("limit", 50, type=int)
        offset = request.args.get("offset", 0, type=int)

        result = AdminService.list_jobs(
            status=status,
            identity_id=identity_id,
            limit=limit,
            offset=offset
        )
        return jsonify({"ok": True, **result})
    except DatabaseError as e:
        print(f"[ADMIN] List jobs error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500


@admin_bp.route("/health", methods=["GET"])
@require_admin
def admin_health():
    """
    Admin health check - verifies admin auth is working.
    """
    return jsonify({
        "ok": True,
        "auth_method": getattr(g, "admin_auth_method", None),
        "admin_email": getattr(g, "admin_email", None),
    })
