"""
/api/admin routes - Admin-only endpoints.

Most endpoints require admin authentication via:
  1. X-Admin-Token header (for scripts/automation)
  2. Session with email in ADMIN_EMAILS list (for browser)

Endpoints:
- GET  /api/admin/overview           - System overview (alias for stats)
- GET  /api/admin/stats              - System statistics
- GET  /api/admin/identities         - List identities
- GET  /api/admin/identities/<id>    - Get identity detail
- GET  /api/admin/purchases          - List purchases
- POST /api/admin/credits/grant      - Grant/deduct credits
- POST /api/admin/wallet/adjust      - Adjust wallet (alias for credits/grant)
- POST /api/admin/wallet/grant       - Simple credit grant (X-Admin-Key auth)
- GET  /api/admin/reservations       - List credit reservations
- POST /api/admin/reservations/<id>/release - Release a reservation
- GET  /api/admin/jobs               - List jobs

Environment variables:
  ADMIN_TOKEN=your-secret-token      # For token-based auth
  ADMIN_EMAILS=admin@example.com     # Comma-separated list for email-based auth
  ADMIN_KEY=your-secret-key          # For simple /wallet/grant endpoint
"""

from flask import Blueprint, request, jsonify, g

from ..middleware import require_admin
from ..services.admin_service import AdminService
from ..services.identity_service import IdentityService
from ..services.wallet_service import WalletService, LedgerEntryType
from ..config import config
from ..db import DatabaseError

admin_bp = Blueprint("admin", __name__)


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


@admin_bp.route("/wallet/grant", methods=["POST"])
def simple_grant_credits():
    """
    Simple admin credit grant endpoint for testing without Stripe.

    Auth: X-Admin-Key header must match ADMIN_KEY env variable.

    Body:
        - identity_id: Target user UUID (optional - uses current session if not provided)
        - credits: Number of credits to grant (required, must be 1-5000)
        - reason: Reason for the grant (optional)

    Returns:
        - ok: true
        - identity_id: The identity that received credits
        - balance: New wallet balance
        - ledger_entry_id: ID of the created ledger entry
    """
    # Simple X-Admin-Key authentication
    admin_key = request.headers.get("X-Admin-Key")

    if not config.ADMIN_KEY:
        return jsonify({
            "ok": False,
            "error": "ADMIN_KEY not configured on server"
        }), 503

    if not admin_key or admin_key != config.ADMIN_KEY:
        return jsonify({
            "ok": False,
            "error": "Invalid or missing X-Admin-Key header"
        }), 403

    try:
        data = request.get_json() or {}

        identity_id = data.get("identity_id")
        credit_amount = data.get("credits")
        reason = data.get("reason", "admin_grant").strip() or "admin_grant"

        # If no identity_id provided, try to use current session
        if not identity_id:
            identity = IdentityService.get_current_identity(request)
            if identity:
                identity_id = str(identity["id"])
            else:
                return jsonify({
                    "ok": False,
                    "error": "identity_id required (no active session found)"
                }), 400

        # Validate credits
        if credit_amount is None:
            return jsonify({"ok": False, "error": "credits is required"}), 400

        if not isinstance(credit_amount, int):
            return jsonify({"ok": False, "error": "credits must be an integer"}), 400

        if credit_amount <= 0:
            return jsonify({"ok": False, "error": "credits must be greater than 0"}), 400

        if credit_amount > 5000:
            return jsonify({"ok": False, "error": "credits cannot exceed 5000"}), 400

        # Ensure wallet exists
        wallet = WalletService.get_or_create_wallet(identity_id)
        if not wallet:
            return jsonify({"ok": False, "error": "Failed to get or create wallet"}), 500

        # Add ledger entry with admin_grant type
        ledger_entry = WalletService.add_credits(
            identity_id=identity_id,
            amount=credit_amount,
            entry_type=LedgerEntryType.ADMIN_GRANT,
            meta={"reason": reason}
        )

        # Get updated balance
        new_balance = WalletService.get_balance(identity_id)

        print(f"[ADMIN] Simple grant: {credit_amount} credits to {identity_id} - {reason}")

        return jsonify({
            "ok": True,
            "identity_id": identity_id,
            "balance": new_balance,
            "ledger_entry_id": str(ledger_entry["id"]) if ledger_entry else None
        })

    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except DatabaseError as e:
        print(f"[ADMIN] Simple grant error: {e}")
        return jsonify({"ok": False, "error": "Database error"}), 500
