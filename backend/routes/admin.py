"""
/api/admin routes - Admin-only endpoints.

Standard admin endpoints require authentication via:
  1. X-Admin-Token header (for scripts/automation)
  2. Session with email in ADMIN_EMAILS list (for browser)

Endpoints:
- GET  /api/admin/overview           - System overview (alias for stats)
- GET  /api/admin/stats              - System statistics
- GET  /api/admin/identities         - List identities
- GET  /api/admin/identities/<id>    - Get identity detail
- GET  /api/admin/purchases          - List purchases
- POST /api/admin/credits/grant      - Grant/deduct credits (requires reason, audit trail)
- POST /api/admin/wallet/adjust      - Adjust wallet (alias for credits/grant)
- GET  /api/admin/reservations       - List credit reservations
- POST /api/admin/reservations/<id>/release - Release a reservation
- GET  /api/admin/jobs               - List jobs
- GET  /api/admin/health             - Admin health check
- GET  /api/admin/debug/user         - Internal debug: user summary (masked email, wallet, history)

Environment variables:
  ADMIN_TOKEN=your-secret-token      # For token-based auth (X-Admin-Token)
  ADMIN_EMAILS=admin@example.com     # Comma-separated list for email-based auth
"""

from flask import Blueprint, request, jsonify, g

from backend.middleware import require_admin
from backend.services.admin_service import AdminService
from backend.db import DatabaseError

bp = Blueprint("admin", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN ENDPOINTS (X-Admin-Token or email-based auth)
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/overview", methods=["GET"])
@bp.route("/stats", methods=["GET"])
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


@bp.route("/identities", methods=["GET"])
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


@bp.route("/identities/<identity_id>", methods=["GET"])
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


@bp.route("/purchases", methods=["GET"])
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


@bp.route("/wallet/adjust", methods=["POST"])
@bp.route("/credits/grant", methods=["POST"])
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


@bp.route("/reservations", methods=["GET"])
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


@bp.route("/reservations/<reservation_id>/release", methods=["POST"])
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


@bp.route("/jobs", methods=["GET"])
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


@bp.route("/health", methods=["GET"])
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


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL DIAGNOSTICS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/email/health", methods=["GET"])
@require_admin
def email_health():
    """
    Check email service health.

    Performs:
    - DNS resolution of SMTP_HOST
    - TCP connection test to SMTP_HOST:SMTP_PORT

    Returns detailed status for debugging.
    """
    try:
        from backend.services.email_service import EmailService
        result = EmailService.healthcheck()
        return jsonify({"ok": result.get("status") == "healthy", **result})
    except ImportError:
        return jsonify({
            "ok": False,
            "error": "email_service not available",
            "message": "EmailService module not found"
        }), 500
    except Exception as e:
        print(f"[ADMIN] Email health check error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/email/test", methods=["POST"])
@require_admin
def email_test():
    """
    Send a test email to verify SMTP configuration.

    Body:
        - to: Recipient email address (required)

    Returns send result with success/failure details.
    """
    try:
        data = request.get_json() or {}
        to_email = data.get("to", "").strip()

        if not to_email:
            return jsonify({
                "ok": False,
                "error": "VALIDATION_ERROR",
                "message": "'to' email address is required"
            }), 400

        if "@" not in to_email or "." not in to_email:
            return jsonify({
                "ok": False,
                "error": "VALIDATION_ERROR",
                "message": "Invalid email format"
            }), 400

        from backend.services.email_service import EmailService

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Sending test email to {to_email} (requested by {admin_email or 'token'})")

        result = EmailService.send_test(to_email)

        return jsonify({
            "ok": result.success,
            "message": result.message,
            "error": result.error,
            "to": to_email,
        })

    except ImportError:
        return jsonify({
            "ok": False,
            "error": "email_service not available",
            "message": "EmailService module not found"
        }), 500
    except Exception as e:
        print(f"[ADMIN] Email test error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/subscriptions/run_grants", methods=["POST"])
@require_admin
def run_subscription_grants():
    """
    Admin endpoint: run pending subscription credit grants.

    Finds active subscriptions that haven't had credits granted for the
    current period and grants them.  Safe to call repeatedly (idempotent).

    Response: { ok: true, granted: 3 }
    """
    from backend.services.subscription_service import SubscriptionService
    granted = SubscriptionService.run_pending_grants()
    return jsonify({"ok": True, "granted": granted})


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL OUTBOX / CRON ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/email/outbox/stats", methods=["GET"])
@require_admin
def email_outbox_stats():
    """
    Get email outbox statistics.

    Returns:
        - pending: Emails waiting to be sent
        - sent: Successfully sent emails
        - failed: Permanently failed emails (after max retries)
        - total: Total emails in outbox
    """
    try:
        from backend.services.email_outbox_service import EmailOutboxService
        stats = EmailOutboxService.get_outbox_stats()
        return jsonify({"ok": True, **stats})
    except Exception as e:
        print(f"[ADMIN] Email outbox stats error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/email/outbox/send-pending", methods=["POST"])
@bp.route("/email/send-pending", methods=["POST"])
@require_admin
def send_pending_emails():
    """
    Process pending emails from the outbox (cron-callable endpoint).

    Call this endpoint periodically (e.g., every minute) from:
    - A Render cron job
    - An external scheduler (e.g., cron.org, EasyCron)
    - A background worker

    Query params:
        - limit: Max emails to process (default 50, max 200)

    Returns:
        - sent: Number of emails successfully sent
        - failed: Number of emails that failed this attempt
        - remaining: Number of emails still pending

    Safe to call frequently - processes oldest pending emails first.
    Failed emails are automatically retried until max_attempts reached.
    """
    try:
        from backend.services.email_outbox_service import EmailOutboxService

        limit = request.args.get("limit", 50, type=int)
        limit = min(limit, 200)  # Cap at 200 to prevent timeout

        result = EmailOutboxService.send_pending_emails(limit=limit)

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Email outbox processed by {admin_email or 'token'}: {result}")

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Email send-pending error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/email/outbox/retry-failed", methods=["POST"])
@require_admin
def retry_failed_emails():
    """
    Reset failed emails to pending for retry.

    Use this to manually retry emails that permanently failed.
    Resets status to 'pending' and clears attempt counter.

    Query params:
        - limit: Max emails to reset (default 10, max 50)

    Returns:
        - reset: Number of emails reset to pending
    """
    try:
        from backend.db import execute

        limit = request.args.get("limit", 10, type=int)
        limit = min(limit, 50)

        count = execute(
            """
            UPDATE timrx_billing.email_outbox
            SET status = 'pending', attempts = 0, last_error = NULL,
                failed_at = NULL
            WHERE status = 'failed'
            AND id IN (
                SELECT id FROM timrx_billing.email_outbox
                WHERE status = 'failed'
                ORDER BY created_at ASC
                LIMIT %s
            )
            """,
            (limit,),
        )

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Reset {count} failed emails to pending by {admin_email or 'token'}")

        return jsonify({"ok": True, "reset": count})
    except Exception as e:
        print(f"[ADMIN] Email retry-failed error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# RECONCILIATION / SAFETY JOB ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/reconcile", methods=["POST"])
@bp.route("/reconcile/run", methods=["POST"])
@require_admin
def run_reconciliation():
    """
    Run the safety reconciliation job.

    This job detects and fixes data inconsistencies:
    - Purchases missing ledger entries
    - Wallet balance mismatches
    - Stale held reservations (job terminal or missing)
    - Completed jobs missing history_items

    Query params:
        - dry_run: If 'true', detect issues but don't fix them (default: false)
        - send_alert: If 'true', send admin email on fixes (default: true)

    Returns:
        Summary of all checks and fixes applied

    Safe to call frequently (every 15 minutes recommended via cron).
    All fixes are idempotent.
    """
    try:
        from backend.services.reconciliation_service import ReconciliationService

        dry_run = request.args.get("dry_run", "false").lower() == "true"
        send_alert = request.args.get("send_alert", "true").lower() != "false"

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Reconciliation triggered by {admin_email or 'token'} (dry_run={dry_run})")

        result = ReconciliationService.reconcile_safety(
            dry_run=dry_run,
            send_alert=send_alert,
        )

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Reconciliation error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/reconcile/stats", methods=["GET"])
@require_admin
def reconciliation_stats():
    """
    Get current reconciliation stats without applying fixes.

    Returns counts of issues that would be fixed:
    - purchases_missing_ledger: Paid purchases without ledger entry
    - wallet_mismatches: Wallets where balance != ledger sum
    - stale_reservations: Held reservations with terminal/missing jobs

    Useful for monitoring dashboards.
    """
    try:
        from backend.services.reconciliation_service import ReconciliationService

        stats = ReconciliationService.get_stats()
        return jsonify({"ok": True, **stats})
    except Exception as e:
        print(f"[ADMIN] Reconciliation stats error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/reconcile/dry-run", methods=["POST"])
@require_admin
def reconciliation_dry_run():
    """
    Run reconciliation in dry-run mode (detect but don't fix).

    Alias for POST /reconcile?dry_run=true
    """
    try:
        from backend.services.reconciliation_service import ReconciliationService

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Reconciliation dry-run by {admin_email or 'token'}")

        result = ReconciliationService.reconcile_safety(
            dry_run=True,
            send_alert=False,
        )

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Reconciliation dry-run error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL DEBUG ENDPOINT
# ─────────────────────────────────────────────────────────────────────────────

def _mask_email(email: str) -> str:
    """Mask email for debug output: jo***@ex***.com"""
    if not email or "@" not in email:
        return email
    local, domain = email.rsplit("@", 1)
    domain_parts = domain.split(".")
    masked_local = local[:2] + "***" if len(local) > 2 else local[0] + "***"
    masked_domain = domain_parts[0][:2] + "***" if len(domain_parts[0]) > 2 else domain_parts[0]
    return f"{masked_local}@{masked_domain}.{'.'.join(domain_parts[1:])}"


@bp.route("/debug/user", methods=["GET"])
@require_admin
def debug_user():
    """
    Internal debug endpoint for user troubleshooting.

    Query params:
        - identity_id: The identity UUID to lookup
        - email: Email address to lookup (alternative to identity_id)

    Returns (non-sensitive data only):
        - identity_id
        - email (masked)
        - wallet.balance_credits
        - reserved_credits
        - last_purchase summary
        - last 10 history items summary
    """
    from backend.db import query_one, query_all, Tables

    identity_id = request.args.get("identity_id")
    email = request.args.get("email")

    if not identity_id and not email:
        return jsonify({"ok": False, "error": "Provide identity_id or email"}), 400

    try:
        # ── Lookup identity ────────────────────────────────────────
        if email and not identity_id:
            identity = query_one(
                f"SELECT id, email, email_verified, created_at FROM {Tables.IDENTITIES} WHERE email = %s",
                (email,),
            )
            if not identity:
                return jsonify({"ok": False, "error": "Identity not found"}), 404
            identity_id = str(identity["id"])
        else:
            identity = query_one(
                f"SELECT id, email, email_verified, created_at FROM {Tables.IDENTITIES} WHERE id = %s",
                (identity_id,),
            )
            if not identity:
                return jsonify({"ok": False, "error": "Identity not found"}), 404

        # ── Get wallet ─────────────────────────────────────────────
        wallet = query_one(
            f"SELECT balance_credits, updated_at FROM {Tables.WALLETS} WHERE identity_id = %s",
            (identity_id,),
        )

        # ── Get reserved credits ───────────────────────────────────
        reserved_row = query_one(
            f"""
            SELECT COALESCE(SUM(cost_credits), 0) as total
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE identity_id = %s AND status = 'held' AND expires_at > NOW()
            """,
            (identity_id,),
        )
        reserved_credits = int(reserved_row["total"]) if reserved_row else 0

        # ── Get last purchase ──────────────────────────────────────
        last_purchase = query_one(
            f"""
            SELECT id, amount_gbp, credits_granted, status, created_at
            FROM {Tables.PURCHASES}
            WHERE identity_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (identity_id,),
        )
        last_purchase_summary = None
        if last_purchase:
            last_purchase_summary = {
                "id": str(last_purchase["id"]),
                "amount_gbp": float(last_purchase["amount_gbp"]) if last_purchase["amount_gbp"] else None,
                "credits": last_purchase["credits_granted"],
                "status": last_purchase["status"],
                "created_at": last_purchase["created_at"].isoformat() if last_purchase["created_at"] else None,
            }

        # ── Get last 10 history items ──────────────────────────────
        history_items = query_all(
            f"""
            SELECT id, item_type, status, created_at
            FROM {Tables.HISTORY_ITEMS}
            WHERE identity_id = %s
            ORDER BY created_at DESC
            LIMIT 10
            """,
            (identity_id,),
        )
        history_summary = [
            {
                "id": str(h["id"]),
                "kind": h["item_type"],
                "status": h["status"],
                "created_at": h["created_at"].isoformat() if h["created_at"] else None,
            }
            for h in history_items
        ]

        # ── Build response ─────────────────────────────────────────
        result = {
            "ok": True,
            "identity_id": identity_id,
            "email": _mask_email(identity.get("email")) if identity.get("email") else None,
            "email_verified": identity.get("email_verified", False),
            "identity_created_at": identity["created_at"].isoformat() if identity.get("created_at") else None,
            "wallet": {
                "balance_credits": wallet["balance_credits"] if wallet else 0,
                "reserved_credits": reserved_credits,
                "available_credits": max(0, (wallet["balance_credits"] if wallet else 0) - reserved_credits),
                "updated_at": wallet["updated_at"].isoformat() if wallet and wallet.get("updated_at") else None,
            },
            "last_purchase": last_purchase_summary,
            "history_items": history_summary,
            "history_count": len(history_summary),
        }

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Debug user lookup: {identity_id[:8]}... by {admin_email or 'token'}")

        return jsonify(result)

    except Exception as e:
        print(f"[ADMIN] Debug user error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500
