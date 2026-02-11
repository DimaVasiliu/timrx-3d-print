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
from backend.db import DatabaseError, query_all, query_one

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


@bp.route("/reconcile/detect", methods=["GET"])
@require_admin
def reconciliation_detect():
    """
    Detect data anomalies (detection only, no fixes).

    This endpoint identifies inconsistencies in the data without applying
    any fixes. Use this for auditing and monitoring.

    Query params:
        - stale_minutes: Threshold for stale held reservations (default: 30)
        - check_s3: If 'true', also check for orphan S3 objects (slow, default: false)
        - limit: Max results per category (default: 100)

    Returns:
        - jobs_missing_history: Jobs with success status but no history item
        - finalized_reservations_missing_ledger: Finalized reservations without ledger entry
        - stale_held_reservations: Held reservations older than stale_minutes
        - orphan_s3_objects: S3 objects with no DB reference (if check_s3=true)
        - summary: Counts and totals

    Detections:
    1. Jobs with status=ready/succeeded/done but no history_items row
       - May indicate: save_finished_job_to_normalized_db() failed silently
       - Impact: User doesn't see their generation in history

    2. Finalized reservations without ledger entries
       - May indicate: finalize_reservation() updated status but ledger insert failed
       - Impact: Credits deducted from available but wallet balance unchanged

    3. Held reservations older than X minutes
       - May indicate: Job never completed, or finalize/release never called
       - Impact: Credits stuck in "held" state, reducing user's available balance

    4. S3 objects with no DB references (optional, slow)
       - May indicate: DB delete succeeded but S3 delete failed
       - Impact: Orphan storage costs, potential data inconsistency
    """
    try:
        from backend.services.reconciliation_service import ReconciliationService

        stale_minutes = request.args.get("stale_minutes", 30, type=int)
        check_s3 = request.args.get("check_s3", "false").lower() == "true"
        limit = min(request.args.get("limit", 100, type=int), 500)

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Anomaly detection by {admin_email or 'token'} (stale_minutes={stale_minutes}, check_s3={check_s3})")

        result = ReconciliationService.detect_anomalies(
            stale_minutes=stale_minutes,
            check_s3=check_s3,
            limit=limit,
        )

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Anomaly detection error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/reconcile/mollie", methods=["POST"])
@require_admin
def reconcile_mollie():
    """
    Run Mollie payment reconciliation.

    Fetches payments from Mollie API and compares to database. Creates missing
    purchases, grants missing subscription credits, and applies missing refunds.

    Query params:
        - days: How many days back to scan (default: 30, max: 90)
        - dry_run: If 'true', detect issues but don't fix them (default: false)
        - run_type: Type of run - 'full', 'mollie_only', 'subscriptions_only' (default: full)

    Returns:
        Summary of reconciliation run including:
        - scanned_count: Total payments scanned from Mollie
        - fixed_count: Total fixes applied
        - purchases_fixed: One-time purchases created
        - subscriptions_fixed: Subscription credits granted
        - refunds_fixed: Refund entries applied

    Safe to call frequently - all fixes are idempotent via unique constraint.
    """
    try:
        from backend.services.reconciliation_service import ReconciliationService

        days = min(request.args.get("days", 30, type=int), 90)
        dry_run = request.args.get("dry_run", "false").lower() == "true"
        run_type = request.args.get("run_type", "full")

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Mollie reconciliation triggered by {admin_email or 'token'} (days={days}, dry_run={dry_run}, run_type={run_type})")

        result = ReconciliationService.reconcile_mollie_payments(
            days_back=days,
            dry_run=dry_run,
            run_type=run_type,
        )

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Mollie reconciliation error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/reconcile/full", methods=["POST"])
@require_admin
def reconcile_full():
    """
    Run full reconciliation (safety checks + Mollie API comparison).

    Combines both reconciliation types:
    1. Safety reconciliation: DB internal consistency checks
    2. Mollie reconciliation: Compare Mollie payments to DB

    Query params:
        - days: How many days back to scan Mollie (default: 30, max: 90)
        - dry_run: If 'true', detect issues but don't fix them (default: false)
        - send_alert: If 'true', send admin email on fixes (default: true)

    Returns:
        Combined summary from both reconciliation types.

    This is the recommended endpoint for daily cron jobs.
    """
    try:
        from backend.services.reconciliation_service import ReconciliationService

        days = min(request.args.get("days", 30, type=int), 90)
        dry_run = request.args.get("dry_run", "false").lower() == "true"
        send_alert = request.args.get("send_alert", "true").lower() != "false"

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Full reconciliation triggered by {admin_email or 'token'} (days={days}, dry_run={dry_run})")

        result = ReconciliationService.reconcile_full(
            days_back=days,
            dry_run=dry_run,
            send_alert=send_alert,
        )

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Full reconciliation error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/reconcile/runs", methods=["GET"])
@require_admin
def reconciliation_runs():
    """
    Get recent reconciliation run history.

    Query params:
        - limit: Number of runs to return (default: 20, max: 100)
        - status: Filter by status - 'running', 'completed', 'failed' (optional)

    Returns:
        List of recent reconciliation runs with their statistics.
    """
    try:
        from backend.db import query_all

        limit = min(request.args.get("limit", 20, type=int), 100)
        status_filter = request.args.get("status")

        if status_filter:
            runs = query_all(
                """
                SELECT id, started_at, finished_at, status, run_type, days_back,
                       scanned_count, fixed_count, errors_count,
                       purchases_fixed, subscriptions_fixed, refunds_fixed, wallets_fixed,
                       notes
                FROM timrx_billing.reconciliation_runs
                WHERE status = %s
                ORDER BY started_at DESC
                LIMIT %s
                """,
                [status_filter, limit],
            )
        else:
            runs = query_all(
                """
                SELECT id, started_at, finished_at, status, run_type, days_back,
                       scanned_count, fixed_count, errors_count,
                       purchases_fixed, subscriptions_fixed, refunds_fixed, wallets_fixed,
                       notes
                FROM timrx_billing.reconciliation_runs
                ORDER BY started_at DESC
                LIMIT %s
                """,
                [limit],
            )

        return jsonify({
            "ok": True,
            "runs": [
                {
                    "id": str(r["id"]),
                    "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                    "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
                    "status": r["status"],
                    "run_type": r["run_type"],
                    "days_back": r["days_back"],
                    "scanned_count": r["scanned_count"],
                    "fixed_count": r["fixed_count"],
                    "errors_count": r["errors_count"],
                    "purchases_fixed": r["purchases_fixed"],
                    "subscriptions_fixed": r["subscriptions_fixed"],
                    "refunds_fixed": r["refunds_fixed"],
                    "wallets_fixed": r["wallets_fixed"],
                    "notes": r["notes"],
                }
                for r in runs
            ],
        })
    except Exception as e:
        print(f"[ADMIN] Reconciliation runs error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/reconcile/fixes", methods=["GET"])
@require_admin
def reconciliation_fixes():
    """
    Get recent reconciliation fixes applied.

    Query params:
        - limit: Number of fixes to return (default: 50, max: 200)
        - run_id: Filter by specific run ID (optional)
        - identity_id: Filter by identity ID (optional)
        - fix_type: Filter by fix type (optional)

    Returns:
        List of individual fixes applied during reconciliation.
    """
    try:
        from backend.db import query_all

        limit = min(request.args.get("limit", 50, type=int), 200)
        run_id = request.args.get("run_id")
        identity_id = request.args.get("identity_id")
        fix_type = request.args.get("fix_type")

        conditions = []
        params = []

        if run_id:
            conditions.append("run_id = %s")
            params.append(run_id)
        if identity_id:
            conditions.append("identity_id = %s")
            params.append(identity_id)
        if fix_type:
            conditions.append("fix_type = %s")
            params.append(fix_type)

        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        params.append(limit)

        fixes = query_all(
            f"""
            SELECT id, run_id, provider, provider_payment_id, fix_type,
                   identity_id, credits_delta, plan_code, amount_gbp,
                   mollie_status, created_at
            FROM timrx_billing.reconciliation_fixes
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT %s
            """,
            params,
        )

        return jsonify({
            "ok": True,
            "fixes": [
                {
                    "id": str(f["id"]),
                    "run_id": str(f["run_id"]) if f["run_id"] else None,
                    "provider": f["provider"],
                    "provider_payment_id": f["provider_payment_id"],
                    "fix_type": f["fix_type"],
                    "identity_id": str(f["identity_id"]) if f["identity_id"] else None,
                    "credits_delta": f["credits_delta"],
                    "plan_code": f["plan_code"],
                    "amount_gbp": float(f["amount_gbp"]) if f["amount_gbp"] else None,
                    "mollie_status": f["mollie_status"],
                    "created_at": f["created_at"].isoformat() if f["created_at"] else None,
                }
                for f in fixes
            ],
        })
    except Exception as e:
        print(f"[ADMIN] Reconciliation fixes error: {e}")
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


# ─────────────────────────────────────────────────────────────────────────────
# DEBUG: OpenAI Image Credit Flow Verification
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/debug/openai-credits", methods=["GET"])
@require_admin
def debug_openai_credits():
    """
    DEBUG: Verify credit flow for OpenAI image jobs.

    Query params:
        - identity_id: User UUID (optional - shows all OPENAI_IMAGE entries if not provided)
        - job_id: Specific job ID to trace (optional)
        - limit: Max results (default 20)

    Returns:
        - reservations: Recent OPENAI_IMAGE reservations with status
        - ledger_entries: Recent OPENAI_IMAGE ledger entries (should be negative)
        - jobs: Recent OpenAI image jobs
        - wallet: Current wallet state if identity_id provided
        - diagnosis: Analysis of whether credits are being deducted
    """
    from backend.db import query_one, query_all, Tables, USE_DB

    if not USE_DB:
        return jsonify({
            "ok": False,
            "error": "Database not configured",
            "diagnosis": "DATABASE NOT AVAILABLE - credits cannot be tracked"
        }), 500

    identity_id = request.args.get("identity_id")
    job_id = request.args.get("job_id")
    limit = min(request.args.get("limit", 20, type=int), 100)

    try:
        result = {"ok": True, "diagnosis": []}

        # ── Check reservations for OPENAI_IMAGE ────────────────────
        if job_id:
            reservations = query_all(
                f"""
                SELECT id, identity_id, action_code, cost_credits, status,
                       ref_job_id, created_at, captured_at, released_at
                FROM {Tables.CREDIT_RESERVATIONS}
                WHERE ref_job_id = %s OR id::text = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (job_id, job_id, limit),
            )
        elif identity_id:
            reservations = query_all(
                f"""
                SELECT id, identity_id, action_code, cost_credits, status,
                       ref_job_id, created_at, captured_at, released_at
                FROM {Tables.CREDIT_RESERVATIONS}
                WHERE identity_id = %s AND action_code = 'OPENAI_IMAGE'
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (identity_id, limit),
            )
        else:
            reservations = query_all(
                f"""
                SELECT id, identity_id, action_code, cost_credits, status,
                       ref_job_id, created_at, captured_at, released_at
                FROM {Tables.CREDIT_RESERVATIONS}
                WHERE action_code = 'OPENAI_IMAGE'
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )

        result["reservations"] = [
            {
                "id": str(r["id"]),
                "identity_id": str(r["identity_id"]),
                "action_code": r["action_code"],
                "cost_credits": r["cost_credits"],
                "status": r["status"],
                "job_id": str(r["ref_job_id"]) if r.get("ref_job_id") else None,
                "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
                "captured_at": r["captured_at"].isoformat() if r.get("captured_at") else None,
                "released_at": r["released_at"].isoformat() if r.get("released_at") else None,
            }
            for r in reservations
        ]

        # Count by status
        held_count = sum(1 for r in reservations if r["status"] == "held")
        finalized_count = sum(1 for r in reservations if r["status"] == "finalized")
        released_count = sum(1 for r in reservations if r["status"] == "released")

        result["reservation_stats"] = {
            "total": len(reservations),
            "held": held_count,
            "finalized": finalized_count,
            "released": released_count,
        }

        if len(reservations) == 0:
            result["diagnosis"].append("⚠️ NO RESERVATIONS FOUND for OPENAI_IMAGE - credits are NOT being held")
        elif finalized_count == 0:
            result["diagnosis"].append("⚠️ NO FINALIZED RESERVATIONS - credits are being held but NOT captured")
        else:
            result["diagnosis"].append(f"✓ Found {finalized_count} finalized reservations - credits ARE being deducted")

        # ── Check ledger entries ───────────────────────────────────
        if identity_id:
            ledger_entries = query_all(
                f"""
                SELECT id, identity_id, entry_type, amount_credits, ref_type, ref_id, meta, created_at
                FROM {Tables.LEDGER_ENTRIES}
                WHERE identity_id = %s
                  AND (entry_type = 'RESERVATION_FINALIZE' OR meta::text LIKE '%%OPENAI_IMAGE%%' OR meta::text LIKE '%%image-studio%%')
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (identity_id, limit),
            )
        else:
            ledger_entries = query_all(
                f"""
                SELECT id, identity_id, entry_type, amount_credits, ref_type, ref_id, meta, created_at
                FROM {Tables.LEDGER_ENTRIES}
                WHERE entry_type = 'RESERVATION_FINALIZE'
                  AND meta::text LIKE '%%OPENAI_IMAGE%%'
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )

        result["ledger_entries"] = [
            {
                "id": str(le["id"]),
                "identity_id": str(le["identity_id"]),
                "entry_type": le["entry_type"],
                "amount_credits": le["amount_credits"],
                "ref_type": le["ref_type"],
                "ref_id": str(le["ref_id"]) if le.get("ref_id") else None,
                "meta": le["meta"],
                "created_at": le["created_at"].isoformat() if le.get("created_at") else None,
            }
            for le in ledger_entries
        ]

        openai_debits = [le for le in ledger_entries if le["amount_credits"] < 0]
        if len(openai_debits) == 0:
            result["diagnosis"].append("⚠️ NO LEDGER DEBITS found - wallet balance is NOT being reduced")
        else:
            total_deducted = sum(abs(le["amount_credits"]) for le in openai_debits)
            result["diagnosis"].append(f"✓ Found {len(openai_debits)} ledger debits totaling -{total_deducted} credits")

        # ── Check jobs ──────────────────────────────────���──────────
        if job_id:
            jobs = query_all(
                f"""
                SELECT id, identity_id, provider, action_code, status, reservation_id, created_at, updated_at
                FROM {Tables.JOBS}
                WHERE id::text = %s OR reservation_id::text = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (job_id, job_id, limit),
            )
        elif identity_id:
            jobs = query_all(
                f"""
                SELECT id, identity_id, provider, action_code, status, reservation_id, created_at, updated_at
                FROM {Tables.JOBS}
                WHERE identity_id = %s AND (provider = 'openai' OR action_code = 'OPENAI_IMAGE')
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (identity_id, limit),
            )
        else:
            jobs = query_all(
                f"""
                SELECT id, identity_id, provider, action_code, status, reservation_id, created_at, updated_at
                FROM {Tables.JOBS}
                WHERE provider = 'openai' OR action_code = 'OPENAI_IMAGE'
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )

        result["jobs"] = [
            {
                "id": str(j["id"]),
                "identity_id": str(j["identity_id"]),
                "provider": j["provider"],
                "action_code": j["action_code"],
                "status": j["status"],
                "reservation_id": str(j["reservation_id"]) if j.get("reservation_id") else None,
                "created_at": j["created_at"].isoformat() if j.get("created_at") else None,
            }
            for j in jobs
        ]

        jobs_without_reservation = [j for j in jobs if not j.get("reservation_id")]
        if jobs_without_reservation:
            result["diagnosis"].append(f"⚠️ {len(jobs_without_reservation)} jobs have NO reservation_id - credits not tracked")

        # ── Get wallet if identity provided ────────────────────────
        if identity_id:
            wallet = query_one(
                f"SELECT balance_credits, updated_at FROM {Tables.WALLETS} WHERE identity_id = %s",
                (identity_id,),
            )
            reserved = query_one(
                f"""
                SELECT COALESCE(SUM(cost_credits), 0) as total
                FROM {Tables.CREDIT_RESERVATIONS}
                WHERE identity_id = %s AND status = 'held'
                """,
                (identity_id,),
            )

            result["wallet"] = {
                "balance_credits": wallet["balance_credits"] if wallet else 0,
                "reserved_credits": int(reserved["total"]) if reserved else 0,
                "available_credits": max(0, (wallet["balance_credits"] if wallet else 0) - int(reserved["total"] if reserved else 0)),
                "updated_at": wallet["updated_at"].isoformat() if wallet and wallet.get("updated_at") else None,
            }

        # ── Final diagnosis ────────────────────────────────────────
        if len(reservations) > 0 and finalized_count > 0 and len(openai_debits) > 0:
            result["diagnosis"].append("✓ CREDITS ARE BEING DEDUCTED for OpenAI images")
        else:
            result["diagnosis"].append("❌ CREDITS ARE NOT BEING DEDUCTED - see individual checks above")

        return jsonify(result)

    except Exception as e:
        print(f"[ADMIN] Debug openai-credits error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# DEBUG: Magic Code Retrieval (for acceptance testing only)
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/debug/magic-code", methods=["GET"])
@require_admin
def debug_magic_code():
    """
    DEBUG: Retrieve the most recent magic code for an email.

    WARNING: This endpoint is for TESTING ONLY. It bypasses email verification.
    In production, this should be disabled or heavily rate-limited.

    Query params:
        - email: Email address to get code for (required)

    Returns:
        - code: The plain-text code (if found)
        - expires_at: When the code expires
        - attempts: Number of failed attempts
    """
    from backend.db import query_one, Tables, USE_DB

    if not USE_DB:
        return jsonify({"ok": False, "error": "Database not configured"}), 500

    email = request.args.get("email", "").strip().lower()
    if not email:
        return jsonify({"ok": False, "error": "email parameter required"}), 400

    try:
        # For testing, we store a copy of the plain code in a test-only column
        # OR we generate a new code and return it
        # Since we hash codes, we can't retrieve them - so generate a fresh one

        from backend.services.magic_code_service import MagicCodeService

        # Check if identity exists
        identity = query_one(
            f"SELECT id FROM {Tables.IDENTITIES} WHERE email = %s",
            (email,),
        )

        if not identity:
            return jsonify({
                "ok": False,
                "error": "No identity found for this email",
            }), 404

        # Generate a fresh code for testing
        plain_code = MagicCodeService.generate_code()
        code_hash = MagicCodeService.hash_code(plain_code)

        # Store it
        from backend.db import execute
        from backend.config import config

        execute(
            f"""
            INSERT INTO {Tables.MAGIC_CODES}
            (email, code_hash, expires_at, attempts, consumed, created_at)
            VALUES (%s, %s, NOW() + INTERVAL '%s minutes', 0, FALSE, NOW())
            """,
            (email, code_hash, config.MAGIC_CODE_EXPIRY_MINUTES),
        )

        print(f"[ADMIN:DEBUG] Generated test magic code for {email}: {plain_code}")

        return jsonify({
            "ok": True,
            "code": plain_code,
            "email": email,
            "expires_in_minutes": config.MAGIC_CODE_EXPIRY_MINUTES,
            "warning": "TEST ONLY - bypasses email verification",
        })

    except Exception as e:
        print(f"[ADMIN] Debug magic-code error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# SUBSCRIPTION CRON ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/subscriptions/process-credits", methods=["POST"])
@require_admin
def process_subscription_credits():
    """
    Process due credit allocations for subscriptions (cron-callable endpoint).

    Call this endpoint periodically (e.g., every hour) from:
    - A Render cron job
    - An external scheduler (e.g., cron.org, EasyCron)
    - A background worker

    This finds subscriptions where next_credit_date <= NOW() and:
    1. Grants the monthly credits
    2. Updates next_credit_date to next month
    3. Sends notification email
    4. For yearly plans, decrements credits_remaining_months

    Returns:
        - processed: Number of subscriptions checked
        - granted: Number of successful credit grants
        - errors: Number of errors encountered

    Safe to call frequently - idempotent credit grants.
    """
    try:
        from backend.services.subscription_service import SubscriptionService

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Subscription credit processing triggered by {admin_email or 'token'}")

        result = SubscriptionService.process_due_credit_allocations()

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Subscription credit processing error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/subscriptions/check-expired", methods=["POST"])
@require_admin
def check_expired_subscriptions():
    """
    Check and expire cancelled subscriptions past their period (cron-callable endpoint).

    Call this endpoint periodically (e.g., every hour) to:
    - Find cancelled subscriptions past their current_period_end
    - Mark them as 'expired'
    - Send expiration notification emails

    Returns:
        - expired: Number of subscriptions expired

    Safe to call frequently - only processes subscriptions once.
    """
    try:
        from backend.services.subscription_service import SubscriptionService

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Subscription expiration check triggered by {admin_email or 'token'}")

        expired = SubscriptionService.check_expired_subscriptions()

        return jsonify({"ok": True, "expired": expired})
    except Exception as e:
        print(f"[ADMIN] Subscription expiration check error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/subscriptions/run-all", methods=["POST"])
@require_admin
def run_all_subscription_jobs():
    """
    Run all subscription maintenance jobs (cron-callable endpoint).

    This is a convenience endpoint that runs:
    1. Credit allocation processing (process_due_credit_allocations)
    2. Expired subscription check (check_expired_subscriptions)

    Recommended frequency: every 1-6 hours

    Returns:
        - credits: Credit allocation results
        - expired: Number of subscriptions expired
    """
    try:
        from backend.services.subscription_service import SubscriptionService

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] All subscription jobs triggered by {admin_email or 'token'}")

        # Process credit allocations
        credit_result = SubscriptionService.process_due_credit_allocations()

        # Check expired subscriptions
        expired_count = SubscriptionService.check_expired_subscriptions()

        return jsonify({
            "ok": True,
            "credits": {
                "processed": credit_result.get("processed", 0),
                "granted": credit_result.get("granted", 0),
                "errors": credit_result.get("errors", 0),
            },
            "expired": expired_count,
        })
    except Exception as e:
        print(f"[ADMIN] Subscription jobs error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/subscriptions/stats", methods=["GET"])
@require_admin
def subscription_stats():
    """
    Get subscription statistics for monitoring.

    Returns:
        - total: Total subscription count
        - by_status: Subscription counts by status (active, past_due, cancelled, expired)
        - by_plan: Subscription counts by plan_code
        - due_for_credits: Subscriptions due for credit allocation (next_credit_date <= NOW)
        - past_due_count: Number of subscriptions with failed payments
    """
    try:
        # Get counts by status
        stats_row = query_one(
            """
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE status = 'active') as active,
                COUNT(*) FILTER (WHERE status = 'past_due') as past_due,
                COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled,
                COUNT(*) FILTER (WHERE status = 'expired') as expired,
                COUNT(*) FILTER (WHERE status = 'active' AND next_credit_date <= NOW()) as due_for_credits
            FROM timrx_billing.subscriptions
            """,
        )

        # Get counts by plan
        plan_rows = query_all(
            """
            SELECT plan_code, COUNT(*) as count
            FROM timrx_billing.subscriptions
            WHERE status = 'active'
            GROUP BY plan_code
            ORDER BY count DESC
            """,
        )

        by_plan = {row["plan_code"]: row["count"] for row in (plan_rows or [])}

        return jsonify({
            "ok": True,
            "total": stats_row["total"] if stats_row else 0,
            "by_status": {
                "active": stats_row["active"] if stats_row else 0,
                "past_due": stats_row["past_due"] if stats_row else 0,
                "cancelled": stats_row["cancelled"] if stats_row else 0,
                "expired": stats_row["expired"] if stats_row else 0,
            },
            "by_plan": by_plan,
            "due_for_credits": stats_row["due_for_credits"] if stats_row else 0,
            "past_due_count": stats_row["past_due"] if stats_row else 0,
        })
    except Exception as e:
        print(f"[ADMIN] Subscription stats error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/subscriptions/audit", methods=["GET"])
@require_admin
def subscription_audit():
    """
    Audit a specific subscription or identity for debugging/support.

    Query params:
        - identity_id: User's identity UUID (optional)
        - subscription_id: Subscription UUID (optional)

    Returns detailed subscription info including:
        - Active plan, status, Mollie subscription ID
        - Current period start/end
        - Last 5 subscription grant ledger entries
        - Next refill date
        - Anomalies: duplicates/missing periods

    Example:
        GET /api/admin/subscriptions/audit?identity_id=123e4567-...
    """
    try:
        from backend.services.subscription_service import SubscriptionService
        from backend.db import query_one, query_all, Tables

        identity_id = request.args.get("identity_id")
        subscription_id = request.args.get("subscription_id")

        if not identity_id and not subscription_id:
            return jsonify({
                "ok": False,
                "error": "Provide identity_id or subscription_id parameter",
            }), 400

        result = {
            "ok": True,
            "identity_id": identity_id,
            "subscription": None,
            "cycles": [],
            "anomalies": [],
            "tier_perks": None,
        }

        # Get subscription (by ID or by identity)
        if subscription_id:
            sub = query_one(
                f"""
                SELECT s.*, i.email as identity_email, i.email_verified
                FROM {Tables.SUBSCRIPTIONS} s
                LEFT JOIN {Tables.IDENTITIES} i ON i.id = s.identity_id
                WHERE s.id::text = %s
                """,
                (subscription_id,),
            )
            if sub:
                identity_id = str(sub["identity_id"])
        else:
            sub = query_one(
                f"""
                SELECT s.*, i.email as identity_email, i.email_verified
                FROM {Tables.SUBSCRIPTIONS} s
                LEFT JOIN {Tables.IDENTITIES} i ON i.id = s.identity_id
                WHERE s.identity_id::text = %s
                  AND s.status IN ('active', 'cancelled', 'past_due')
                ORDER BY s.created_at DESC
                LIMIT 1
                """,
                (identity_id,),
            )

        if sub:
            result["identity_id"] = str(sub["identity_id"])
            result["subscription"] = {
                "id": str(sub["id"]),
                "plan_code": sub["plan_code"],
                "status": sub["status"],
                "provider": sub.get("provider"),
                "provider_subscription_id": sub.get("provider_subscription_id"),
                "current_period_start": sub["current_period_start"].isoformat() if sub.get("current_period_start") else None,
                "current_period_end": sub["current_period_end"].isoformat() if sub.get("current_period_end") else None,
                "next_credit_date": sub["next_credit_date"].isoformat() if sub.get("next_credit_date") else None,
                "billing_day": sub.get("billing_day"),
                "credits_remaining_months": sub.get("credits_remaining_months"),
                "customer_email": sub.get("customer_email"),
                "identity_email": sub.get("identity_email"),
                "email_verified": sub.get("email_verified"),
                "cancelled_at": sub["cancelled_at"].isoformat() if sub.get("cancelled_at") else None,
                "created_at": sub["created_at"].isoformat() if sub.get("created_at") else None,
            }

            # Get last 5 cycles
            cycles = query_all(
                f"""
                SELECT id, period_start, period_end, credits_granted, granted_at,
                       provider, provider_payment_id
                FROM {Tables.SUBSCRIPTION_CYCLES}
                WHERE subscription_id::text = %s
                ORDER BY period_start DESC
                LIMIT 5
                """,
                (str(sub["id"]),),
            )
            result["cycles"] = [
                {
                    "id": str(c["id"]),
                    "period_start": c["period_start"].isoformat() if c.get("period_start") else None,
                    "period_end": c["period_end"].isoformat() if c.get("period_end") else None,
                    "credits_granted": c["credits_granted"],
                    "granted_at": c["granted_at"].isoformat() if c.get("granted_at") else None,
                    "provider_payment_id": c.get("provider_payment_id"),
                }
                for c in (cycles or [])
            ]

            # Check for anomalies
            anomalies = []

            # 1. Check for duplicate periods
            dup_check = query_one(
                f"""
                SELECT period_start, COUNT(*) as cnt
                FROM {Tables.SUBSCRIPTION_CYCLES}
                WHERE subscription_id::text = %s
                GROUP BY period_start
                HAVING COUNT(*) > 1
                LIMIT 1
                """,
                (str(sub["id"]),),
            )
            if dup_check:
                anomalies.append({
                    "type": "duplicate_period",
                    "message": f"Duplicate grants found for period {dup_check['period_start']}",
                    "period_start": str(dup_check["period_start"]),
                })

            # 2. Check for missing months (gaps > 35 days between cycles)
            gap_check = query_all(
                f"""
                SELECT period_start, LAG(period_start) OVER (ORDER BY period_start) as prev_start,
                       period_start - LAG(period_start) OVER (ORDER BY period_start) as gap
                FROM {Tables.SUBSCRIPTION_CYCLES}
                WHERE subscription_id::text = %s
                ORDER BY period_start
                """,
                (str(sub["id"]),),
            )
            for gap in (gap_check or []):
                if gap.get("gap") and gap["gap"].days > 35:
                    anomalies.append({
                        "type": "missing_period",
                        "message": f"Gap of {gap['gap'].days} days between cycles",
                        "gap_after": str(gap["prev_start"]) if gap.get("prev_start") else None,
                    })

            # 3. Check for future next_credit_date being too far out
            if sub.get("next_credit_date"):
                from datetime import datetime, timezone
                now = datetime.now(timezone.utc)
                days_until = (sub["next_credit_date"] - now).days
                if days_until > 35:
                    anomalies.append({
                        "type": "next_credit_far",
                        "message": f"Next credit date is {days_until} days away",
                        "next_credit_date": sub["next_credit_date"].isoformat(),
                    })

            # 4. Check email verification status
            if not sub.get("email_verified"):
                anomalies.append({
                    "type": "email_unverified",
                    "message": "Identity email is not verified - credits may be paused",
                })

            result["anomalies"] = anomalies

        # Get tier perks for identity
        if identity_id:
            perks = SubscriptionService.get_tier_perks(identity_id)
            result["tier_perks"] = perks

        return jsonify(result)

    except Exception as e:
        print(f"[ADMIN] Subscription audit error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# WALLET DRIFT AUDIT ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/wallet-drift/audit", methods=["POST"])
@require_admin
def wallet_drift_audit():
    """
    Run wallet drift audit (cron-callable endpoint).

    Detects wallets where balance_credits != SUM(ledger_entries) and repairs them.
    The ledger is the immutable source of truth; the wallet balance is a cache.

    Query params:
        - dry_run: If 'true', detect drifts but don't repair (default: false)

    Returns:
        - total_wallets: Number of wallets checked
        - drifts_found: Number of wallets with drift
        - repairs_applied: Number of repairs made (0 if dry_run)
        - total_drift_amount: Sum of absolute drift values

    Safe to call frequently - all repairs are idempotent.
    """
    try:
        from backend.services.wallet_drift_service import WalletDriftService

        dry_run = request.args.get("dry_run", "false").lower() == "true"

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Wallet drift audit triggered by {admin_email or 'token'} (dry_run={dry_run})")

        result = WalletDriftService.run_daily_wallet_audit(dry_run=dry_run)

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Wallet drift audit error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/wallet-drift/drifts", methods=["GET"])
@require_admin
def wallet_drift_list():
    """
    List wallets with drift (detection only).

    Query params:
        - limit: Max results (default: 100, max: 500)
        - offset: Pagination offset (default: 0)

    Returns:
        - drifts: List of wallets where balance != ledger_sum
        - count: Total drifts found (up to limit)
    """
    try:
        from backend.services.wallet_drift_service import WalletDriftService

        limit = min(request.args.get("limit", 100, type=int), 500)
        offset = request.args.get("offset", 0, type=int)

        drifts = WalletDriftService.find_drifts(limit=limit, offset=offset)
        total = WalletDriftService.count_drifts()

        return jsonify({
            "ok": True,
            "drifts": [
                {
                    "identity_id": str(d["identity_id"]),
                    "wallet_id": str(d["wallet_id"]) if d.get("wallet_id") else None,
                    "cached_balance": d.get("cached_balance", 0),
                    "ledger_sum": d.get("ledger_sum", 0),
                    "drift": d.get("drift", 0),
                    "entry_count": d.get("entry_count", 0),
                }
                for d in drifts
            ],
            "count": len(drifts),
            "total": total,
        })
    except Exception as e:
        print(f"[ADMIN] Wallet drift list error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/wallet-drift/repair/<identity_id>", methods=["POST"])
@require_admin
def wallet_drift_repair(identity_id):
    """
    Repair drift for a specific wallet.

    Args:
        identity_id: The identity UUID to repair

    Body (optional):
        - reason: Reason for repair (default: 'admin_repair')

    Returns:
        - repaired: bool - True if repair was applied
        - old_balance: Balance before repair
        - new_balance: Balance after repair
        - drift_amount: Difference corrected
        - repair_id: UUID of repair record (if applied)
    """
    try:
        from backend.services.wallet_drift_service import WalletDriftService

        data = request.get_json() or {}
        reason = data.get("reason", "admin_repair")

        admin_email = getattr(g, "admin_email", None)
        print(f"[ADMIN] Wallet drift repair for {identity_id} by {admin_email or 'token'}")

        result = WalletDriftService.repair_wallet(
            identity_id=identity_id,
            reason=reason,
            trigger_source="admin_endpoint",
        )

        return jsonify({"ok": True, **result})
    except Exception as e:
        print(f"[ADMIN] Wallet drift repair error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/wallet-drift/check/<identity_id>", methods=["GET"])
@require_admin
def wallet_drift_check(identity_id):
    """
    Check drift for a specific identity.

    Returns wallet vs ledger comparison without making any changes.
    """
    try:
        from backend.services.wallet_drift_service import WalletDriftService

        comparison = WalletDriftService.get_wallet_comparison(identity_id)
        if not comparison:
            return jsonify({
                "ok": False,
                "error": "Wallet not found for this identity"
            }), 404

        return jsonify({
            "ok": True,
            "identity_id": str(comparison["identity_id"]),
            "wallet_id": str(comparison["wallet_id"]) if comparison.get("wallet_id") else None,
            "cached_balance": comparison.get("cached_balance", 0),
            "ledger_sum": comparison.get("ledger_sum", 0),
            "drift": comparison.get("drift", 0),
            "has_drift": comparison.get("has_drift", False),
            "entry_count": comparison.get("entry_count", 0),
        })
    except Exception as e:
        print(f"[ADMIN] Wallet drift check error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/wallet-drift/repairs", methods=["GET"])
@require_admin
def wallet_drift_repairs():
    """
    Get recent wallet repair history.

    Query params:
        - identity_id: Filter by identity (optional)
        - limit: Max results (default: 50, max: 200)
        - offset: Pagination offset (default: 0)

    Returns:
        - repairs: List of repair records
    """
    try:
        from backend.services.wallet_drift_service import WalletDriftService

        identity_id = request.args.get("identity_id")
        limit = min(request.args.get("limit", 50, type=int), 200)
        offset = request.args.get("offset", 0, type=int)

        repairs = WalletDriftService.get_recent_repairs(
            identity_id=identity_id,
            limit=limit,
            offset=offset,
        )

        return jsonify({
            "ok": True,
            "repairs": [
                {
                    "id": str(r["id"]),
                    "identity_id": str(r["identity_id"]),
                    "wallet_id": str(r["wallet_id"]) if r.get("wallet_id") else None,
                    "old_balance": r.get("old_balance", 0),
                    "new_balance": r.get("new_balance", 0),
                    "drift_amount": r.get("drift_amount", 0),
                    "reason": r.get("reason"),
                    "trigger_source": r.get("trigger_source"),
                    "ledger_entry_count": r.get("ledger_entry_count"),
                    "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
                }
                for r in repairs
            ],
        })
    except Exception as e:
        print(f"[ADMIN] Wallet drift repairs error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


@bp.route("/wallet-drift/stats", methods=["GET"])
@require_admin
def wallet_drift_stats():
    """
    Get wallet drift and repair statistics.

    Query params:
        - days: Number of days to look back (default: 30)

    Returns:
        - current_drifts: Number of wallets currently with drift
        - total_repairs: Repairs in the period
        - total_drift_corrected: Sum of absolute drift amounts corrected
        - by_reason: Breakdown by repair reason
        - by_trigger: Breakdown by trigger source
    """
    try:
        from backend.services.wallet_drift_service import WalletDriftService

        days = request.args.get("days", 30, type=int)

        current_drifts = WalletDriftService.count_drifts()
        stats = WalletDriftService.get_repair_stats(days=days)

        return jsonify({
            "ok": True,
            "current_drifts": current_drifts,
            **stats,
        })
    except Exception as e:
        print(f"[ADMIN] Wallet drift stats error: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# BILLING HEALTH VIEW (UI + Read-Only)
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/billing/health", methods=["GET"])
@require_admin
def billing_health():
    """
    Get billing health overview for admin dashboard.

    Returns the last 200 subscriptions with status indicators for monitoring.
    This is a read-only endpoint for the admin billing health UI.

    Query params:
        - limit: Max subscriptions to return (default: 200, max: 500)
        - status: Filter by status (active, cancelled, past_due, suspended, expired)
        - offset: Pagination offset (default: 0)

    Returns:
        - subscriptions: List of subscription records with row_hint colors
        - stats: Summary counts by status
        - total: Total subscription count

    Row hints (for UI color coding):
        - "green": active subscriptions
        - "amber": cancelled (still has access until period end)
        - "red": past_due, suspended, or expired
    """
    try:
        limit = min(request.args.get("limit", 200, type=int), 500)
        offset = request.args.get("offset", 0, type=int)
        status_filter = request.args.get("status")

        # Build query with optional status filter
        conditions = []
        params = []

        if status_filter:
            conditions.append("s.status = %s")
            params.append(status_filter)

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Fetch subscriptions with identity email
        subscriptions = query_all(
            f"""
            SELECT
                s.id,
                s.identity_id,
                i.email,
                s.plan_code,
                s.status,
                s.provider,
                s.provider_subscription_id,
                s.current_period_start,
                s.current_period_end,
                s.next_credit_date,
                s.billing_day,
                s.credits_remaining_months,
                s.customer_email,
                s.cancelled_at,
                s.suspend_reason,
                s.created_at
            FROM timrx_billing.subscriptions s
            LEFT JOIN timrx_billing.identities i ON i.id = s.identity_id
            {where_clause}
            ORDER BY s.created_at DESC
            LIMIT %s OFFSET %s
            """,
            params + [limit, offset],
        )

        # Get status counts for summary
        stats_rows = query_all(
            """
            SELECT status, COUNT(*) as count
            FROM timrx_billing.subscriptions
            GROUP BY status
            """,
        )
        stats = {row["status"]: row["count"] for row in (stats_rows or [])}

        # Get total count
        total_row = query_one("SELECT COUNT(*) as total FROM timrx_billing.subscriptions")
        total = total_row["total"] if total_row else 0

        # Format response with row hints
        def get_row_hint(status):
            if status == "active":
                return "green"
            elif status == "cancelled":
                return "amber"
            elif status in ("past_due", "suspended", "expired"):
                return "red"
            return "default"

        formatted_subscriptions = [
            {
                "id": str(s["id"]),
                "identity_id": str(s["identity_id"]),
                "email": _mask_email(s["email"]) if s.get("email") else None,
                "plan_code": s["plan_code"],
                "status": s["status"],
                "provider": s.get("provider"),
                "provider_subscription_id": s.get("provider_subscription_id"),
                "current_period_start": s["current_period_start"].isoformat() if s.get("current_period_start") else None,
                "current_period_end": s["current_period_end"].isoformat() if s.get("current_period_end") else None,
                "next_credit_date": s["next_credit_date"].isoformat() if s.get("next_credit_date") else None,
                "billing_day": s.get("billing_day"),
                "credits_remaining_months": s.get("credits_remaining_months"),
                "customer_email": _mask_email(s["customer_email"]) if s.get("customer_email") else None,
                "cancelled_at": s["cancelled_at"].isoformat() if s.get("cancelled_at") else None,
                "suspend_reason": s.get("suspend_reason"),
                "created_at": s["created_at"].isoformat() if s.get("created_at") else None,
                "row_hint": get_row_hint(s["status"]),
            }
            for s in (subscriptions or [])
        ]

        return jsonify({
            "ok": True,
            "subscriptions": formatted_subscriptions,
            "stats": {
                "active": stats.get("active", 0),
                "cancelled": stats.get("cancelled", 0),
                "past_due": stats.get("past_due", 0),
                "suspended": stats.get("suspended", 0),
                "expired": stats.get("expired", 0),
                "pending_payment": stats.get("pending_payment", 0),
            },
            "total": total,
            "limit": limit,
            "offset": offset,
        })

    except Exception as e:
        print(f"[ADMIN] Billing health error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500
