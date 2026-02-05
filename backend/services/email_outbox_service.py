"""
Email Outbox Service - Durable email queue for guaranteed delivery.

This service ensures every purchase email is delivered by:
1. Writing email jobs to the database within the same transaction as purchases
2. Attempting immediate send after commit
3. Retrying failed emails via a background worker/cron
4. Alerting admins after max retries

Usage in purchase flow:
    with transaction() as cur:
        # ... create purchase, grant credits ...
        EmailOutboxService.queue_purchase_email(
            cur, purchase_id, identity_id, to_email, template, payload
        )
    # Transaction committed - email is now durable

    # Try immediate send (best-effort, failures are already queued)
    EmailOutboxService.send_pending_emails(limit=1, purchase_id=purchase_id)

Cron/background usage:
    # Call periodically to retry failed emails
    EmailOutboxService.send_pending_emails(limit=50)
"""

import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from backend.db import (
    fetch_one, fetch_all, transaction, query_one, query_all, execute, Tables
)


class EmailOutboxStatus:
    """Valid email outbox statuses."""
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"  # After max retries


class EmailTemplate:
    """Email template identifiers."""
    PURCHASE_RECEIPT = "purchase_receipt"        # Simple HTML receipt
    INVOICE_WITH_PDF = "invoice_with_pdf"        # Full invoice + receipt PDFs
    PAYMENT_RECEIVED = "payment_received"        # Fallback HTML-only confirmation
    ADMIN_ALERT = "admin_alert"                  # Admin notification


# Default max attempts before marking as failed
DEFAULT_MAX_ATTEMPTS = 5


class EmailOutboxService:
    """Service for durable email delivery with retry."""

    # ─────────────────────────────────────────────────────────────
    # Queue Operations (call within existing transaction)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def queue_email(
        cur,
        to_email: str,
        template: str,
        payload: Dict[str, Any],
        subject: Optional[str] = None,
        identity_id: Optional[str] = None,
        purchase_id: Optional[str] = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> Dict[str, Any]:
        """
        Queue an email for sending within an existing transaction.

        This ensures the email job is committed atomically with the purchase.

        Args:
            cur: Database cursor (from transaction context)
            to_email: Recipient email address
            template: Email template identifier (see EmailTemplate)
            payload: Template-specific data (plan_name, credits, etc.)
            subject: Optional email subject override
            identity_id: Optional identity UUID for tracking
            purchase_id: Optional purchase UUID for tracking
            max_attempts: Max send attempts before marking failed

        Returns:
            The created outbox row as dict
        """
        cur.execute(
            f"""
            INSERT INTO {Tables.EMAIL_OUTBOX}
            (to_email, template, subject, payload, identity_id, purchase_id,
             status, attempts, max_attempts, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 0, %s, NOW())
            RETURNING *
            """,
            (
                to_email.lower().strip(),
                template,
                subject,
                json.dumps(payload),
                identity_id,
                purchase_id,
                EmailOutboxStatus.PENDING,
                max_attempts,
            ),
        )
        row = fetch_one(cur)
        print(f"[EMAIL_OUTBOX] Queued: template={template} to={to_email} purchase={purchase_id}")
        return row

    @staticmethod
    def queue_purchase_emails(
        cur,
        purchase_id: str,
        identity_id: str,
        to_email: str,
        plan_name: str,
        credits: int,
        amount_gbp: float,
        plan_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Queue all emails needed for a purchase (receipt + admin notification).

        Call this within the same transaction as purchase creation to ensure
        emails are never lost.

        Args:
            cur: Database cursor (from transaction context)
            purchase_id: The purchase UUID
            identity_id: The buyer's identity UUID
            to_email: Buyer's email address
            plan_name: Display name of the purchased plan
            credits: Number of credits purchased
            amount_gbp: Amount paid in GBP
            plan_code: Optional plan code for invoice generation

        Returns:
            List of created outbox rows
        """
        rows = []

        # Primary: Full invoice email (will be attempted first)
        # If PDF generation fails, the send logic falls back to simple receipt
        payload = {
            "purchase_id": purchase_id,
            "identity_id": identity_id,
            "plan_name": plan_name,
            "plan_code": plan_code,
            "credits": credits,
            "amount_gbp": amount_gbp,
        }

        row = EmailOutboxService.queue_email(
            cur,
            to_email=to_email,
            template=EmailTemplate.INVOICE_WITH_PDF,
            payload=payload,
            subject=f"TimrX Receipt - {plan_name}",
            identity_id=identity_id,
            purchase_id=purchase_id,
        )
        rows.append(row)

        # Admin notification (separate queue entry for independent retry)
        from backend.config import config
        if config.NOTIFY_ON_PURCHASE and config.ADMIN_EMAIL:
            admin_payload = {
                "identity_id": identity_id,
                "email": to_email,
                "plan_name": plan_name,
                "credits": credits,
                "amount_gbp": amount_gbp,
            }
            admin_row = EmailOutboxService.queue_email(
                cur,
                to_email=config.ADMIN_EMAIL,
                template=EmailTemplate.ADMIN_ALERT,
                payload={
                    "subject": "New Purchase",
                    "message": f"A user has purchased the {plan_name} plan.",
                    "data": admin_payload,
                },
                subject="[TimrX Admin] New Purchase",
                identity_id=identity_id,
                purchase_id=purchase_id,
                max_attempts=3,  # Admin notifications can fail more gracefully
            )
            rows.append(admin_row)

        return rows

    # ─────────────────────────────────────────────────────────────
    # Send Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def send_pending_emails(
        limit: int = 50,
        purchase_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Process pending emails from the outbox.

        Call this:
        - Immediately after a purchase commits (with purchase_id filter)
        - Periodically from a cron job (without filter, to retry failures)

        Args:
            limit: Maximum emails to process in this batch
            purchase_id: Optional filter to only send emails for a specific purchase

        Returns:
            Summary: {sent: N, failed: N, remaining: N}
        """
        # Fetch pending emails (oldest first)
        if purchase_id:
            pending = query_all(
                f"""
                SELECT * FROM {Tables.EMAIL_OUTBOX}
                WHERE status = %s AND purchase_id = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (EmailOutboxStatus.PENDING, purchase_id, limit),
            )
        else:
            pending = query_all(
                f"""
                SELECT * FROM {Tables.EMAIL_OUTBOX}
                WHERE status = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (EmailOutboxStatus.PENDING, limit),
            )

        if not pending:
            return {"sent": 0, "failed": 0, "remaining": 0}

        sent = 0
        failed = 0

        for email_job in pending:
            success = EmailOutboxService._send_single_email(email_job)
            if success:
                sent += 1
            else:
                failed += 1

        # Count remaining pending
        remaining_count = query_one(
            f"SELECT COUNT(*) as cnt FROM {Tables.EMAIL_OUTBOX} WHERE status = %s",
            (EmailOutboxStatus.PENDING,),
        )
        remaining = remaining_count["cnt"] if remaining_count else 0

        print(f"[EMAIL_OUTBOX] Batch complete: sent={sent} failed={failed} remaining={remaining}")
        return {"sent": sent, "failed": failed, "remaining": remaining}

    @staticmethod
    def _send_single_email(email_job: Dict[str, Any]) -> bool:
        """
        Attempt to send a single email from the outbox.

        Updates the outbox row with attempt count and status.

        Args:
            email_job: Row from email_outbox table

        Returns:
            True if sent successfully, False otherwise
        """
        outbox_id = str(email_job["id"])
        template = email_job["template"]
        to_email = email_job["to_email"]
        payload = email_job["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        attempts = email_job.get("attempts", 0) + 1
        max_attempts = email_job.get("max_attempts", DEFAULT_MAX_ATTEMPTS)

        print(f"[EMAIL_OUTBOX] Sending: id={outbox_id[:8]} template={template} to={to_email} attempt={attempts}/{max_attempts}")

        try:
            success, error = EmailOutboxService._dispatch_email(template, to_email, payload)

            if success:
                # Mark as sent
                with transaction() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.EMAIL_OUTBOX}
                        SET status = %s, sent_at = NOW(), attempts = %s, last_attempt_at = NOW()
                        WHERE id = %s
                        """,
                        (EmailOutboxStatus.SENT, attempts, outbox_id),
                    )

                    # Update purchase email_status if linked
                    if email_job.get("purchase_id") and template != EmailTemplate.ADMIN_ALERT:
                        cur.execute(
                            f"""
                            UPDATE {Tables.PURCHASES}
                            SET email_status = 'sent'
                            WHERE id = %s AND (email_status IS NULL OR email_status = 'pending')
                            """,
                            (email_job["purchase_id"],),
                        )

                print(f"[EMAIL_OUTBOX] Sent successfully: id={outbox_id[:8]}")
                return True
            else:
                # Send failed
                return EmailOutboxService._handle_send_failure(
                    outbox_id, email_job, attempts, max_attempts, error
                )

        except Exception as e:
            error_msg = str(e)
            print(f"[EMAIL_OUTBOX] Exception sending {outbox_id[:8]}: {error_msg}")
            return EmailOutboxService._handle_send_failure(
                outbox_id, email_job, attempts, max_attempts, error_msg
            )

    @staticmethod
    def _handle_send_failure(
        outbox_id: str,
        email_job: Dict[str, Any],
        attempts: int,
        max_attempts: int,
        error: str,
    ) -> bool:
        """Handle a failed send attempt - update counters and maybe mark as failed."""

        if attempts >= max_attempts:
            # Max retries reached - mark as failed and alert admin
            with transaction() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.EMAIL_OUTBOX}
                    SET status = %s, failed_at = NOW(), attempts = %s,
                        last_attempt_at = NOW(), last_error = %s
                    WHERE id = %s
                    """,
                    (EmailOutboxStatus.FAILED, attempts, error, outbox_id),
                )

                # Update purchase email_status if linked
                if email_job.get("purchase_id") and email_job["template"] != EmailTemplate.ADMIN_ALERT:
                    cur.execute(
                        f"""
                        UPDATE {Tables.PURCHASES}
                        SET email_status = 'failed'
                        WHERE id = %s
                        """,
                        (email_job["purchase_id"],),
                    )

            print(f"[EMAIL_OUTBOX] FAILED permanently after {attempts} attempts: id={outbox_id[:8]} error={error}")

            # Send admin alert about the failure
            EmailOutboxService._alert_admin_email_failure(email_job, error, attempts)

            return False
        else:
            # Still have retries left - increment attempts and keep pending
            with transaction() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.EMAIL_OUTBOX}
                    SET attempts = %s, last_attempt_at = NOW(), last_error = %s
                    WHERE id = %s
                    """,
                    (attempts, error, outbox_id),
                )

            print(f"[EMAIL_OUTBOX] Retry queued: id={outbox_id[:8]} attempt={attempts}/{max_attempts}")
            return False

    @staticmethod
    def _dispatch_email(
        template: str,
        to_email: str,
        payload: Dict[str, Any],
    ) -> tuple:
        """
        Dispatch email based on template type.

        Returns:
            (success: bool, error: Optional[str])
        """
        try:
            if template == EmailTemplate.INVOICE_WITH_PDF:
                return EmailOutboxService._send_invoice_email(to_email, payload)

            elif template == EmailTemplate.PURCHASE_RECEIPT:
                return EmailOutboxService._send_receipt_email(to_email, payload)

            elif template == EmailTemplate.PAYMENT_RECEIVED:
                return EmailOutboxService._send_payment_received_email(to_email, payload)

            elif template == EmailTemplate.ADMIN_ALERT:
                return EmailOutboxService._send_admin_alert(to_email, payload)

            else:
                return False, f"Unknown template: {template}"

        except Exception as e:
            return False, str(e)

    @staticmethod
    def _send_invoice_email(to_email: str, payload: Dict[str, Any]) -> tuple:
        """
        Send full invoice email with PDF attachments.
        Falls back to simple receipt if PDF generation fails.
        """
        purchase_id = payload.get("purchase_id")
        identity_id = payload.get("identity_id")
        plan_name = payload.get("plan_name")
        plan_code = payload.get("plan_code")
        credits = payload.get("credits")
        amount_gbp = payload.get("amount_gbp")

        # Try full invoice pipeline first
        try:
            from backend.services.invoicing_service import InvoicingService
            InvoicingService.process_purchase_invoice(
                purchase_id=purchase_id,
                identity_id=identity_id,
                plan_code=plan_code,
                plan_name=plan_name,
                credits=credits,
                amount_gbp=amount_gbp,
                customer_email=to_email,
            )
            return True, None
        except Exception as inv_err:
            print(f"[EMAIL_OUTBOX] Invoice pipeline failed, falling back to receipt: {inv_err}")
            # Fall back to simple receipt
            return EmailOutboxService._send_receipt_email(to_email, payload)

    @staticmethod
    def _send_receipt_email(to_email: str, payload: Dict[str, Any]) -> tuple:
        """Send simple HTML purchase receipt (no PDFs)."""
        from backend.emailer import send_purchase_receipt

        success = send_purchase_receipt(
            to_email=to_email,
            plan_name=payload.get("plan_name"),
            credits=payload.get("credits"),
            amount_gbp=payload.get("amount_gbp"),
        )

        if success:
            return True, None
        return False, "send_purchase_receipt returned False"

    @staticmethod
    def _send_payment_received_email(to_email: str, payload: Dict[str, Any]) -> tuple:
        """Send minimal payment received confirmation (HTML only)."""
        from backend.emailer import send_payment_received

        success = send_payment_received(
            to_email=to_email,
            plan_name=payload.get("plan_name"),
            credits=payload.get("credits"),
            amount_gbp=payload.get("amount_gbp"),
        )

        if success:
            return True, None
        return False, "send_payment_received returned False"

    @staticmethod
    def _send_admin_alert(to_email: str, payload: Dict[str, Any]) -> tuple:
        """Send admin notification email."""
        from backend.emailer import notify_admin

        success = notify_admin(
            subject=payload.get("subject", "Notification"),
            message=payload.get("message", ""),
            data=payload.get("data"),
        )

        if success:
            return True, None
        return False, "notify_admin returned False"

    @staticmethod
    def _alert_admin_email_failure(email_job: Dict[str, Any], error: str, attempts: int):
        """Send admin alert when an email permanently fails."""
        try:
            from backend.emailer import notify_admin
            from backend.config import config

            if not config.ADMIN_EMAIL:
                print("[EMAIL_OUTBOX] Cannot alert admin - ADMIN_EMAIL not configured")
                return

            # Don't send failure alerts about failure alerts (prevent infinite loop)
            if email_job.get("template") == EmailTemplate.ADMIN_ALERT:
                print("[EMAIL_OUTBOX] Skipping failure alert for admin email")
                return

            notify_admin(
                subject="Email Delivery Failed",
                message=f"An email failed to send after {attempts} attempts and has been marked as failed.",
                data={
                    "Outbox ID": str(email_job.get("id", ""))[:8] + "...",
                    "Template": email_job.get("template"),
                    "Recipient": email_job.get("to_email"),
                    "Purchase ID": str(email_job.get("purchase_id", ""))[:8] + "..." if email_job.get("purchase_id") else "N/A",
                    "Attempts": attempts,
                    "Last Error": error[:200] if error else "Unknown",
                },
            )
        except Exception as e:
            # Log but don't fail - we've already recorded the failure in the DB
            print(f"[EMAIL_OUTBOX] Failed to send admin failure alert: {e}")

    # ─────────────────────────────────────────────────────────────
    # Query Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_pending_count() -> int:
        """Get count of pending emails."""
        result = query_one(
            f"SELECT COUNT(*) as cnt FROM {Tables.EMAIL_OUTBOX} WHERE status = %s",
            (EmailOutboxStatus.PENDING,),
        )
        return result["cnt"] if result else 0

    @staticmethod
    def get_failed_count() -> int:
        """Get count of permanently failed emails."""
        result = query_one(
            f"SELECT COUNT(*) as cnt FROM {Tables.EMAIL_OUTBOX} WHERE status = %s",
            (EmailOutboxStatus.FAILED,),
        )
        return result["cnt"] if result else 0

    @staticmethod
    def get_outbox_stats() -> Dict[str, Any]:
        """Get email outbox statistics."""
        result = query_one(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending') as pending,
                COUNT(*) FILTER (WHERE status = 'sent') as sent,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                COUNT(*) as total
            FROM {Tables.EMAIL_OUTBOX}
            """,
        )
        return result if result else {"pending": 0, "sent": 0, "failed": 0, "total": 0}

    @staticmethod
    def get_purchase_email_status(purchase_id: str) -> Optional[Dict[str, Any]]:
        """Get email status for a specific purchase."""
        return query_one(
            f"""
            SELECT id, template, status, attempts, last_error, sent_at, failed_at
            FROM {Tables.EMAIL_OUTBOX}
            WHERE purchase_id = %s AND template != %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (purchase_id, EmailTemplate.ADMIN_ALERT),
        )
