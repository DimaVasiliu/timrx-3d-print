"""
Mollie Payment Service - Handles credit purchases via Mollie.

Flow:
1. create_checkout(identity_id, plan_code, email) -> checkout_url
2. User completes payment on Mollie
3. handle_webhook() processes payment.paid:
   - Creates purchases row
   - Adds ledger entry purchase_credit (+credits)
   - Updates wallet balance
   - Attaches email to identity (if not already)
   - Sends receipt email to user
   - Sends admin notification email

Idempotency:
- Purchases are keyed by provider_payment_id (Mollie payment ID)
- Repeated webhooks for same payment are safely ignored

Environment variables:
- MOLLIE_API_KEY: Your Mollie API key (test_xxx or live_xxx)
- MOLLIE_ENV: 'test' or 'live' (default: test)
- PUBLIC_BASE_URL: Backend API URL for webhooks (e.g., https://3d.timrx.live)
- FRONTEND_BASE_URL: Frontend site URL for redirects (e.g., https://timrx.live)
"""

import json
import requests
from typing import Optional, Dict, Any
from datetime import datetime

from backend.config import config
from backend.services.pricing_service import PricingService


class MollieCreateError(Exception):
    """Raised when Mollie payment creation fails."""
    def __init__(self, detail: str):
        self.detail = detail
        super().__init__(detail)


# Check if Mollie is configured
MOLLIE_AVAILABLE = config.MOLLIE_CONFIGURED

if MOLLIE_AVAILABLE:
    print(f"[MOLLIE] Mollie configured and ready (mode: {config.MOLLIE_MODE})")
else:
    print("[MOLLIE] Mollie not configured - Mollie payments disabled")


class MollieService:
    """Service for handling credit purchases via Mollie."""

    MOLLIE_API_BASE = "https://api.mollie.com/v2"

    @staticmethod
    def is_available() -> bool:
        """Check if Mollie functionality is available."""
        return MOLLIE_AVAILABLE

    @staticmethod
    def _get_headers() -> Dict[str, str]:
        """Get headers for Mollie API requests."""
        return {
            "Authorization": f"Bearer {config.MOLLIE_API_KEY}",
            "Content-Type": "application/json",
        }

    # ─────────────────────────────────────────────────────────────
    # Checkout Flow
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def create_checkout(
        identity_id: str,
        plan_code: str,
        email: str,
        success_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a Mollie payment for purchasing credits.

        Args:
            identity_id: The user's identity ID
            plan_code: The plan code to purchase (e.g., 'starter_80')
            email: User's email (stored in metadata)
            success_url: URL to redirect after payment (optional, defaults to hub.html?checkout=success)

        Returns:
            {
                "checkout_url": "https://www.mollie.com/checkout/...",
                "payment_id": "tr_..."
            }

        Raises:
            ValueError: If Mollie not configured, plan not found, or API error
        """
        if not MOLLIE_AVAILABLE:
            raise ValueError("Mollie is not configured")

        # Validate plan exists
        plan = PricingService.get_plan_by_code(plan_code)
        if not plan:
            raise ValueError(f"Plan '{plan_code}' not found or inactive")

        # Get plan details
        plan_id = plan["id"]
        plan_name = plan["name"]
        price_gbp = plan["price"]
        credits = plan["credits"]

        # Build redirect URL - Mollie redirects user HERE after payment
        # MUST use FRONTEND_BASE_URL (timrx.live), NOT backend URL (3d.timrx.live)
        frontend_url = config.FRONTEND_BASE_URL.rstrip("/") if config.FRONTEND_BASE_URL else ""
        if not frontend_url:
            # Fallback to PUBLIC_BASE_URL for backward compatibility (not recommended)
            frontend_url = config.PUBLIC_BASE_URL.rstrip("/") if config.PUBLIC_BASE_URL else ""
            print("[MOLLIE] WARNING: FRONTEND_BASE_URL not set, using PUBLIC_BASE_URL for redirects")

        if not success_url:
            # Redirect to hub.html with checkout=success query param for frontend detection
            success_url = f"{frontend_url}/hub.html?checkout=success"

        # Build webhook URL - MUST use backend API URL (3d.timrx.live)
        backend_url = config.PUBLIC_BASE_URL.rstrip("/") if config.PUBLIC_BASE_URL else ""
        webhook_url = f"{backend_url}/api/billing/webhook/mollie"

        # Build metadata (stored with payment, returned in webhook)
        metadata = {
            "identity_id": identity_id,
            "plan_code": plan_code,
            "plan_id": str(plan_id),
            "credits": str(credits),
            "email": email,
        }

        # Create Mollie payment
        # Note: profileId is NOT a valid field - profile is determined by API key
        payment_data = {
            "amount": {
                "currency": "GBP",
                "value": f"{price_gbp:.2f}",
            },
            "description": f"{plan_name} - {credits} Credits",
            "redirectUrl": success_url,
            "webhookUrl": webhook_url,
            "metadata": metadata,
            "locale": "en_GB",
        }

        try:
            response = requests.post(
                f"{MollieService.MOLLIE_API_BASE}/payments",
                headers=MollieService._get_headers(),
                json=payment_data,
                timeout=30,
            )

            if response.status_code not in (200, 201):
                error_data = response.json() if response.content else {}
                error_detail = error_data.get("detail", response.text)
                print(f"[MOLLIE] API error creating payment: {response.status_code} - {error_detail}")
                raise MollieCreateError(error_detail)

            payment = response.json()
            payment_id = payment["id"]
            checkout_url = payment["_links"]["checkout"]["href"]

            print(
                f"[MOLLIE] Payment created: payment_id={payment_id}, "
                f"identity={identity_id}, plan={plan_code}, credits={credits}"
            )

            return {
                "checkout_url": checkout_url,
                "payment_id": payment_id,
            }

        except requests.RequestException as e:
            print(f"[MOLLIE] Request error creating payment: {e}")
            raise ValueError(f"Payment service error: {str(e)}")

    # ─────────────────────────────────────────────────────────────
    # Webhook Processing
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def handle_webhook(payment_id: str) -> Dict[str, Any]:
        """
        Handle a Mollie webhook notification.

        Mollie webhooks only send the payment ID - we fetch full details.

        Args:
            payment_id: The Mollie payment ID (tr_xxx)

        Returns:
            {
                "ok": True/False,
                "status": "paid" / "failed" / etc,
                "message": "...",
                "purchase_id": "..." (if applicable)
            }
        """
        if not MOLLIE_AVAILABLE:
            return {"ok": False, "error": "Mollie not configured"}

        # Fetch payment details from Mollie
        try:
            response = requests.get(
                f"{MollieService.MOLLIE_API_BASE}/payments/{payment_id}",
                headers=MollieService._get_headers(),
                timeout=30,
            )

            if response.status_code != 200:
                print(f"[MOLLIE] Failed to fetch payment {payment_id}: {response.status_code}")
                return {"ok": False, "error": f"Failed to fetch payment: {response.status_code}"}

            payment = response.json()

        except requests.RequestException as e:
            print(f"[MOLLIE] Request error fetching payment: {e}")
            return {"ok": False, "error": f"Request error: {str(e)}"}

        status = payment.get("status")
        metadata = payment.get("metadata", {})
        identity_id = metadata.get("identity_id", "unknown")
        plan_code = metadata.get("plan_code", "unknown")
        credits = metadata.get("credits", "0")

        print(
            f"[MOLLIE] Webhook received: payment_id={payment_id}, status={status}, "
            f"identity_id={identity_id}, plan_code={plan_code}, credits={credits}"
        )

        # Handle different payment statuses
        if status == "paid":
            # Process the paid payment - grant credits (idempotent by payment_id)
            result = MollieService._handle_payment_paid(payment)

            if result:
                was_existing = result.get("was_existing", False)
                if was_existing:
                    print(
                        f"[MOLLIE] Duplicate webhook ignored (already processed): "
                        f"payment_id={payment_id}, identity_id={identity_id}"
                    )
                else:
                    print(
                        f"[MOLLIE] Credits granted: payment_id={payment_id}, "
                        f"identity_id={identity_id}, plan_code={plan_code}, credits={credits}"
                    )
                return {
                    "ok": True,
                    "status": "paid",
                    "message": "Purchase completed successfully" if not was_existing else "Already processed",
                    "purchase_id": result.get("purchase_id"),
                }
            else:
                print(
                    f"[MOLLIE] ERROR: Failed to grant credits: payment_id={payment_id}, "
                    f"identity_id={identity_id}, plan_code={plan_code}"
                )
                return {
                    "ok": False,
                    "status": "paid",
                    "error": "Failed to process paid payment",
                }

        # Refund statuses: refunded, charged_back
        # Revoke credits (idempotent by checking existing refund ledger entry)
        if status in ("refunded", "charged_back"):
            result = MollieService._handle_payment_refunded(payment)

            if result:
                was_existing = result.get("was_existing", False)
                if was_existing:
                    print(
                        f"[MOLLIE] Duplicate refund webhook ignored (already processed): "
                        f"payment_id={payment_id}, identity_id={identity_id}"
                    )
                else:
                    print(
                        f"[MOLLIE] Credits revoked (refund): payment_id={payment_id}, "
                        f"identity_id={identity_id}, credits={result.get('credits_revoked', 0)}"
                    )
                return {
                    "ok": True,
                    "status": status,
                    "message": "Refund processed successfully" if not was_existing else "Already processed",
                    "credits_revoked": result.get("credits_revoked", 0),
                }
            else:
                print(
                    f"[MOLLIE] ERROR: Failed to process refund: payment_id={payment_id}, "
                    f"identity_id={identity_id}"
                )
                return {
                    "ok": False,
                    "status": status,
                    "error": "Failed to process refund",
                }

        # Non-paid statuses: failed, canceled, expired, open, pending
        # No credits granted - just acknowledge the webhook
        if status in ("failed", "canceled", "expired"):
            print(
                f"[MOLLIE] Payment {status} - no credits granted: payment_id={payment_id}, "
                f"identity_id={identity_id}, plan_code={plan_code}"
            )

        return {
            "ok": True,
            "status": status,
            "message": f"Payment status is '{status}', no action taken",
        }

    @staticmethod
    def confirm_payment(payment_id: str, identity_id: str) -> Dict[str, Any]:
        """
        Confirm a payment and grant credits if paid.
        Called by frontend after redirect to ensure credits are granted
        (in case webhook is delayed).

        This is idempotent - won't double-grant due to unique constraint
        on provider_payment_id in purchases table.

        Args:
            payment_id: The Mollie payment ID (tr_xxx)
            identity_id: The identity ID to verify ownership

        Returns:
            {
                "ok": True/False,
                "status": "paid" / "open" / "failed" / etc,
                "credits_granted": True/False,
                "message": "..."
            }
        """
        if not MOLLIE_AVAILABLE:
            return {"ok": False, "error": "Mollie not configured"}

        # Fetch payment details from Mollie
        try:
            response = requests.get(
                f"{MollieService.MOLLIE_API_BASE}/payments/{payment_id}",
                headers=MollieService._get_headers(),
                timeout=30,
            )

            if response.status_code != 200:
                print(f"[MOLLIE] Confirm: Failed to fetch payment {payment_id}: {response.status_code}")
                return {
                    "ok": False,
                    "error": f"Failed to fetch payment: {response.status_code}",
                }

            payment = response.json()

        except requests.RequestException as e:
            print(f"[MOLLIE] Confirm: Request error fetching payment: {e}")
            return {"ok": False, "error": f"Request error: {str(e)}"}

        status = payment.get("status")
        metadata = payment.get("metadata", {})
        payment_identity_id = metadata.get("identity_id")
        plan_code = metadata.get("plan_code", "unknown")
        credits = metadata.get("credits", "0")

        # Verify ownership - the payment must belong to the requesting identity
        if payment_identity_id != identity_id:
            print(
                f"[MOLLIE] Confirm: Identity mismatch for payment {payment_id}: "
                f"expected {identity_id}, got {payment_identity_id}"
            )
            return {
                "ok": False,
                "error": "Payment does not belong to this identity",
            }

        print(
            f"[MOLLIE] Confirm: payment_id={payment_id}, status={status}, "
            f"identity_id={identity_id}, plan_code={plan_code}"
        )

        # Only process paid payments
        if status != "paid":
            return {
                "ok": True,
                "status": status,
                "credits_granted": False,
                "message": f"Payment status is '{status}'",
            }

        # Process the paid payment - grant credits (idempotent by payment_id)
        result = MollieService._handle_payment_paid(payment)

        if result:
            was_existing = result.get("was_existing", False)
            if was_existing:
                print(
                    f"[MOLLIE] Confirm: Already processed (idempotent): "
                    f"payment_id={payment_id}, identity_id={identity_id}"
                )
            else:
                print(
                    f"[MOLLIE] Confirm: Credits granted: payment_id={payment_id}, "
                    f"identity_id={identity_id}, plan_code={plan_code}, credits={credits}"
                )

            # Fetch current wallet balance to return in response
            # This allows frontend to update UI immediately without polling /api/me
            from backend.services.wallet_service import WalletService
            wallet = WalletService.get_or_create_wallet(identity_id)
            balance_credits = wallet.get("balance_credits", 0) if wallet else 0
            reserved_credits = wallet.get("reserved_credits", 0) if wallet else 0
            available_credits = max(0, balance_credits - reserved_credits)

            return {
                "ok": True,
                "status": "paid",
                "credits_granted": True,
                "was_existing": was_existing,
                "message": "Credits granted" if not was_existing else "Already processed",
                # Include wallet balance for frontend to use directly
                "balance_credits": balance_credits,
                "reserved_credits": reserved_credits,
                "available_credits": available_credits,
                "identity_id": identity_id,
            }
        else:
            print(
                f"[MOLLIE] Confirm: ERROR: Failed to grant credits: payment_id={payment_id}, "
                f"identity_id={identity_id}"
            )
            return {
                "ok": False,
                "status": "paid",
                "credits_granted": False,
                "error": "Failed to process payment",
            }

    @staticmethod
    def _handle_payment_paid(payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle a paid payment - create purchase and grant credits.

        Args:
            payment: Full Mollie payment object

        Returns:
            Dict with purchase info, or None on failure
        """
        # Import here to avoid circular import
        from backend.services.purchase_service import PurchaseService

        payment_id = payment.get("id")
        metadata = payment.get("metadata", {})

        # Subscription payments are handled separately
        if metadata.get("type") == "subscription":
            return MollieService._handle_subscription_paid(payment)

        # Extract metadata
        identity_id = metadata.get("identity_id")
        plan_code = metadata.get("plan_code")
        plan_id = metadata.get("plan_id")
        credits_str = metadata.get("credits")
        customer_email = metadata.get("email")

        if not identity_id or not plan_code or not credits_str:
            print(f"[MOLLIE] Missing metadata in payment {payment_id}: {metadata}")
            return None

        credits = int(credits_str)

        # Get amount from payment
        amount_data = payment.get("amount", {})
        amount_gbp = float(amount_data.get("value", 0))

        # Get plan name
        plan = PricingService.get_plan_by_code(plan_code)
        plan_name = plan["name"] if plan else plan_code

        # Idempotency check: see if purchase already exists for this payment
        existing = PurchaseService.get_purchase_by_provider_id(payment_id)
        if existing:
            print(f"[MOLLIE] Already processed payment {payment_id}, purchase_id={existing['id']}")
            return {
                "purchase_id": existing["id"],
                "was_existing": True,
            }

        # Record the purchase (this handles credits, wallet, email attachment)
        try:
            result = MollieService._record_mollie_purchase(
                identity_id=identity_id,
                plan_id=plan_id,
                plan_code=plan_code,
                provider_payment_id=payment_id,
                amount_gbp=amount_gbp,
                credits_granted=credits,
                customer_email=customer_email,
            )

            if result:
                purchase_id = result["purchase"]["id"]
                was_existing = result.get("was_existing", False)

                # Only send emails / invoices for NEW purchases (not duplicates)
                # Emails are non-blocking - failures are logged as warnings only
                if not was_existing and customer_email:
                    # Invoice pipeline: create invoice + receipt, generate PDFs,
                    # upload to S3, send email with attachments
                    try:
                        from backend.services.invoicing_service import InvoicingService
                        InvoicingService.process_purchase_invoice(
                            purchase_id=purchase_id,
                            identity_id=identity_id,
                            plan_code=plan_code,
                            plan_name=plan_name,
                            credits=credits,
                            amount_gbp=amount_gbp,
                            customer_email=customer_email,
                        )
                    except Exception as inv_err:
                        print(f"[MOLLIE] WARNING: Invoice pipeline failed for payment {payment_id}: {inv_err}")
                        # Fallback to simple receipt email (no PDF attachments)
                        try:
                            from backend.emailer import send_purchase_receipt
                            send_purchase_receipt(
                                to_email=customer_email,
                                plan_name=plan_name,
                                credits=credits,
                                amount_gbp=amount_gbp,
                            )
                        except Exception as email_err:
                            print(f"[MOLLIE] WARNING: Fallback email also failed: {email_err} (credits already granted)")

                    # Admin notification (always, independent of invoice)
                    try:
                        from backend.emailer import notify_purchase
                        notify_purchase(
                            identity_id=identity_id,
                            email=customer_email,
                            plan_name=plan_name,
                            credits=credits,
                            amount_gbp=amount_gbp,
                        )
                    except Exception as admin_err:
                        print(f"[MOLLIE] WARNING: Admin notification failed: {admin_err}")

                return {
                    "purchase_id": purchase_id,
                    "was_existing": was_existing,
                }

        except Exception as e:
            print(f"[MOLLIE] Error processing paid payment: {e}")
            return None

        return None

    @staticmethod
    def _handle_payment_refunded(payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle a refunded/charged_back payment - revoke credits.

        Idempotent: uses INSERT ... ON CONFLICT DO NOTHING with partial unique index.
        Safe: wallet balance never goes below 0 (uses GREATEST in SQL).

        Args:
            payment: Full Mollie payment object

        Returns:
            Dict with refund info, or None on failure
        """
        from backend.db import fetch_one, transaction, Tables
        from backend.services.wallet_service import LedgerEntryType
        from backend.services.purchase_service import PurchaseService

        payment_id = payment.get("id")
        status = payment.get("status")  # 'refunded' or 'charged_back'
        metadata = payment.get("metadata", {})
        identity_id = metadata.get("identity_id")

        if not payment_id:
            print("[MOLLIE] Refund skipped - no payment_id in payment object")
            return None

        if not identity_id:
            print(f"[MOLLIE] Refund skipped - no identity_id in metadata: payment_id={payment_id}")
            return None

        # Determine entry_type based on status
        if status == "charged_back":
            entry_type = LedgerEntryType.CHARGEBACK
            purchase_status = "charged_back"
        else:
            entry_type = LedgerEntryType.REFUND
            purchase_status = "refunded"

        try:
            # Find the original purchase by (provider='mollie', provider_payment_id=payment_id)
            purchase = PurchaseService.get_purchase_by_provider_id(payment_id)
            if not purchase:
                print(f"[MOLLIE] Refund skipped - no purchase found for payment_id={payment_id}")
                return {"was_existing": False, "credits_revoked": 0, "reason": "no_purchase"}

            purchase_id = str(purchase["id"])
            credits_to_revoke = purchase.get("credits_granted", 0)

            if credits_to_revoke <= 0:
                print(f"[MOLLIE] Refund skipped - no credits to revoke: payment_id={payment_id}")
                return {"was_existing": False, "credits_revoked": 0, "reason": "no_credits"}

            with transaction() as cur:
                # Lock wallet row first to prevent concurrent balance updates
                cur.execute(
                    f"""
                    SELECT balance_credits
                    FROM {Tables.WALLETS}
                    WHERE identity_id = %s
                    FOR UPDATE
                    """,
                    (identity_id,),
                )
                wallet = fetch_one(cur)
                current_balance = wallet.get("balance_credits", 0) if wallet else 0

                # Insert refund/chargeback ledger entry (idempotent via ON CONFLICT DO NOTHING)
                # Relies on partial unique index: uq_ledger_refund_per_purchase
                cur.execute(
                    f"""
                    INSERT INTO {Tables.LEDGER_ENTRIES}
                    (identity_id, entry_type, amount_credits, ref_type, ref_id, meta, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (identity_id, ref_type, ref_id)
                        WHERE entry_type IN ('refund', 'chargeback') AND ref_type = 'purchase'
                    DO NOTHING
                    RETURNING id
                    """,
                    (
                        identity_id,
                        entry_type,
                        -credits_to_revoke,  # Full negative amount (actual deduction handled by GREATEST)
                        "purchase",
                        purchase_id,
                        json.dumps({
                            "payment_id": payment_id,
                            "status": status,
                            "credits_granted": credits_to_revoke,
                            "balance_before": current_balance,
                            "provider": "mollie",
                        }),
                    ),
                )
                ledger_result = fetch_one(cur)

                # If no row returned, the entry already exists (ON CONFLICT fired)
                if not ledger_result:
                    print(f"[MOLLIE] {entry_type} already applied: payment_id={payment_id}, purchase_id={purchase_id}")
                    return {
                        "was_existing": True,
                        "credits_revoked": credits_to_revoke,
                    }

                ledger_entry_id = str(ledger_result["id"])

                # Update wallet balance safely: never go below 0
                cur.execute(
                    f"""
                    UPDATE {Tables.WALLETS}
                    SET balance_credits = GREATEST(balance_credits - %s, 0),
                        updated_at = NOW()
                    WHERE identity_id = %s
                    RETURNING balance_credits
                    """,
                    (credits_to_revoke, identity_id),
                )
                wallet_result = fetch_one(cur)
                new_balance = wallet_result["balance_credits"] if wallet_result else 0

                # Update purchase status
                cur.execute(
                    f"""
                    UPDATE {Tables.PURCHASES}
                    SET status = %s
                    WHERE id = %s
                    """,
                    (purchase_status, purchase_id),
                )

                actual_deduction = current_balance - new_balance
                print(
                    f"[MOLLIE] {entry_type} applied: payment_id={payment_id}, purchase_id={purchase_id}, "
                    f"credits_revoked={actual_deduction}, balance: {current_balance} -> {new_balance}"
                )

                return {
                    "was_existing": False,
                    "credits_revoked": actual_deduction,
                    "ledger_entry_id": ledger_entry_id,
                    "new_balance": new_balance,
                }

        except Exception as e:
            print(f"[MOLLIE] Error processing {entry_type}: {e}")
            return None

    @staticmethod
    def _record_mollie_purchase(
        identity_id: str,
        plan_id: str,
        plan_code: str,
        provider_payment_id: str,
        amount_gbp: float,
        credits_granted: int,
        customer_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Record a completed Mollie purchase and grant credits.

        This is similar to PurchaseService.record_purchase but uses 'mollie' as provider.
        """
        from backend.db import fetch_one, transaction, Tables
        from backend.services.wallet_service import LedgerEntryType
        from backend.services.purchase_service import PurchaseStatus, PurchaseService

        with transaction() as cur:
            # 1. Create purchase record (idempotent via ON CONFLICT DO NOTHING)
            # If webhook and confirm endpoint race, only one will succeed
            cur.execute(
                f"""
                INSERT INTO {Tables.PURCHASES}
                (identity_id, plan_id, provider, provider_payment_id,
                 amount_gbp, currency, credits_granted, status, paid_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (provider, provider_payment_id) DO NOTHING
                RETURNING *
                """,
                (
                    identity_id,
                    plan_id,
                    "mollie",
                    provider_payment_id,
                    amount_gbp,
                    "GBP",
                    credits_granted,
                    PurchaseStatus.COMPLETED,
                ),
            )
            purchase = fetch_one(cur)

            # If no row returned, this was a duplicate (ON CONFLICT fired)
            # Query for the existing purchase and return it as "was_existing"
            if not purchase:
                print(f"[PURCHASE] Duplicate payment ignored: provider=mollie payment_id={provider_payment_id}")
                # Fetch the existing purchase
                cur.execute(
                    f"""
                    SELECT * FROM {Tables.PURCHASES}
                    WHERE provider = 'mollie' AND provider_payment_id = %s
                    """,
                    (provider_payment_id,),
                )
                existing = fetch_one(cur)
                if existing:
                    return {
                        "purchase": PurchaseService._format_purchase(existing),
                        "was_existing": True,
                    }
                return None

            purchase_id = str(purchase["id"])

            # 2. Lock wallet for update
            cur.execute(
                f"""
                SELECT identity_id, balance_credits
                FROM {Tables.WALLETS}
                WHERE identity_id = %s
                FOR UPDATE
                """,
                (identity_id,),
            )
            wallet = fetch_one(cur)

            if not wallet:
                # Create wallet if doesn't exist
                cur.execute(
                    f"""
                    INSERT INTO {Tables.WALLETS} (identity_id, balance_credits, updated_at)
                    VALUES (%s, 0, NOW())
                    ON CONFLICT (identity_id) DO NOTHING
                    RETURNING *
                    """,
                    (identity_id,),
                )
                wallet = fetch_one(cur)
                if not wallet:
                    # Conflict means it was created, fetch it
                    cur.execute(
                        f"SELECT * FROM {Tables.WALLETS} WHERE identity_id = %s FOR UPDATE",
                        (identity_id,),
                    )
                    wallet = fetch_one(cur)

            current_balance = wallet.get("balance_credits", 0) or 0
            new_balance = current_balance + credits_granted

            # 3. Insert ledger entry
            cur.execute(
                f"""
                INSERT INTO {Tables.LEDGER_ENTRIES}
                (identity_id, entry_type, amount_credits, ref_type, ref_id, meta, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                RETURNING *
                """,
                (
                    identity_id,
                    LedgerEntryType.PURCHASE_CREDIT,
                    credits_granted,  # Positive amount
                    "purchase",
                    purchase_id,
                    json.dumps({"plan_code": plan_code, "amount_gbp": amount_gbp, "provider": "mollie"}),
                ),
            )
            ledger_entry = fetch_one(cur)

            # 4. Update wallet balance
            cur.execute(
                f"""
                UPDATE {Tables.WALLETS}
                SET balance_credits = %s, updated_at = NOW()
                WHERE identity_id = %s
                """,
                (new_balance, identity_id),
            )

            # NOTE: Email is NOT auto-attached during purchase.
            # The email is stored in purchase metadata for the receipt only.
            # User must explicitly use "Secure Your Credits" to attach & verify email.
            # This prevents the UI from showing "verify code" state when no code was sent.

            print(
                f"[MOLLIE] Purchase recorded: purchase_id={purchase_id}, identity={identity_id}, "
                f"credits={credits_granted}, balance: {current_balance} -> {new_balance}"
            )

            return {
                "purchase": PurchaseService._format_purchase(purchase),
                "ledger_entry_id": str(ledger_entry["id"]),
                "balance": new_balance,
            }

    # ─────────────────────────────────────────────────────────────
    # Payment Status Check
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_payment_status(payment_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a Mollie payment.

        Args:
            payment_id: The Mollie payment ID (tr_xxx)

        Returns:
            Dict with payment status info, or None on error
        """
        if not MOLLIE_AVAILABLE:
            return None

        try:
            response = requests.get(
                f"{MollieService.MOLLIE_API_BASE}/payments/{payment_id}",
                headers=MollieService._get_headers(),
                timeout=30,
            )

            if response.status_code != 200:
                return None

            payment = response.json()
            return {
                "id": payment.get("id"),
                "status": payment.get("status"),
                "amount": payment.get("amount"),
                "description": payment.get("description"),
                "created_at": payment.get("createdAt"),
                "paid_at": payment.get("paidAt"),
            }

        except requests.RequestException:
            return None

    # ─────────────────────────────────────────────────────────────
    # Subscription Payment Handling
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _handle_subscription_paid(payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle a paid subscription payment — create subscription row,
        grant first period credits.

        Idempotent via the unique index on (provider, provider_subscription_id).
        """
        from backend.services.subscription_service import SubscriptionService
        from backend.services.purchase_service import PurchaseService
        from datetime import datetime, timezone, timedelta

        payment_id = payment.get("id")
        metadata = payment.get("metadata", {})
        identity_id = metadata.get("identity_id")
        plan_code = metadata.get("plan_code")
        cadence = metadata.get("cadence", "monthly")
        customer_email = metadata.get("email")

        if not identity_id or not plan_code:
            print(f"[MOLLIE] Subscription payment {payment_id}: missing identity_id or plan_code in metadata")
            return None

        # Idempotency: check if subscription already created for this payment
        existing_sub = SubscriptionService.get_subscription_by_provider_id("mollie", payment_id)
        if existing_sub:
            print(f"[MOLLIE] Subscription already exists for payment {payment_id}")
            return {"purchase_id": str(existing_sub["id"]), "was_existing": True}

        # Also check for existing purchase (credit bundle idempotency)
        existing_purchase = PurchaseService.get_purchase_by_provider_id(payment_id)
        if existing_purchase:
            print(f"[MOLLIE] Purchase already exists for payment {payment_id}")
            return {"purchase_id": existing_purchase["id"], "was_existing": True}

        now = datetime.now(timezone.utc)
        if cadence == "yearly":
            period_end = now + timedelta(days=365)
        else:
            period_end = now + timedelta(days=30)

        # Create subscription
        sub = SubscriptionService.create_subscription(
            identity_id=identity_id,
            plan_code=plan_code,
            provider="mollie",
            provider_subscription_id=payment_id,
            period_start=now,
            period_end=period_end,
            customer_email=customer_email,
        )

        if not sub:
            print(f"[MOLLIE] Failed to create subscription for payment {payment_id}")
            return None

        sub_id = str(sub["id"])

        # Grant first period credits immediately
        cycle_id = SubscriptionService.grant_subscription_credits(
            sub_id, now, period_end if cadence == "monthly" else now + timedelta(days=30),
        )

        if cycle_id:
            print(f"[MOLLIE] Subscription {sub_id} activated + credits granted for payment {payment_id}")
        else:
            print(f"[MOLLIE] Subscription {sub_id} activated but credit grant skipped (already granted or error)")

        # Attach email to identity if not already set
        if customer_email:
            try:
                from backend.services.identity_service import IdentityService
                IdentityService.attach_email_if_missing(identity_id, customer_email)
            except Exception as e:
                print(f"[MOLLIE] Warning: could not attach email: {e}")

        # Send subscription confirmation email + admin notification
        if customer_email:
            try:
                from backend.emailer import send_subscription_confirmation, notify_admin
                from backend.services.subscription_service import SUBSCRIPTION_PLANS
                plan_info = SUBSCRIPTION_PLANS.get(plan_code, {})
                plan_name = plan_info.get("name", plan_code)
                credits_per_month = plan_info.get("credits_per_month", 0)
                price_gbp = plan_info.get("price_gbp", 0)

                send_subscription_confirmation(
                    to_email=customer_email,
                    plan_name=plan_name,
                    plan_code=plan_code,
                    credits_per_month=credits_per_month,
                    price_gbp=price_gbp,
                    cadence=cadence,
                )
                print(f"[MOLLIE] Subscription confirmation email sent to {customer_email}")

                # Admin notification
                notify_admin(
                    subject="New Subscription",
                    message=f"A user has subscribed to the {plan_name} plan ({cadence}).",
                    data={
                        "Identity ID": identity_id,
                        "Email": customer_email,
                        "Plan": plan_name,
                        "Cadence": cadence,
                        "Credits/month": f"{credits_per_month:,}",
                        "Price": f"£{price_gbp:.2f}/{cadence}",
                    },
                )
            except Exception as email_err:
                print(f"[MOLLIE] WARNING: Subscription email failed: {email_err}")

        return {"purchase_id": sub_id, "was_existing": False}

    # ─────────────────────────────────────────────────────────────
    # Subscription Checkout
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def create_subscription_checkout(
        identity_id: str,
        plan_code: str,
        email: str,
        success_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a Mollie one-off payment for a subscription plan.

        We use a single payment (not Mollie Subscriptions API) so the
        webhook can activate + grant credits.  Recurring billing can be
        added later by converting to Mollie mandates/subscriptions.

        Returns: { checkout_url, payment_id }
        """
        if not MOLLIE_AVAILABLE:
            raise ValueError("Mollie is not configured")

        from backend.services.subscription_service import SubscriptionService, SUBSCRIPTION_PLANS

        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            raise ValueError(f"Unknown subscription plan: {plan_code}")

        price_gbp = plan["price_gbp"]
        plan_name = plan["name"]
        cadence = plan["cadence"]
        credits = plan["credits_per_month"]

        description = f"{plan_name} Subscription ({cadence.title()}) - {credits} credits/mo"

        frontend_url = config.FRONTEND_BASE_URL.rstrip("/") if config.FRONTEND_BASE_URL else ""
        if not frontend_url:
            frontend_url = config.PUBLIC_BASE_URL.rstrip("/") if config.PUBLIC_BASE_URL else ""

        if not success_url:
            success_url = f"{frontend_url}/hub.html?checkout=success&type=subscription&plan={plan_code}"

        backend_url = config.PUBLIC_BASE_URL.rstrip("/") if config.PUBLIC_BASE_URL else ""
        webhook_url = f"{backend_url}/api/billing/webhook/mollie"

        metadata = {
            "identity_id": identity_id,
            "type": "subscription",
            "plan_code": plan_code,
            "cadence": cadence,
            "credits_per_month": str(credits),
            "email": email,
        }

        payment_data = {
            "amount": {
                "currency": "GBP",
                "value": f"{price_gbp:.2f}",
            },
            "description": description,
            "redirectUrl": success_url,
            "webhookUrl": webhook_url,
            "metadata": metadata,
            "locale": "en_GB",
        }

        try:
            response = requests.post(
                f"{MollieService.MOLLIE_API_BASE}/payments",
                headers=MollieService._get_headers(),
                json=payment_data,
                timeout=30,
            )

            if response.status_code not in (200, 201):
                error_data = response.json() if response.content else {}
                error_detail = error_data.get("detail", response.text)
                print(f"[MOLLIE] API error creating subscription payment: {response.status_code} - {error_detail}")
                raise MollieCreateError(error_detail)

            payment = response.json()
            payment_id = payment["id"]
            checkout_url = payment["_links"]["checkout"]["href"]

            print(
                f"[MOLLIE] Subscription payment created: payment_id={payment_id}, "
                f"identity={identity_id}, plan={plan_code}"
            )

            return {
                "checkout_url": checkout_url,
                "payment_id": payment_id,
            }

        except requests.RequestException as e:
            print(f"[MOLLIE] Request error creating subscription payment: {e}")
            raise ValueError(f"Payment service error: {str(e)}")
