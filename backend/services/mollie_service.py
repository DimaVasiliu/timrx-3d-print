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
from datetime import datetime, timedelta, timezone

from backend.config import config
from backend.db import get_conn, Tables
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
    # Payment Methods for Recurring
    # ─────────────────────────────────────────────────────────────

    # Methods that support Mollie recurring (sequenceType: first/recurring)
    # These can establish mandates for automatic future charges
    RECURRING_METHODS = {"creditcard", "directdebit", "paypal"}

    # Methods that DO NOT support recurring - must be filtered out for subscriptions
    # These include bank transfers, iDEAL, Bancontact, etc.
    NON_RECURRING_METHODS = {
        "ideal", "bancontact", "banktransfer", "sofort", "eps",
        "giropay", "kbc", "belfius", "przelewy24", "applepay",
    }

    @staticmethod
    def get_recurring_payment_methods(
        amount_gbp: float,
        include_inactive: bool = False,
    ) -> Dict[str, Any]:
        """
        Get payment methods that support recurring billing (subscriptions).

        Only returns methods that can establish a mandate for automatic future charges:
        - Credit/Debit Card (creditcard)
        - SEPA Direct Debit (directdebit)
        - PayPal (paypal)

        Methods like iDEAL, Bancontact, Bank Transfer do NOT support recurring
        and are filtered out.

        Args:
            amount_gbp: Payment amount (some methods have minimums)
            include_inactive: Include methods not yet activated (for testing)

        Returns:
            {
                "methods": [
                    {"id": "creditcard", "description": "Credit card", ...},
                    {"id": "directdebit", "description": "SEPA Direct Debit", ...},
                    {"id": "paypal", "description": "PayPal", ...},
                ],
                "count": 3
            }
        """
        if not MOLLIE_AVAILABLE:
            return {"methods": [], "count": 0, "error": "Mollie not configured"}

        try:
            # Query Mollie for methods supporting first payment sequence
            # This filters to only methods that can establish a mandate
            params = {
                "sequenceType": "first",
                "amount[currency]": "GBP",
                "amount[value]": f"{amount_gbp:.2f}",
            }
            if include_inactive:
                params["includeWallets"] = "applepay"

            response = requests.get(
                f"{MollieService.MOLLIE_API_BASE}/methods",
                headers=MollieService._get_headers(),
                params=params,
                timeout=15,
            )

            if response.status_code != 200:
                print(f"[MOLLIE] Error fetching recurring methods: {response.status_code}")
                # Fallback to hardcoded list
                return MollieService._get_fallback_recurring_methods()

            data = response.json()
            methods = data.get("_embedded", {}).get("methods", [])

            # Double-filter to only include known recurring methods
            # (Mollie API should handle this, but we validate anyway)
            recurring_methods = [
                m for m in methods
                if m.get("id") in MollieService.RECURRING_METHODS
            ]

            print(
                f"[MOLLIE] Recurring methods for £{amount_gbp:.2f}: "
                f"{[m['id'] for m in recurring_methods]}"
            )

            return {
                "methods": recurring_methods,
                "count": len(recurring_methods),
            }

        except requests.RequestException as e:
            print(f"[MOLLIE] Error fetching recurring methods: {e}")
            return MollieService._get_fallback_recurring_methods()

    @staticmethod
    def _get_fallback_recurring_methods() -> Dict[str, Any]:
        """Fallback recurring methods if API fails."""
        return {
            "methods": [
                {
                    "id": "creditcard",
                    "description": "Credit card",
                    "image": {"size1x": "", "size2x": "", "svg": ""},
                },
                {
                    "id": "directdebit",
                    "description": "SEPA Direct Debit",
                    "image": {"size1x": "", "size2x": "", "svg": ""},
                },
                {
                    "id": "paypal",
                    "description": "PayPal",
                    "image": {"size1x": "", "size2x": "", "svg": ""},
                },
            ],
            "count": 3,
            "fallback": True,
        }

    @staticmethod
    def is_recurring_method(method_id: str) -> bool:
        """Check if a payment method supports recurring billing."""
        return method_id in MollieService.RECURRING_METHODS

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
        payment_type = metadata.get("type", "one_time")
        subscription_id = payment.get("subscriptionId")  # Set for recurring payments

        # Determine sequence type for logging
        seq_type = "recurring" if subscription_id else ("first" if payment_type == "subscription_first" else "one_time")

        print(
            f"[SUB] payment_id={payment_id} status={status} seq={seq_type} "
            f"type={payment_type} identity={identity_id} plan={plan_code}"
        )

        # ─────────────────────────────────────────────────────────────
        # PAYMENT STATUS HANDLING
        # ─────────────────────────────────────────────────────────────
        # SEPA/Bank payments may be "pending" or "open" before becoming "paid"
        # We ONLY grant credits when status == "paid"

        if status in ("pending", "open"):
            # Payment is processing (common for SEPA, bank transfers)
            # Do NOT grant credits yet - wait for "paid" status
            print(
                f"[SUB] payment_id={payment_id} status={status} seq={seq_type} "
                f"granted=false reason=awaiting_payment"
            )
            return {
                "ok": True,
                "status": status,
                "message": f"Payment is {status} - waiting for confirmation",
                "granted": False,
            }

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
        # For subscriptions: suspend subscription and stop future grants
        # For one-time purchases: revoke credits
        if status in ("refunded", "charged_back"):
            # Check if this is a subscription payment
            is_subscription = subscription_id or payment_type in ("subscription_first", "subscription_recurring")

            if is_subscription:
                # Handle subscription refund/chargeback - suspend subscription
                result = MollieService._handle_subscription_refund(payment)
                if result:
                    was_existing = result.get("was_existing", False)
                    print(
                        f"[SUB] Subscription {status}: payment_id={payment_id}, "
                        f"sub_id={result.get('subscription_id')}, suspended={result.get('suspended')}"
                    )
                    return {
                        "ok": True,
                        "status": status,
                        "message": f"Subscription suspended due to {status}" if not was_existing else "Already processed",
                        "subscription_suspended": result.get("suspended", False),
                    }
                else:
                    print(f"[SUB] ERROR: Failed to process subscription {status}: payment_id={payment_id}")
                    return {"ok": False, "status": status, "error": f"Failed to process subscription {status}"}

            # One-time purchase refund
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
        payment_type = metadata.get("type")
        if payment_type == "subscription":
            return MollieService._handle_subscription_paid(payment)

        # TRUE RECURRING: First payment establishes mandate, then create Mollie subscription
        if payment_type == "subscription_first":
            return MollieService._handle_subscription_first_paid(payment)

        # RECURRING PAYMENT: Automatic charge from Mollie subscription
        # Mollie sets subscriptionId field for payments created by subscriptions
        subscription_id = payment.get("subscriptionId")
        if subscription_id:
            return MollieService._handle_subscription_recurring_paid(payment)

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
    def _handle_subscription_refund(payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle a refund/chargeback for a SUBSCRIPTION payment.

        This suspends the subscription and logs an admin entry.
        The cron/webhook checks suspended_at before granting credits.

        Args:
            payment: Full Mollie payment object

        Returns:
            Dict with suspension info, or None on failure
        """
        from backend.services.subscription_service import SubscriptionService

        payment_id = payment.get("id")
        status = payment.get("status")  # 'refunded' or 'charged_back'
        metadata = payment.get("metadata", {})
        identity_id = metadata.get("identity_id")
        plan_code = metadata.get("plan_code")
        subscription_id_mollie = payment.get("subscriptionId")

        if not identity_id:
            print(f"[SUB] Subscription refund skipped - no identity_id: payment_id={payment_id}")
            return None

        suspend_reason = "charged_back" if status == "charged_back" else "refunded"

        try:
            # Find the subscription by Mollie subscription ID or identity
            sub = None
            if subscription_id_mollie:
                sub = SubscriptionService.get_subscription_by_provider_id("mollie", subscription_id_mollie)

            if not sub:
                # Try to find by identity and plan
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            SELECT * FROM {Tables.SUBSCRIPTIONS}
                            WHERE identity_id = %s
                              AND status IN ('active', 'cancelled')
                            ORDER BY created_at DESC
                            LIMIT 1
                            """,
                            (identity_id,),
                        )
                        sub = cur.fetchone()

            if not sub:
                print(f"[SUB] Subscription refund skipped - no subscription found: identity={identity_id}")
                return {"suspended": False, "reason": "no_subscription"}

            sub_id = str(sub["id"])

            # Check if already suspended
            if sub.get("suspended_at"):
                print(f"[SUB] Subscription already suspended: sub_id={sub_id}")
                return {"suspended": True, "was_existing": True, "subscription_id": sub_id}

            # Suspend the subscription
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'suspended',
                            suspended_at = NOW(),
                            suspend_reason = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        RETURNING id, customer_email
                        """,
                        (suspend_reason, sub["id"]),
                    )
                    updated_sub = cur.fetchone()

                    # Log admin entry
                    cur.execute(
                        """
                        INSERT INTO timrx_billing.admin_logs
                            (event_type, subscription_id, identity_id, payment_id, details)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            f"subscription_{suspend_reason}",
                            sub["id"],
                            identity_id,
                            payment_id,
                            json.dumps({
                                "plan_code": plan_code,
                                "mollie_subscription_id": subscription_id_mollie,
                                "status": status,
                                "action": "subscription_suspended",
                            }),
                        ),
                    )
                conn.commit()

            # Log event
            SubscriptionService._log_event(sub_id, "suspended", {
                "reason": suspend_reason,
                "payment_id": payment_id,
            })

            print(
                f"[SUB] Subscription SUSPENDED due to {suspend_reason}: "
                f"sub_id={sub_id} payment_id={payment_id} identity={identity_id}"
            )

            # TODO: Send email notification to user about suspension

            return {
                "suspended": True,
                "was_existing": False,
                "subscription_id": sub_id,
                "reason": suspend_reason,
            }

        except Exception as e:
            print(f"[SUB] Error suspending subscription: {e}")
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
        from backend.services.wallet_service import LedgerEntryType, CreditType, get_credit_type_for_plan
        from backend.services.purchase_service import PurchaseStatus, PurchaseService

        # Determine credit type based on plan code
        credit_type = get_credit_type_for_plan(plan_code) if plan_code else CreditType.GENERAL
        balance_column = "balance_video_credits" if credit_type == CreditType.VIDEO else "balance_credits"

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
                SELECT identity_id, balance_credits, balance_video_credits
                FROM {Tables.WALLETS}
                WHERE identity_id = %s
                FOR UPDATE
                """,
                (identity_id,),
            )
            wallet = fetch_one(cur)

            if not wallet:
                # Create wallet if doesn't exist (with both credit types)
                cur.execute(
                    f"""
                    INSERT INTO {Tables.WALLETS} (identity_id, balance_credits, balance_video_credits, updated_at)
                    VALUES (%s, 0, 0, NOW())
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

            current_balance = wallet.get(balance_column, 0) or 0
            new_balance = current_balance + credits_granted

            # 3. Insert ledger entry with credit_type
            cur.execute(
                f"""
                INSERT INTO {Tables.LEDGER_ENTRIES}
                (identity_id, entry_type, amount_credits, ref_type, ref_id, meta, credit_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING *
                """,
                (
                    identity_id,
                    LedgerEntryType.PURCHASE_CREDIT,
                    credits_granted,  # Positive amount
                    "purchase",
                    purchase_id,
                    json.dumps({"plan_code": plan_code, "amount_gbp": amount_gbp, "provider": "mollie", "credit_type": credit_type}),
                    credit_type,
                ),
            )
            ledger_entry = fetch_one(cur)

            # 4. Update wallet balance for the correct credit type
            cur.execute(
                f"""
                UPDATE {Tables.WALLETS}
                SET {balance_column} = %s, updated_at = NOW()
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

    @staticmethod
    def _handle_subscription_first_paid(payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle first payment for TRUE RECURRING subscription.

        This is triggered when a payment with type=subscription_first is paid.
        It:
        1. Gets the mandate established by this payment
        2. Creates a Mollie Subscription for automatic future charges
        3. Updates pending subscription to active (or creates if not exists)
        4. Grants first month credits

        The Mollie Subscription will automatically charge monthly/yearly and
        send webhooks for each payment, which triggers credit grants.
        """
        from backend.services.subscription_service import SubscriptionService, SUBSCRIPTION_PLANS
        from backend.db import get_conn, Tables
        from datetime import datetime, timezone, timedelta

        payment_id = payment.get("id")
        metadata = payment.get("metadata", {})
        identity_id = metadata.get("identity_id")
        plan_code = metadata.get("plan_code")
        cadence = metadata.get("cadence", "monthly")
        customer_email = metadata.get("email")

        if not identity_id or not plan_code:
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=first "
                f"granted=false reason=missing_metadata"
            )
            return None

        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=first "
                f"granted=false reason=unknown_plan plan={plan_code}"
            )
            return None

        # Get mandateId from payment (Mollie sets this after first payment)
        mandate_id = payment.get("mandateId")
        customer_id = payment.get("customerId")  # Should be cst_xxx

        if not mandate_id or not customer_id:
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=first "
                f"granted=false reason=missing_mandate_or_customer "
                f"mandate={mandate_id} customer={customer_id}"
            )
            # Fall back to legacy one-time subscription handling
            return MollieService._handle_subscription_paid(payment)

        # Idempotency: check if we already processed this first payment (active subscription exists)
        existing_sub = SubscriptionService.get_subscription_by_provider_id("mollie", payment_id)
        if existing_sub and existing_sub.get("status") == "active":
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=first "
                f"sub_id={existing_sub['id']} granted=false reason=already_processed"
            )
            return {"purchase_id": str(existing_sub["id"]), "was_existing": True}

        # Check if there's a pending subscription waiting for this payment
        pending_sub = None
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT * FROM {Tables.SUBSCRIPTIONS}
                        WHERE identity_id = %s
                          AND mollie_first_payment_id = %s
                          AND status = 'pending_payment'
                        LIMIT 1
                        """,
                        (identity_id, payment_id),
                    )
                    pending_sub = cur.fetchone()
        except Exception as e:
            print(f"[SUB] Error checking for pending subscription: {e}")

        try:
            # Create Mollie Subscription for automatic recurring charges
            mollie_sub = MollieService.create_mollie_subscription(
                mollie_customer_id=customer_id,
                plan_code=plan_code,
                mandate_id=mandate_id,
                identity_id=identity_id,
            )
            mollie_subscription_id = mollie_sub["mollie_subscription_id"]
            next_payment_date = mollie_sub.get("next_payment_date")

            print(
                f"[SUB] payment_id={payment_id} status=paid seq=first "
                f"mollie_sub_id={mollie_subscription_id} identity={identity_id} "
                f"next_payment={next_payment_date} action=created_mollie_subscription"
            )

        except Exception as e:
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=first "
                f"granted=false reason=mollie_sub_create_failed error={e}"
            )
            # Fall back to legacy handling - at least grant first credits
            return MollieService._handle_subscription_paid(payment)

        # Create or update subscription record
        now = datetime.now(timezone.utc)
        billing_day = now.day

        # Calculate period end and prepaid tracking for yearly plans
        is_yearly = cadence == "yearly"
        if is_yearly:
            period_end = now + timedelta(days=365)
            credits_remaining_months = 12
            prepaid_until = period_end  # Yearly: prepaid for full year
        else:
            # For monthly, period_end is next billing date
            period_end = SubscriptionService.calculate_next_credit_date(now, billing_day)
            credits_remaining_months = None
            prepaid_until = None  # Monthly: no prepaid concept

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    if pending_sub:
                        # UPDATE existing pending subscription to active
                        cur.execute(
                            f"""
                            UPDATE {Tables.SUBSCRIPTIONS}
                            SET status = 'active',
                                provider_subscription_id = %s,
                                mollie_mandate_id = %s,
                                current_period_start = %s,
                                current_period_end = %s,
                                billing_day = %s,
                                next_credit_date = %s,
                                credits_remaining_months = %s,
                                first_paid_at = %s,
                                prepaid_until = %s,
                                updated_at = NOW()
                            WHERE id = %s
                            RETURNING *
                            """,
                            (
                                mollie_subscription_id,
                                mandate_id,
                                now,
                                period_end,
                                billing_day,
                                SubscriptionService.calculate_next_credit_date(now, billing_day),
                                credits_remaining_months,
                                now,  # first_paid_at
                                prepaid_until,  # prepaid_until (only set for yearly)
                                pending_sub["id"],
                            ),
                        )
                        sub = cur.fetchone()
                        print(
                            f"[SUB] payment_id={payment_id} action=activated_pending_subscription "
                            f"sub_id={pending_sub['id']} mollie_sub_id={mollie_subscription_id} "
                            f"prepaid_until={prepaid_until}"
                        )
                    else:
                        # CREATE new subscription (fallback if pending wasn't created)
                        cur.execute(
                            f"""
                            INSERT INTO {Tables.SUBSCRIPTIONS}
                                (identity_id, plan_code, status, provider,
                                 provider_subscription_id, mollie_customer_id, mollie_mandate_id,
                                 mollie_first_payment_id, is_mollie_recurring,
                                 current_period_start, current_period_end,
                                 billing_day, next_credit_date,
                                 credits_remaining_months, customer_email,
                                 first_paid_at, prepaid_until)
                            VALUES (%s, %s, 'active', 'mollie',
                                    %s, %s, %s, %s, TRUE,
                                    %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING *
                            """,
                            (
                                identity_id,
                                plan_code,
                                mollie_subscription_id,  # provider_subscription_id = Mollie sub ID
                                customer_id,
                                mandate_id,
                                payment_id,  # First payment ID
                                now,
                                period_end,
                                billing_day,
                                SubscriptionService.calculate_next_credit_date(now, billing_day),
                                credits_remaining_months,
                                customer_email,
                                now,  # first_paid_at
                                prepaid_until,  # prepaid_until (only set for yearly)
                            ),
                        )
                        sub = cur.fetchone()
                conn.commit()

            if not sub:
                print(f"[MOLLIE] Failed to create/update subscription for {payment_id}")
                return None

            sub_id = str(sub["id"])

            # Grant first month credits (payment already confirmed)
            first_period_end = SubscriptionService.calculate_next_credit_date(now, billing_day)
            cycle_result = SubscriptionService.grant_subscription_credits(
                sub_id, now, first_period_end,
                provider="mollie",
                provider_payment_id=payment_id,
            )

            if cycle_result:
                print(f"[MOLLIE] First credits granted for recurring subscription {sub_id}")

                # Mark cycle as paid (it's the first payment)
                try:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                f"""
                                UPDATE {Tables.SUBSCRIPTION_CYCLES}
                                SET payment_status = 'paid'
                                WHERE id = %s
                                """,
                                (cycle_result["cycle_id"],),
                            )
                        conn.commit()
                except Exception:
                    pass  # Non-critical

            # Log event
            SubscriptionService._log_event(sub_id, "created", {
                "plan_code": plan_code,
                "cadence": cadence,
                "is_mollie_recurring": True,
                "mollie_subscription_id": mollie_subscription_id,
                "first_payment_id": payment_id,
            })

            # Send confirmation email
            if customer_email:
                try:
                    from backend.emailer import send_subscription_confirmation, notify_admin
                    plan_name = plan.get("name", plan_code)
                    credits_per_month = plan.get("credits_per_month", 0)
                    price_gbp = plan.get("price_gbp", 0)

                    send_subscription_confirmation(
                        to_email=customer_email,
                        plan_name=plan_name,
                        plan_code=plan_code,
                        credits_per_month=credits_per_month,
                        price_gbp=price_gbp,
                        cadence=cadence,
                    )

                    # Admin notification
                    notify_admin(
                        subject="New Recurring Subscription",
                        message=f"A user has subscribed to the {plan_name} plan ({cadence}) with automatic billing.",
                        data={
                            "Identity ID": identity_id,
                            "Email": customer_email,
                            "Plan": plan_name,
                            "Cadence": cadence,
                            "Credits/month": f"{credits_per_month:,}",
                            "Price": f"£{price_gbp:.2f}/{cadence}",
                            "Mollie Sub ID": mollie_subscription_id,
                            "Next Payment": next_payment_date or "N/A",
                        },
                    )
                except Exception as email_err:
                    print(f"[MOLLIE] WARNING: Subscription email failed: {email_err}")

            return {"purchase_id": sub_id, "was_existing": False}

        except Exception as e:
            print(f"[MOLLIE] Error creating internal subscription: {e}")
            import traceback
            traceback.print_exc()
            return None

    @staticmethod
    def _handle_subscription_recurring_paid(payment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle a recurring payment from an active Mollie subscription.

        This is triggered when Mollie charges the customer automatically
        based on the subscription interval. We:
        1. Find the internal subscription by mollie_subscription_id
        2. Calculate the billing period for this payment
        3. Grant credits for that period (idempotent by payment_id)
        4. Update subscription period dates
        """
        from backend.services.subscription_service import SubscriptionService, SUBSCRIPTION_PLANS
        from backend.db import get_conn, Tables
        from datetime import datetime, timezone

        payment_id = payment.get("id")
        subscription_id = payment.get("subscriptionId")  # Mollie subscription ID (sub_xxx)
        metadata = payment.get("metadata", {})

        if not subscription_id:
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=recurring "
                f"granted=false reason=missing_subscription_id"
            )
            return None

        # Parse payment timestamp
        paid_at_str = payment.get("paidAt") or payment.get("createdAt")
        try:
            paid_at = datetime.fromisoformat(paid_at_str.replace("Z", "+00:00"))
        except Exception:
            paid_at = datetime.now(timezone.utc)

        # Find our subscription by Mollie subscription ID
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT * FROM {Tables.SUBSCRIPTIONS}
                        WHERE provider = 'mollie'
                          AND provider_subscription_id = %s
                          AND is_mollie_recurring = TRUE
                        LIMIT 1
                        """,
                        (subscription_id,),
                    )
                    sub = cur.fetchone()
        except Exception as e:
            print(f"[MOLLIE] Error finding subscription for {subscription_id}: {e}")
            return None

        if not sub:
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=recurring "
                f"mollie_sub_id={subscription_id} granted=false reason=subscription_not_found"
            )
            return None

        sub_id = str(sub["id"])
        plan_code = sub["plan_code"]
        billing_day = sub.get("billing_day") or paid_at.day
        is_yearly = "_yearly" in plan_code

        # Calculate period for this payment
        period_start, period_end = SubscriptionService.calculate_cycle_period(paid_at, billing_day)

        # Check if this payment already granted credits
        if SubscriptionService.check_payment_already_granted("mollie", payment_id):
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=recurring "
                f"sub_id={sub_id} granted=false reason=already_processed"
            )
            return {"purchase_id": sub_id, "was_existing": True}

        # ─────────────────────────────────────────────────────────────
        # YEARLY RENEWAL: Reset credits_remaining_months for new year
        # When Mollie charges for year 2+, reset the 12-month counter
        # ─────────────────────────────────────────────────────────────
        if is_yearly:
            current_remaining = sub.get("credits_remaining_months", 0) or 0
            if current_remaining <= 0:
                # This is a yearly renewal payment - reset to 12 months
                print(
                    f"[SUB] payment_id={payment_id} seq=recurring sub_id={sub_id} "
                    f"action=yearly_renewal resetting_credits_remaining_months=12"
                )
                try:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                f"""
                                UPDATE {Tables.SUBSCRIPTIONS}
                                SET credits_remaining_months = 12,
                                    current_period_end = %s,
                                    updated_at = NOW()
                                WHERE id = %s
                                """,
                                (paid_at + timedelta(days=365), sub_id),
                            )
                        conn.commit()
                except Exception as e:
                    print(f"[SUB] Error resetting yearly credits for {sub_id}: {e}")

        # Grant credits for this period
        cycle_result = SubscriptionService.grant_subscription_credits(
            sub_id, period_start, period_end,
            provider="mollie",
            provider_payment_id=payment_id,
        )

        if cycle_result:
            # Mark as paid
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            UPDATE {Tables.SUBSCRIPTION_CYCLES}
                            SET payment_status = 'paid'
                            WHERE id = %s
                            """,
                            (cycle_result["cycle_id"],),
                        )
                    conn.commit()
            except Exception:
                pass

            # Update subscription dates
            try:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            UPDATE {Tables.SUBSCRIPTIONS}
                            SET current_period_start = %s,
                                current_period_end = %s,
                                next_credit_date = %s,
                                failed_at = NULL,
                                failure_count = 0,
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (
                                period_start,
                                period_end,
                                SubscriptionService.calculate_next_credit_date(period_start, billing_day),
                                sub_id,
                            ),
                        )
                    conn.commit()
            except Exception as e:
                print(f"[MOLLIE] Error updating subscription dates: {e}")

            # Send credits delivered email
            customer_email = sub.get("customer_email")
            if customer_email:
                plan = SUBSCRIPTION_PLANS.get(plan_code, {})
                SubscriptionService._send_credits_delivered_email(
                    subscription_id=sub_id,
                    customer_email=customer_email,
                    plan_code=plan_code,
                    credits_granted=plan.get("credits_per_month", 0),
                    is_first_grant=False,
                    next_credit_date=period_end,
                )

            # Log event
            SubscriptionService._log_event(sub_id, "recurring_payment_received", {
                "payment_id": payment_id,
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
                "credits": cycle_result.get("credits_granted", 0),
            })

            plan = SUBSCRIPTION_PLANS.get(plan_code, {})
            credits = plan.get("credits_per_month", 0)
            print(
                f"[SUB] payment_id={payment_id} status=paid seq=recurring "
                f"sub_id={sub_id} granted=true credits={credits} "
                f"period={period_start.date()}→{period_end.date()}"
            )

            return {"purchase_id": sub_id, "was_existing": False}

        print(
            f"[SUB] payment_id={payment_id} status=paid seq=recurring "
            f"sub_id={sub_id} granted=false reason=grant_failed"
        )
        return None

    # ─────────────────────────────────────────────────────────────
    # Subscription Checkout
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def create_subscription_checkout(
        identity_id: str,
        plan_code: str,
        email: str,
        success_url: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a Mollie one-off payment for a subscription plan.

        We use a single payment (not Mollie Subscriptions API) so the
        webhook can activate + grant credits.  Recurring billing can be
        added later by converting to Mollie mandates/subscriptions.

        Args:
            identity_id: User's identity UUID
            plan_code: Subscription plan code
            email: Customer email
            success_url: Optional redirect URL after payment
            subscription_id: Optional existing subscription ID for renewals
                           (makes reconciliation more reliable)

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

        # Include subscription_id for renewals (makes reconciliation deterministic)
        if subscription_id:
            metadata["subscription_id"] = subscription_id

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

    # ─────────────────────────────────────────────────────────────
    # TRUE RECURRING SUBSCRIPTIONS (Mollie Subscriptions API)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_or_create_customer(
        identity_id: str,
        email: str,
        name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get or create a Mollie customer for recurring subscriptions.

        Args:
            identity_id: User's identity UUID
            email: Customer email
            name: Optional customer name

        Returns:
            { mollie_customer_id: "cst_xxx", email: "...", is_new: bool }
        """
        if not MOLLIE_AVAILABLE:
            raise ValueError("Mollie is not configured")

        from backend.db import query_one, get_conn, Tables

        # Check if customer exists in DB
        existing = query_one(
            f"""
            SELECT mollie_customer_id, email
            FROM {Tables.MOLLIE_CUSTOMERS}
            WHERE identity_id::text = %s
            """,
            (identity_id,),
        )

        if existing:
            return {
                "mollie_customer_id": existing["mollie_customer_id"],
                "email": existing["email"],
                "is_new": False,
            }

        # Create customer in Mollie
        customer_data = {
            "email": email,
            "metadata": {"identity_id": identity_id},
        }
        if name:
            customer_data["name"] = name

        try:
            response = requests.post(
                f"{MollieService.MOLLIE_API_BASE}/customers",
                headers=MollieService._get_headers(),
                json=customer_data,
                timeout=30,
            )

            if response.status_code not in (200, 201):
                error_data = response.json() if response.content else {}
                raise MollieCreateError(error_data.get("detail", response.text))

            customer = response.json()
            mollie_customer_id = customer["id"]

            # Save to DB
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.MOLLIE_CUSTOMERS}
                            (identity_id, mollie_customer_id, email, name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (identity_id) DO UPDATE SET
                            mollie_customer_id = EXCLUDED.mollie_customer_id,
                            email = EXCLUDED.email,
                            updated_at = NOW()
                        """,
                        (identity_id, mollie_customer_id, email, name),
                    )
                conn.commit()

            print(f"[MOLLIE] Created customer {mollie_customer_id} for identity {identity_id}")

            return {
                "mollie_customer_id": mollie_customer_id,
                "email": email,
                "is_new": True,
            }

        except requests.RequestException as e:
            print(f"[MOLLIE] Error creating customer: {e}")
            raise ValueError(f"Payment service error: {str(e)}")

    @staticmethod
    def create_recurring_subscription_checkout(
        identity_id: str,
        plan_code: str,
        email: str,
        success_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a first payment to establish mandate, then create recurring subscription.

        This is a two-phase flow:
        1. Create first payment with sequenceType=first
        2. Create "pending_payment" subscription record immediately
        3. On payment.paid webhook, transition to "active" and create Mollie subscription

        This ensures SEPA payments (which can be pending 1-2 days) show proper
        "processing" status to users.

        Args:
            identity_id: User's identity UUID
            plan_code: Subscription plan code
            email: Customer email
            success_url: Redirect URL after payment

        Returns:
            { checkout_url, payment_id, mollie_customer_id, subscription_id }
        """
        if not MOLLIE_AVAILABLE:
            raise ValueError("Mollie is not configured")

        from backend.services.subscription_service import SUBSCRIPTION_PLANS, SubscriptionService

        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            raise ValueError(f"Unknown subscription plan: {plan_code}")

        # ═══════════════════════════════════════════════════════════════════
        # PART 5: PREVENT DUPLICATE PENDING SUBSCRIPTIONS
        # Expire any existing pending_payment subscriptions for this identity
        # This handles the case where user abandons checkout and starts again
        # ═══════════════════════════════════════════════════════════════════
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Expire old pending subscriptions (allow new checkout)
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'expired',
                            expired_at = NOW(),
                            updated_at = NOW()
                        WHERE identity_id = %s
                          AND status = 'pending_payment'
                        RETURNING id
                        """,
                        (identity_id,),
                    )
                    expired = cur.fetchall()
                conn.commit()

            if expired:
                expired_ids = [str(r["id"]) for r in expired]
                print(f"[SUB] Expired {len(expired)} old pending subscription(s): {expired_ids}")
                for old_id in expired_ids:
                    SubscriptionService._log_event(old_id, "expired", {
                        "reason": "new_checkout_started",
                        "new_plan_code": plan_code,
                    })
        except Exception as e:
            print(f"[SUB] Warning: Could not expire old pending subscriptions: {e}")

        # Get or create Mollie customer
        customer_result = MollieService.get_or_create_customer(identity_id, email)
        mollie_customer_id = customer_result["mollie_customer_id"]

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

        # Mollie interval format
        interval = "1 month" if cadence == "monthly" else "12 months"

        metadata = {
            "identity_id": identity_id,
            "type": "subscription_first",  # Marks this as first payment for recurring
            "plan_code": plan_code,
            "cadence": cadence,
            "credits_per_month": str(credits),
            "email": email,
            "interval": interval,
        }

        # Create first payment with sequenceType=first to establish mandate
        payment_data = {
            "amount": {
                "currency": "GBP",
                "value": f"{price_gbp:.2f}",
            },
            "customerId": mollie_customer_id,
            "sequenceType": "first",  # CRITICAL: Establishes mandate for recurring
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
                raise MollieCreateError(error_data.get("detail", response.text))

            payment = response.json()
            payment_id = payment["id"]
            checkout_url = payment["_links"]["checkout"]["href"]

            print(
                f"[MOLLIE] Recurring subscription first payment created: "
                f"payment_id={payment_id}, customer={mollie_customer_id}, plan={plan_code}"
            )

            # ═══════════════════════════════════════════════════════════════════
            # CREATE PENDING SUBSCRIPTION RECORD
            # This ensures SEPA users see "processing" status while payment clears
            # ═══════════════════════════════════════════════════════════════════
            subscription_id = None
            try:
                now = datetime.now(timezone.utc)
                billing_day = now.day

                if cadence == "yearly":
                    period_end = now + timedelta(days=365)
                    credits_remaining_months = 12
                else:
                    period_end = SubscriptionService.calculate_next_credit_date(now, billing_day)
                    credits_remaining_months = None

                with get_conn() as conn:
                    with conn.cursor() as cur:
                        # Create subscription with status='pending_payment'
                        # Will be activated when payment webhook shows status=paid
                        cur.execute(
                            f"""
                            INSERT INTO {Tables.SUBSCRIPTIONS}
                                (identity_id, plan_code, status, provider,
                                 mollie_customer_id, mollie_first_payment_id,
                                 is_mollie_recurring,
                                 current_period_start, current_period_end,
                                 billing_day, next_credit_date,
                                 credits_remaining_months, customer_email)
                            VALUES (%s, %s, 'pending_payment', 'mollie',
                                    %s, %s, TRUE,
                                    %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (identity_id) WHERE status IN ('pending_payment')
                            DO UPDATE SET
                                plan_code = EXCLUDED.plan_code,
                                mollie_customer_id = EXCLUDED.mollie_customer_id,
                                mollie_first_payment_id = EXCLUDED.mollie_first_payment_id,
                                updated_at = NOW()
                            RETURNING id
                            """,
                            (
                                identity_id,
                                plan_code,
                                mollie_customer_id,
                                payment_id,  # Track which payment this is waiting for
                                now,
                                period_end,
                                billing_day,
                                SubscriptionService.calculate_next_credit_date(now, billing_day),
                                credits_remaining_months,
                                email,
                            ),
                        )
                        result = cur.fetchone()
                        if result:
                            subscription_id = str(result["id"])
                    conn.commit()

                if subscription_id:
                    print(
                        f"[SUB] Created pending subscription: sub_id={subscription_id} "
                        f"payment_id={payment_id} plan={plan_code} status=pending_payment"
                    )
                    # Log event
                    SubscriptionService._log_event(subscription_id, "checkout_started", {
                        "plan_code": plan_code,
                        "cadence": cadence,
                        "payment_id": payment_id,
                        "mollie_customer_id": mollie_customer_id,
                    })

            except Exception as e:
                # Non-fatal - subscription will be created on paid webhook if this fails
                print(f"[SUB] Warning: Could not create pending subscription: {e}")

            return {
                "checkout_url": checkout_url,
                "payment_id": payment_id,
                "mollie_customer_id": mollie_customer_id,
                "subscription_id": subscription_id,
            }

        except requests.RequestException as e:
            print(f"[MOLLIE] Error creating recurring subscription checkout: {e}")
            raise ValueError(f"Payment service error: {str(e)}")

    @staticmethod
    def create_mollie_subscription(
        mollie_customer_id: str,
        plan_code: str,
        mandate_id: str,
        identity_id: str,
    ) -> Dict[str, Any]:
        """
        Create a Mollie Subscription object for automatic recurring billing.

        Called after first payment succeeds and mandate is established.

        Args:
            mollie_customer_id: Mollie customer ID (cst_xxx)
            plan_code: Subscription plan code
            mandate_id: Mollie mandate ID (mdt_xxx) from first payment
            identity_id: User's identity UUID

        Returns:
            { mollie_subscription_id, interval, next_payment_date }
        """
        if not MOLLIE_AVAILABLE:
            raise ValueError("Mollie is not configured")

        from backend.services.subscription_service import SUBSCRIPTION_PLANS

        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            raise ValueError(f"Unknown subscription plan: {plan_code}")

        price_gbp = plan["price_gbp"]
        plan_name = plan["name"]
        cadence = plan["cadence"]
        credits = plan["credits_per_month"]

        # Mollie interval format
        interval = "1 month" if cadence == "monthly" else "12 months"

        description = f"{plan_name} Subscription - {credits} credits/mo"

        backend_url = config.PUBLIC_BASE_URL.rstrip("/") if config.PUBLIC_BASE_URL else ""
        webhook_url = f"{backend_url}/api/billing/webhook/mollie"

        subscription_data = {
            "amount": {
                "currency": "GBP",
                "value": f"{price_gbp:.2f}",
            },
            "interval": interval,
            "description": description,
            "webhookUrl": webhook_url,
            "mandateId": mandate_id,
            "metadata": {
                "identity_id": identity_id,
                "plan_code": plan_code,
                "type": "subscription_recurring",
            },
        }

        try:
            response = requests.post(
                f"{MollieService.MOLLIE_API_BASE}/customers/{mollie_customer_id}/subscriptions",
                headers=MollieService._get_headers(),
                json=subscription_data,
                timeout=30,
            )

            if response.status_code not in (200, 201):
                error_data = response.json() if response.content else {}
                raise MollieCreateError(error_data.get("detail", response.text))

            subscription = response.json()
            mollie_subscription_id = subscription["id"]
            next_payment_date = subscription.get("nextPaymentDate")

            print(
                f"[MOLLIE] Created Mollie subscription {mollie_subscription_id} "
                f"for customer {mollie_customer_id}, interval={interval}"
            )

            return {
                "mollie_subscription_id": mollie_subscription_id,
                "interval": interval,
                "next_payment_date": next_payment_date,
                "status": subscription.get("status"),
            }

        except requests.RequestException as e:
            print(f"[MOLLIE] Error creating Mollie subscription: {e}")
            raise ValueError(f"Payment service error: {str(e)}")

    @staticmethod
    def get_mandate_for_customer(mollie_customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the valid mandate for a customer (established by first payment).

        Returns:
            { mandate_id, status, method } or None
        """
        if not MOLLIE_AVAILABLE:
            return None

        try:
            response = requests.get(
                f"{MollieService.MOLLIE_API_BASE}/customers/{mollie_customer_id}/mandates",
                headers=MollieService._get_headers(),
                timeout=30,
            )

            if response.status_code != 200:
                return None

            mandates = response.json()
            # Find first valid (active) mandate
            for mandate in mandates.get("_embedded", {}).get("mandates", []):
                if mandate.get("status") == "valid":
                    return {
                        "mandate_id": mandate["id"],
                        "status": mandate["status"],
                        "method": mandate.get("method"),
                    }

            return None

        except requests.RequestException as e:
            print(f"[MOLLIE] Error getting mandates: {e}")
            return None

    @staticmethod
    def cancel_mollie_subscription(
        mollie_customer_id: str,
        mollie_subscription_id: str,
    ) -> bool:
        """
        Cancel a Mollie subscription (stops future charges).

        Args:
            mollie_customer_id: Mollie customer ID
            mollie_subscription_id: Mollie subscription ID (sub_xxx)

        Returns:
            True if cancelled successfully
        """
        if not MOLLIE_AVAILABLE:
            return False

        try:
            response = requests.delete(
                f"{MollieService.MOLLIE_API_BASE}/customers/{mollie_customer_id}/subscriptions/{mollie_subscription_id}",
                headers=MollieService._get_headers(),
                timeout=30,
            )

            if response.status_code in (200, 204):
                print(f"[MOLLIE] Cancelled subscription {mollie_subscription_id}")
                return True
            else:
                print(f"[MOLLIE] Failed to cancel subscription: {response.status_code}")
                return False

        except requests.RequestException as e:
            print(f"[MOLLIE] Error cancelling subscription: {e}")
            return False

    @staticmethod
    def get_mollie_subscription_status(
        mollie_customer_id: str,
        mollie_subscription_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get current status of a Mollie subscription.

        Returns:
            { status, next_payment_date, times, ... } or None
        """
        if not MOLLIE_AVAILABLE:
            return None

        try:
            response = requests.get(
                f"{MollieService.MOLLIE_API_BASE}/customers/{mollie_customer_id}/subscriptions/{mollie_subscription_id}",
                headers=MollieService._get_headers(),
                timeout=30,
            )

            if response.status_code != 200:
                return None

            sub = response.json()
            return {
                "status": sub.get("status"),  # active, pending, canceled, suspended, completed
                "next_payment_date": sub.get("nextPaymentDate"),
                "times": sub.get("times"),  # number of times charged
                "description": sub.get("description"),
            }

        except requests.RequestException as e:
            print(f"[MOLLIE] Error getting subscription status: {e}")
            return None
