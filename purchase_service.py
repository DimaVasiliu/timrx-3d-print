"""
Purchase Service - Handles credit purchases via Stripe.

Flow:
1. start_checkout(identity_id, plan_code, email) -> checkout_url
2. User completes payment on Stripe
3. handle_webhook() processes checkout.session.completed:
   - Creates purchases row
   - Adds ledger entry purchase_credit (+credits)
   - Updates wallet balance
   - Attaches email to identity (if not already)
   - Sends receipt email to user
   - Sends admin notification email

Idempotency:
- Purchases are keyed by provider_payment_id (Stripe session ID)
- Repeated webhooks for same session are safely ignored
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
import json

from db import fetch_one, fetch_all, transaction, query_one, query_all, Tables
from config import config
from emailer import send_purchase_receipt, notify_purchase
from pricing_service import PricingService
from wallet_service import WalletService, LedgerEntryType
from identity_service import IdentityService

# Stripe import (only if enabled via PAYMENTS_PROVIDER)
stripe = None
STRIPE_AVAILABLE = False

# Check PAYMENTS_PROVIDER directly (avoids property issues on some deployments)
_payments_provider = getattr(config, 'PAYMENTS_PROVIDER', 'mollie').lower()
_use_stripe = _payments_provider in ('stripe', 'both')

if _use_stripe:
    try:
        import stripe as stripe_module
        stripe = stripe_module
        stripe.api_key = config.STRIPE_SECRET_KEY
        STRIPE_AVAILABLE = bool(config.STRIPE_SECRET_KEY)
        if STRIPE_AVAILABLE:
            stripe_mode = "live" if config.STRIPE_SECRET_KEY.startswith("sk_live_") else "test"
            print(f"[STRIPE] Stripe configured and ready (mode: {stripe_mode})")
        else:
            print("[STRIPE] Stripe enabled but not configured (missing STRIPE_SECRET_KEY)")
    except ImportError:
        print("[STRIPE] Stripe package not installed")
# If PAYMENTS_PROVIDER is not 'stripe' or 'both', Stripe is silently disabled (no warnings)


class PurchaseStatus:
    """Valid purchase statuses."""
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"


class PurchaseService:
    """Service for handling credit purchases via Stripe."""

    @staticmethod
    def is_available() -> bool:
        """Check if purchase functionality is available."""
        return STRIPE_AVAILABLE

    # ─────────────────────────────────────────────────────────────
    # Checkout Flow
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def start_checkout(
        identity_id: str,
        plan_code: str,
        email: str,
        success_url: str,
        cancel_url: str,
    ) -> Dict[str, Any]:
        """
        Create a Stripe Checkout session for purchasing credits.

        Args:
            identity_id: The user's identity ID
            plan_code: The plan code to purchase (e.g., 'starter_80')
            email: User's email (pre-filled in checkout)
            success_url: URL to redirect on success (can include {CHECKOUT_SESSION_ID})
            cancel_url: URL to redirect on cancel

        Returns:
            {
                "checkout_url": "https://checkout.stripe.com/...",
                "session_id": "cs_..."
            }

        Raises:
            ValueError: If Stripe not configured, plan not found, or API error
        """
        if not STRIPE_AVAILABLE:
            raise ValueError("Stripe is not configured")

        # Validate plan exists
        plan = PricingService.get_plan_by_code(plan_code)
        if not plan:
            raise ValueError(f"Plan '{plan_code}' not found or inactive")

        # Get plan details
        plan_id = plan["id"]
        plan_name = plan["name"]
        price_gbp = plan["price"]
        credits = plan["credits"]

        # Price in pence (Stripe uses smallest currency unit)
        price_pence = int(price_gbp * 100)

        try:
            # Create Stripe Checkout session
            session = stripe.checkout.Session.create(
                payment_method_types=["card"],
                mode="payment",
                customer_email=email,
                line_items=[
                    {
                        "price_data": {
                            "currency": "gbp",
                            "unit_amount": price_pence,
                            "product_data": {
                                "name": f"{plan_name} - {credits} Credits",
                                "description": f"Purchase {credits:,} credits for TimrX 3D Print Hub",
                            },
                        },
                        "quantity": 1,
                    }
                ],
                metadata={
                    "identity_id": identity_id,
                    "plan_code": plan_code,
                    "plan_id": plan_id,
                    "credits": str(credits),
                },
                success_url=success_url,
                cancel_url=cancel_url,
            )

            print(
                f"[PURCHASE] Checkout session created: session={session.id}, "
                f"identity={identity_id}, plan={plan_code}, credits={credits}"
            )

            return {
                "checkout_url": session.url,
                "session_id": session.id,
            }

        except stripe.error.StripeError as e:
            print(f"[PURCHASE] Stripe error creating checkout: {e}")
            raise ValueError(f"Payment service error: {str(e)}")

    # ─────────────────────────────────────────────────────────────
    # Webhook Processing
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def process_webhook(payload: bytes, signature: str) -> Dict[str, Any]:
        """
        Process a Stripe webhook event.

        Args:
            payload: Raw request body
            signature: Stripe-Signature header value

        Returns:
            {
                "ok": True/False,
                "event_type": "checkout.session.completed",
                "message": "...",
                "purchase_id": "..." (if applicable)
            }
        """
        if not STRIPE_AVAILABLE:
            return {"ok": False, "error": "Stripe not configured"}

        # Verify webhook signature (REQUIRED in production)
        try:
            if config.STRIPE_WEBHOOK_SECRET:
                event = stripe.Webhook.construct_event(
                    payload, signature, config.STRIPE_WEBHOOK_SECRET
                )
            elif config.IS_DEV:
                # Dev only: allow unverified webhooks for local testing
                print("[PURCHASE] WARNING: Webhook signature not verified (dev mode, no secret)")
                event = stripe.Event.construct_from(
                    json.loads(payload), stripe.api_key
                )
            else:
                # Production requires webhook secret
                print("[PURCHASE] ERROR: STRIPE_WEBHOOK_SECRET not configured in production")
                return {"ok": False, "error": "Webhook secret not configured"}
        except stripe.error.SignatureVerificationError as e:
            print(f"[PURCHASE] Webhook signature verification failed: {e}")
            return {"ok": False, "error": "Invalid signature"}
        except json.JSONDecodeError as e:
            print(f"[PURCHASE] Webhook JSON parse error: {e}")
            return {"ok": False, "error": "Invalid payload"}

        event_type = event.get("type", "unknown")
        print(f"[PURCHASE] Webhook received: {event_type}")

        # Handle supported events
        if event_type == "checkout.session.completed":
            session = event["data"]["object"]
            result = PurchaseService.handle_checkout_completed(session)

            if result:
                return {
                    "ok": True,
                    "event_type": event_type,
                    "message": "Purchase completed successfully",
                    "purchase_id": result.get("purchase_id"),
                }
            else:
                return {
                    "ok": False,
                    "event_type": event_type,
                    "error": "Failed to process checkout completion",
                }

        # Log but acknowledge other events
        print(f"[PURCHASE] Ignoring event type: {event_type}")
        return {
            "ok": True,
            "event_type": event_type,
            "message": f"Event type '{event_type}' acknowledged but not processed",
        }

    @staticmethod
    def handle_checkout_completed(session: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle checkout.session.completed event.
        Creates purchase record and grants credits.

        Args:
            session: Stripe checkout session object

        Returns:
            Dict with purchase info, or None on failure
        """
        session_id = session.get("id")
        payment_status = session.get("payment_status")

        # Only process paid sessions
        if payment_status != "paid":
            print(f"[PURCHASE] Session {session_id} not paid (status: {payment_status})")
            return None

        # Extract metadata
        metadata = session.get("metadata", {})
        identity_id = metadata.get("identity_id")
        plan_code = metadata.get("plan_code")
        plan_id = metadata.get("plan_id")
        credits_str = metadata.get("credits")

        if not identity_id or not plan_code or not credits_str:
            print(f"[PURCHASE] Missing metadata in session {session_id}: {metadata}")
            return None

        credits = int(credits_str)

        # Get email from session
        customer_email = session.get("customer_email") or session.get("customer_details", {}).get("email")

        # Get amount from session (in pence)
        amount_total = session.get("amount_total", 0)
        amount_gbp = amount_total / 100.0

        # Get plan name
        plan = PricingService.get_plan_by_code(plan_code)
        plan_name = plan["name"] if plan else plan_code

        # Idempotency check: see if purchase already exists for this session
        existing = PurchaseService.get_purchase_by_provider_id(session_id)
        if existing:
            print(f"[PURCHASE] Already processed session {session_id}, purchase_id={existing['id']}")
            return {
                "purchase_id": existing["id"],
                "was_existing": True,
            }

        # Process the purchase in a transaction
        try:
            result = PurchaseService.record_purchase(
                identity_id=identity_id,
                plan_id=plan_id,
                plan_code=plan_code,
                provider_payment_id=session_id,
                amount_gbp=amount_gbp,
                credits_granted=credits,
                customer_email=customer_email,
            )

            if result:
                purchase_id = result["purchase"]["id"]

                # Send emails (non-blocking - failures are logged as warnings only)
                if customer_email:
                    try:
                        send_purchase_receipt(
                            to_email=customer_email,
                            plan_name=plan_name,
                            credits=credits,
                            amount_gbp=amount_gbp,
                        )

                        # Send admin notification
                        notify_purchase(
                            identity_id=identity_id,
                            email=customer_email,
                            plan_name=plan_name,
                            credits=credits,
                            amount_gbp=amount_gbp,
                        )
                    except Exception as email_err:
                        # Never fail purchase due to email errors
                        print(f"[PURCHASE] WARNING: Email failed for purchase {purchase_id}: {email_err} (credits already granted)")

                return {
                    "purchase_id": purchase_id,
                    "was_existing": False,
                }

        except Exception as e:
            print(f"[PURCHASE] Error processing checkout completion: {e}")
            return None

        return None

    # ─────────────────────────────────────────────────────────────
    # Purchase Recording
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def record_purchase(
        identity_id: str,
        plan_id: str,
        plan_code: str,
        provider_payment_id: str,
        amount_gbp: float,
        credits_granted: int,
        customer_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Record a completed purchase and grant credits.
        This is the core transactional operation.

        Performs in a single transaction:
        1. Create purchase record
        2. Add ledger entry (purchase_credit, +credits)
        3. Update wallet balance
        4. Attach email to identity (if provided and not already set)

        Args:
            identity_id: The user's identity ID
            plan_id: The plan UUID
            plan_code: The plan code (for reference)
            provider_payment_id: Stripe session/payment ID
            amount_gbp: Amount paid in GBP
            credits_granted: Number of credits to grant
            customer_email: Customer email from checkout

        Returns:
            Dict with purchase and wallet info, or None on failure
        """
        with transaction() as cur:
            # 1. Create purchase record
            cur.execute(
                f"""
                INSERT INTO {Tables.PURCHASES}
                (identity_id, plan_id, provider, provider_payment_id,
                 amount, currency, credits_granted, status, purchased_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING *
                """,
                (
                    identity_id,
                    plan_id,
                    "stripe",
                    provider_payment_id,
                    amount_gbp,
                    "GBP",
                    credits_granted,
                    PurchaseStatus.COMPLETED,
                ),
            )
            purchase = fetch_one(cur)
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
                    "purchases",
                    purchase_id,
                    json.dumps({"plan_code": plan_code, "amount_gbp": amount_gbp}),
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

            # 5. Attach email to identity if provided
            email_attached = False
            if customer_email:
                cur.execute(
                    f"""
                    UPDATE {Tables.IDENTITIES}
                    SET email = %s, last_seen_at = NOW()
                    WHERE id = %s AND email IS NULL
                    """,
                    (customer_email.lower().strip(), identity_id),
                )
                email_attached = cur.rowcount > 0

            print(
                f"[PURCHASE] Recorded: purchase_id={purchase_id}, identity={identity_id}, "
                f"credits={credits_granted}, balance: {current_balance} -> {new_balance}, "
                f"email_attached={email_attached}"
            )

            return {
                "purchase": PurchaseService._format_purchase(purchase),
                "ledger_entry_id": str(ledger_entry["id"]),
                "balance": new_balance,
                "email_attached": email_attached,
            }

    # ─────────────────────────────────────────────────────────────
    # Read Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_purchase(purchase_id: str) -> Optional[Dict[str, Any]]:
        """Get a purchase by ID."""
        purchase = query_one(
            f"""
            SELECT p.*, pl.code as plan_code, pl.name as plan_name
            FROM {Tables.PURCHASES} p
            LEFT JOIN {Tables.PLANS} pl ON p.plan_id = pl.id
            WHERE p.id = %s
            """,
            (purchase_id,),
        )
        if purchase:
            return PurchaseService._format_purchase(purchase)
        return None

    @staticmethod
    def get_purchase_by_provider_id(provider_payment_id: str) -> Optional[Dict[str, Any]]:
        """Get a purchase by Stripe session/payment ID (for idempotency)."""
        purchase = query_one(
            f"""
            SELECT p.*, pl.code as plan_code, pl.name as plan_name
            FROM {Tables.PURCHASES} p
            LEFT JOIN {Tables.PLANS} pl ON p.plan_id = pl.id
            WHERE p.provider_payment_id = %s
            """,
            (provider_payment_id,),
        )
        if purchase:
            return PurchaseService._format_purchase(purchase)
        return None

    @staticmethod
    def get_purchases_for_identity(
        identity_id: str,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Get purchases for an identity, most recent first."""
        purchases = query_all(
            f"""
            SELECT p.*, pl.code as plan_code, pl.name as plan_name
            FROM {Tables.PURCHASES} p
            LEFT JOIN {Tables.PLANS} pl ON p.plan_id = pl.id
            WHERE p.identity_id = %s
            ORDER BY p.purchased_at DESC
            LIMIT %s OFFSET %s
            """,
            (identity_id, limit, offset),
        )
        return [PurchaseService._format_purchase(p) for p in purchases]

    # ─────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _format_purchase(purchase: Dict[str, Any]) -> Dict[str, Any]:
        """Format purchase for API response."""
        return {
            "id": str(purchase["id"]),
            "identity_id": str(purchase["identity_id"]),
            "plan_id": str(purchase["plan_id"]) if purchase.get("plan_id") else None,
            "plan_code": purchase.get("plan_code"),
            "plan_name": purchase.get("plan_name"),
            "provider": purchase.get("provider"),
            "amount": float(purchase.get("amount", 0)),
            "currency": purchase.get("currency", "GBP"),
            "credits_granted": purchase.get("credits_granted", 0),
            "status": purchase.get("status"),
            "purchased_at": purchase["purchased_at"].isoformat() if purchase.get("purchased_at") else None,
        }
