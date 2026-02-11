"""
/api/billing routes - Credits and purchases.

Handles:
- GET /api/billing/plans - List available credit plans
- GET /api/billing/action-costs - Get action costs
- GET /api/billing/ledger - Get ledger entries for current identity
- POST /api/billing/reserve - Reserve credits for a job
- POST /api/billing/checkout - Create Mollie payment (primary)
- POST /api/billing/checkout/start - Create Stripe checkout session (disabled when PAYMENTS_PROVIDER != 'stripe')
- POST /api/billing/webhook/mollie - Mollie webhook handler
- POST /api/billing/webhook - Stripe webhook handler (disabled when PAYMENTS_PROVIDER != 'stripe')
- GET /api/billing/purchase/:id - Get purchase details
- GET /api/billing/purchases - Get purchase history
"""

from flask import Blueprint, request, jsonify, g, make_response

from backend.middleware import require_session, require_email, require_verified_email, no_cache
from backend.db import get_conn, Tables
from backend.services.pricing_service import PricingService
from backend.services.wallet_service import WalletService
from backend.services.reservation_service import ReservationService
from backend.services.expense_guard import ExpenseGuard
from backend.services.purchase_service import PurchaseService
from backend.services.mollie_service import MollieService, MollieCreateError
from backend.services.subscription_service import SubscriptionService

bp = Blueprint("billing", __name__)

# Cache TTL for pricing data (5 minutes)
CACHE_TTL_SECONDS = 300


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL VALIDATION FOR CHECKOUT (Security hardening)
# ─────────────────────────────────────────────────────────────────────────────

def validate_checkout_email(identity: dict, request_email: str | None) -> tuple[bool, dict | None]:
    """
    Validate that the checkout email matches the identity's verified email.

    SECURITY RULE: When a user has a verified email, all checkout requests
    MUST use that exact email. This prevents:
    - Confusion about which account is being credited
    - Potential abuse by crediting different accounts
    - Receipt/invoice email mismatch issues

    Args:
        identity: The identity dict from g.identity (must have email and email_verified)
        request_email: The email provided in the checkout request body (optional)

    Returns:
        (True, None) if valid
        (False, error_dict) if invalid - error_dict contains code, message, identity_email

    Test cases:
        - identity email A verified, request email B → EMAIL_MISMATCH (403)
        - identity email A verified, request email A → OK
        - identity email A verified, request email a (lowercase) → OK (case-insensitive)
        - identity email A verified, no request email → OK (use identity email)
    """
    identity_email = identity.get("email", "").strip().lower()

    # If identity has no email, this should have been caught by @require_verified_email
    # but let's be defensive
    if not identity_email:
        return False, {
            "code": "EMAIL_REQUIRED",
            "message": "Email address required for checkout",
        }

    # If no request email provided, that's fine - we'll use identity email
    if not request_email:
        return True, None

    # Normalize request email
    normalized_request = request_email.strip().lower()

    # Check for mismatch
    if normalized_request != identity_email:
        return False, {
            "code": "EMAIL_MISMATCH",
            "message": f"You're logged in as {identity.get('email')}. Use that email to checkout, or switch accounts.",
            "identity_email": identity.get("email"),
            "request_email": request_email.strip(),
        }

    return True, None


def get_checkout_email(identity: dict) -> str:
    """
    Get the authoritative email to use for checkout.

    ALWAYS uses the identity's verified email - never trust request body email.
    This is the single source of truth for:
    - Mollie customer email
    - Receipt email
    - Payment metadata

    Args:
        identity: The identity dict from g.identity

    Returns:
        The verified email address
    """
    return identity.get("email", "").strip()


def _add_cache_headers(response, max_age: int = CACHE_TTL_SECONDS):
    """Add caching headers to response."""
    response.headers["Cache-Control"] = f"public, max-age={max_age}"
    return response


@bp.route("/plans", methods=["GET"])
def get_plans():
    """
    List available credit plans for purchase.
    Returns active plans with prices, credits, perks, and estimated outputs.

    Query params:
    - estimates: Set to "true" to include output estimates (default: true)

    Response:
    {
        "ok": true,
        "plans": [
            {
                "id": "uuid",
                "code": "starter_250",
                "name": "Starter",
                "price_gbp": 7.99,
                "credits": 250,
                "perks": {
                    "priority": false,
                    "retention_days": 30
                },
                "estimates": {
                    "ai_images": 50,
                    "text_to_3d": 13,
                    "image_to_3d": 10
                }
            },
            ...
        ]
    }
    """
    try:
        include_estimates = request.args.get("estimates", "true").lower() != "false"

        if include_estimates:
            plans = PricingService.get_plans_with_estimates(active_only=True)
        else:
            plans = PricingService.get_plans_with_perks(active_only=True)

        response = make_response(jsonify({
            "ok": True,
            "plans": plans,
        }))
        return _add_cache_headers(response)
    except Exception as e:
        print(f"[BILLING] Error fetching plans: {e}")
        return jsonify({
            "ok": True,
            "plans": [],
        })


@bp.route("/action-costs", methods=["GET"])
def get_action_costs():
    """
    Get action costs in credits.
    Returns cost for each action type using stable frontend keys.

    Response:
    {
        "ok": true,
        "action_costs": [
            {"action_key": "text_to_3d_generate", "credits": 20},
            {"action_key": "image_to_3d_generate", "credits": 30},
            {"action_key": "refine", "credits": 10},
            {"action_key": "remesh", "credits": 10},
            {"action_key": "texture", "credits": 10},
            {"action_key": "rig", "credits": 10},
            {"action_key": "image_studio_generate", "credits": 12}
        ]
    }
    """
    try:
        action_costs = PricingService.get_action_costs_list()
        # Diagnostic logging
        if action_costs:
            print(f"[BILLING] Returning {len(action_costs)} action costs: {[c['action_key'] for c in action_costs]}")
        else:
            print("[BILLING] WARNING: No action costs returned from PricingService!")
        response = make_response(jsonify({
            "ok": True,
            "action_costs": action_costs,
        }))
        return _add_cache_headers(response)
    except Exception as e:
        print(f"[BILLING] Error fetching action costs: {e}")
        import traceback
        traceback.print_exc()
        # Return hardcoded fallback if DB fails
        return jsonify({
            "ok": True,
            "action_costs": [
                {"action_key": "text_to_3d_generate", "credits": 20},
                {"action_key": "image_to_3d_generate", "credits": 30},
                {"action_key": "refine", "credits": 10},
                {"action_key": "remesh", "credits": 10},
                {"action_key": "texture", "credits": 10},
                {"action_key": "rig", "credits": 10},
                {"action_key": "image_studio_generate", "credits": 12},
                {"action_key": "video", "credits": 60},
                {"action_key": "video_generate", "credits": 60},
            ],
        })


# Keep /costs as alias for backward compatibility
@bp.route("/costs", methods=["GET"])
def get_costs():
    """Alias for /action-costs (backward compatibility)."""
    return get_action_costs()


@bp.route("/ledger", methods=["GET"])
@require_session
@no_cache
def get_ledger():
    """
    Get ledger entries for the current identity.
    Returns recent credit transactions (purchases, usage, refunds).

    Query params:
    - limit: Max entries to return (default 50, max 100)
    - offset: Pagination offset (default 0)

    Response:
    {
        "ok": true,
        "entries": [
            {
                "id": "uuid",
                "type": "purchase_credit",
                "amount": 80,
                "ref_type": "purchase",
                "ref_id": "uuid",
                "meta": {...},
                "created_at": "2024-01-15T12:00:00Z"
            },
            ...
        ],
        "limit": 50,
        "offset": 0
    }
    """
    try:
        limit = min(request.args.get("limit", 50, type=int), 100)
        offset = request.args.get("offset", 0, type=int)

        entries = WalletService.get_ledger_entries(
            g.identity_id,
            limit=limit,
            offset=offset,
        )

        return jsonify({
            "ok": True,
            "entries": entries,
            "limit": limit,
            "offset": offset,
        })
    except Exception as e:
        print(f"[BILLING] Error fetching ledger: {e}")
        return jsonify({
            "ok": True,
            "entries": [],
            "limit": 50,
            "offset": 0,
        })


@bp.route("/reserve", methods=["POST"])
@require_session
def reserve_credits():
    """
    Reserve credits for a job action.
    This creates a hold on credits to prevent overspend during async processing.

    Request body:
    {
        "action_key": "text_to_3d_generate",
        "job_id": "unique-job-identifier"
    }

    Response (success - 200):
    {
        "ok": true,
        "reservation": {
            "id": "uuid",
            "action_code": "MESHY_TEXT_TO_3D",
            "cost_credits": 20,
            "status": "held",
            "job_id": "...",
            "expires_at": "..."
        },
        "balance": 100,
        "reserved": 20,
        "available": 80
    }

    Response (insufficient credits - 402):
    {
        "error": {
            "code": "INSUFFICIENT_CREDITS",
            "message": "Not enough credits for this action",
            "required": 20,
            "balance": 10,
            "available": 5
        }
    }
    """
    data = request.get_json() or {}
    action_key = data.get("action_key")
    job_id = data.get("job_id")

    if not action_key:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "action_key is required",
            }
        }), 400

    if not job_id:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "job_id is required",
            }
        }), 400

    try:
        result = ReservationService.reserve_credits(
            identity_id=g.identity_id,
            action_key=action_key,
            job_id=job_id,
            meta={"source": "api"},
        )

        return jsonify({
            "ok": True,
            "reservation": result["reservation"],
            "balance": result["balance"],
            "reserved": result["reserved"],
            "available": result["available"],
            "is_existing": result.get("is_existing", False),
        })

    except ValueError as e:
        error_msg = str(e)

        # Parse INSUFFICIENT_CREDITS error
        if "INSUFFICIENT_CREDITS" in error_msg:
            # Format: INSUFFICIENT_CREDITS:required=X:balance=Y:reserved=Z:available=W
            parts = error_msg.split(":")
            error_data = {}
            for part in parts[1:]:
                if "=" in part:
                    key, val = part.split("=", 1)
                    error_data[key] = int(val)

            return jsonify({
                "error": {
                    "code": "INSUFFICIENT_CREDITS",
                    "message": "Not enough credits for this action",
                    "required": error_data.get("required", 0),
                    "balance": error_data.get("balance", 0),
                    "available": error_data.get("available", 0),
                }
            }), 402

        # Unknown action
        if "Unknown action" in error_msg:
            return jsonify({
                "error": {
                    "code": "INVALID_ACTION",
                    "message": error_msg,
                }
            }), 400

        # Wallet not found
        if "Wallet not found" in error_msg:
            return jsonify({
                "error": {
                    "code": "WALLET_NOT_FOUND",
                    "message": "User wallet not initialized",
                }
            }), 400

        # Generic error
        return jsonify({
            "error": {
                "code": "RESERVATION_ERROR",
                "message": str(e),
            }
        }), 400

    except Exception as e:
        print(f"[BILLING] Error reserving credits: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to reserve credits",
            }
        }), 500


# ─────────────────────────────────────────────────────────────────────────────
# MOLLIE CHECKOUT (Primary payment provider)
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/checkout", methods=["POST"])
@require_verified_email
def create_mollie_checkout():
    """
    Create a Mollie payment for purchasing credits.

    Request body:
    {
        "plan": "creator_900",   // plan code (e.g., starter_250, creator_900, studio_2200)
        "email": "user@example.com"
    }

    Alternative (backward compat):
    {
        "plan_code": "creator_900",
        "email": "user@example.com"
    }

    Response (success - 200):
    {
        "ok": true,
        "checkout_url": "https://www.mollie.com/checkout/..."
    }

    Response (error - 400):
    {
        "ok": false,
        "error_code": "PLAN_NOT_FOUND"
    }
    """
    # Check if Mollie is configured
    if not MollieService.is_available():
        print("[BILLING] Checkout failed: Mollie service unavailable")
        return jsonify({
            "ok": False,
            "error_code": "SERVICE_UNAVAILABLE",
            "error": "payment_service_unavailable",
        }), 503

    # Parse and log request body
    data = request.get_json(silent=True)
    if data is None:
        print(f"[BILLING] Checkout failed: Invalid JSON body. Raw: {request.data[:200] if request.data else '(empty)'}")
        return jsonify({
            "ok": False,
            "error_code": "VALIDATION_ERROR",
            "error": "invalid_json",
            "message": "Request body must be valid JSON",
        }), 400

    print(f"[BILLING] Checkout request: plan={data.get('plan') or data.get('plan_code')}, request_email={data.get('email', '')[:20]}...")

    # Accept both "plan" and "plan_code" for flexibility
    plan_code = data.get("plan") or data.get("plan_code")
    request_email = data.get("email", "").strip()

    # Validation with logging
    if not plan_code:
        print(f"[BILLING] Checkout validation failed: plan_code missing. Body keys: {list(data.keys())}")
        return jsonify({
            "ok": False,
            "error_code": "VALIDATION_ERROR",
            "error": "plan_required",
            "message": "plan or plan_code is required",
        }), 400

    # ══════════════════════════════════════════════════════════════════════════
    # SECURITY: Validate checkout email matches identity email (if provided)
    # ══════════════════════════════════════════════════════════════════════════
    is_valid, email_error = validate_checkout_email(g.identity, request_email)
    if not is_valid and email_error:
        print(f"[BILLING] Checkout rejected: {email_error['code']} - identity={g.identity.get('email')}, request={request_email}")
        return jsonify({
            "ok": False,
            "error_code": email_error["code"],
            "error": email_error["code"].lower(),
            "message": email_error["message"],
            "identity_email": email_error.get("identity_email"),
        }), 403

    # ALWAYS use identity email for checkout - never trust request body
    email = get_checkout_email(g.identity)
    print(f"[BILLING] Using identity email for checkout: {email[:20]}...")

    # Lookup plan by code (uses credit_grant column for credits amount)
    plan = PricingService.get_plan_by_code(plan_code)
    if not plan:
        print(f"[BILLING] Checkout failed: plan not found. code={plan_code}")
        # List available plans for debugging
        try:
            available = PricingService.get_plans(active_only=True)
            available_codes = [p.get("code") for p in available] if available else []
            print(f"[BILLING] Available plans: {available_codes}")
        except Exception as e:
            print(f"[BILLING] Could not list plans: {e}")
            available_codes = []

        return jsonify({
            "ok": False,
            "error_code": "PLAN_NOT_FOUND",
            "error": "plan_not_found",
            "plan_code": plan_code,
            "available_plans": available_codes,
        }), 400

    # Log plan details (credit_grant is mapped to "credits" key)
    print(f"[BILLING] Plan found: {plan_code} -> {plan.get('credits')} credits @ £{plan.get('price')}")

    try:
        result = MollieService.create_checkout(
            identity_id=g.identity_id,
            plan_code=plan_code,
            email=email,
        )

        return jsonify({
            "ok": True,
            "checkout_url": result["checkout_url"],
            "payment_id": result.get("payment_id"),  # For post-redirect confirmation
        })

    except MollieCreateError as e:
        print(f"[BILLING] Mollie create failed: {e.detail}")
        return jsonify({
            "ok": False,
            "error_code": "MOLLIE_CREATE_FAILED",
            "detail": e.detail,
        }), 400

    except ValueError as e:
        error_msg = str(e)
        print(f"[BILLING] Mollie checkout error: {error_msg}")

        if "not configured" in error_msg.lower():
            return jsonify({
                "ok": False,
                "error_code": "SERVICE_UNAVAILABLE",
            }), 503

        return jsonify({
            "ok": False,
            "error_code": "CHECKOUT_ERROR",
            "detail": error_msg,
        }), 400

    except Exception as e:
        print(f"[BILLING] Error creating Mollie checkout: {e}")
        return jsonify({
            "ok": False,
            "error_code": "INTERNAL_ERROR",
            "detail": str(e),
        }), 500


@bp.route("/confirm", methods=["GET"])
@require_session
@no_cache
def confirm_payment():
    """
    Confirm a Mollie payment and grant credits if paid.
    Called by frontend after redirect to ensure credits are granted
    (in case webhook is delayed).

    Query params:
    - payment_id: The Mollie payment ID (tr_xxx)

    Response (success - 200):
    {
        "ok": true,
        "status": "paid",
        "credits_granted": true,
        "message": "Credits granted"
    }

    Response (not paid - 200):
    {
        "ok": true,
        "status": "open",
        "credits_granted": false,
        "message": "Payment status is 'open'"
    }

    This endpoint is idempotent - calling it multiple times for the
    same payment will only grant credits once.
    """
    payment_id = request.args.get("payment_id")

    if not payment_id:
        return jsonify({
            "ok": False,
            "error_code": "VALIDATION_ERROR",
            "message": "payment_id is required",
        }), 400

    # Basic validation - Mollie payment IDs start with "tr_"
    if not payment_id.startswith("tr_"):
        return jsonify({
            "ok": False,
            "error_code": "VALIDATION_ERROR",
            "message": "Invalid payment_id format",
        }), 400

    try:
        result = MollieService.confirm_payment(
            payment_id=payment_id,
            identity_id=g.identity_id,
        )

        if result.get("ok"):
            return jsonify({
                "ok": True,
                "status": result.get("status"),
                "credits_granted": result.get("credits_granted", False),
                "message": result.get("message"),
            })
        else:
            return jsonify({
                "ok": False,
                "error_code": "CONFIRM_FAILED",
                "detail": result.get("error"),
            }), 400

    except Exception as e:
        print(f"[BILLING] Error confirming payment: {e}")
        return jsonify({
            "ok": False,
            "error_code": "INTERNAL_ERROR",
            "detail": str(e),
        }), 500


# ─────────────────────────────────────────────────────────────────────────────
# STRIPE CHECKOUT (Disabled when PAYMENTS_PROVIDER != 'stripe' or 'both')
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/checkout/start", methods=["POST"])
@require_verified_email
def create_checkout():
    """
    Create a Stripe checkout session.
    Email is captured during checkout and attached to identity.

    NOTE: This endpoint is disabled when PAYMENTS_PROVIDER is 'mollie' (default).
    Use /checkout for Mollie payments instead.

    Request body:
    {
        "plan_code": "starter_250",
        "email": "user@example.com",
        "success_url": "https://app.timrx.com/checkout/success",  (optional)
        "cancel_url": "https://app.timrx.com/checkout/cancel"     (optional)
    }

    Response (success - 200):
    {
        "ok": true,
        "checkout_url": "https://checkout.stripe.com/..."
    }
    """
    # Check if Stripe is enabled via PAYMENTS_PROVIDER
    _payments_provider = getattr(config, 'PAYMENTS_PROVIDER', 'mollie').lower()
    if _payments_provider not in ('stripe', 'both'):
        return jsonify({
            "error": {
                "code": "SERVICE_DISABLED",
                "message": "Stripe is disabled. Use /checkout for Mollie payments.",
            }
        }), 503

    # Check if Stripe is configured
    if not PurchaseService.is_available():
        return jsonify({
            "error": {
                "code": "SERVICE_UNAVAILABLE",
                "message": "Payment service is not configured",
            }
        }), 503

    data = request.get_json() or {}
    plan_code = data.get("plan_code")
    request_email = data.get("email", "").strip()

    # Optional: allow frontend to specify redirect URLs
    # Default to request origin if not provided
    origin = request.headers.get("Origin", "")
    success_url = data.get("success_url") or f"{origin}/checkout/success?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = data.get("cancel_url") or f"{origin}/checkout/cancel"

    # Validation
    if not plan_code:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "plan_code is required",
            }
        }), 400

    # ══════════════════════════════════════════════════════════════════════════
    # SECURITY: Validate checkout email matches identity email (if provided)
    # ══════════════════════════════════════════════════════════════════════════
    is_valid, email_error = validate_checkout_email(g.identity, request_email)
    if not is_valid and email_error:
        print(f"[BILLING] Stripe checkout rejected: {email_error['code']} - identity={g.identity.get('email')}, request={request_email}")
        return jsonify({
            "error": {
                "code": email_error["code"],
                "message": email_error["message"],
                "identity_email": email_error.get("identity_email"),
            }
        }), 403

    # ALWAYS use identity email for checkout - never trust request body
    email = get_checkout_email(g.identity)

    try:
        result = PurchaseService.start_checkout(
            identity_id=g.identity_id,
            plan_code=plan_code,
            email=email,
            success_url=success_url,
            cancel_url=cancel_url,
        )

        return jsonify({
            "ok": True,
            "checkout_url": result["checkout_url"],
        })

    except ValueError as e:
        error_msg = str(e)

        if "not found" in error_msg.lower():
            return jsonify({
                "error": {
                    "code": "INVALID_PLAN",
                    "message": error_msg,
                }
            }), 400

        if "not configured" in error_msg.lower():
            return jsonify({
                "error": {
                    "code": "SERVICE_UNAVAILABLE",
                    "message": "Payment service is not available",
                }
            }), 503

        return jsonify({
            "error": {
                "code": "CHECKOUT_ERROR",
                "message": error_msg,
            }
        }), 400

    except Exception as e:
        print(f"[BILLING] Error creating checkout: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to create checkout session",
            }
        }), 500


@bp.route("/webhook", methods=["POST"])
def stripe_webhook():
    """
    Handle Stripe webhook events.
    Processes checkout.session.completed to grant credits.

    NOTE: This endpoint is disabled when PAYMENTS_PROVIDER is 'mollie' (default).

    This endpoint receives raw POST body with Stripe-Signature header.
    No authentication required (verified via webhook signature).

    Returns 200 OK to acknowledge receipt (even on processing errors,
    to prevent Stripe from retrying indefinitely).
    """
    # Check if Stripe is enabled via PAYMENTS_PROVIDER
    _payments_provider = getattr(config, 'PAYMENTS_PROVIDER', 'mollie').lower()
    if _payments_provider not in ('stripe', 'both'):
        return jsonify({
            "ok": False,
            "error": "Stripe is disabled",
        }), 503

    # Get raw body (required for signature verification)
    payload = request.get_data()
    signature = request.headers.get("Stripe-Signature", "")

    # Process the webhook
    result = PurchaseService.process_webhook(payload, signature)

    if result.get("ok"):
        return jsonify({
            "ok": True,
            "event_type": result.get("event_type"),
            "message": result.get("message"),
        })
    else:
        # Log error but still return 200 to prevent retries
        print(f"[BILLING] Webhook processing error: {result.get('error')}")
        return jsonify({
            "ok": False,
            "error": result.get("error"),
        })


# ─────────────────────────────────────────────────────────────────────────────
# MOLLIE WEBHOOK
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/webhook/mollie", methods=["POST"])
def mollie_webhook():
    """
    Handle Mollie webhook notifications.

    Mollie sends a POST with form data containing `id` (payment ID).
    We fetch the payment details and process if paid.

    No authentication required - Mollie verifies by us fetching payment details.

    Returns 200 OK to acknowledge receipt (even on processing errors,
    to prevent Mollie from retrying indefinitely).
    """
    # Mollie sends payment ID as form data
    payment_id = request.form.get("id")

    if not payment_id:
        # Try JSON body as fallback
        data = request.get_json(silent=True) or {}
        payment_id = data.get("id")

    if not payment_id:
        print("[BILLING] Mollie webhook received without payment ID")
        return jsonify({"ok": False, "error": "Missing payment ID"}), 400

    print(f"[BILLING] Mollie webhook received: payment_id={payment_id}")

    # Process the webhook
    result = MollieService.handle_webhook(payment_id)

    if result.get("ok"):
        return jsonify({
            "ok": True,
            "status": result.get("status"),
            "message": result.get("message"),
        })
    else:
        # Log error but still return 200 to prevent retries
        print(f"[BILLING] Mollie webhook error: {result.get('error')}")
        return jsonify({
            "ok": False,
            "error": result.get("error"),
        })


# ─────────────────────────────────────────────────────────────────────────────
# PURCHASE QUERIES
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/purchase/<purchase_id>", methods=["GET"])
@require_session
@no_cache
def get_purchase(purchase_id):
    """
    Get purchase details by ID.
    Only returns purchases belonging to the current identity.

    Response (success - 200):
    {
        "ok": true,
        "purchase": {
            "id": "uuid",
            "plan_code": "starter_250",
            "plan_name": "Starter",
            "amount": 7.99,
            "currency": "GBP",
            "credits_granted": 80,
            "status": "completed",
            "purchased_at": "2024-01-15T12:00:00Z"
        }
    }
    """
    try:
        purchase = PurchaseService.get_purchase(purchase_id)

        if not purchase:
            return jsonify({
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Purchase not found",
                }
            }), 404

        # Verify ownership
        if purchase.get("identity_id") != g.identity_id:
            return jsonify({
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Purchase not found",
                }
            }), 404

        return jsonify({
            "ok": True,
            "purchase": purchase,
        })

    except Exception as e:
        print(f"[BILLING] Error fetching purchase: {e}")
        return jsonify({
            "error": {
                "code": "INTERNAL_ERROR",
                "message": "Failed to fetch purchase",
            }
        }), 500


@bp.route("/purchases", methods=["GET"])
@require_session
@no_cache
def get_purchases():
    """
    Get purchase history for the current identity.

    Query params:
    - limit: Max entries to return (default 20, max 50)
    - offset: Pagination offset (default 0)

    Response:
    {
        "ok": true,
        "purchases": [...],
        "limit": 20,
        "offset": 0
    }
    """
    try:
        limit = min(request.args.get("limit", 20, type=int), 50)
        offset = request.args.get("offset", 0, type=int)

        purchases = PurchaseService.get_purchases_for_identity(
            g.identity_id,
            limit=limit,
            offset=offset,
        )

        return jsonify({
            "ok": True,
            "purchases": purchases,
            "limit": limit,
            "offset": offset,
        })

    except Exception as e:
        print(f"[BILLING] Error fetching purchases: {e}")
        return jsonify({
            "ok": True,
            "purchases": [],
            "limit": 20,
            "offset": 0,
        })


# ─────────────────────────────────────────────────────────────────────────────
# SUBSCRIPTIONS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/subscriptions/plans", methods=["GET"])
def subscription_plans():
    """List available subscription plans."""
    plans = SubscriptionService.list_plans()
    return jsonify({"ok": True, "plans": plans})


@bp.route("/subscriptions/me", methods=["GET"])
@require_session
@no_cache
def subscription_me():
    """
    Get the current user's active subscription.

    Response (with subscription):
    { ok: true, subscription: { plan_code, status, current_period_start, current_period_end, ... } }

    Response (no subscription):
    { ok: true, subscription: null }
    """
    sub = SubscriptionService.get_active_subscription(g.identity_id)
    if not sub:
        return jsonify({"ok": True, "subscription": None})

    plan_info = SubscriptionService.get_plan_info(sub["plan_code"]) or {}
    return jsonify({
        "ok": True,
        "subscription": {
            "id": str(sub["id"]),
            "plan_code": sub["plan_code"],
            "plan_name": plan_info.get("name", sub["plan_code"]),
            "credits_per_month": plan_info.get("credits_per_month", 0),
            "cadence": plan_info.get("cadence", "monthly"),
            "status": sub["status"],
            "current_period_start": sub["current_period_start"].isoformat() if sub.get("current_period_start") else None,
            "current_period_end": sub["current_period_end"].isoformat() if sub.get("current_period_end") else None,
            "cancelled_at": sub["cancelled_at"].isoformat() if sub.get("cancelled_at") else None,
        },
    })


@bp.route("/subscriptions/payment-methods", methods=["GET"])
@require_session
@no_cache
def subscription_payment_methods():
    """
    Get payment methods that support recurring billing (subscriptions).

    Only returns methods that can establish a mandate for automatic future charges:
    - Credit/Debit Card (creditcard)
    - SEPA Direct Debit (directdebit)
    - PayPal (paypal)

    Methods like iDEAL, Bancontact, Bank Transfer do NOT support recurring
    and will NOT be returned.

    Query params:
        plan_code: Optional - to get methods for specific plan amount

    Returns:
        {
            "ok": true,
            "methods": [
                {"id": "creditcard", "description": "Credit card", ...},
                {"id": "directdebit", "description": "SEPA Direct Debit", ...},
                {"id": "paypal", "description": "PayPal", ...},
            ],
            "count": 3,
            "note": "Only payment methods supporting recurring billing are shown."
        }
    """
    from backend.services.subscription_service import SUBSCRIPTION_PLANS

    plan_code = request.args.get("plan_code", "").strip()

    # Default amount for method availability check
    amount_gbp = 9.99  # Starter monthly price

    if plan_code:
        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if plan:
            amount_gbp = plan.get("price_gbp", 9.99)

    result = MollieService.get_recurring_payment_methods(amount_gbp)

    return jsonify({
        "ok": True,
        "methods": result.get("methods", []),
        "count": result.get("count", 0),
        "note": "Only payment methods supporting recurring billing are shown.",
    })


@bp.route("/subscriptions/checkout", methods=["POST"])
@require_verified_email
def subscription_checkout():
    """
    Create a Mollie payment for a subscription plan.

    Body: { "plan_code": "creator_monthly" }
    Returns: { ok: true, checkout_url: "..." }
    """
    body = request.get_json(silent=True) or {}
    plan_code = body.get("plan_code", "").strip()

    if not plan_code:
        return jsonify({"error": "missing_plan_code", "message": "plan_code is required"}), 400

    plan = SubscriptionService.get_plan_info(plan_code)
    if not plan:
        return jsonify({"error": "invalid_plan", "message": f"Unknown plan: {plan_code}"}), 400

    # Check if user already has an active subscription
    # Block checkout to prevent duplicate active subscriptions per identity
    existing = SubscriptionService.get_active_subscription(g.identity_id)
    if existing and existing["status"] in ("active", "pending_payment"):
        msg = "You already have an active subscription. Manage it in Billing."
        if existing["status"] == "pending_payment":
            msg = "You have a subscription payment in progress. Please wait for it to complete or manage it in Billing."
        return jsonify({
            "error": "already_subscribed",
            "message": msg,
            "current_plan": existing["plan_code"],
            "current_status": existing["status"],
        }), 409

    if not MollieService.is_available():
        return jsonify({"error": "payments_unavailable", "message": "Payment service is not available"}), 503

    # ══════════════════════════════════════════════════════════════════════════
    # SECURITY: Validate checkout email matches identity email (if provided)
    # ══════════════════════════════════════════════════════════════════════════
    request_email = body.get("email", "").strip()
    is_valid, email_error = validate_checkout_email(g.identity, request_email)
    if not is_valid and email_error:
        print(f"[BILLING] Subscription checkout rejected: {email_error['code']} - identity={g.identity.get('email')}, request={request_email}")
        return jsonify({
            "ok": False,
            "error": email_error["code"].lower(),
            "error_code": email_error["code"],
            "message": email_error["message"],
            "identity_email": email_error.get("identity_email"),
        }), 403

    # ALWAYS use identity email for checkout - never trust request body
    # (g.identity is populated by @require_verified_email decorator)
    email = get_checkout_email(g.identity)

    try:
        from backend.config import config
        frontend_url = (config.FRONTEND_BASE_URL or config.PUBLIC_BASE_URL or "").rstrip("/")
        success_url = f"{frontend_url}/hub.html?checkout=success&type=subscription&plan={plan_code}"

        # Use TRUE RECURRING checkout - establishes mandate for automatic billing
        result = MollieService.create_recurring_subscription_checkout(
            identity_id=g.identity_id,
            plan_code=plan_code,
            email=email,
            success_url=success_url,
        )

        return jsonify({
            "ok": True,
            "checkout_url": result["checkout_url"],
            "payment_id": result.get("payment_id"),
            "mollie_customer_id": result.get("mollie_customer_id"),
        })

    except (MollieCreateError, ValueError) as e:
        print(f"[BILLING] Subscription checkout error: {e}")
        return jsonify({"error": "checkout_failed", "message": str(e)}), 500
    except Exception as e:
        print(f"[BILLING] Unexpected subscription checkout error: {e}")
        return jsonify({"error": "checkout_failed", "message": "Failed to create checkout"}), 500


@bp.route("/subscriptions/status", methods=["GET"])
@require_session
@no_cache
def subscription_status():
    """
    Get detailed subscription status for the current user.

    This provides comprehensive information about the user's subscription,
    including billing status, next payment date, credit allocation info,
    and whether automatic recurring is active.

    Response (with active subscription):
    {
        "ok": true,
        "has_subscription": true,
        "subscription": {
            "id": "uuid",
            "plan_code": "creator_monthly",
            "plan_name": "Creator",
            "status": "active",
            "credits_per_month": 1300,
            "cadence": "monthly",
            "tier": "creator",
            "current_period_start": "2026-02-01T...",
            "current_period_end": "2026-03-01T...",
            "next_credit_date": "2026-03-01T...",
            "billing_day": 1,
            "is_mollie_recurring": true,
            "cancelled_at": null,
            "credits_remaining_months": null  // Only for yearly
        },
        "billing": {
            "is_automatic": true,
            "next_payment_date": "2026-03-01",
            "payment_method": "card",
            "mandate_status": "valid"
        },
        "tier_perks": {...}
    }

    Response (pending payment - SEPA processing):
    {
        "ok": true,
        "has_subscription": true,
        "subscription": {
            "status": "processing",
            "status_message": "Your payment is being processed. SEPA payments typically take 1-2 business days. Credits will be unlocked once payment is confirmed.",
            ...
        },
        "billing": {
            "payment_pending": true,
            ...
        }
    }

    Response (no subscription):
    {
        "ok": true,
        "has_subscription": false,
        "subscription": null,
        "tier_perks": {...}
    }
    """
    from backend.services.subscription_service import SUBSCRIPTION_PLANS

    # Get active subscription (includes pending_payment status)
    sub = SubscriptionService.get_active_subscription(g.identity_id)

    # Also check for pending_payment subscriptions if no active found
    if not sub:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT * FROM {Tables.SUBSCRIPTIONS}
                        WHERE identity_id = %s
                          AND status = 'pending_payment'
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (g.identity_id,),
                    )
                    sub = cur.fetchone()
        except Exception as e:
            print(f"[BILLING] Error checking pending subscriptions: {e}")

    # Get tier perks (works even without subscription - returns free tier)
    tier_perks = SubscriptionService.get_tier_perks(g.identity_id)

    if not sub:
        return jsonify({
            "ok": True,
            "has_subscription": False,
            "subscription": None,
            "billing": None,
            "tier_perks": tier_perks,
        })

    plan_code = sub.get("plan_code")
    plan_info = SUBSCRIPTION_PLANS.get(plan_code, {})
    db_status = sub.get("status")

    # ═══════════════════════════════════════════════════════════════════════════
    # SEPA PENDING PAYMENT HANDLING
    # Show "processing" status with user-friendly message
    # ═══════════════════════════════════════════════════════════════════════════
    display_status = db_status
    status_message = None

    if db_status == "pending_payment":
        display_status = "processing"
        status_message = (
            "Your payment is being processed. SEPA and bank transfer payments "
            "typically take 1-2 business days. Your credits will be unlocked "
            "automatically once payment is confirmed."
        )

    # Build subscription response
    subscription_data = {
        "id": str(sub["id"]),
        "plan_code": plan_code,
        "plan_name": plan_info.get("name", plan_code),
        "status": display_status,
        "status_message": status_message,
        "credits_per_month": plan_info.get("credits_per_month", 0),
        "cadence": plan_info.get("cadence", "monthly"),
        "tier": plan_info.get("tier", "free"),
        "current_period_start": sub["current_period_start"].isoformat() if sub.get("current_period_start") else None,
        "current_period_end": sub["current_period_end"].isoformat() if sub.get("current_period_end") else None,
        "next_credit_date": sub["next_credit_date"].isoformat() if sub.get("next_credit_date") else None,
        "billing_day": sub.get("billing_day"),
        "is_mollie_recurring": sub.get("is_mollie_recurring", False),
        "cancelled_at": sub["cancelled_at"].isoformat() if sub.get("cancelled_at") else None,
        "credits_remaining_months": sub.get("credits_remaining_months"),
    }

    # Build billing info
    billing_data = {
        "is_automatic": sub.get("is_mollie_recurring", False),
        "next_payment_date": None,
        "payment_method": None,
        "mandate_status": None,
        "payment_pending": db_status == "pending_payment",
    }

    # If using Mollie recurring and subscription is active, fetch additional billing details
    if sub.get("is_mollie_recurring") and sub.get("mollie_customer_id") and db_status == "active":
        try:
            mollie_customer_id = sub.get("mollie_customer_id")
            mollie_sub_id = sub.get("provider_subscription_id")

            # Get subscription status from Mollie
            if mollie_sub_id:
                mollie_status = MollieService.get_mollie_subscription_status(
                    mollie_customer_id, mollie_sub_id
                )
                if mollie_status:
                    billing_data["next_payment_date"] = mollie_status.get("next_payment_date")
                    billing_data["mollie_status"] = mollie_status.get("status")

            # Get mandate info
            mandate = MollieService.get_mandate_for_customer(mollie_customer_id)
            if mandate:
                billing_data["mandate_status"] = mandate.get("status")
                billing_data["payment_method"] = mandate.get("method")

        except Exception as e:
            print(f"[BILLING] Error fetching Mollie billing details: {e}")

    return jsonify({
        "ok": True,
        "has_subscription": True,
        "subscription": subscription_data,
        "billing": billing_data,
        "tier_perks": tier_perks,
    })


@bp.route("/subscriptions/summary", methods=["GET"])
@require_session
@no_cache
def subscription_summary():
    """
    Get a concise subscription summary for the current user.

    This endpoint provides the essential subscription fields for UI display
    and decision-making. Use /subscriptions/status for full details.

    Response (with subscription):
    {
        "ok": true,
        "has_subscription": true,
        "status": "active",              // processing, active, past_due, cancelled, suspended, expired
        "plan_code": "creator_monthly",
        "interval": "monthly",           // monthly or yearly
        "next_credit_date": "2026-03-01T00:00:00Z",
        "ends_at": null,                 // Set when cancelled - when access truly ends
        "prepaid_until": null,           // Yearly only - when prepaid period ends
        "credits_remaining_months": null, // Yearly only - months of credits left
        "last_payment_status": "paid",   // pending, paid, failed
        "last_payment_at": "2026-02-01T12:00:00Z",
        "suspend_reason": null           // Set if suspended (refunded, charged_back, fraud, manual)
    }

    Response (no subscription):
    {
        "ok": true,
        "has_subscription": false
    }
    """
    # Get any subscription (active, cancelled, pending_payment, suspended)
    sub = None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT s.*,
                           (SELECT sc.payment_status FROM {Tables.SUBSCRIPTION_CYCLES} sc
                            WHERE sc.subscription_id = s.id
                            ORDER BY sc.created_at DESC LIMIT 1) as last_payment_status,
                           (SELECT sc.created_at FROM {Tables.SUBSCRIPTION_CYCLES} sc
                            WHERE sc.subscription_id = s.id
                            ORDER BY sc.created_at DESC LIMIT 1) as last_payment_at
                    FROM {Tables.SUBSCRIPTIONS} s
                    WHERE s.identity_id = %s
                      AND s.status IN ('active', 'cancelled', 'pending_payment', 'suspended', 'past_due')
                    ORDER BY
                        CASE s.status
                            WHEN 'active' THEN 1
                            WHEN 'pending_payment' THEN 2
                            WHEN 'cancelled' THEN 3
                            WHEN 'suspended' THEN 4
                            WHEN 'past_due' THEN 5
                        END,
                        s.created_at DESC
                    LIMIT 1
                    """,
                    (g.identity_id,),
                )
                sub = cur.fetchone()
    except Exception as e:
        print(f"[BILLING] Error fetching subscription summary: {e}")
        return jsonify({"ok": False, "error": "internal_error"}), 500

    if not sub:
        return jsonify({
            "ok": True,
            "has_subscription": False,
        })

    # Map DB status to user-friendly status
    db_status = sub.get("status")
    display_status = db_status
    if db_status == "pending_payment":
        display_status = "processing"

    # Determine interval from plan_code
    plan_code = sub.get("plan_code", "")
    interval = "yearly" if plan_code.endswith("_yearly") else "monthly"

    return jsonify({
        "ok": True,
        "has_subscription": True,
        "status": display_status,
        "plan_code": plan_code,
        "interval": interval,
        "next_credit_date": sub["next_credit_date"].isoformat() if sub.get("next_credit_date") else None,
        "ends_at": sub["ends_at"].isoformat() if sub.get("ends_at") else None,
        "prepaid_until": sub["prepaid_until"].isoformat() if sub.get("prepaid_until") else None,
        "credits_remaining_months": sub.get("credits_remaining_months"),
        "last_payment_status": sub.get("last_payment_status"),
        "last_payment_at": sub["last_payment_at"].isoformat() if sub.get("last_payment_at") else None,
        "suspend_reason": sub.get("suspend_reason"),
    })


@bp.route("/subscriptions/cancel", methods=["POST"])
@require_session
def subscription_cancel():
    """Cancel the current subscription at period end."""
    sub = SubscriptionService.get_active_subscription(g.identity_id)
    if not sub or sub["status"] != "active":
        return jsonify({"error": "no_active_subscription", "message": "No active subscription to cancel"}), 404

    sub_id = str(sub["id"])

    # If this is a Mollie recurring subscription, cancel the Mollie subscription first
    # This stops future automatic charges
    if sub.get("is_mollie_recurring") and sub.get("mollie_customer_id") and sub.get("provider_subscription_id"):
        try:
            mollie_cancelled = MollieService.cancel_mollie_subscription(
                mollie_customer_id=sub["mollie_customer_id"],
                mollie_subscription_id=sub["provider_subscription_id"],
            )
            if mollie_cancelled:
                print(f"[BILLING] Cancelled Mollie subscription {sub['provider_subscription_id']}")
            else:
                print(f"[BILLING] Warning: Failed to cancel Mollie subscription {sub['provider_subscription_id']}")
        except Exception as e:
            print(f"[BILLING] Error cancelling Mollie subscription: {e}")
            # Continue with local cancellation even if Mollie fails

    ok = SubscriptionService.cancel_subscription_with_email(sub_id)
    if ok:
        return jsonify({
            "ok": True,
            "message": "Subscription cancelled. You'll keep access until the end of your billing period.",
            "period_end": sub["current_period_end"].isoformat() if sub.get("current_period_end") else None,
        })
    return jsonify({"error": "cancel_failed", "message": "Failed to cancel subscription"}), 500


# ─────────────────────────────────────────────────────────────────────────────
# INVOICE / RECEIPT PDF DOWNLOADS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/invoice/<invoice_id>/pdf", methods=["GET"])
@require_session
@no_cache
def download_invoice_pdf(invoice_id):
    """
    Download invoice PDF.  Ownership enforced via identity_id.

    Returns a 302 redirect to a presigned S3 URL (valid for 1 hour).
    """
    from backend.services.invoicing_service import InvoicingService
    from backend.services.s3_service import presign_s3_key

    invoice = InvoicingService.get_invoice(invoice_id)
    if not invoice:
        return jsonify({"error": {"code": "NOT_FOUND", "message": "Invoice not found"}}), 404

    # Ownership check
    if str(invoice.get("identity_id")) != g.identity_id:
        return jsonify({"error": {"code": "NOT_FOUND", "message": "Invoice not found"}}), 404

    s3_key = invoice.get("pdf_s3_key")
    if not s3_key:
        return jsonify({"error": {"code": "PDF_NOT_AVAILABLE", "message": "Invoice PDF not yet generated"}}), 404

    presigned = presign_s3_key(s3_key, expires_in=3600)
    if not presigned:
        return jsonify({"error": {"code": "INTERNAL_ERROR", "message": "Failed to generate download URL"}}), 500

    from flask import redirect
    return redirect(presigned)


@bp.route("/receipt/<receipt_id>/pdf", methods=["GET"])
@require_session
@no_cache
def download_receipt_pdf(receipt_id):
    """
    Download receipt PDF.  Ownership enforced via identity_id.

    Returns a 302 redirect to a presigned S3 URL (valid for 1 hour).
    """
    from backend.services.invoicing_service import InvoicingService
    from backend.services.s3_service import presign_s3_key

    receipt = InvoicingService.get_receipt(receipt_id)
    if not receipt:
        return jsonify({"error": {"code": "NOT_FOUND", "message": "Receipt not found"}}), 404

    # Ownership check
    if str(receipt.get("identity_id")) != g.identity_id:
        return jsonify({"error": {"code": "NOT_FOUND", "message": "Receipt not found"}}), 404

    s3_key = receipt.get("pdf_s3_key")
    if not s3_key:
        return jsonify({"error": {"code": "PDF_NOT_AVAILABLE", "message": "Receipt PDF not yet generated"}}), 404

    presigned = presign_s3_key(s3_key, expires_in=3600)
    if not presigned:
        return jsonify({"error": {"code": "INTERNAL_ERROR", "message": "Failed to generate download URL"}}), 500

    from flask import redirect
    return redirect(presigned)


# ─────────────────────────────────────────────────────────────
# Debug endpoint for testing PDF generation and email
# ─────────────────────────────────────────────────────────────
@bp.route("/debug/test-invoice-pdf", methods=["POST"])
def debug_test_invoice_pdf():
    """
    Debug endpoint to test PDF generation and optionally send test email.

    Request body:
    {
        "send_email": false,           // Optional: actually send email
        "email": "test@example.com"    // Required if send_email=true
    }

    Returns:
    {
        "ok": true,
        "invoice_pdf_bytes": 12345,
        "receipt_pdf_bytes": 12345,
        "logo_loaded": true,
        "logo_bytes": 5000,
        "errors": []
    }
    """
    from backend.config import config

    # Only allow in dev mode or with admin token
    admin_token = request.headers.get("X-Admin-Token") or request.args.get("admin_token")
    if not config.IS_DEV and admin_token != config.ADMIN_TOKEN:
        return jsonify({"error": "Forbidden - requires dev mode or admin token"}), 403

    errors = []
    result = {
        "ok": True,
        "invoice_pdf_bytes": 0,
        "receipt_pdf_bytes": 0,
        "logo_loaded": False,
        "logo_bytes": 0,
        "errors": errors,
    }

    # Test logo loading
    logo = None
    try:
        from backend.services.invoicing_service import _load_logo
        logo = _load_logo()
        result["logo_loaded"] = logo is not None
        result["logo_bytes"] = len(logo) if logo else 0
    except Exception as e:
        errors.append(f"Logo load error: {e}")

    # Test PDF generation with mock data
    mock_invoice = {
        "id": "test-invoice-id",
        "invoice_number": "INV-2026-TEST",
        "issued_at": None,
        "customer_email": "test@example.com",
        "subtotal": 9.99,
        "tax_amount": 0,
        "total": 9.99,
    }
    mock_receipt = {
        "id": "test-receipt-id",
        "receipt_number": "RCPT-2026-TEST",
        "paid_at": None,
        "payment_method": "mollie",
        "currency": "GBP",
        "amount_paid": 9.99,
    }
    mock_items = [
        {
            "description": "Test Plan - 100 Credits",
            "quantity": 1,
            "unit_price": 9.99,
            "total": 9.99,
        }
    ]

    invoice_pdf = None
    receipt_pdf = None

    try:
        from backend.services.invoicing_service import InvoicingService
        invoice_pdf = InvoicingService.generate_invoice_pdf(mock_invoice, mock_items)
        result["invoice_pdf_bytes"] = len(invoice_pdf) if invoice_pdf else 0
    except Exception as e:
        errors.append(f"Invoice PDF error: {e}")
        result["ok"] = False

    try:
        from backend.services.invoicing_service import InvoicingService
        receipt_pdf = InvoicingService.generate_receipt_pdf(mock_receipt, mock_invoice)
        result["receipt_pdf_bytes"] = len(receipt_pdf) if receipt_pdf else 0
    except Exception as e:
        errors.append(f"Receipt PDF error: {e}")
        result["ok"] = False

    # Optionally send test email
    data = request.get_json(silent=True) or {}
    if data.get("send_email") and data.get("email"):
        try:
            from backend.emailer import send_invoice_email
            send_invoice_email(
                to_email=data["email"],
                invoice_number="INV-2026-TEST",
                receipt_number="RCPT-2026-TEST",
                plan_name="Test Plan",
                credits=100,
                amount_gbp=9.99,
                invoice_pdf=invoice_pdf or b"",
                receipt_pdf=receipt_pdf or b"",
                logo_bytes=logo,
            )
            result["email_sent"] = True
            result["email_to"] = data["email"]
        except Exception as e:
            errors.append(f"Email send error: {e}")
            result["email_sent"] = False

    return jsonify(result)


@bp.route("/debug/email-preview", methods=["GET"])
def debug_email_preview():
    """
    Debug endpoint to preview email templates in browser (HTML render, no sending).

    Query params:
        ?template=magic_code|purchase_receipt|invoice|admin

    Returns rendered HTML for browser preview.
    """
    from backend.config import config
    from flask import Response

    # Only allow in dev mode or with admin token
    admin_token = request.headers.get("X-Admin-Token") or request.args.get("admin_token")
    if not config.IS_DEV and admin_token != config.ADMIN_TOKEN:
        return jsonify({"error": "Forbidden - requires dev mode or admin token"}), 403

    template = request.args.get("template", "purchase_receipt")

    from backend.emailer import (
        render_email_html,
        render_detail_card,
        render_highlight_box,
        render_amount_display,
        TEXT_PRIMARY,
        TEXT_SECONDARY,
    )

    if template == "magic_code":
        code_box = render_highlight_box(
            f'<span style="font-size:32px;font-weight:700;letter-spacing:8px;color:{TEXT_PRIMARY};font-family:\'Courier New\',Courier,monospace;">ABC123</span>'
        )
        body_html = f'''
            {code_box}
            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:24px;">
                <tr>
                    <td>
                        <p style="margin:0 0 8px 0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                            This code expires in <strong style="color:{TEXT_PRIMARY};">15 minutes</strong>.
                        </p>
                        <p style="margin:0;font-size:13px;line-height:1.6;color:#888888;font-family:Arial,Helvetica,sans-serif;">
                            If you didn't request this code, you can safely ignore this email.
                        </p>
                    </td>
                </tr>
            </table>
        '''
        html = render_email_html(
            title="Your Access Code",
            intro="Use the code below to sign in to your TimrX account:",
            body_html=body_html,
            logo_cid=None,  # No logo in preview
        )

    elif template == "invoice":
        amount_display = render_amount_display("9.99", "GBP", "Paid February 5, 2026")
        ref_card = render_detail_card([
            ("Invoice number", "INV-2026-0001"),
            ("Receipt number", "RCPT-2026-0001"),
        ], header="Reference")
        summary_card = render_detail_card([
            ("Starter Plan", "&pound;9.99"),
            ("Credits added", "100"),
            ("Amount paid", "&pound;9.99"),
        ], header="Summary")
        body_html = f'''
            {amount_display}
            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:24px;">
                <tr><td style="padding-bottom:16px;">{ref_card}</td></tr>
                <tr><td>{summary_card}</td></tr>
                <tr>
                    <td style="padding-top:24px;">
                        <p style="margin:0 0 8px 0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                            Your invoice and receipt PDFs are attached to this email.
                        </p>
                        <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                            Your credits are now available in your account.
                        </p>
                    </td>
                </tr>
            </table>
        '''
        html = render_email_html(
            title="Purchase Confirmed",
            intro="Thank you for your purchase. Here's your receipt with invoice and receipt documents attached.",
            body_html=body_html,
            logo_cid=None,
            footer_extra="Your PDF documents are attached to this email.",
        )

    elif template == "admin":
        data_card = render_detail_card([
            ("Identity ID", "id_abc123xyz"),
            ("Email", "user@example.com"),
            ("Plan", "Starter Plan"),
            ("Credits", "100"),
            ("Amount", "9.99 GBP"),
        ], header="Details")
        body_html = f'''
            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                    <td style="padding-bottom:16px;">
                        <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">A user has purchased the Starter Plan.</p>
                    </td>
                </tr>
                <tr><td>{data_card}</td></tr>
            </table>
        '''
        html = render_email_html(
            title="New Purchase",
            intro="Admin notification from TimrX system.",
            body_html=body_html,
            logo_cid=None,
            footer_extra="This is an automated admin notification.",
        )

    else:  # purchase_receipt (default)
        amount_display = render_amount_display("9.99", "GBP", "Paid February 5, 2026")
        summary_card = render_detail_card([
            ("Starter Plan", "&pound;9.99"),
            ("Credits added", "100"),
            ("Amount paid", "&pound;9.99"),
        ], header="Summary")
        body_html = f'''
            {amount_display}
            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:24px;">
                <tr><td>{summary_card}</td></tr>
                <tr>
                    <td style="padding-top:24px;">
                        <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                            Your credits are now available in your account.
                        </p>
                    </td>
                </tr>
            </table>
        '''
        html = render_email_html(
            title="Purchase Confirmed",
            intro="Thank you for your purchase. Here's your receipt.",
            body_html=body_html,
            logo_cid=None,
        )

    return Response(html, mimetype="text/html")


# ─────────────────────────────────────────────────────────────────────────────
# EXPENSE GUARDRAILS STATUS
# ─────────────────────────────────────────────────────────────────────────────

@bp.route("/guardrails/status", methods=["GET"])
@require_session
@no_cache
def expense_guardrails_status():
    """
    Get current expense guardrails status.

    Returns:
    {
        "ok": true,
        "guardrails": {
            "enabled": true,
            "active_jobs": 2,
            "max_concurrent_jobs": 5,
            "limits": {...},
            "costs": {...}
        }
    }
    """
    status = ExpenseGuard.get_status()
    return jsonify({
        "ok": True,
        "guardrails": status,
    })


# ─────────────────────────────────────────────────────────────────────────────
# FX RATES (Public - for multi-currency price display)
# ─────────────────────────────────────────────────────────────────────────────

import time
import requests as http_requests

# In-memory cache for FX rates (24 hours)
_fx_cache = {
    "rates": None,
    "fetched_at": 0,
}
_FX_CACHE_TTL = 86400  # 24 hours in seconds

# Fallback rates if API fails (updated periodically)
_FX_FALLBACK_RATES = {
    "USD": 1.26,
    "EUR": 1.17,
    "CAD": 1.71,
    "AUD": 1.93,
}


def _fetch_fx_rates():
    """
    Fetch current FX rates from exchangerate-api.com (free tier).
    Returns rates dict with GBP as base, or None on failure.
    """
    try:
        # Free API - no key required for GBP base
        resp = http_requests.get(
            "https://api.exchangerate-api.com/v4/latest/GBP",
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            if "rates" in data:
                return {
                    "USD": round(data["rates"].get("USD", 1.26), 4),
                    "EUR": round(data["rates"].get("EUR", 1.17), 4),
                    "CAD": round(data["rates"].get("CAD", 1.71), 4),
                    "AUD": round(data["rates"].get("AUD", 1.93), 4),
                }
    except Exception as e:
        print(f"[FX] Failed to fetch rates: {e}")
    return None


def get_fx_rates_cached():
    """
    Get FX rates with 24h caching.
    Returns cached rates or fetches fresh rates if cache expired.
    Falls back to hardcoded rates if fetch fails.
    """
    global _fx_cache
    now = time.time()

    # Check if cache is still valid
    if _fx_cache["rates"] and (now - _fx_cache["fetched_at"]) < _FX_CACHE_TTL:
        return _fx_cache["rates"]

    # Try to fetch fresh rates
    rates = _fetch_fx_rates()
    if rates:
        _fx_cache["rates"] = rates
        _fx_cache["fetched_at"] = now
        print(f"[FX] Rates updated: {rates}")
        return rates

    # If fetch failed but we have stale cache, use it
    if _fx_cache["rates"]:
        print("[FX] Using stale cached rates")
        return _fx_cache["rates"]

    # Last resort: hardcoded fallback
    print("[FX] Using fallback rates")
    return _FX_FALLBACK_RATES


@bp.route("/public/fx", methods=["GET"])
def public_fx_rates():
    """
    Public endpoint for FX rates (no auth required).

    Used by frontend to display estimated prices in user's local currency.
    All billing remains in GBP - this is display only.

    Returns:
    {
        "ok": true,
        "base": "GBP",
        "rates": {
            "USD": 1.26,
            "EUR": 1.17,
            "CAD": 1.71,
            "AUD": 1.93
        },
        "as_of": "2026-02-11"
    }
    """
    from datetime import date

    rates = get_fx_rates_cached()

    response = jsonify({
        "ok": True,
        "base": "GBP",
        "rates": rates,
        "as_of": date.today().isoformat(),
    })

    # Allow caching by CDN/browser for 1 hour (rates don't change fast)
    response.headers["Cache-Control"] = "public, max-age=3600"
    return response
