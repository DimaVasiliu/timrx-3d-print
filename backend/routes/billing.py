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

from backend.middleware import require_session, require_email, no_cache
from backend.services.pricing_service import PricingService
from backend.services.wallet_service import WalletService
from backend.services.reservation_service import ReservationService
from backend.services.purchase_service import PurchaseService
from backend.services.mollie_service import MollieService, MollieCreateError

bp = Blueprint("billing", __name__)

# Cache TTL for pricing data (5 minutes)
CACHE_TTL_SECONDS = 300


def _add_cache_headers(response, max_age: int = CACHE_TTL_SECONDS):
    """Add caching headers to response."""
    response.headers["Cache-Control"] = f"public, max-age={max_age}"
    return response


@bp.route("/plans", methods=["GET"])
def get_plans():
    """
    List available credit plans for purchase.
    Returns active plans with prices, credits, and perks.

    Response:
    {
        "ok": true,
        "plans": [
            {
                "id": "uuid",
                "code": "starter_80",
                "name": "Starter",
                "price_gbp": 7.99,
                "credits": 80,
                "perks": {
                    "priority": false,
                    "retention_days": 30
                }
            },
            ...
        ]
    }
    """
    try:
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
@require_session
def create_mollie_checkout():
    """
    Create a Mollie payment for purchasing credits.

    Request body:
    {
        "plan": "creator_300",   // plan code (e.g., starter_80, creator_300, studio_600)
        "email": "user@example.com"
    }

    Alternative (backward compat):
    {
        "plan_code": "creator_300",
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

    print(f"[BILLING] Checkout request: plan={data.get('plan') or data.get('plan_code')}, email={data.get('email', '')[:20]}...")

    # Accept both "plan" and "plan_code" for flexibility
    plan_code = data.get("plan") or data.get("plan_code")
    email = data.get("email", "").strip()

    # Validation with logging
    if not plan_code:
        print(f"[BILLING] Checkout validation failed: plan_code missing. Body keys: {list(data.keys())}")
        return jsonify({
            "ok": False,
            "error_code": "VALIDATION_ERROR",
            "error": "plan_required",
            "message": "plan or plan_code is required",
        }), 400

    if not email:
        # Try to get email from identity if available
        identity_email = getattr(g, 'identity_email', None)
        if identity_email:
            email = identity_email
            print(f"[BILLING] Using identity email: {email[:20]}...")
        else:
            print(f"[BILLING] Checkout validation failed: email missing for plan={plan_code}")
            return jsonify({
                "ok": False,
                "error_code": "VALIDATION_ERROR",
                "error": "email_required",
                "message": "email is required",
            }), 400

    # Basic email validation
    if "@" not in email or "." not in email:
        print(f"[BILLING] Checkout validation failed: invalid email format for plan={plan_code}")
        return jsonify({
            "ok": False,
            "error_code": "VALIDATION_ERROR",
            "error": "invalid_email",
            "message": "Invalid email format",
        }), 400

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
@require_session
def create_checkout():
    """
    Create a Stripe checkout session.
    Email is captured during checkout and attached to identity.

    NOTE: This endpoint is disabled when PAYMENTS_PROVIDER is 'mollie' (default).
    Use /checkout for Mollie payments instead.

    Request body:
    {
        "plan_code": "starter_80",
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
    email = data.get("email", "").strip()

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

    if not email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "email is required",
            }
        }), 400

    # Basic email validation
    if "@" not in email or "." not in email:
        return jsonify({
            "error": {
                "code": "VALIDATION_ERROR",
                "message": "Invalid email format",
            }
        }), 400

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
            "plan_code": "starter_80",
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
