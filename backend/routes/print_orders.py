"""
/api/print-orders routes — TimrX print-on-demand fulfillment.

Endpoints:
  POST /api/print-orders               — create new order, returns checkout_url
  GET  /api/print-orders               — list current user's orders
  GET  /api/print-orders/<id_or_num>   — get order status
  POST /api/print-orders/webhook/mollie  — Mollie payment.paid webhook
  POST /api/print-orders/webhook/paypal  — PayPal payment.capture.completed webhook
  POST /api/print-orders/quote         — server-side price quote (no DB write)
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict

from flask import Blueprint, g, jsonify, request

from backend.config import config
from backend.middleware import require_session
from backend.services import print_order_service
from backend.services.paypal_service import PayPalService
from backend.services.print_order_pricing import compute as compute_price, PriceError, pick_currency

bp = Blueprint("print_orders", __name__)

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
POSTAL_RE = re.compile(r"^[A-Za-z0-9 \-]{2,12}$")

# Allowed countries for shipping (mirror frontend select)
ALLOWED_COUNTRIES = {"US", "CA", "GB", "EU", "AU", "JP", "OTHER"}
ALLOWED_SPEEDS    = {"standard", "express", "priority"}
ALLOWED_PROVIDERS = {"mollie", "paypal"}


def _err(code: str, msg: str, status: int = 400):
    return jsonify({"ok": False, "error": {"code": code, "message": msg}}), status


def _validate_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate the request body. Raises ValueError on bad input."""
    if not isinstance(data, dict):
        raise ValueError("Request body must be a JSON object")

    provider = (data.get("provider") or "mollie").lower()
    if provider not in ALLOWED_PROVIDERS:
        raise ValueError(f"Unsupported provider: {provider}")

    spec = data.get("spec") or {}
    if not isinstance(spec, dict):
        raise ValueError("Invalid spec")

    shipping = data.get("shipping") or {}
    if not isinstance(shipping, dict):
        raise ValueError("Invalid shipping")

    # Shipping validation
    for k in ("first_name", "last_name", "email", "address", "city", "postal"):
        v = (shipping.get(k) or "").strip()
        if not v:
            raise ValueError(f"Shipping field '{k}' is required")
        shipping[k] = v[:120]  # cap length

    if not EMAIL_RE.match(shipping["email"]):
        raise ValueError("Invalid shipping email")

    if not POSTAL_RE.match(shipping["postal"]):
        raise ValueError("Invalid postal code")

    country = (shipping.get("country") or "").upper().strip()
    if country not in ALLOWED_COUNTRIES:
        raise ValueError(f"Country '{country}' is not supported")
    shipping["country"] = country

    speed = (shipping.get("speed") or "standard").lower()
    if speed not in ALLOWED_SPEEDS:
        raise ValueError(f"Speed '{speed}' is not supported")
    shipping["speed"] = speed

    shipping["notes"] = (shipping.get("notes") or "")[:1000]

    model = data.get("model") or {}
    if not isinstance(model, dict):
        raise ValueError("Invalid model")

    return {
        "provider": provider,
        "spec": spec,
        "shipping": shipping,
        "model": {
            "id":         (model.get("id") or "")[:128] or None,
            "name":       (model.get("name") or "")[:255] or None,
            "glb_url":    (model.get("glb_url") or "")[:1024] or None,
            "thumb_url":  (model.get("thumb_url") or "")[:1024] or None,
        },
    }


# ─────────────────────────────────────────────────────────────
# POST /api/print-orders/quote  (auth required)
# ─────────────────────────────────────────────────────────────
@bp.route("/quote", methods=["POST", "OPTIONS"])
@require_session
def quote():
    """Server-side price recomputation, returned without persisting an order."""
    if request.method == "OPTIONS":
        return ("", 204)

    data = request.get_json(silent=True) or {}
    spec = data.get("spec") or {}
    shipping = data.get("shipping") or {}
    country = (shipping.get("country") or data.get("country") or "").upper()
    speed   = (shipping.get("speed") or data.get("speed") or "standard").lower()

    try:
        price = compute_price(spec=spec, country=country, speed=speed)
    except PriceError as e:
        return _err("BAD_SPEC", str(e), 400)
    except Exception as e:
        return _err("QUOTE_FAILED", str(e), 500)

    providers = []
    if config.MOLLIE_CONFIGURED:
        providers.append("mollie")
    if config.PAYPAL_CONFIGURED:
        providers.append("paypal")

    return jsonify({
        "ok": True,
        "currency_detected": pick_currency(country),
        "providers_available": providers,
        "quote": price.to_dict(),
    })


# ─────────────────────────────────────────────────────────────
# POST /api/print-orders  (auth required, email verified)
# ─────────────────────────────────────────────────────────────
@bp.route("", methods=["POST", "OPTIONS"])
@bp.route("/", methods=["POST", "OPTIONS"])
@require_session
def create():
    if request.method == "OPTIONS":
        return ("", 204)

    identity = g.identity
    if not identity.get("email") or not identity.get("email_verified"):
        return _err("EMAIL_NOT_VERIFIED",
                    "Please verify your email before ordering a print.", 403)
    customer_email = identity["email"]

    try:
        payload = _validate_payload(request.get_json(silent=True) or {})
    except ValueError as e:
        return _err("INVALID_INPUT", str(e), 400)

    try:
        result = print_order_service.create_order(
            identity_id=g.identity_id,
            customer_email=customer_email,
            provider=payload["provider"],
            spec=payload["spec"],
            shipping=payload["shipping"],
            model=payload["model"],
        )
    except print_order_service.PrintOrderError as e:
        return _err("ORDER_FAILED", str(e), 400)
    except Exception as e:
        print(f"[PRINT-ORDER] create unexpected error: {e}")
        return _err("ORDER_FAILED", "Could not create order", 500)

    return jsonify({
        "ok": True,
        "order_id":     result["order_id"],
        "order_number": result["order_number"],
        "checkout_url": result["checkout_url"],
        "total":        result["total"],
        "currency":     result["currency"],
        "provider":     result["provider"],
    })


# ─────────────────────────────────────────────────────────────
# GET /api/print-orders  (auth required)
# ─────────────────────────────────────────────────────────────
@bp.route("", methods=["GET"])
@bp.route("/", methods=["GET"])
@require_session
def list_orders():
    limit = int(request.args.get("limit", 20))
    return jsonify({
        "ok": True,
        "orders": print_order_service.list_my_orders(g.identity_id, limit=limit),
    })


# ─────────────────────────────────────────────────────────────
# GET /api/print-orders/<id_or_number>  (auth required)
# ─────────────────────────────────────────────────────────────
@bp.route("/<order_ref>", methods=["GET"])
@require_session
def get_one(order_ref: str):
    order = print_order_service.get_order_public(order_ref, g.identity_id)
    if not order:
        return _err("NOT_FOUND", "Order not found", 404)
    return jsonify({"ok": True, "order": order})


# ─────────────────────────────────────────────────────────────
# POST /api/print-orders/webhook/mollie  (no auth — Mollie origin)
# ─────────────────────────────────────────────────────────────
@bp.route("/webhook/mollie", methods=["POST"])
def webhook_mollie():
    # Mollie sends application/x-www-form-urlencoded with one field 'id'
    payment_id = (request.form.get("id") or "").strip()
    if not payment_id:
        # Some Mollie test deliveries send JSON — handle both gracefully
        body = request.get_json(silent=True) or {}
        payment_id = (body.get("id") or "").strip()
    if not payment_id:
        return jsonify({"ok": False, "error": "missing id"}), 400

    try:
        result = print_order_service.handle_mollie_webhook(payment_id)
    except Exception as e:
        print(f"[PRINT-ORDER] mollie webhook unexpected: {e}")
        # Return 200 so Mollie does not endlessly retry on transient bugs;
        # we'll see it in logs and process via admin tools if needed.
        return jsonify({"ok": True, "swallowed": True}), 200

    return jsonify(result), 200


# ─────────────────────────────────────────────────────────────
# POST /api/print-orders/webhook/paypal  (no auth — PayPal origin, signed)
# ─────────────────────────────────────────────────────────────
@bp.route("/webhook/paypal", methods=["POST"])
def webhook_paypal():
    raw = request.get_data(cache=False, as_text=False) or b""

    # Verify signature using PayPal API (defensive: skip verification when
    # PAYPAL_WEBHOOK_ID is not set, but log loudly)
    if config.PAYPAL_WEBHOOK_ID:
        try:
            ok = PayPalService.verify_webhook(
                headers=dict(request.headers),
                body=raw,
                webhook_id=config.PAYPAL_WEBHOOK_ID,
            )
        except Exception as e:
            print(f"[PAYPAL] verify_webhook crashed: {e}")
            ok = False
        if not ok:
            return jsonify({"ok": False, "error": "signature verification failed"}), 401
    else:
        print("[PAYPAL] WARN: PAYPAL_WEBHOOK_ID not set — accepting unverified webhook")

    try:
        event = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    try:
        result = print_order_service.handle_paypal_webhook(event)
    except Exception as e:
        print(f"[PRINT-ORDER] paypal webhook unexpected: {e}")
        return jsonify({"ok": True, "swallowed": True}), 200

    return jsonify(result), 200
