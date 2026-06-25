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

from flask import Blueprint, g, jsonify, redirect, request

from backend.config import config
from backend.middleware import require_admin, require_session
from backend.services import print_offer_service, print_order_service, s3_service
from backend.services.paypal_service import PayPalService
from backend.services.print_order_archive import get_admin_download_target
from backend.services.print_order_pricing import compute as compute_price, PriceError, pick_currency
from backend.services.download_link_signer import verify as _verify_download_link

bp = Blueprint("print_orders", __name__)

EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
POSTAL_RE = re.compile(r"^[A-Za-z0-9 \-]{2,12}$")
MOLLIE_PAYMENT_ID_RE = re.compile(r"^[A-Za-z]{2,12}_[A-Za-z0-9]{6,80}$")

# Allowed countries for shipping (mirror frontend select)
ALLOWED_COUNTRIES = {"US", "CA", "GB", "EU", "AU", "JP", "OTHER"}
ALLOWED_SPEEDS    = {"standard", "express", "priority"}
ALLOWED_PROVIDERS = {"mollie", "paypal"}


def _err(code: str, msg: str, status: int = 400):
    return jsonify({"ok": False, "error": {"code": code, "message": msg}}), status


def _get_client_ip() -> str:
    # Do not blindly trust spoofable forwarding headers. Only use
    # CF-Connecting-IP when the deployment explicitly says requests arrive via
    # a trusted proxy/CDN path; otherwise record Flask's peer address.
    if getattr(config, "HOMEPAGE_FREE_TRUST_PROXY_HEADERS", False):
        cf_ip = (request.headers.get("CF-Connecting-IP") or "").strip()
        if cf_ip:
            return cf_ip.split(",", 1)[0].strip()
    return request.remote_addr or ""


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
        base_price = compute_price(spec=spec, country=country, speed=speed)
        price = print_offer_service.compute_offer_quote(
            identity_id=g.identity_id,
            base=base_price,
            spec=spec,
            shipping=shipping,
            request_ip=_get_client_ip(),
        )
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
        "quote": price,
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
            request_ip=_get_client_ip(),
        )
    except print_order_service.PrintOrderError as e:
        return _err("ORDER_FAILED", str(e), 400)
    except Exception as e:
        import traceback
        print(
            f"[PRINT-ORDER] create unexpected error: {type(e).__name__}: {e!r}\n"
            f"{traceback.format_exc()}"
        )
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
# Referral helpers
# ─────────────────────────────────────────────────────────────
@bp.route("/referrals/me", methods=["GET"])
@require_session
def referrals_me():
    base = (config.FRONTEND_BASE_URL or config.PUBLIC_BASE_URL or "").rstrip("/")
    return jsonify({
        "ok": True,
        "referral": print_offer_service.referral_summary(g.identity_id, base_url=base),
    })


@bp.route("/referrals/claim", methods=["POST", "OPTIONS"])
@require_session
def referrals_claim():
    if request.method == "OPTIONS":
        return ("", 204)
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or data.get("ref") or "").strip()
    result = print_offer_service.claim_referral_token(
        identity_id=g.identity_id,
        token=token,
        request_ip=_get_client_ip(),
    )
    return jsonify({"ok": True, "referral": result})


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
    if not MOLLIE_PAYMENT_ID_RE.match(payment_id):
        return jsonify({"ok": False, "error": "invalid id"}), 400

    try:
        result = print_order_service.handle_mollie_webhook(payment_id)
    except Exception as e:
        print(f"[PRINT-ORDER] mollie webhook unexpected: {e}")
        return jsonify({"ok": False, "error": "webhook_processing_failed"}), 503

    if not result.get("ok"):
        return jsonify(result), 503

    return jsonify(result), 200


# ─────────────────────────────────────────────────────────────
# GET /api/print-orders/admin/<id>/download?type=glb|stl|thumb
# Admin download — 302 to a 1-hour presigned S3 URL.
#
# Auth: accepts either
#   (a) an authenticated admin session / X-Admin-Token (via require_admin),
#   (b) a valid HMAC signature in the query string (`exp`, `sig`).
#
# The HMAC path lets the admin click the download buttons in the order
# confirmation email from any device — including a phone they aren't
# signed in on. The signature binds the order number + file kind, so a
# leaked link only works for that single file until its embedded
# expiry passes.
# ─────────────────────────────────────────────────────────────
def _admin_download_handler(order_ref: str):
    kind = (request.args.get("type") or "glb").lower()
    if kind not in ("glb", "stl", "thumb"):
        return _err("BAD_TYPE", "type must be glb|stl|thumb", 400)

    target = get_admin_download_target(order_ref, kind)
    if not target:
        return _err("NOT_FOUND", f"No archived {kind} for this order", 404)

    s3_key, _content_type, filename = target
    # Presign with response-content-disposition so the browser saves it
    # with our chosen filename.
    try:
        url = s3_service.presign_s3_key(s3_key, expires_in=3600)
    except Exception as e:
        print(f"[PRINT-ORDER] presign failed for {s3_key}: {e}")
        return _err("PRESIGN_FAILED", "Could not generate download URL", 500)
    if not url:
        return _err("PRESIGN_FAILED", "S3 not configured", 500)

    # Append disposition hint to the presigned URL (S3 honors this param)
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}response-content-disposition=attachment%3B%20filename%3D{filename}"
    return redirect(url, code=302)


@bp.route("/admin/<order_ref>/download", methods=["GET"])
def admin_download(order_ref: str):
    # Path A: valid HMAC signature in the URL — bypass admin gate. This is
    # what makes email buttons clickable from any device.
    exp = request.args.get("exp")
    sig = request.args.get("sig")
    kind_for_sig = (request.args.get("type") or "glb").lower()
    if exp and sig and _verify_download_link(order_ref, kind_for_sig, exp, sig):
        return _admin_download_handler(order_ref)

    # Path B: fall back to admin auth (existing behavior — preserves the
    # dashboard download path and X-Admin-Token clients).
    return require_admin(_admin_download_handler)(order_ref)


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
        print("[PAYPAL] ERROR: PAYPAL_WEBHOOK_ID not set — rejecting unverified webhook")
        return jsonify({"ok": False, "error": "webhook_verification_unavailable"}), 503

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
