"""
PayPal Orders v2 service — used by the print-on-demand flow.

Flow:
  1. create_order(order_uuid, amount, currency, return_url, cancel_url)
     → returns {approve_url, paypal_order_id}
  2. User completes payment on PayPal, redirected to return_url
  3. capture_order(paypal_order_id) → 'COMPLETED' on success
  4. webhook (PAYMENT.CAPTURE.COMPLETED) is the authoritative confirmation
     and includes signature verification.

Env:
  PAYPAL_CLIENT_ID
  PAYPAL_CLIENT_SECRET
  PAYPAL_WEBHOOK_ID         (set after creating webhook in PayPal dashboard)
  PAYPAL_ENV                'live' | 'sandbox' (default sandbox)
  PUBLIC_BASE_URL           backend, for webhook URL
  FRONTEND_BASE_URL         frontend, for return / cancel URLs
"""

from __future__ import annotations

import time
import threading
from typing import Any, Dict, Optional

import requests

from backend.config import config


class PayPalError(Exception):
    """Raised when a PayPal API call fails."""


class PayPalService:
    """Thin client for PayPal Orders v2 + webhook signature verification."""

    _token_lock = threading.Lock()
    _token: Optional[str] = None
    _token_exp: float = 0.0

    # ─────────────────────────────────────────────────────────────
    # Availability
    # ─────────────────────────────────────────────────────────────
    @staticmethod
    def is_available() -> bool:
        return bool(config.PAYPAL_CONFIGURED)

    # ─────────────────────────────────────────────────────────────
    # OAuth (cached access token)
    # ─────────────────────────────────────────────────────────────
    @classmethod
    def _get_access_token(cls) -> str:
        """Get a cached OAuth access token (refresh ~5min before expiry)."""
        now = time.time()
        with cls._token_lock:
            if cls._token and now < cls._token_exp:
                return cls._token

            resp = requests.post(
                f"{config.PAYPAL_API_BASE}/v1/oauth2/token",
                auth=(config.PAYPAL_CLIENT_ID, config.PAYPAL_CLIENT_SECRET),
                data={"grant_type": "client_credentials"},
                headers={"Accept": "application/json"},
                timeout=15,
            )
            if resp.status_code != 200:
                raise PayPalError(f"OAuth failed: {resp.status_code} {resp.text}")
            data = resp.json()
            cls._token = data["access_token"]
            cls._token_exp = now + max(60, int(data.get("expires_in", 3000)) - 300)
            return cls._token

    @classmethod
    def _headers(cls, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        h = {
            "Authorization": f"Bearer {cls._get_access_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if extra:
            h.update(extra)
        return h

    # ─────────────────────────────────────────────────────────────
    # Orders v2
    # ─────────────────────────────────────────────────────────────
    @classmethod
    def create_order(
        cls,
        order_number: str,
        amount: float,
        currency: str,
        description: str,
        return_url: str,
        cancel_url: str,
        invoice_id: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Create a PayPal order and return the approval link + paypal order id.
        """
        if not cls.is_available():
            raise PayPalError("PayPal is not configured")

        payload = {
            "intent": "CAPTURE",
            "purchase_units": [
                {
                    "reference_id": order_number,
                    "description": description[:127],  # PayPal max 127 chars
                    "invoice_id": invoice_id or order_number,
                    "amount": {
                        "currency_code": currency,
                        "value": f"{amount:.2f}",
                    },
                }
            ],
            "application_context": {
                "brand_name": "TimrX",
                "user_action": "PAY_NOW",
                "shipping_preference": "NO_SHIPPING",  # We already collected it.
                "return_url": return_url,
                "cancel_url": cancel_url,
            },
        }

        # Retry once on 401 (token may have just expired)
        for attempt in (1, 2):
            resp = requests.post(
                f"{config.PAYPAL_API_BASE}/v2/checkout/orders",
                headers=cls._headers({"PayPal-Request-Id": order_number}),
                json=payload,
                timeout=30,
            )
            if resp.status_code == 401 and attempt == 1:
                cls._token = None  # force refresh
                continue
            break

        if resp.status_code not in (200, 201):
            raise PayPalError(f"create_order failed: {resp.status_code} {resp.text}")

        body = resp.json()
        approve = next(
            (l.get("href") for l in body.get("links", []) if l.get("rel") == "approve"),
            None,
        )
        if not approve:
            raise PayPalError("create_order: missing approve link in response")

        return {"approve_url": approve, "paypal_order_id": body["id"]}

    @classmethod
    def capture_order(cls, paypal_order_id: str) -> Dict[str, Any]:
        """
        Capture an approved order.  Returns the raw PayPal response.
        Status will be 'COMPLETED' on success.
        """
        if not cls.is_available():
            raise PayPalError("PayPal is not configured")

        resp = requests.post(
            f"{config.PAYPAL_API_BASE}/v2/checkout/orders/{paypal_order_id}/capture",
            headers=cls._headers(),
            timeout=30,
        )
        # 422 with INSTRUMENT_DECLINED → user should retry with different method
        if resp.status_code not in (200, 201):
            raise PayPalError(f"capture_order failed: {resp.status_code} {resp.text}")
        return resp.json()

    @classmethod
    def get_order(cls, paypal_order_id: str) -> Dict[str, Any]:
        """Read an order's current state (used to verify webhook claims)."""
        resp = requests.get(
            f"{config.PAYPAL_API_BASE}/v2/checkout/orders/{paypal_order_id}",
            headers=cls._headers(),
            timeout=20,
        )
        if resp.status_code != 200:
            raise PayPalError(f"get_order failed: {resp.status_code} {resp.text}")
        return resp.json()

    # ─────────────────────────────────────────────────────────────
    # Webhook signature verification
    # ─────────────────────────────────────────────────────────────
    @classmethod
    def verify_webhook(
        cls,
        headers: Dict[str, str],
        body: bytes,
        webhook_id: Optional[str] = None,
    ) -> bool:
        """
        Verify a webhook payload using PayPal's verify-webhook-signature API.

        headers: the request headers dict from Flask request.headers (case-insensitive)
        body: raw bytes of the request body
        """
        webhook_id = webhook_id or config.PAYPAL_WEBHOOK_ID
        if not webhook_id:
            print("[PAYPAL] WARN: PAYPAL_WEBHOOK_ID not set — webhook signature cannot be verified")
            return False

        # Header names per PayPal docs
        def h(name: str) -> Optional[str]:
            return headers.get(name) or headers.get(name.lower()) or headers.get(name.upper())

        required = {
            "auth_algo":        h("paypal-auth-algo"),
            "cert_url":         h("paypal-cert-url"),
            "transmission_id":  h("paypal-transmission-id"),
            "transmission_sig": h("paypal-transmission-sig"),
            "transmission_time":h("paypal-transmission-time"),
        }
        if not all(required.values()):
            print(f"[PAYPAL] verify_webhook: missing required headers {required}")
            return False

        try:
            import json as _json
            event = _json.loads(body.decode("utf-8"))
        except Exception as e:
            print(f"[PAYPAL] verify_webhook: bad body json: {e}")
            return False

        payload = {
            **required,
            "webhook_id": webhook_id,
            "webhook_event": event,
        }

        try:
            resp = requests.post(
                f"{config.PAYPAL_API_BASE}/v1/notifications/verify-webhook-signature",
                headers=cls._headers(),
                json=payload,
                timeout=20,
            )
            if resp.status_code != 200:
                print(f"[PAYPAL] verify_webhook: API status {resp.status_code} {resp.text}")
                return False
            return resp.json().get("verification_status") == "SUCCESS"
        except requests.RequestException as e:
            print(f"[PAYPAL] verify_webhook: request error: {e}")
            return False
