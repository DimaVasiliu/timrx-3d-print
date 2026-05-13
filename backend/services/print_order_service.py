"""
Print order service — DB + payment provider orchestration.

Responsibilities:
  - Insert a new print_orders row (status=pending_payment)
  - Create a Mollie or PayPal payment session, store provider_payment_id +
    checkout_url, return checkout_url to caller
  - Process Mollie / PayPal webhooks: verify, mark order paid, send emails
  - Helpers for status lookup, listing, idempotent admin updates
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from backend.config import config
from backend.db import get_conn, Tables
from backend.services.email_service import EmailService
from backend.services.paypal_service import PayPalService, PayPalError
from backend.services.print_order_pricing import PriceBreakdown, PriceError, compute as compute_price
from backend.services import print_order_emails


class PrintOrderError(Exception):
    """Generic print-order error."""


MOLLIE_API_BASE = "https://api.mollie.com/v2"


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _next_order_number(cur) -> str:
    cur.execute("SELECT nextval('timrx_billing.print_order_number_seq') AS n")
    row = cur.fetchone()
    # Cursor uses dict_row by default; fall back to positional if not.
    n = row["n"] if isinstance(row, dict) else row[0]
    return f"TX-PR-{int(n):06d}"


def _mollie_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {config.MOLLIE_API_KEY}",
        "Content-Type": "application/json",
    }


def _create_mollie_payment(
    order_id: str,
    order_number: str,
    amount: float,
    currency: str,
    description: str,
    customer_email: str,
    redirect_url: str,
    webhook_url: str,
) -> Dict[str, str]:
    """Create a Mollie payment and return {checkout_url, payment_id}."""
    if not config.MOLLIE_CONFIGURED:
        raise PrintOrderError("Mollie is not configured")

    # Mollie supports USD/EUR/GBP — currency must be one Mollie accepts.
    payload = {
        "amount":      {"currency": currency, "value": f"{amount:.2f}"},
        "description": description[:255],
        "redirectUrl": redirect_url,
        "webhookUrl":  webhook_url,
        "metadata": {
            "order_id":     order_id,
            "order_number": order_number,
            "type":         "print_order",
            "email":        customer_email,
        },
    }

    resp = requests.post(
        f"{MOLLIE_API_BASE}/payments",
        headers=_mollie_headers(),
        json=payload,
        timeout=30,
    )
    if resp.status_code not in (200, 201):
        raise PrintOrderError(f"Mollie create failed: {resp.status_code} {resp.text[:300]}")
    body = resp.json()
    return {
        "checkout_url": body["_links"]["checkout"]["href"],
        "payment_id":   body["id"],
    }


def _resolve_model_from_history(
    model_id: Optional[str],
    identity_id: str,
) -> Dict[str, Any]:
    """
    Server-side authoritative model lookup.

    If model_id is provided, find the GLB URL, title and thumbnail in
    history_items / models — DO NOT trust the URLs the frontend posted
    (they may be empty or expired).

    Returns {id, name, glb_url, thumb_url}.  Falls back to empty dict if
    nothing is found (the caller is responsible for refusing the order).
    """
    out: Dict[str, Any] = {"id": None, "name": None, "glb_url": None, "thumb_url": None}
    if not model_id:
        return out

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # 1) history_items (canonical, has title + thumbnail)
                cur.execute(
                    f"""
                    SELECT
                        id,
                        title,
                        thumbnail_url,
                        COALESCE(
                            glb_url,
                            payload->>'glb_url',
                            payload->>'textured_glb_url',
                            payload->'model_urls'->>'glb',
                            payload->'textured_model_urls'->>'glb'
                        ) AS resolved_glb_url,
                        payload->>'prompt' AS prompt_text
                    FROM {Tables.HISTORY_ITEMS}
                    WHERE (id = %s OR payload->>'original_job_id' = %s)
                      AND identity_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (model_id, model_id, identity_id),
                )
                row = cur.fetchone()
                if row:
                    r = dict(row)
                    out["id"]        = str(r["id"]) if r.get("id") else model_id
                    out["name"]      = r.get("title") or r.get("prompt_text")
                    out["thumb_url"] = r.get("thumbnail_url")
                    out["glb_url"]   = r.get("resolved_glb_url")

                # 2) models table fallback for GLB if still missing
                if not out["glb_url"]:
                    cur.execute(
                        f"""
                        SELECT COALESCE(
                            glb_url,
                            meta->>'glb_url',
                            meta->>'textured_glb_url',
                            meta->'model_urls'->>'glb',
                            meta->'textured_model_urls'->>'glb'
                        ) AS resolved_glb_url
                        FROM {Tables.MODELS}
                        WHERE (id = %s OR upstream_job_id = %s)
                          AND identity_id = %s
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (model_id, model_id, identity_id),
                    )
                    row = cur.fetchone()
                    if row:
                        r = dict(row)
                        out["glb_url"] = r.get("resolved_glb_url")
    except Exception as e:
        print(f"[PRINT-ORDER] history lookup failed for id={model_id!r}: {e}")

    return out


def _fetch_mollie_payment(payment_id: str) -> Dict[str, Any]:
    resp = requests.get(
        f"{MOLLIE_API_BASE}/payments/{payment_id}",
        headers=_mollie_headers(),
        timeout=20,
    )
    if resp.status_code != 200:
        raise PrintOrderError(f"Mollie fetch failed: {resp.status_code}")
    return resp.json()


# ─────────────────────────────────────────────────────────────
# Order creation
# ─────────────────────────────────────────────────────────────
def create_order(
    identity_id: str,
    customer_email: str,
    provider: str,
    spec: Dict[str, Any],
    shipping: Dict[str, Any],
    model: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Create a new print order, create the payment session, return checkout URL.

    Args:
        identity_id: caller identity (g.identity_id)
        customer_email: from session or shipping form
        provider: 'mollie' | 'paypal'
        spec: validated spec (process/material/color/quality/finish/infill_pct/quantity/scaled_dimensions_mm)
        shipping: validated shipping dict (first_name/last_name/email/address/city/postal/country/speed/notes)
        model: {id, name, glb_url, thumb_url} snapshot

    Returns:
        {order_id, order_number, checkout_url, total, currency}
    """
    provider = (provider or "mollie").lower()
    if provider not in ("mollie", "paypal"):
        raise PrintOrderError(f"Unknown payment provider: {provider}")

    if provider == "paypal" and not config.PAYPAL_CONFIGURED:
        raise PrintOrderError("PayPal is not configured")
    if provider == "mollie" and not config.MOLLIE_CONFIGURED:
        raise PrintOrderError("Mollie is not configured")

    # ── Authoritative price recomputation ────────────────────────────
    try:
        price: PriceBreakdown = compute_price(
            spec=spec,
            country=(shipping.get("country") or "").upper(),
            speed=(shipping.get("speed") or "standard"),
        )
    except PriceError as e:
        raise PrintOrderError(f"Invalid order: {e}")

    # ── Resolve the model authoritatively from history_items / models.
    # The frontend may have an empty or stale URL — never trust it for the
    # value we'll archive and send to the operator.  Fall back to the
    # frontend payload only when DB lookup misses (e.g. third-party links).
    resolved = _resolve_model_from_history(model.get("id"), identity_id)
    final_model = {
        "id":        resolved["id"]        or model.get("id"),
        "name":      resolved["name"]      or model.get("name") or "Untitled model",
        "glb_url":   resolved["glb_url"]   or model.get("glb_url"),
        "thumb_url": resolved["thumb_url"] or model.get("thumb_url"),
    }

    if not final_model["glb_url"]:
        # No printable model — fail loudly so the user can fix it instead of
        # paying for an order we can't fulfill.
        raise PrintOrderError(
            "We couldn't find a printable model for this order. Open a "
            "completed model from your history (or generate one) before "
            "placing the order."
        )

    # ── Insert order row ─────────────────────────────────────────────
    order_id = str(uuid.uuid4())
    with get_conn() as conn:
        with conn.cursor() as cur:
            order_number = _next_order_number(cur)

            cur.execute(
                f"""
                INSERT INTO {Tables.PRINT_ORDERS} (
                    id, identity_id, customer_email, order_number, status,
                    model_id, model_name, model_glb_url, model_thumb_url,
                    spec, shipping,
                    currency, subtotal_cents, shipping_cents, total_cents, estimate,
                    payment_provider
                ) VALUES (
                    %s, %s, %s, %s, 'pending_payment',
                    %s, %s, %s, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s, %s, %s::jsonb,
                    %s
                )
                """,
                (
                    order_id, identity_id, customer_email, order_number,
                    final_model["id"], final_model["name"],
                    final_model["glb_url"], final_model["thumb_url"],
                    json.dumps(spec), json.dumps(shipping),
                    price.currency, price.subtotal_cents, price.shipping_cents, price.total_cents,
                    json.dumps(price.to_dict()),
                    provider,
                ),
            )
        conn.commit()

    # ── Build return / webhook URLs ──────────────────────────────────
    frontend = (config.FRONTEND_BASE_URL or config.PUBLIC_BASE_URL or "").rstrip("/")
    backend  = (config.PUBLIC_BASE_URL or "").rstrip("/")
    return_url = f"{frontend}/3dprint?print_order={order_number}"
    cancel_url = f"{frontend}/3dprint?print_order={order_number}&cancelled=1"

    description = f"TimrX print order {order_number} — {final_model['name']}"[:255]

    # ── Create provider checkout ─────────────────────────────────────
    if provider == "mollie":
        webhook_url = f"{backend}/api/print-orders/webhook/mollie"
        try:
            res = _create_mollie_payment(
                order_id=order_id,
                order_number=order_number,
                amount=price.total,
                currency=price.currency,
                description=description,
                customer_email=customer_email,
                redirect_url=return_url,
                webhook_url=webhook_url,
            )
        except Exception:
            _mark_order_failed(order_id, "checkout_creation_failed")
            raise
        provider_payment_id = res["payment_id"]
        checkout_url = res["checkout_url"]
    else:  # paypal
        try:
            res = PayPalService.create_order(
                order_number=order_number,
                amount=price.total,
                currency=price.currency,
                description=description,
                return_url=return_url,
                cancel_url=cancel_url,
                invoice_id=order_number,
            )
        except PayPalError as e:
            _mark_order_failed(order_id, "checkout_creation_failed")
            raise PrintOrderError(f"PayPal create failed: {e}")
        provider_payment_id = res["paypal_order_id"]
        checkout_url = res["approve_url"]

    # ── Persist provider_payment_id + checkout_url ───────────────────
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.PRINT_ORDERS}
                SET provider_payment_id = %s,
                    checkout_url = %s
                WHERE id = %s
                """,
                (provider_payment_id, checkout_url, order_id),
            )
        conn.commit()

    return {
        "order_id":     order_id,
        "order_number": order_number,
        "checkout_url": checkout_url,
        "total":        price.total,
        "currency":     price.currency,
        "provider":     provider,
    }


def _mark_order_failed(order_id: str, reason: str = "") -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE {Tables.PRINT_ORDERS} SET status = 'failed' WHERE id = %s",
                    (order_id,),
                )
            conn.commit()
        print(f"[PRINT-ORDER] order {order_id} marked failed: {reason or 'unspecified'}")
    except Exception as e:
        print(f"[PRINT-ORDER] mark_failed error: {e}")


# ─────────────────────────────────────────────────────────────
# Payment confirmation
# ─────────────────────────────────────────────────────────────
def mark_paid_and_notify(
    order_id: str,
    provider: str,
    provider_payment_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Idempotently mark an order paid and fire admin + customer emails.

    Returns the order row dict if newly marked paid, or None if it was
    already paid / not found.
    """
    now = datetime.now(timezone.utc)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.PRINT_ORDERS}
                SET status = 'paid', paid_at = %s
                WHERE id = %s
                  AND payment_provider = %s
                  AND provider_payment_id = %s
                  AND status = 'pending_payment'
                RETURNING id, identity_id, customer_email, order_number, status,
                          model_id, model_name, model_glb_url, model_thumb_url,
                          spec, shipping, currency,
                          subtotal_cents, shipping_cents, total_cents, estimate,
                          payment_provider, provider_payment_id, paid_at
                """,
                (now, order_id, provider, provider_payment_id),
            )
            row = cur.fetchone()
        conn.commit()

    if not row:
        # Already paid (idempotent webhook redelivery) or order not found.
        return None

    # Cursor uses dict_row by default — row is a Mapping.
    order: Dict[str, Any] = dict(row) if not isinstance(row, dict) else dict(row)

    # JSONB columns come back as dicts already (psycopg) — be defensive against str.
    for k in ("spec", "shipping", "estimate"):
        if isinstance(order.get(k), (bytes, str)):
            try:
                order[k] = json.loads(order[k])
            except Exception:
                order[k] = {}

    # Decorate for templates.  'shipping' is the address dict; the cost lives
    # in shipping_cents — surface both, keyed distinctly.
    est = order.get("estimate") or {}
    order["subtotal"]        = float(order["subtotal_cents"]) / 100.0
    order["shipping_amount"] = float(order["shipping_cents"]) / 100.0
    order["total"]           = float(order["total_cents"]) / 100.0
    order["material_label"]  = est.get("material_label")
    order["color_label"]     = est.get("color_label")

    # Archive the model file + thumb to S3 BEFORE emails so the admin links
    # are permanent (Meshy URLs can expire). Best-effort: emails still go out
    # even if archiving fails — admin will see archive_error in the row.
    try:
        from backend.services.print_order_archive import archive_for_order
        arch = archive_for_order(order_id)
        order["archived_glb_key"]   = arch.get("glb_key")
        order["archived_stl_key"]   = arch.get("stl_key")
        order["archived_thumb_key"] = arch.get("thumb_key")
    except Exception as e:
        print(f"[PRINT-ORDER] archive failed for {order_id}: {e}")

    # Send emails (best-effort, never raise)
    _send_admin_email(order)
    _send_customer_email(order)
    return order


def _send_admin_email(order: Dict[str, Any]) -> None:
    try:
        msg = print_order_emails.admin_email(_render_friendly(order))
        EmailService.send(
            to=config.PRINT_ORDER_ADMIN_EMAIL,
            subject=msg["subject"],
            html=msg["html"],
            text=msg["text"],
            from_email=config.PRINT_ORDER_FROM_EMAIL,
            from_name=config.PRINT_ORDER_FROM_NAME,
        )
    except Exception as e:
        print(f"[PRINT-ORDER] admin email failed: {e}")


def _send_customer_email(order: Dict[str, Any]) -> None:
    to = (order.get("shipping") or {}).get("email") or order.get("customer_email")
    if not to:
        print(f"[PRINT-ORDER] no customer email on order {order.get('order_number')}; skip receipt")
        return
    try:
        msg = print_order_emails.customer_email(_render_friendly(order))
        EmailService.send(
            to=to,
            subject=msg["subject"],
            html=msg["html"],
            text=msg["text"],
            from_email=config.PRINT_ORDER_FROM_EMAIL,
            from_name=config.PRINT_ORDER_FROM_NAME,
            reply_to=config.PRINT_ORDER_ADMIN_EMAIL,
            reply_to_name="TimrX Support",
        )
    except Exception as e:
        print(f"[PRINT-ORDER] customer email failed: {e}")


def _render_friendly(order: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adapt the internal row to what the email templates expect:
      - 'shipping' is the shipping ADDRESS dict (template _shipping_rows)
      - 'shipping' in _totals_rows is read as a number — we patch the template
        helpers to use 'shipping_amount' instead so there's no key collision.
    """
    o = dict(order)
    o["shipping"] = order.get("shipping") or {}
    o["subtotal"] = float(order.get("subtotal", 0))
    o["shipping_amount"] = float(order.get("shipping_amount", 0))
    o["total"]    = float(order.get("total", 0))
    o["currency"] = order.get("currency") or "USD"
    return o


# ─────────────────────────────────────────────────────────────
# Webhook handlers
# ─────────────────────────────────────────────────────────────
def handle_mollie_webhook(payment_id: str) -> Dict[str, Any]:
    """Process a Mollie webhook for a print order payment."""
    try:
        payment = _fetch_mollie_payment(payment_id)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    metadata = payment.get("metadata") or {}
    if metadata.get("type") != "print_order":
        return {"ok": True, "ignored": True, "reason": "not a print_order payment"}

    order_id = metadata.get("order_id")
    status = payment.get("status")

    if not order_id:
        return {"ok": False, "error": "missing order_id in metadata"}

    if status == "paid":
        order = mark_paid_and_notify(order_id, "mollie", payment_id)
        return {"ok": True, "status": "paid", "newly_paid": bool(order)}

    if status in ("failed", "canceled", "expired"):
        _mark_order_failed(order_id, reason=status)
        return {"ok": True, "status": status}

    # pending/open/authorized — wait for next webhook delivery
    return {"ok": True, "status": status, "noop": True}


def handle_paypal_webhook(event: Dict[str, Any]) -> Dict[str, Any]:
    """Process a verified PayPal webhook event for a print order."""
    event_type = event.get("event_type") or ""
    resource = event.get("resource") or {}

    # The capture resource has supplementary_data.related_ids.order_id pointing at the order.
    # The custom_id / invoice_id we set when creating the order is our order_number.
    purchase_units = resource.get("purchase_units") or []
    invoice_id = None
    paypal_order_id = None

    if purchase_units:
        pu0 = purchase_units[0] or {}
        invoice_id = pu0.get("invoice_id") or pu0.get("reference_id")
        paypal_order_id = resource.get("id")
    else:
        # Capture-level webhook
        invoice_id = resource.get("invoice_id") or resource.get("custom_id")
        rel = (resource.get("supplementary_data") or {}).get("related_ids") or {}
        paypal_order_id = rel.get("order_id") or resource.get("id")

    if not invoice_id:
        return {"ok": True, "ignored": True, "reason": "no invoice_id"}

    # Look up order by order_number
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id FROM {Tables.PRINT_ORDERS} WHERE order_number = %s",
                (invoice_id,),
            )
            row = cur.fetchone()
    if not row:
        return {"ok": True, "ignored": True, "reason": "unknown order_number"}
    order_id = str(row["id"] if isinstance(row, dict) else row[0])

    if event_type in ("CHECKOUT.ORDER.APPROVED", "PAYMENT.CAPTURE.COMPLETED"):
        result = mark_paid_and_notify(order_id, "paypal", paypal_order_id or invoice_id)
        return {"ok": True, "status": "paid", "newly_paid": bool(result)}

    if event_type in ("PAYMENT.CAPTURE.DENIED", "PAYMENT.CAPTURE.REFUNDED", "CHECKOUT.ORDER.VOIDED"):
        _mark_order_failed(order_id, reason=event_type)
        return {"ok": True, "status": event_type}

    return {"ok": True, "noop": True, "event_type": event_type}


# ─────────────────────────────────────────────────────────────
# Reads (status endpoint + lists)
# ─────────────────────────────────────────────────────────────
def get_order_public(order_id_or_number: str, identity_id: str) -> Optional[Dict[str, Any]]:
    """Return a minimal order status — only if it belongs to this identity."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, order_number, status, total_cents, currency,
                       payment_provider, checkout_url, paid_at, created_at
                FROM {Tables.PRINT_ORDERS}
                WHERE (id::text = %s OR order_number = %s)
                  AND identity_id = %s
                LIMIT 1
                """,
                (order_id_or_number, order_id_or_number, identity_id),
            )
            row = cur.fetchone()
    if not row:
        return None
    r = dict(row)
    return {
        "id":           str(r["id"]),
        "order_number": r["order_number"],
        "status":       r["status"],
        "total":        float(r["total_cents"]) / 100.0,
        "currency":     r["currency"],
        "provider":     r["payment_provider"],
        "checkout_url": r["checkout_url"],
        "paid_at":      r["paid_at"].isoformat() if r.get("paid_at") else None,
        "created_at":   r["created_at"].isoformat() if r.get("created_at") else None,
    }


def list_my_orders(identity_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, order_number, status, total_cents, currency,
                       payment_provider, created_at, paid_at
                FROM {Tables.PRINT_ORDERS}
                WHERE identity_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (identity_id, min(100, max(1, limit))),
            )
            rows = cur.fetchall()
    out = []
    for raw in rows:
        r = dict(raw)
        out.append({
            "id":           str(r["id"]),
            "order_number": r["order_number"],
            "status":       r["status"],
            "total":        float(r["total_cents"]) / 100.0,
            "currency":     r["currency"],
            "provider":     r["payment_provider"],
            "created_at":   r["created_at"].isoformat() if r.get("created_at") else None,
            "paid_at":      r["paid_at"].isoformat() if r.get("paid_at") else None,
        })
    return out
