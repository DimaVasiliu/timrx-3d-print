"""
HTML email templates for print-on-demand orders.

Two emails fire on successful payment (via webhook):
- admin_email(): full details to admin@timrx.live so you can fulfill
- customer_email(): polite receipt + tracking info to the buyer
"""

from __future__ import annotations

from typing import Any, Dict


def _esc(s: Any) -> str:
    """HTML-escape any value."""
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def _fmt_money(amount: float, currency: str) -> str:
    sym = {"USD": "$", "EUR": "€", "GBP": "£"}.get(currency, "")
    return f"{sym}{amount:,.2f} {currency}"


def _row(label: str, value: str) -> str:
    return (
        f"<tr>"
        f"<td style='padding:6px 12px 6px 0;color:#6b7280;font-size:13px;'>{label}</td>"
        f"<td style='padding:6px 0;color:#111827;font-size:13px;font-weight:600;text-align:right'>{value}</td>"
        f"</tr>"
    )


def _section(title: str, rows_html: str) -> str:
    return (
        f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0' "
        f"style='border:1px solid #e5e7eb;border-radius:10px;margin:10px 0;'>"
        f"<tr><td style='padding:12px 16px;background:#f9fafb;border-bottom:1px solid #e5e7eb;"
        f"font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#0ea5e9;font-weight:700;'>"
        f"{title}</td></tr>"
        f"<tr><td style='padding:8px 16px;'>"
        f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0'>{rows_html}</table>"
        f"</td></tr>"
        f"</table>"
    )


def _spec_rows(order: Dict[str, Any]) -> str:
    spec = order.get("spec") or {}
    dims = spec.get("scaled_dimensions_mm") or []
    size_str = (
        f"{dims[0]:.0f} × {dims[1]:.0f} × {dims[2]:.0f} mm" if len(dims) == 3 else "—"
    )
    process = "FDM (filament)" if (spec.get("process") == "fdm") else "Resin (SLA)"
    infill = "100% (resin)" if spec.get("process") == "resin" else f"{spec.get('infill_pct', 20)}%"
    quality_labels = {
        "draft": "Draft — 0.28mm", "standard": "Standard — 0.20mm",
        "fine": "Fine — 0.12mm",  "ultra": "Ultra — 0.08mm",
    }
    finish_labels = {
        "raw": "Standard (as printed)", "sanded": "Sanded",
        "primed": "Primed & sanded",    "painted": "Hand-painted",
    }
    quality_key = str(spec.get("quality") or "")
    finish_key  = str(spec.get("finish") or "")
    return (
        _row("Process",  _esc(process))
        + _row("Material", _esc(order.get("material_label") or spec.get("material") or "—"))
        + _row("Color",    _esc(order.get("color_label") or spec.get("color") or "—"))
        + _row("Quality",  _esc(quality_labels.get(quality_key, quality_key or "—")))
        + _row("Infill",   _esc(infill))
        + _row("Finish",   _esc(finish_labels.get(finish_key, finish_key or "—")))
        + _row("Size",     _esc(size_str))
        + _row("Quantity", _esc(spec.get("quantity") or 1))
    )


def _shipping_rows(order: Dict[str, Any]) -> str:
    s = order.get("shipping") or {}
    speed_labels = {"standard": "Standard (5–8 days)", "express": "Express (2–3 days)", "priority": "Priority (1 day)"}
    speed_key = str(s.get("speed") or "")
    return (
        _row("Recipient", _esc(f"{s.get('first_name','')} {s.get('last_name','')}".strip() or "—"))
        + _row("Email",   _esc(s.get("email") or "—"))
        + _row("Address", _esc(s.get("address") or "—"))
        + _row("City",    _esc(s.get("city") or "—"))
        + _row("Postal",  _esc(s.get("postal") or "—"))
        + _row("Country", _esc(s.get("country") or "—"))
        + _row("Speed",   _esc(speed_labels.get(speed_key, speed_key or "—")))
    )


def _totals_rows(order: Dict[str, Any]) -> str:
    cur = order.get("currency") or "USD"
    # 'shipping_amount' is the cost; 'shipping' is the address dict.
    return (
        _row("Subtotal", _esc(_fmt_money(float(order.get("subtotal", 0)), cur)))
        + _row("Shipping", _esc(_fmt_money(float(order.get("shipping_amount", 0)), cur)))
        + _row(
            "<span style='color:#111827'>Total paid</span>",
            f"<span style='color:#0ea5e9;font-size:15px'>{_esc(_fmt_money(float(order.get('total', 0)), cur))}</span>",
        )
    )


def _wrap_html(title: str, preheader: str, body: str) -> str:
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>{_esc(title)}</title></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#111827;">
<div style="display:none;max-height:0;overflow:hidden;font-size:1px;line-height:1px;color:#f3f4f6;">{_esc(preheader)}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:24px 12px;">
<tr><td align="center">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;background:#ffffff;border-radius:14px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
<tr><td style="padding:24px 28px 0;">
<div style="font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:#0ea5e9;font-weight:700;">TimrX Print</div>
<h1 style="margin:6px 0 0;font-size:22px;color:#0f172a;letter-spacing:-0.01em;">{_esc(title)}</h1>
</td></tr>
<tr><td style="padding:14px 28px 24px;">{body}</td></tr>
<tr><td style="padding:18px 28px;border-top:1px solid #e5e7eb;background:#f9fafb;font-size:11px;color:#6b7280;">
This message was sent by TimrX. Questions? Reply to this email or write to <a href="mailto:admin@timrx.live" style="color:#0ea5e9;text-decoration:none;">admin@timrx.live</a>.
</td></tr>
</table>
</td></tr>
</table>
</body></html>
"""


# ─────────────────────────────────────────────────────────────
# Admin notification (admin@timrx.live)
# ─────────────────────────────────────────────────────────────
def admin_email(order: Dict[str, Any]) -> Dict[str, str]:
    """Build subject/html/text for the admin notification email."""
    order_number = order.get("order_number") or order.get("id") or "(no id)"
    total = _fmt_money(float(order.get("total", 0)), order.get("currency", "USD"))
    customer = (order.get("shipping") or {}).get("email") or order.get("customer_email") or "—"

    subject = f"🖨️ New print order {order_number} — {total}"

    body = (
        f"<p style='margin:0 0 8px;font-size:14px;color:#374151;line-height:1.6'>"
        f"<strong>{_esc(customer)}</strong> just placed a print order. Payment "
        f"<strong style='color:#16a34a'>confirmed via {_esc((order.get('payment_provider') or 'mollie').title())}</strong>.</p>"
        f"<p style='margin:0 0 16px;font-size:13px;color:#6b7280'>"
        f"Order <strong>{_esc(order_number)}</strong> · "
        f"Paid <strong>{_esc(total)}</strong>"
        f"</p>"
        + _section("Model", _model_rows(order))
        + _section("Print specification", _spec_rows(order))
        + _section("Shipping", _shipping_rows(order))
        + _section("Payment", _payment_rows(order))
        + _section("Totals", _totals_rows(order))
        + _next_steps_block()
    )

    html = _wrap_html("New print order received", f"Order {order_number} — {total}", body)
    text = (
        f"New print order {order_number}\n"
        f"Customer: {customer}\n"
        f"Total: {total}\n"
        f"Provider: {order.get('payment_provider')}\n"
        f"Model: {order.get('model_name')} ({order.get('model_glb_url')})\n"
    )
    return {"subject": subject, "html": html, "text": text}


def _model_rows(order: Dict[str, Any]) -> str:
    return (
        _row("Name", _esc(order.get("model_name") or "Untitled model"))
        + _row(
            "GLB",
            f"<a href='{_esc(order.get('model_glb_url') or '#')}' "
            f"style='color:#0ea5e9;text-decoration:none;'>Open file ↗</a>"
            if order.get("model_glb_url") else "—",
        )
        + _row("Model ID", _esc(order.get("model_id") or "—"))
    )


def _payment_rows(order: Dict[str, Any]) -> str:
    return (
        _row("Provider", _esc((order.get("payment_provider") or "—").title()))
        + _row("Payment ID", _esc(order.get("provider_payment_id") or "—"))
        + _row("Paid at", _esc(order.get("paid_at") or "—"))
    )


def _next_steps_block() -> str:
    return (
        "<table role='presentation' width='100%' cellpadding='0' cellspacing='0' "
        "style='border:1px solid #e5e7eb;border-radius:10px;margin:14px 0 0;background:#fffbeb;'>"
        "<tr><td style='padding:12px 16px;font-size:12px;color:#92400e;'>"
        "<strong>Next steps:</strong> download the GLB, slice & print to spec, "
        "send the QC photo to the customer, then ship and mark the order as <em>shipped</em>."
        "</td></tr></table>"
    )


# ─────────────────────────────────────────────────────────────
# Customer receipt (no-reply@timrx.live)
# ─────────────────────────────────────────────────────────────
def customer_email(order: Dict[str, Any]) -> Dict[str, str]:
    """Build subject/html/text for the customer order-confirmation receipt."""
    order_number = order.get("order_number") or "(no id)"
    total = _fmt_money(float(order.get("total", 0)), order.get("currency", "USD"))

    subject = f"Your TimrX print order {order_number} is confirmed"

    body = (
        f"<p style='margin:0 0 8px;font-size:14px;color:#374151;line-height:1.6'>"
        f"Thanks for ordering with TimrX! Your payment was received and our print team "
        f"has been notified.</p>"
        f"<p style='margin:0 0 16px;font-size:13px;color:#6b7280'>"
        f"Order number: <strong style='color:#111827'>{_esc(order_number)}</strong>"
        f"</p>"
        + _section("Order summary", _spec_rows(order))
        + _section("Shipping to", _shipping_rows(order))
        + _section("Totals", _totals_rows(order))
        + "<p style='margin:18px 0 0;font-size:13px;color:#374151;line-height:1.6'>"
        "You'll receive a QC photo before your model ships, plus a tracking link once it's on the way. "
        "Replies to this email reach our support team."
        "</p>"
    )

    html = _wrap_html(
        "Order confirmed — we're on it.",
        f"Your TimrX print order {order_number} — {total}",
        body,
    )
    text = (
        f"TimrX Print — order {order_number} confirmed.\n"
        f"Total: {total}.\n"
        f"We'll send a QC photo before shipping and a tracking link once dispatched.\n"
        f"Questions? Reply to this email."
    )
    return {"subject": subject, "html": html, "text": text}
