"""
HTML email templates for print-on-demand orders.

Two emails fire on successful payment (via webhook):
- admin_email(): full details to admin@timrx.live so you can fulfill
- customer_email(): polite receipt + tracking info to the buyer
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from backend.config import config

try:
    from backend.services import s3_service
except Exception:
    s3_service = None  # type: ignore


def _admin_download_url(order: Dict[str, Any], kind: str) -> Optional[str]:
    """Build the admin-auth download URL for an archived order file."""
    if not order.get(f"archived_{kind}_key"):
        return None
    base = (config.PUBLIC_BASE_URL or "").rstrip("/")
    if not base:
        return None
    return f"{base}/api/print-orders/admin/{order.get('order_number')}/download?type={kind}"


def _presigned_thumb_url(order: Dict[str, Any]) -> Optional[str]:
    """Return a 7-day presigned URL to the thumbnail for inline <img> in email."""
    key = order.get("archived_thumb_key")
    if not key or not s3_service:
        return None
    try:
        return s3_service.presign_s3_key(key, expires_in=7 * 24 * 3600)
    except Exception:
        return None


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
        + _model_card(order)
        + _downloads_block(order)
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


def _model_card(order: Dict[str, Any]) -> str:
    """Model summary card with embedded thumbnail (if archived)."""
    name = _esc(order.get("model_name") or "Untitled model")
    model_id = _esc(order.get("model_id") or "—")
    thumb_url = _presigned_thumb_url(order)

    thumb_html = ""
    if thumb_url:
        thumb_html = (
            f"<td style='padding:0 14px 0 0;vertical-align:top;width:120px;'>"
            f"<img src='{_esc(thumb_url)}' alt='Model preview' width='120' height='120' "
            f"style='display:block;width:120px;height:120px;object-fit:cover;border-radius:10px;"
            f"border:1px solid #e5e7eb;background:#0a0a0a;'/>"
            f"</td>"
        )

    meta_rows = (
        _row("Name", name)
        + _row("Model ID", model_id)
    )

    return (
        f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0' "
        f"style='border:1px solid #e5e7eb;border-radius:10px;margin:10px 0;'>"
        f"<tr><td style='padding:12px 16px;background:#f9fafb;border-bottom:1px solid #e5e7eb;"
        f"font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#0ea5e9;font-weight:700;'>"
        f"Model</td></tr>"
        f"<tr><td style='padding:14px 16px;'>"
        f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0'>"
        f"<tr>{thumb_html}"
        f"<td style='vertical-align:top;'>"
        f"<table role='presentation' width='100%' cellpadding='0' cellspacing='0'>{meta_rows}</table>"
        f"</td></tr></table>"
        f"</td></tr>"
        f"</table>"
    )


def _downloads_block(order: Dict[str, Any]) -> str:
    """Big GLB + STL download buttons that hit the admin-auth endpoint."""
    glb_url = _admin_download_url(order, "glb")
    stl_url = _admin_download_url(order, "stl")
    err = order.get("archived_glb_key") is None  # archive failed entirely

    if not glb_url and not stl_url:
        # No archived files — fall back to the (possibly expiring) source URL.
        src = order.get("model_glb_url")
        if not src:
            return ""
        return (
            "<table role='presentation' width='100%' cellpadding='0' cellspacing='0' "
            "style='border:1px solid #fcd34d;border-radius:10px;margin:10px 0;background:#fffbeb;'>"
            "<tr><td style='padding:12px 16px;font-size:12px;color:#92400e;line-height:1.55;'>"
            "<strong>⚠️ Model archive failed.</strong> Use this temporary link "
            f"(may expire): <a href='{_esc(src)}' style='color:#0ea5e9;'>{_esc(src[:80])}</a>"
            "</td></tr></table>"
        )

    btn_style = (
        "display:inline-block;padding:12px 22px;border-radius:10px;text-decoration:none;"
        "font-weight:700;font-size:13px;letter-spacing:0.01em;"
    )
    glb_btn = (
        f"<a href='{_esc(glb_url)}' style='{btn_style}"
        f"background:#0ea5e9;color:#fff;margin-right:10px;'>⬇ Download GLB</a>"
        if glb_url else ""
    )
    stl_btn = (
        f"<a href='{_esc(stl_url)}' style='{btn_style}"
        f"background:#0f172a;color:#fff;border:1px solid #334155;'>⬇ Download STL (ready for slicer)</a>"
        if stl_url else ""
    )

    warn = ""
    if not stl_url and not err:
        warn = (
            "<p style='margin:10px 0 0;font-size:11px;color:#92400e;'>"
            "Note: STL conversion failed — convert from GLB manually in Blender or your slicer."
            "</p>"
        )

    return (
        "<table role='presentation' width='100%' cellpadding='0' cellspacing='0' "
        "style='border:1px solid #e5e7eb;border-radius:10px;margin:10px 0;background:#f8fafc;'>"
        "<tr><td style='padding:14px 16px;'>"
        "<p style='margin:0 0 10px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;"
        "color:#0ea5e9;font-weight:700;'>Model files</p>"
        f"{glb_btn}{stl_btn}"
        "<p style='margin:10px 0 0;font-size:11px;color:#64748b;line-height:1.5;'>"
        "Files are archived in TimrX S3 and downloadable indefinitely. "
        "Links require your admin session (open in the same browser you use for the dashboard)."
        "</p>"
        f"{warn}"
        "</td></tr></table>"
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
