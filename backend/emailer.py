"""
Email utilities for TimrX Backend.
Handles sending transactional emails via the EmailService.

This module provides high-level email functions (send_magic_code, send_purchase_receipt, etc.)
while delegating actual sending to EmailService in email_service.py.
"""

from typing import Optional, Dict, Any

from backend.config import config

# Import send_email from EmailService for actual sending
# Try multiple import paths for flexibility (app.py adds services/ to sys.path)
EMAIL_SERVICE_AVAILABLE = False
_send_email = None

try:
    from backend.services.email_service import send_email as _send_email
    EMAIL_SERVICE_AVAILABLE = True
except ImportError:
    try:
        from backend.services.email_service import send_email as _send_email
        EMAIL_SERVICE_AVAILABLE = True
    except ImportError:
        pass

if not EMAIL_SERVICE_AVAILABLE:
    print("[EMAIL] WARNING: email_service not available, using fallback")


# ─────────────────────────────────────────────────────────────
# Logo loaders (self-contained — no external dependency)
# ─────────────────────────────────────────────────────────────
from pathlib import Path

_logo_bytes: Optional[bytes] = None
_logo_loaded = False
_blogs_logo_bytes: Optional[bytes] = None
_blogs_logo_loaded = False


def _load_logo() -> Optional[bytes]:
    """Load TimrX email logo. Tries Render paths, local paths, then web fallback. Cached."""
    global _logo_bytes, _logo_loaded
    if _logo_loaded:
        return _logo_bytes

    _logo_loaded = True

    # Build list of candidate paths (ordered by priority)
    candidates = [
        # Render deployment paths (most likely in production)
        Path("/opt/render/project/src/backend/assets/logo.png"),
        Path("/opt/render/project/src/assets/logo.png"),
        # APP_DIR-relative paths (APP_DIR = meshy/ in this project)
        config.APP_DIR / "assets" / "logo.png",
        config.APP_DIR / "backend" / "assets" / "logo.png",
        # Local development paths
        config.APP_DIR / ".." / ".." / "Frontend" / "img" / "logo.png",
    ]

    for p in candidates:
        try:
            resolved = p.resolve()
            if resolved.is_file():
                _logo_bytes = resolved.read_bytes()
                print(f"[EMAIL] Logo loaded from {resolved} ({len(_logo_bytes)} bytes)")
                return _logo_bytes
        except Exception:
            continue

    # Fallback: download from public URL
    try:
        import requests as _req
        resp = _req.get("https://timrx.live/img/logo.png", timeout=10)
        if resp.status_code == 200 and len(resp.content) > 100:
            _logo_bytes = resp.content
            print(f"[EMAIL] Logo downloaded from web ({len(_logo_bytes) if _logo_bytes else 0} bytes)")
            return _logo_bytes
    except Exception as e:
        print(f"[EMAIL] Could not download logo: {e}")

    print("[EMAIL] Logo not found in any location")
    return None


def _load_blogs_logo() -> Optional[bytes]:
    """Load TimrX blogs logo. Tries Render paths, local paths, then web fallback. Cached."""
    global _blogs_logo_bytes, _blogs_logo_loaded
    if _blogs_logo_loaded:
        return _blogs_logo_bytes

    _blogs_logo_loaded = True

    # Build list of candidate paths (ordered by priority)
    candidates = [
        # Render deployment paths
        Path("/opt/render/project/src/backend/assets/blogs.png"),
        Path("/opt/render/project/src/assets/blogs.png"),
        # APP_DIR-relative paths
        config.APP_DIR / "assets" / "blogs.png",
        config.APP_DIR / "backend" / "assets" / "blogs.png",
        # Local development paths
        config.APP_DIR / ".." / ".." / "Frontend" / "img" / "blogs.png",
    ]

    for p in candidates:
        try:
            resolved = p.resolve()
            if resolved.is_file():
                _blogs_logo_bytes = resolved.read_bytes()
                print(f"[EMAIL] Blogs logo loaded from {resolved} ({len(_blogs_logo_bytes)} bytes)")
                return _blogs_logo_bytes
        except Exception:
            continue

    # Fallback: download from public URL
    try:
        import requests as _req
        resp = _req.get("https://timrx.live/img/blogs.png", timeout=10)
        if resp.status_code == 200 and len(resp.content) > 100:
            _blogs_logo_bytes = resp.content
            print(f"[EMAIL] Blogs logo downloaded from web ({len(_blogs_logo_bytes) if _blogs_logo_bytes else 0} bytes)")
            return _blogs_logo_bytes
    except Exception as e:
        print(f"[EMAIL] Could not download blogs logo: {e}")

    print("[EMAIL] Blogs logo not found in any location")
    return None


# ─────────────────────────────────────────────────────────────
# Email Template Constants & Wrapper
# ─────────────────────────────────────────────────────────────

# Brand colors (email-safe)
ACCENT_COLOR = "#C97A2B"  # Warm amber/copper
TEXT_PRIMARY = "#111111"
TEXT_SECONDARY = "#555555"
TEXT_MUTED = "#888888"
BG_WHITE = "#ffffff"
BG_LIGHT = "#f7f7f7"
BORDER_COLOR = "#e5e5e5"
SUCCESS_COLOR = "#22863a"


def render_email_html(
    title: str,
    intro: str,
    body_html: str,
    logo_cid: Optional[str] = "timrx_logo",
    footer_extra: str = "",
) -> str:
    """
    Render a cross-client compatible HTML email template.

    Uses table-based layout with inline styles for maximum compatibility
    with Gmail, Outlook, Yahoo, Apple Mail, etc.

    Args:
        title: Main heading (e.g. "Purchase Confirmed")
        intro: Lead paragraph text
        body_html: Main content HTML (cards, tables, etc.)
        logo_cid: Content-ID for inline logo image, or None to skip
        footer_extra: Additional footer content (e.g. "Reply to this email for help")

    Returns:
        Complete HTML email string
    """
    logo_img = ""
    if logo_cid:
        logo_img = f'''<img src="cid:{logo_cid}" alt="TimrX" width="32" height="32" style="display:block;width:32px;height:32px;border:0;" />'''

    return f'''<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="color-scheme" content="light" />
    <meta name="supported-color-schemes" content="light" />
    <title>{title}</title>
    <!--[if mso]>
    <style type="text/css">
        table {{border-collapse:collapse;border-spacing:0;margin:0;}}
        div, td {{padding:0;}}
        div {{margin:0 !important;}}
    </style>
    <noscript>
    <xml>
        <o:OfficeDocumentSettings>
            <o:PixelsPerInch>96</o:PixelsPerInch>
        </o:OfficeDocumentSettings>
    </xml>
    </noscript>
    <![endif]-->
</head>
<body style="margin:0;padding:0;background-color:{BG_LIGHT};font-family:Arial,Helvetica,sans-serif;-webkit-font-smoothing:antialiased;">
    <!-- Outer wrapper table -->
    <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:{BG_LIGHT};">
        <tr>
            <td align="center" style="padding:32px 16px;">
                <!-- Main content table (600px max) -->
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="600" style="max-width:600px;width:100%;background-color:{BG_WHITE};border:1px solid {BORDER_COLOR};border-radius:8px;">

                    <!-- Header -->
                    <tr>
                        <td style="padding:24px 32px;border-bottom:1px solid {BORDER_COLOR};">
                            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                                <tr>
                                    <td width="40" valign="middle" style="padding-right:12px;">
                                        {logo_img}
                                    </td>
                                    <td valign="middle">
                                        <span style="font-size:22px;font-weight:700;color:{TEXT_PRIMARY};letter-spacing:-0.5px;font-family:Arial,Helvetica,sans-serif;">TimrX</span>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                    <!-- Title & Intro -->
                    <tr>
                        <td style="padding:32px 32px 16px 32px;">
                            <h1 style="margin:0 0 12px 0;font-size:24px;font-weight:700;color:{TEXT_PRIMARY};line-height:1.3;font-family:Arial,Helvetica,sans-serif;">{title}</h1>
                            <p style="margin:0;font-size:15px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">{intro}</p>
                        </td>
                    </tr>

                    <!-- Body Content -->
                    <tr>
                        <td style="padding:0 32px 32px 32px;">
                            {body_html}
                        </td>
                    </tr>

                    <!-- Footer -->
                    <tr>
                        <td style="padding:24px 32px;border-top:1px solid {BORDER_COLOR};background-color:{BG_LIGHT};">
                            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                                <tr>
                                    <td align="center">
                                        <p style="margin:0 0 8px 0;font-size:13px;color:{TEXT_MUTED};font-family:Arial,Helvetica,sans-serif;">
                                            TimrX &bull; 3D Print Hub
                                        </p>
                                        <p style="margin:0;font-size:12px;color:{TEXT_MUTED};font-family:Arial,Helvetica,sans-serif;">
                                            If you need help, reply to this email or contact
                                            <a href="mailto:support@timrx.live" style="color:{ACCENT_COLOR};text-decoration:underline;">support@timrx.live</a>
                                        </p>
                                        {f'<p style="margin:8px 0 0 0;font-size:11px;color:{TEXT_MUTED};font-family:Arial,Helvetica,sans-serif;">{footer_extra}</p>' if footer_extra else ''}
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>

                </table>
                <!-- /Main content table -->
            </td>
        </tr>
    </table>
    <!-- /Outer wrapper table -->
</body>
</html>'''


def render_detail_card(rows: list, header: str = "Details") -> str:
    """
    Render a bordered card with key-value rows.

    Args:
        rows: List of (label, value) tuples
        header: Card header text

    Returns:
        HTML string for the card
    """
    rows_html = ""
    for i, (label, value) in enumerate(rows):
        border_style = f"border-bottom:1px solid {BORDER_COLOR};" if i < len(rows) - 1 else ""
        rows_html += f'''
            <tr>
                <td style="padding:12px 16px;font-size:14px;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;{border_style}">{label}</td>
                <td style="padding:12px 16px;font-size:14px;color:{TEXT_PRIMARY};font-family:Arial,Helvetica,sans-serif;text-align:right;font-weight:600;{border_style}">{value}</td>
            </tr>'''

    return f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border:1px solid {BORDER_COLOR};border-radius:6px;border-collapse:separate;">
            <tr>
                <td colspan="2" style="padding:12px 16px;background-color:{BG_LIGHT};border-bottom:1px solid {BORDER_COLOR};border-radius:6px 6px 0 0;">
                    <span style="font-size:14px;font-weight:700;color:{TEXT_PRIMARY};font-family:Arial,Helvetica,sans-serif;">{header}</span>
                </td>
            </tr>
            {rows_html}
        </table>'''


def render_highlight_box(content: str, bg_color: str = BG_LIGHT) -> str:
    """Render a highlighted box (e.g. for codes, amounts)."""
    return f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border:2px dashed {BORDER_COLOR};border-radius:8px;background-color:{bg_color};">
            <tr>
                <td align="center" style="padding:24px 16px;">
                    {content}
                </td>
            </tr>
        </table>'''


def render_amount_display(amount: str, currency: str = "GBP", status: str = "Paid") -> str:
    """Render a large amount display with status."""
    symbol = "£" if currency == "GBP" else "$" if currency == "USD" else "€"
    return f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td style="padding-bottom:24px;border-bottom:1px solid {BORDER_COLOR};">
                    <p style="margin:0 0 8px 0;font-size:14px;color:{TEXT_MUTED};font-family:Arial,Helvetica,sans-serif;">Amount</p>
                    <p style="margin:0 0 8px 0;font-size:36px;font-weight:700;color:{TEXT_PRIMARY};font-family:Arial,Helvetica,sans-serif;line-height:1;">{symbol}{amount}</p>
                    <p style="margin:0;font-size:14px;font-weight:600;color:{SUCCESS_COLOR};font-family:Arial,Helvetica,sans-serif;">{status}</p>
                </td>
            </tr>
        </table>'''


# ─────────────────────────────────────────────────────────────
# Core Email Function
# ─────────────────────────────────────────────────────────────
def send_email(
    to_email: str,
    subject: str,
    html_body: str,
    text_body: Optional[str] = None,
    from_email: Optional[str] = None,
    from_name: Optional[str] = None,
) -> bool:
    """
    Send an email via EmailService.
    Returns True on success, False on failure.
    Never throws - safe to call from any endpoint.
    """
    if EMAIL_SERVICE_AVAILABLE and _send_email is not None:
        return _send_email(
            to_email=to_email,
            subject=subject,
            html_body=html_body,
            text_body=text_body,
            from_email=from_email,
            from_name=from_name,
        )

    # Fallback if email_service not available
    print(f"[EMAIL] Would send to {to_email}: {subject}")
    return True


# ─────────────────────────────────────────────────────────────
# Transactional Email Templates
# ─────────────────────────────────────────────────────────────
def send_magic_code(to_email: str, code: str) -> bool:
    """Send a magic login code to the user."""
    subject = "Your TimrX Access Code"

    # Load logo for inline CID embedding
    logo_bytes = _load_logo()

    # Build code box HTML
    code_box = render_highlight_box(
        f'<span style="font-size:32px;font-weight:700;letter-spacing:8px;color:{TEXT_PRIMARY};font-family:\'Courier New\',Courier,monospace;">{code}</span>'
    )

    body_html = f'''
        {code_box}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:24px;">
            <tr>
                <td>
                    <p style="margin:0 0 8px 0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        This code expires in <strong style="color:{TEXT_PRIMARY};">15 minutes</strong>.
                    </p>
                    <p style="margin:0;font-size:13px;line-height:1.6;color:{TEXT_MUTED};font-family:Arial,Helvetica,sans-serif;">
                        If you didn't request this code, you can safely ignore this email.
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Your Access Code",
        intro="Use the code below to sign in to your TimrX account:",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Your TimrX Access Code

Use the code below to sign in to your TimrX account:

{code}

This code expires in 15 minutes.

If you didn't request this code, you can safely ignore this email.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    # Use send_raw with inline logo if logo available, otherwise simple send
    if logo_bytes:
        try:
            from backend.services.email_service import EmailService
            result = EmailService.send_raw(
                to=to_email,
                subject=subject,
                html=html_body,
                text=text_body,
                inline_images=[{
                    "cid": "timrx_logo",
                    "data": logo_bytes,
                    "content_type": "image/png",
                }],
            )
            if result.success:
                return True
            print(f"[EMAIL] send_magic_code send_raw failed: {result.message}, falling back to simple send")
        except Exception as e:
            print(f"[EMAIL] send_magic_code send_raw error: {e}, falling back to simple send")

    return send_email(to_email, subject, html_body, text_body)


def send_purchase_receipt(
    to_email: str,
    plan_name: str,
    credits: int,
    amount_gbp: float,
) -> bool:
    """Send a purchase confirmation receipt."""
    from datetime import datetime, timezone

    subject = f"TimrX Receipt - {plan_name}"
    paid_date = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # Load logo for inline CID embedding
    logo_bytes = _load_logo()

    # Build body HTML using helper functions
    amount_display = render_amount_display(f"{amount_gbp:.2f}", "GBP", f"Paid {paid_date}")

    summary_card = render_detail_card([
        (plan_name, f"&pound;{amount_gbp:.2f}"),
        ("Credits added", f"{credits:,}"),
        ("Amount paid", f"&pound;{amount_gbp:.2f}"),
    ], header="Summary")

    body_html = f'''
        {amount_display}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:24px;">
            <tr>
                <td>
                    {summary_card}
                </td>
            </tr>
            <tr>
                <td style="padding-top:24px;">
                    <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        Your credits are now available in your account.
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Purchase Confirmed",
        intro="Thank you for your purchase. Here's your receipt.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Receipt from TimrX

Amount: £{amount_gbp:.2f}
Paid: {paid_date}

Summary:
  {plan_name}: £{amount_gbp:.2f}
  Credits added: {credits:,}
  Amount paid: £{amount_gbp:.2f}

Your credits are now available in your account.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    # Use send_raw with inline logo if logo available, otherwise simple send
    if logo_bytes:
        try:
            from backend.services.email_service import EmailService
            result = EmailService.send_raw(
                to=to_email,
                subject=subject,
                html=html_body,
                text=text_body,
                inline_images=[{
                    "cid": "timrx_logo",
                    "data": logo_bytes,
                    "content_type": "image/png",
                }],
            )
            if result.success:
                return True
            print(f"[EMAIL] send_purchase_receipt send_raw failed: {result.message}, falling back to simple send")
        except Exception as e:
            print(f"[EMAIL] send_purchase_receipt send_raw error: {e}, falling back to simple send")

    return send_email(to_email, subject, html_body, text_body)


# ─────────────────────────────────────────────────────────────
# Invoice / Receipt Email (with PDF attachments)
# ─────────────────────────────────────────────────────────────
def send_invoice_email(
    to_email: str,
    invoice_number: str,
    receipt_number: str,
    plan_name: str,
    credits: int,
    amount_gbp: float,
    invoice_pdf: bytes,
    receipt_pdf: bytes,
    logo_bytes: Optional[bytes] = None,
) -> bool:
    """
    Send a purchase confirmation email with invoice + receipt PDFs attached.

    Uses EmailService.send_raw() for MIME multipart with attachments and
    inline logo image.  Falls back to simple send_purchase_receipt() on error.
    """
    from datetime import datetime, timezone

    subject = f"TimrX Receipt - {plan_name}"
    paid_date = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # If no logo_bytes passed in, load it locally
    if not logo_bytes:
        logo_bytes = _load_logo()

    # Build body HTML using helper functions
    amount_display = render_amount_display(f"{amount_gbp:.2f}", "GBP", f"Paid {paid_date}")

    ref_card = render_detail_card([
        ("Invoice number", invoice_number),
        ("Receipt number", receipt_number),
    ], header="Reference")

    summary_card = render_detail_card([
        (plan_name, f"&pound;{amount_gbp:.2f}"),
        ("Credits added", f"{credits:,}"),
        ("Amount paid", f"&pound;{amount_gbp:.2f}"),
    ], header="Summary")

    body_html = f'''
        {amount_display}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:24px;">
            <tr>
                <td style="padding-bottom:16px;">
                    {ref_card}
                </td>
            </tr>
            <tr>
                <td>
                    {summary_card}
                </td>
            </tr>
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

    html_body = render_email_html(
        title="Purchase Confirmed",
        intro="Thank you for your purchase. Here's your receipt with invoice and receipt documents attached.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
        footer_extra="Your PDF documents are attached to this email.",
    )

    text_body = f"""Receipt from TimrX

Amount: £{amount_gbp:.2f}
Paid: {paid_date}

Invoice: {invoice_number}
Receipt: {receipt_number}

Summary:
  {plan_name}: £{amount_gbp:.2f}
  Credits added: {credits:,}
  Amount paid: £{amount_gbp:.2f}

Your invoice and receipt PDFs are attached.
Your credits are now available in your account.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    # Build attachments list
    attachments = []
    if invoice_pdf:
        attachments.append({
            "filename": f"{invoice_number}.pdf",
            "data": invoice_pdf,
            "content_type": "application/pdf",
        })
    if receipt_pdf:
        attachments.append({
            "filename": f"{receipt_number}.pdf",
            "data": receipt_pdf,
            "content_type": "application/pdf",
        })

    # Inline images
    inline_images = []
    if logo_bytes:
        inline_images.append({
            "cid": "timrx_logo",
            "data": logo_bytes,
            "content_type": "image/png",
        })

    try:
        from backend.services.email_service import EmailService
        result = EmailService.send_raw(
            to=to_email,
            subject=subject,
            html=html_body,
            text=text_body,
            attachments=attachments if attachments else None,
            inline_images=inline_images if inline_images else None,
        )
        return result.success
    except Exception as e:
        print(f"[EMAIL] send_invoice_email error: {e}")
        # Fallback to simple receipt (no attachments)
        return send_purchase_receipt(to_email, plan_name, credits, amount_gbp)


# ─────────────────────────────────────────────────────────────
# Admin Notifications
# ─────────────────────────────────────────────────────────────
def send_payment_received(
    to_email: str,
    plan_name: str,
    credits: int,
    amount_gbp: float,
) -> bool:
    """
    Send a minimal "Payment Received" confirmation email (HTML only, no PDFs).

    This is used as a fallback when invoice/receipt PDF generation fails,
    ensuring the buyer always receives immediate confirmation.
    """
    from datetime import datetime, timezone

    subject = f"TimrX Payment Received - {plan_name}"
    paid_date = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # Load logo for inline CID embedding
    logo_bytes = _load_logo()

    # Build body HTML with success message
    body_html = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#f0fdf4;border:1px solid #86efac;border-radius:8px;">
            <tr>
                <td style="padding:20px;">
                    <p style="margin:0 0 8px 0;font-size:16px;font-weight:600;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        &#10003; Payment Successful
                    </p>
                    <p style="margin:0;font-size:14px;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        Your {credits:,} credits have been added to your account.
                    </p>
                </td>
            </tr>
        </table>

        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-top:24px;">
            <tr>
                <td style="padding:16px;background-color:{BG_LIGHT};border-radius:6px;">
                    <p style="margin:0 0 8px 0;font-size:14px;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        <strong>Plan:</strong> {plan_name}
                    </p>
                    <p style="margin:0 0 8px 0;font-size:14px;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        <strong>Amount:</strong> &pound;{amount_gbp:.2f}
                    </p>
                    <p style="margin:0 0 8px 0;font-size:14px;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        <strong>Credits:</strong> {credits:,}
                    </p>
                    <p style="margin:0;font-size:14px;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        <strong>Date:</strong> {paid_date}
                    </p>
                </td>
            </tr>
            <tr>
                <td style="padding-top:16px;">
                    <p style="margin:0;font-size:13px;color:{TEXT_MUTED};font-family:Arial,Helvetica,sans-serif;">
                        Your full invoice and receipt will be sent separately.
                        Your credits are available immediately.
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Payment Received",
        intro="Thank you for your purchase! Your payment has been confirmed.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Payment Received - TimrX

Thank you for your purchase! Your payment has been confirmed.

Plan: {plan_name}
Amount: £{amount_gbp:.2f}
Credits: {credits:,}
Date: {paid_date}

Your credits have been added to your account and are available immediately.
Your full invoice and receipt will be sent separately.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    # Use send_raw with inline logo if logo available
    if logo_bytes:
        try:
            from backend.services.email_service import EmailService
            result = EmailService.send_raw(
                to=to_email,
                subject=subject,
                html=html_body,
                text=text_body,
                inline_images=[{
                    "cid": "timrx_logo",
                    "data": logo_bytes,
                    "content_type": "image/png",
                }],
            )
            if result.success:
                return True
            print(f"[EMAIL] send_payment_received send_raw failed: {result.message}, falling back to simple send")
        except Exception as e:
            print(f"[EMAIL] send_payment_received send_raw error: {e}, falling back to simple send")

    return send_email(to_email, subject, html_body, text_body)


def notify_admin(subject: str, message: str, data: Optional[Dict[str, Any]] = None) -> bool:
    """Send a notification to the admin email."""
    admin_email = config.ADMIN_EMAIL
    if not admin_email:
        print(f"[EMAIL] Admin notification (no ADMIN_EMAIL configured): {subject}")
        return False

    # Load logo for inline CID embedding
    logo_bytes = _load_logo()

    # Build data card if data provided
    data_card = ""
    if data:
        data_card = render_detail_card(
            [(k, v) for k, v in data.items()],
            header="Details"
        )

    body_html = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td style="padding-bottom:16px;">
                    <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">{message}</p>
                </td>
            </tr>
            {f'<tr><td>{data_card}</td></tr>' if data_card else ''}
        </table>
    '''

    html_body = render_email_html(
        title=subject,
        intro="Admin notification from TimrX system.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
        footer_extra="This is an automated admin notification.",
    )

    # Build plain text version
    data_text = ""
    if data:
        data_text = "\n".join(f"  {k}: {v}" for k, v in data.items())
        data_text = f"\nDetails:\n{data_text}\n"

    text_body = f"""{subject}

{message}
{data_text}
---
TimrX Admin Notification
"""

    # Use send_raw with inline logo if logo available
    if logo_bytes:
        try:
            from backend.services.email_service import EmailService
            result = EmailService.send_raw(
                to=admin_email,
                subject=f"[TimrX Admin] {subject}",
                html=html_body,
                text=text_body,
                inline_images=[{
                    "cid": "timrx_logo",
                    "data": logo_bytes,
                    "content_type": "image/png",
                }],
            )
            if result.success:
                return True
            print(f"[EMAIL] notify_admin send_raw failed: {result.message}, falling back to simple send")
        except Exception as e:
            print(f"[EMAIL] notify_admin send_raw error: {e}, falling back to simple send")

    return send_email(admin_email, f"[TimrX Admin] {subject}", html_body, text_body)


def notify_new_identity(identity_id: str, email: Optional[str] = None) -> bool:
    """Notify admin when a new identity is created (with email)."""
    if not config.NOTIFY_ON_NEW_IDENTITY:
        return False
    if not email:  # Only notify when email is attached
        return False

    return notify_admin(
        "New User Registered",
        "A new user has registered with an email address.",
        {"Identity ID": identity_id, "Email": email},
    )


def notify_purchase(
    identity_id: str,
    email: str,
    plan_name: str,
    credits: int,
    amount_gbp: float,
) -> bool:
    """Notify admin when a purchase is completed."""
    if not config.NOTIFY_ON_PURCHASE:
        return False

    return notify_admin(
        "New Purchase",
        f"A user has purchased the {plan_name} plan.",
        {
            "Identity ID": identity_id,
            "Email": email,
            "Plan": plan_name,
            "Credits": f"{credits:,}",
            "Amount": f"{amount_gbp:.2f} GBP",
        },
    )


def notify_restore_request(email: str) -> bool:
    """Notify admin when a restore code is requested."""
    if not config.NOTIFY_ON_RESTORE_REQUEST:
        return False

    return notify_admin(
        "Restore Code Requested",
        "A user has requested an account restore code.",
        {"Email": email},
    )


# ─────────────────────────────────────────────────────────────
# Subscription Emails
# ─────────────────────────────────────────────────────────────
def send_subscription_confirmation(
    to_email: str,
    plan_name: str,
    plan_code: str,  # noqa: ARG001 - reserved for future invoice generation
    credits_per_month: int,
    price_gbp: float,
    cadence: str = "monthly",
) -> bool:
    """
    Send a subscription confirmation email to the user.

    Sent when a user completes a subscription checkout (monthly or yearly).
    """
    from datetime import datetime, timezone

    subject = f"TimrX Subscription Confirmed - {plan_name}"
    start_date = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # Load logo for inline CID embedding
    logo_bytes = _load_logo()

    # Calculate billing info
    if cadence == "yearly":
        price_display = f"£{price_gbp:.2f}/year"
        next_billing = "in 12 months"
    else:
        price_display = f"£{price_gbp:.2f}/month"
        next_billing = "in 30 days"

    # Build subscription summary card
    summary_card = render_detail_card([
        ("Plan", plan_name),
        ("Credits", f"{credits_per_month:,} per month"),
        ("Billing", price_display),
        ("Started", start_date),
        ("Next billing", next_billing),
    ], header="Subscription Details")

    # Success banner
    success_banner = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#f0fdf4;border:1px solid #86efac;border-radius:8px;margin-bottom:20px;">
            <tr>
                <td style="padding:20px;">
                    <p style="margin:0 0 8px 0;font-size:16px;font-weight:600;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        &#10003; Subscription Active
                    </p>
                    <p style="margin:0;font-size:14px;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        Your first {credits_per_month:,} credits have been added to your account.
                    </p>
                </td>
            </tr>
        </table>
    '''

    body_html = f'''
        {success_banner}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td>
                    {summary_card}
                </td>
            </tr>
            <tr>
                <td style="padding-top:24px;">
                    <p style="margin:0 0 8px 0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        Your subscription is now active. You'll receive {credits_per_month:,} credits at the start of each billing period.
                    </p>
                    <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        You can manage your subscription anytime from your account settings.
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Subscription Confirmed",
        intro=f"Welcome to {plan_name}! Your subscription is now active.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Subscription Confirmed - TimrX

Welcome to {plan_name}! Your subscription is now active.

Subscription Details:
  Plan: {plan_name}
  Credits: {credits_per_month:,} per month
  Billing: {price_display}
  Started: {start_date}
  Next billing: {next_billing}

Your first {credits_per_month:,} credits have been added to your account.

You can manage your subscription anytime from your account settings.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    # Use send_raw with inline logo if logo available
    if logo_bytes:
        try:
            from backend.services.email_service import EmailService
            result = EmailService.send_raw(
                to=to_email,
                subject=subject,
                html=html_body,
                text=text_body,
                inline_images=[{
                    "cid": "timrx_logo",
                    "data": logo_bytes,
                    "content_type": "image/png",
                }],
            )
            if result.success:
                return True
            print(f"[EMAIL] send_subscription_confirmation send_raw failed: {result.message}, falling back to simple send")
        except Exception as e:
            print(f"[EMAIL] send_subscription_confirmation send_raw error: {e}, falling back to simple send")

    return send_email(to_email, subject, html_body, text_body)


def send_credits_delivered_email(
    to_email: str,
    plan_code: str,
    credits_granted: int,
    is_first_grant: bool,
    next_credit_date,  # datetime
    remaining_months: Optional[int] = None,
) -> bool:
    """
    Send email notification when monthly credits are delivered.

    Sent on each credit allocation for both monthly and yearly plans.
    """
    from datetime import datetime, timezone

    # Get plan info
    try:
        from backend.services.subscription_service import SUBSCRIPTION_PLANS
        plan_info = SUBSCRIPTION_PLANS.get(plan_code, {})
        plan_name = plan_info.get("name", plan_code.replace("_", " ").title())
    except Exception:
        plan_name = plan_code.replace("_", " ").title()

    subject = f"Your {credits_granted:,} TimrX Credits Are Ready"

    # Load logo for inline CID embedding
    logo_bytes = _load_logo()

    # Format next credit date
    if hasattr(next_credit_date, 'strftime'):
        next_date_str = next_credit_date.strftime("%B %d, %Y")
    else:
        next_date_str = str(next_credit_date)[:10]

    # Build summary rows
    summary_rows = [
        ("Plan", plan_name),
        ("Credits delivered", f"{credits_granted:,}"),
        ("Next delivery", next_date_str),
    ]

    if remaining_months is not None:
        summary_rows.append(("Months remaining", f"{remaining_months}"))

    summary_card = render_detail_card(summary_rows, header="Credit Details")

    # Success banner
    if is_first_grant:
        banner_title = "Welcome! Your first credits are ready"
        banner_text = f"Your subscription is now active with {credits_granted:,} credits."
    else:
        banner_title = "Monthly credits delivered"
        banner_text = f"Your {credits_granted:,} credits for this month have been added."

    success_banner = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#f0fdf4;border:1px solid #86efac;border-radius:8px;margin-bottom:20px;">
            <tr>
                <td style="padding:20px;">
                    <p style="margin:0 0 8px 0;font-size:16px;font-weight:600;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        &#10003; {banner_title}
                    </p>
                    <p style="margin:0;font-size:14px;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        {banner_text}
                    </p>
                </td>
            </tr>
        </table>
    '''

    body_html = f'''
        {success_banner}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td>
                    {summary_card}
                </td>
            </tr>
            <tr>
                <td style="padding-top:24px;">
                    <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        Your credits are ready to use. Start creating amazing 3D content!
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Credits Delivered",
        intro=banner_text,
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    remaining_text = f"\nMonths remaining: {remaining_months}" if remaining_months is not None else ""

    text_body = f"""Credits Delivered - TimrX

{banner_title}

{banner_text}

Credit Details:
  Plan: {plan_name}
  Credits delivered: {credits_granted:,}
  Next delivery: {next_date_str}{remaining_text}

Your credits are ready to use. Start creating amazing 3D content!

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    return _send_with_logo(to_email, subject, html_body, text_body, logo_bytes)


def send_payment_failed_email(
    to_email: str,
    plan_code: str,
    failure_count: int,
) -> bool:
    """
    Send email notification when subscription payment fails.

    Alerts the user to update their payment method.
    """
    # Get plan info
    try:
        from backend.services.subscription_service import SUBSCRIPTION_PLANS
        plan_info = SUBSCRIPTION_PLANS.get(plan_code, {})
        plan_name = plan_info.get("name", plan_code.replace("_", " ").title())
    except Exception:
        plan_name = plan_code.replace("_", " ").title()

    subject = "Action Required: Payment Failed for Your TimrX Subscription"

    # Load logo
    logo_bytes = _load_logo()

    # Warning banner
    warning_banner = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#fef2f2;border:1px solid #fca5a5;border-radius:8px;margin-bottom:20px;">
            <tr>
                <td style="padding:20px;">
                    <p style="margin:0 0 8px 0;font-size:16px;font-weight:600;color:#991b1b;font-family:Arial,Helvetica,sans-serif;">
                        &#9888; Payment Failed
                    </p>
                    <p style="margin:0;font-size:14px;color:#991b1b;font-family:Arial,Helvetica,sans-serif;">
                        We couldn't process your payment for the {plan_name} subscription.
                    </p>
                </td>
            </tr>
        </table>
    '''

    urgency_text = "Please update your payment method as soon as possible to continue receiving credits." if failure_count == 1 else f"This is attempt {failure_count}. Your subscription will be suspended if payment continues to fail."

    body_html = f'''
        {warning_banner}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td>
                    <p style="margin:0 0 16px 0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        {urgency_text}
                    </p>
                    <p style="margin:0 0 16px 0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        <strong>What to do:</strong>
                    </p>
                    <ul style="margin:0 0 16px 0;padding-left:20px;font-size:14px;line-height:1.8;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        <li>Check that your card details are up to date</li>
                        <li>Ensure sufficient funds are available</li>
                        <li>Contact your bank if the problem persists</li>
                    </ul>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Payment Failed",
        intro=f"We couldn't process your payment for your {plan_name} subscription.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Payment Failed - TimrX

We couldn't process your payment for the {plan_name} subscription.

{urgency_text}

What to do:
- Check that your card details are up to date
- Ensure sufficient funds are available
- Contact your bank if the problem persists

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    return _send_with_logo(to_email, subject, html_body, text_body, logo_bytes)


def send_subscription_reactivated_email(
    to_email: str,
    plan_code: str,
) -> bool:
    """
    Send email when subscription is reactivated after payment resolved.
    """
    # Get plan info
    try:
        from backend.services.subscription_service import SUBSCRIPTION_PLANS
        plan_info = SUBSCRIPTION_PLANS.get(plan_code, {})
        plan_name = plan_info.get("name", plan_code.replace("_", " ").title())
    except Exception:
        plan_name = plan_code.replace("_", " ").title()

    subject = f"Your TimrX {plan_name} Subscription is Active Again"

    logo_bytes = _load_logo()

    success_banner = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#f0fdf4;border:1px solid #86efac;border-radius:8px;margin-bottom:20px;">
            <tr>
                <td style="padding:20px;">
                    <p style="margin:0 0 8px 0;font-size:16px;font-weight:600;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        &#10003; Subscription Reactivated
                    </p>
                    <p style="margin:0;font-size:14px;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        Your payment has been processed and your {plan_name} subscription is active again.
                    </p>
                </td>
            </tr>
        </table>
    '''

    body_html = f'''
        {success_banner}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td>
                    <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        Thank you for resolving the payment issue. Your monthly credit allocations will continue as normal.
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Subscription Reactivated",
        intro=f"Great news! Your {plan_name} subscription is active again.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Subscription Reactivated - TimrX

Great news! Your {plan_name} subscription is active again.

Your payment has been processed and your monthly credit allocations will continue as normal.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    return _send_with_logo(to_email, subject, html_body, text_body, logo_bytes)


def send_subscription_renewed_email(
    to_email: str,
    plan_code: str,
    next_billing_date,  # datetime
) -> bool:
    """
    Send email when subscription is renewed.
    """
    # Get plan info
    try:
        from backend.services.subscription_service import SUBSCRIPTION_PLANS
        plan_info = SUBSCRIPTION_PLANS.get(plan_code, {})
        plan_name = plan_info.get("name", plan_code.replace("_", " ").title())
        cadence = plan_info.get("cadence", "monthly")
    except Exception:
        plan_name = plan_code.replace("_", " ").title()
        cadence = "monthly" if "monthly" in plan_code else "yearly"

    subject = f"Your TimrX {plan_name} Subscription Has Been Renewed"

    logo_bytes = _load_logo()

    # Format next billing date
    if hasattr(next_billing_date, 'strftime'):
        next_date_str = next_billing_date.strftime("%B %d, %Y")
    else:
        next_date_str = str(next_billing_date)[:10]

    success_banner = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#f0fdf4;border:1px solid #86efac;border-radius:8px;margin-bottom:20px;">
            <tr>
                <td style="padding:20px;">
                    <p style="margin:0 0 8px 0;font-size:16px;font-weight:600;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        &#10003; Subscription Renewed
                    </p>
                    <p style="margin:0;font-size:14px;color:#166534;font-family:Arial,Helvetica,sans-serif;">
                        Your {plan_name} subscription has been successfully renewed.
                    </p>
                </td>
            </tr>
        </table>
    '''

    summary_card = render_detail_card([
        ("Plan", plan_name),
        ("Billing period", cadence.title()),
        ("Next billing date", next_date_str),
    ], header="Renewal Details")

    body_html = f'''
        {success_banner}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td>
                    {summary_card}
                </td>
            </tr>
            <tr>
                <td style="padding-top:24px;">
                    <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        Your credits will continue to be delivered monthly as part of your subscription.
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Subscription Renewed",
        intro=f"Your {plan_name} subscription has been successfully renewed.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Subscription Renewed - TimrX

Your {plan_name} subscription has been successfully renewed.

Renewal Details:
  Plan: {plan_name}
  Billing period: {cadence.title()}
  Next billing date: {next_date_str}

Your credits will continue to be delivered monthly as part of your subscription.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    return _send_with_logo(to_email, subject, html_body, text_body, logo_bytes)


def send_subscription_cancelled_email(
    to_email: str,
    plan_code: str,
    access_until,  # datetime or None
) -> bool:
    """
    Send email when subscription is cancelled.
    """
    # Get plan info
    try:
        from backend.services.subscription_service import SUBSCRIPTION_PLANS
        plan_info = SUBSCRIPTION_PLANS.get(plan_code, {})
        plan_name = plan_info.get("name", plan_code.replace("_", " ").title())
    except Exception:
        plan_name = plan_code.replace("_", " ").title()

    subject = f"Your TimrX {plan_name} Subscription Has Been Cancelled"

    logo_bytes = _load_logo()

    # Format access until date
    if access_until and hasattr(access_until, 'strftime'):
        access_date_str = access_until.strftime("%B %d, %Y")
        access_text = f"You can continue using your remaining credits until {access_date_str}."
    else:
        access_text = "Your remaining credits will be available until the end of your current billing period."

    info_banner = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#fef3c7;border:1px solid #fcd34d;border-radius:8px;margin-bottom:20px;">
            <tr>
                <td style="padding:20px;">
                    <p style="margin:0 0 8px 0;font-size:16px;font-weight:600;color:#92400e;font-family:Arial,Helvetica,sans-serif;">
                        Subscription Cancelled
                    </p>
                    <p style="margin:0;font-size:14px;color:#92400e;font-family:Arial,Helvetica,sans-serif;">
                        Your {plan_name} subscription has been cancelled as requested.
                    </p>
                </td>
            </tr>
        </table>
    '''

    body_html = f'''
        {info_banner}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td>
                    <p style="margin:0 0 16px 0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        {access_text}
                    </p>
                    <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        We're sorry to see you go! If you change your mind, you can resubscribe anytime from your account.
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Subscription Cancelled",
        intro=f"Your {plan_name} subscription has been cancelled.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Subscription Cancelled - TimrX

Your {plan_name} subscription has been cancelled as requested.

{access_text}

We're sorry to see you go! If you change your mind, you can resubscribe anytime from your account.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    return _send_with_logo(to_email, subject, html_body, text_body, logo_bytes)


def send_subscription_expired_email(
    to_email: str,
    plan_code: str,
) -> bool:
    """
    Send email when subscription expires.
    """
    # Get plan info
    try:
        from backend.services.subscription_service import SUBSCRIPTION_PLANS
        plan_info = SUBSCRIPTION_PLANS.get(plan_code, {})
        plan_name = plan_info.get("name", plan_code.replace("_", " ").title())
    except Exception:
        plan_name = plan_code.replace("_", " ").title()

    subject = f"Your TimrX {plan_name} Subscription Has Expired"

    logo_bytes = _load_logo()

    info_banner = f'''
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:#f3f4f6;border:1px solid #d1d5db;border-radius:8px;margin-bottom:20px;">
            <tr>
                <td style="padding:20px;">
                    <p style="margin:0 0 8px 0;font-size:16px;font-weight:600;color:#374151;font-family:Arial,Helvetica,sans-serif;">
                        Subscription Expired
                    </p>
                    <p style="margin:0;font-size:14px;color:#6b7280;font-family:Arial,Helvetica,sans-serif;">
                        Your {plan_name} subscription period has ended.
                    </p>
                </td>
            </tr>
        </table>
    '''

    body_html = f'''
        {info_banner}
        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
            <tr>
                <td>
                    <p style="margin:0 0 16px 0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        Your monthly credit allocations have ended. Any remaining credits in your account can still be used.
                    </p>
                    <p style="margin:0;font-size:14px;line-height:1.6;color:{TEXT_SECONDARY};font-family:Arial,Helvetica,sans-serif;">
                        Want to continue creating? You can resubscribe anytime or purchase a one-time credit pack from your account.
                    </p>
                </td>
            </tr>
        </table>
    '''

    html_body = render_email_html(
        title="Subscription Expired",
        intro=f"Your {plan_name} subscription period has ended.",
        body_html=body_html,
        logo_cid="timrx_logo" if logo_bytes else None,
    )

    text_body = f"""Subscription Expired - TimrX

Your {plan_name} subscription period has ended.

Your monthly credit allocations have ended. Any remaining credits in your account can still be used.

Want to continue creating? You can resubscribe anytime or purchase a one-time credit pack from your account.

---
TimrX - 3D Print Hub
Need help? Reply to this email or contact support@timrx.live
"""

    return _send_with_logo(to_email, subject, html_body, text_body, logo_bytes)


# ─────────────────────────────────────────────────────────────
# Helper for sending emails with logo
# ─────────────────────────────────────────────────────────────
def _send_with_logo(
    to_email: str,
    subject: str,
    html_body: str,
    text_body: str,
    logo_bytes: Optional[bytes],
) -> bool:
    """Helper to send email with inline logo, falling back to simple send."""
    if logo_bytes:
        try:
            from backend.services.email_service import EmailService
            result = EmailService.send_raw(
                to=to_email,
                subject=subject,
                html=html_body,
                text=text_body,
                inline_images=[{
                    "cid": "timrx_logo",
                    "data": logo_bytes,
                    "content_type": "image/png",
                }],
            )
            if result.success:
                return True
            print(f"[EMAIL] send_raw failed: {result.message}, falling back to simple send")
        except Exception as e:
            print(f"[EMAIL] send_raw error: {e}, falling back to simple send")

    return send_email(to_email, subject, html_body, text_body)
