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
# Logo loader (self-contained — no external dependency)
# ─────────────────────────────────────────────────────────────
_logo_bytes: Optional[bytes] = None
_logo_loaded = False


def _load_logo() -> Optional[bytes]:
    """Load TimrX logo PNG from local paths. Cached only on successful load."""
    global _logo_bytes, _logo_loaded
    if _logo_loaded:
        return _logo_bytes

    candidates = [
        config.APP_DIR / "backend" / "assets" / "logo.png",
        config.APP_DIR / "assets" / "logo.png",
        config.APP_DIR / ".." / ".." / "Frontend" / "img" / "logo (1).png",
        config.APP_DIR / ".." / ".." / "Frontend" / "img" / "logo.png",
    ]
    for p in candidates:
        try:
            resolved = p.resolve()
            if resolved.is_file():
                _logo_bytes = resolved.read_bytes()
                _logo_loaded = True  # only cache when actually found
                print(f"[EMAIL] Logo loaded from {resolved} ({len(_logo_bytes)} bytes)")
                return _logo_bytes
        except Exception as exc:
            print(f"[EMAIL] Logo candidate {p} failed: {exc}")
            continue

    print(f"[EMAIL] Logo not found at any candidate path (APP_DIR={config.APP_DIR})")
    return None


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

    # Logo tag: CID if logo available, hidden otherwise
    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" height="24" '
            'style="height:24px; width:auto; display:block;" />'
        )

    html_body = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto;
                background-color: #000000; border-radius: 12px; overflow: hidden;">

        <!-- Header with logo -->
        <div style="background-color: #000000; padding: 12px 20px;">
            <table cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                    <td>
                        <table cellpadding="0" cellspacing="0" border="0">
                            <tr>
                                <td style="vertical-align: middle; padding-right: 10px; line-height: 0;">
                                    {logo_img_tag}
                                </td>
                                <td style="vertical-align: middle;">
                                    <span style="font-size: 20px; font-weight: 800; color: #ffffff;
                                                 letter-spacing: 0.5px;">TimrX</span>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        </div>

        <!-- Body -->
        <div style="padding: 36px 32px 28px;">
            <h2 style="color: #ffffff; margin: 0 0 8px; font-size: 22px; font-weight: 600;">
                Your Access Code
            </h2>
            <p style="color: #aaa; font-size: 15px; line-height: 1.5; margin: 0 0 24px;">
                Use the code below to access your TimrX account:
            </p>

            <!-- Code box -->
            <div style="background-color: #111111; border: 2px dashed #333; border-radius: 10px;
                        padding: 24px; text-align: center; margin: 0 0 24px;">
                <span style="font-size: 36px; font-weight: 700; letter-spacing: 8px;
                             color: #ffffff; font-family: 'Courier New', monospace;">{code}</span>
            </div>

            <p style="color: #888; font-size: 14px; line-height: 1.5; margin: 0 0 6px;">
                This code expires in <strong style="color: #ccc;">15 minutes</strong>.
            </p>
            <p style="color: #666; font-size: 13px; line-height: 1.5; margin: 0;">
                If you didn't request this code, you can safely ignore this email.
            </p>
        </div>

        <!-- Footer -->
        <div style="border-top: 1px solid #222; padding: 20px 32px;
                    text-align: center;">
            <p style="color: #555; font-size: 12px; margin: 0 0 6px;">
                TimrX &mdash; 3D Print Hub
            </p>
            <p style="color: #444; font-size: 11px; margin: 0;">
                Need help? Contact us at
                <a href="mailto:support@timrx.live"
                   style="color: #5b7cfa; text-decoration: none;">support@timrx.live</a>
            </p>
        </div>
    </div>
    """

    text_body = f"""Your TimrX Access Code

Use the code below to access your TimrX account:

{code}

This code expires in 15 minutes.

If you didn't request this code, you can safely ignore this email.

---
TimrX - 3D Print Hub
Need help? Contact us at support@timrx.live
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
    """Send a purchase confirmation receipt (Stripe-like design)."""
    from datetime import datetime, timezone

    subject = f"TimrX Receipt - {plan_name}"
    paid_date = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # Load logo for inline CID embedding
    logo_bytes = _load_logo()

    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" '
            'style="height: 8px; width: auto;" />'
        )

    # Build header: logo + TimrX text, or just text
    header_html = f"""
        <table cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td style="vertical-align: middle; padding-right: 8px;">{logo_img_tag}</td>
                <td style="vertical-align: middle;">
                    <span style="font-size: 15px; font-weight: 700; color: #ffffff; letter-spacing: 0.5px;">TimrX</span>
                </td>
            </tr>
        </table>
    """ if logo_bytes else """
        <span style="font-size: 15px; font-weight: 700; color: #ffffff; letter-spacing: 0.5px;">TimrX</span>
    """

    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                max-width: 600px; margin: 0 auto; color: #ffffff; padding: 0 16px;
                background-color: #000000; border-radius: 12px;">

        <!-- Logo -->
        <div style="padding: 20px 0 16px;">
            {header_html}
        </div>

        <!-- Amount section -->
        <div style="padding: 0 0 24px; border-bottom: 1px solid #222;">
            <p style="color: #888; font-size: 15px; margin: 0 0 6px;">Receipt from TimrX</p>
            <p style="font-size: 36px; font-weight: 700; margin: 0 0 8px; color: #ffffff;">&pound;{amount_gbp:.2f}</p>
            <p style="color: #27ae60; font-size: 14px; font-weight: 600; margin: 0;">Paid {paid_date}</p>
        </div>

        <!-- Summary card -->
        <div style="margin: 24px 0;">
            <div style="border: 1px solid #222; border-radius: 8px; overflow: hidden;">
                <div style="padding: 12px 16px; background-color: #111; border-bottom: 1px solid #222;">
                    <span style="font-weight: 600; font-size: 14px; color: #ffffff;">Summary</span>
                </div>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #222;">
                        <td style="padding: 12px 16px; font-size: 14px; color: #ffffff;">{plan_name}</td>
                        <td style="padding: 12px 16px; text-align: right; font-size: 14px; color: #ffffff;">&pound;{amount_gbp:.2f}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #222;">
                        <td style="padding: 12px 16px; font-size: 14px; color: #888;">Credits added</td>
                        <td style="padding: 12px 16px; text-align: right; font-size: 14px; color: #ffffff;">{credits:,}</td>
                    </tr>
                    <tr style="background-color: #111;">
                        <td style="padding: 12px 16px; font-weight: 700; font-size: 14px; color: #ffffff;">Amount paid</td>
                        <td style="padding: 12px 16px; text-align: right; font-weight: 700; font-size: 14px; color: #ffffff;">&pound;{amount_gbp:.2f}</td>
                    </tr>
                </table>
            </div>
        </div>

        <p style="color: #888; font-size: 14px; line-height: 1.5; margin: 0 0 24px;">
            Your credits are now available in your account.
        </p>

        <!-- Footer -->
        <div style="padding: 20px 0; border-top: 1px solid #222; text-align: center;">
            <p style="color: #555; font-size: 12px; margin: 0 0 4px;">
                Questions? Contact us at
                <a href="mailto:support@timrx.live" style="color: #5b7cfa; text-decoration: none;">support@timrx.live</a>
            </p>
            <p style="color: #444; font-size: 11px; margin: 8px 0 0;">TimrX &mdash; 3D Print Hub</p>
        </div>
    </div>
    """

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
Questions? Contact us at support@timrx.live
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
    Stripe-like design matching send_purchase_receipt.

    Uses EmailService.send_raw() for MIME multipart with attachments and
    inline logo image.  Falls back to simple send_purchase_receipt() on error.
    """
    from datetime import datetime, timezone

    subject = f"TimrX Receipt — {plan_name}"
    paid_date = datetime.now(timezone.utc).strftime("%B %d, %Y")

    # If no logo_bytes passed in, load it locally
    if not logo_bytes:
        logo_bytes = _load_logo()

    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" '
            'style="height: 3px; width: auto;" />'
        )

    # Build header: logo + TimrX text, or just text
    header_html = f"""
        <table cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td style="vertical-align: middle; padding-right: 8px;">{logo_img_tag}</td>
                <td style="vertical-align: middle;">
                    <span style="font-size: 15px; font-weight: 700; color: #ffffff; letter-spacing: 0.5px;">TimrX</span>
                </td>
            </tr>
        </table>
    """ if logo_bytes else """
        <span style="font-size: 15px; font-weight: 700; color: #ffffff; letter-spacing: 0.5px;">TimrX</span>
    """

    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                max-width: 600px; margin: 0 auto; color: #ffffff; padding: 0 16px;
                background-color: #000000; border-radius: 12px;">

        <!-- Logo -->
        <div style="padding: 20px 0 16px;">
            {header_html}
        </div>

        <!-- Amount section -->
        <div style="padding: 0 0 24px; border-bottom: 1px solid #222;">
            <p style="color: #888; font-size: 15px; margin: 0 0 6px;">Receipt from TimrX</p>
            <p style="font-size: 36px; font-weight: 700; margin: 0 0 8px; color: #ffffff;">&pound;{amount_gbp:.2f}</p>
            <p style="color: #27ae60; font-size: 14px; font-weight: 600; margin: 0;">Paid {paid_date}</p>
        </div>

        <!-- Reference numbers -->
        <div style="margin: 24px 0;">
            <div style="border: 1px solid #222; border-radius: 8px; overflow: hidden;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #222;">
                        <td style="padding: 12px 16px; font-size: 14px; color: #888;">Invoice number</td>
                        <td style="padding: 12px 16px; text-align: right; font-size: 14px; color: #ffffff;">{invoice_number}</td>
                    </tr>
                    <tr>
                        <td style="padding: 12px 16px; font-size: 14px; color: #888;">Receipt number</td>
                        <td style="padding: 12px 16px; text-align: right; font-size: 14px; color: #ffffff;">{receipt_number}</td>
                    </tr>
                </table>
            </div>
        </div>

        <!-- Summary card -->
        <div style="margin: 0 0 24px;">
            <div style="border: 1px solid #222; border-radius: 8px; overflow: hidden;">
                <div style="padding: 12px 16px; background-color: #111; border-bottom: 1px solid #222;">
                    <span style="font-weight: 600; font-size: 14px; color: #ffffff;">Summary</span>
                </div>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #222;">
                        <td style="padding: 12px 16px; font-size: 14px; color: #ffffff;">{plan_name}</td>
                        <td style="padding: 12px 16px; text-align: right; font-size: 14px; color: #ffffff;">&pound;{amount_gbp:.2f}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #222;">
                        <td style="padding: 12px 16px; font-size: 14px; color: #888;">Credits added</td>
                        <td style="padding: 12px 16px; text-align: right; font-size: 14px; color: #ffffff;">{credits:,}</td>
                    </tr>
                    <tr style="background-color: #111;">
                        <td style="padding: 12px 16px; font-weight: 700; font-size: 14px; color: #ffffff;">Amount paid</td>
                        <td style="padding: 12px 16px; text-align: right; font-weight: 700; font-size: 14px; color: #ffffff;">&pound;{amount_gbp:.2f}</td>
                    </tr>
                </table>
            </div>
        </div>

        <p style="color: #888; font-size: 14px; line-height: 1.5; margin: 0 0 6px;">
            Your invoice and receipt PDFs are attached to this email.
        </p>
        <p style="color: #888; font-size: 14px; line-height: 1.5; margin: 0 0 24px;">
            Your credits are now available in your account.
        </p>

        <!-- Footer -->
        <div style="padding: 20px 0; border-top: 1px solid #222; text-align: center;">
            <p style="color: #555; font-size: 12px; margin: 0 0 4px;">
                Questions? Contact us at
                <a href="mailto:support@timrx.live" style="color: #5b7cfa; text-decoration: none;">support@timrx.live</a>
            </p>
            <p style="color: #444; font-size: 11px; margin: 8px 0 0;">TimrX &mdash; 3D Print Hub</p>
        </div>
    </div>
    """

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
Questions? Contact us at support@timrx.live
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
def notify_admin(subject: str, message: str, data: Optional[Dict[str, Any]] = None) -> bool:
    """Send a notification to the admin email."""
    admin_email = config.ADMIN_EMAIL
    if not admin_email:
        print(f"[EMAIL] Admin notification (no ADMIN_EMAIL configured): {subject}")
        return False

    data_html = ""
    if data:
        rows = "".join(
            f"<tr><td style='padding: 4px 8px; color: #666;'>{k}:</td><td style='padding: 4px 8px;'>{v}</td></tr>"
            for k, v in data.items()
        )
        data_html = f"<table style='margin-top: 15px;'>{rows}</table>"

    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px;">
        <h3 style="color: #333;">{subject}</h3>
        <p>{message}</p>
        {data_html}
        <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
        <p style="color: #999; font-size: 11px;">TimrX Admin Notification</p>
    </div>
    """

    return send_email(admin_email, f"[TimrX Admin] {subject}", html_body)


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
