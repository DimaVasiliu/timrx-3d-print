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
    try:
        from backend.services.invoicing_service import _load_logo
        logo_bytes = _load_logo()
    except Exception:
        logo_bytes = None

    # Logo tag: CID if logo available, hidden otherwise
    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" '
            'style="height: 36px; width: auto; display: block;" />'
        )

    html_body = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto;
                background: #ffffff; border-radius: 12px; overflow: hidden;
                border: 1px solid #e8e8e8;">

        <!-- Header with logo -->
        <div style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
                    padding: 28px 32px; text-align: center;">
            <table cellpadding="0" cellspacing="0" border="0" style="margin: 0 auto;">
                <tr>
                    <td style="vertical-align: middle; padding-right: 12px;">
                        {logo_img_tag}
                    </td>
                    <td style="vertical-align: middle;">
                        <span style="font-size: 24px; font-weight: 700; color: #ffffff;
                                     letter-spacing: 1px;">TimrX</span>
                    </td>
                </tr>
            </table>
        </div>

        <!-- Body -->
        <div style="padding: 36px 32px 28px;">
            <h2 style="color: #1a1a2e; margin: 0 0 8px; font-size: 22px; font-weight: 600;">
                Your Access Code
            </h2>
            <p style="color: #555; font-size: 15px; line-height: 1.5; margin: 0 0 24px;">
                Use the code below to access your TimrX account:
            </p>

            <!-- Code box -->
            <div style="background: #f0f4ff; border: 2px dashed #c0cfff; border-radius: 10px;
                        padding: 24px; text-align: center; margin: 0 0 24px;">
                <span style="font-size: 36px; font-weight: 700; letter-spacing: 8px;
                             color: #1a1a2e; font-family: 'Courier New', monospace;">{code}</span>
            </div>

            <p style="color: #777; font-size: 14px; line-height: 1.5; margin: 0 0 6px;">
                This code expires in <strong style="color: #555;">15 minutes</strong>.
            </p>
            <p style="color: #999; font-size: 13px; line-height: 1.5; margin: 0;">
                If you didn't request this code, you can safely ignore this email.
            </p>
        </div>

        <!-- Footer -->
        <div style="background: #f9f9fb; border-top: 1px solid #eee; padding: 20px 32px;
                    text-align: center;">
            <p style="color: #999; font-size: 12px; margin: 0 0 6px;">
                TimrX &mdash; 3D Print Hub
            </p>
            <p style="color: #aaa; font-size: 11px; margin: 0;">
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
            return result.success
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
    subject = f"TimrX Purchase Receipt - {plan_name}"

    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #333;">Purchase Confirmed!</h2>
        <p>Thank you for your purchase. Here are your details:</p>
        <div style="background: #f5f5f5; padding: 20px; margin: 20px 0;">
            <table style="width: 100%;">
                <tr>
                    <td style="padding: 8px 0; color: #666;">Plan:</td>
                    <td style="padding: 8px 0; font-weight: bold;">{plan_name}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #666;">Credits Added:</td>
                    <td style="padding: 8px 0; font-weight: bold;">{credits:,}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #666;">Amount Paid:</td>
                    <td style="padding: 8px 0; font-weight: bold;">&pound;{amount_gbp:.2f}</td>
                </tr>
            </table>
        </div>
        <p style="color: #666;">Your credits are now available in your account.</p>
        <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
        <p style="color: #999; font-size: 12px;">TimrX - 3D Print Hub</p>
    </div>
    """

    text_body = f"""
Purchase Confirmed!

Thank you for your purchase. Here are your details:

Plan: {plan_name}
Credits Added: {credits:,}
Amount Paid: {amount_gbp:.2f} GBP

Your credits are now available in your account.

- TimrX
    """

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
    subject = f"TimrX Purchase Receipt — {plan_name}"

    # Logo CID reference (only if logo bytes available)
    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" '
            'style="height:32px; width:auto; margin-bottom:16px;" /><br>'
        )

    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;
                background: #fafafa; padding: 32px; border-radius: 8px;">
        {logo_img_tag}
        <h2 style="color: #222; margin-top: 0;">Payment Confirmed</h2>
        <p style="color: #555;">Thank you for your purchase. Your invoice and receipt are attached to this email.</p>

        <div style="background: #fff; border: 1px solid #e5e5e5; border-radius: 6px;
                    padding: 20px; margin: 20px 0;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td style="padding: 8px 0; color: #888; font-size: 14px;">Plan</td>
                    <td style="padding: 8px 0; font-weight: bold; text-align: right;">{plan_name}</td>
                </tr>
                <tr>
                    <td style="padding: 8px 0; color: #888; font-size: 14px;">Credits</td>
                    <td style="padding: 8px 0; font-weight: bold; text-align: right;">{credits:,}</td>
                </tr>
                <tr style="border-top: 1px solid #eee;">
                    <td style="padding: 10px 0 4px; color: #888; font-size: 14px;">Amount Paid</td>
                    <td style="padding: 10px 0 4px; font-weight: bold; font-size: 18px; text-align: right;">
                        &pound;{amount_gbp:.2f}
                    </td>
                </tr>
            </table>
        </div>

        <p style="color: #555; font-size: 13px;">
            <strong>Invoice:</strong> {invoice_number}<br>
            <strong>Receipt:</strong> {receipt_number}
        </p>

        <p style="color: #999; font-size: 12px; margin-top: 24px;">
            Your credits are now available in your account.
        </p>

        <hr style="border: none; border-top: 1px solid #e5e5e5; margin: 24px 0 16px;">
        <p style="color: #aaa; font-size: 11px; text-align: center;">
            TimrX &mdash; 3D Print Hub &nbsp;|&nbsp; timrx.live &nbsp;|&nbsp; support@timrx.live
        </p>
    </div>
    """

    text_body = f"""Payment Confirmed — {plan_name}

Thank you for your purchase.

Plan: {plan_name}
Credits: {credits:,}
Amount Paid: £{amount_gbp:.2f}

Invoice: {invoice_number}
Receipt: {receipt_number}

Your credits are now available in your account.
Your invoice and receipt PDFs are attached.

— TimrX · 3D Print Hub · support@timrx.live
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
