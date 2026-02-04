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
    except Exception as e:
        print(f"[EMAIL] send_magic_code: failed to load logo: {e}")
        logo_bytes = None

    if logo_bytes:
        print(f"[EMAIL] send_magic_code: logo loaded ({len(logo_bytes)} bytes)")
    else:
        print("[EMAIL] send_magic_code: no logo available, sending without logo")

    # Logo tag: CID if logo available, hidden otherwise
    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" '
            'style="height: 22px; width: auto; display: block;" />'
        )

    html_body = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto;
                background-color: #ffffff; border-radius: 12px; overflow: hidden;
                border: 1px solid #e8e8e8;">

        <!-- Header with logo -->
        <div style="background-color: #1a1a2e; padding: 18px 24px;">
            <table cellpadding="0" cellspacing="0" border="0">
                <tr>
                    <td style="vertical-align: middle; padding-right: 8px;">
                        {logo_img_tag}
                    </td>
                    <td style="vertical-align: middle;">
                        <span style="font-size: 15px; font-weight: 700; color: #ffffff;
                                     letter-spacing: 0.5px;">TimrX</span>
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
            <div style="background-color: #f0f4ff; border: 2px dashed #c0cfff; border-radius: 10px;
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
        <div style="background-color: #f9f9fb; border-top: 1px solid #eee; padding: 20px 32px;
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
    try:
        from backend.services.invoicing_service import _load_logo
        logo_bytes = _load_logo()
    except Exception:
        logo_bytes = None

    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" '
            'style="height: 22px; width: auto;" />'
        )

    # Build header: logo + TimrX text, or just text
    header_html = f"""
        <table cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td style="vertical-align: middle; padding-right: 8px;">{logo_img_tag}</td>
                <td style="vertical-align: middle;">
                    <span style="font-size: 15px; font-weight: 700; color: #1a1a2e; letter-spacing: 0.5px;">TimrX</span>
                </td>
            </tr>
        </table>
    """ if logo_bytes else """
        <span style="font-size: 15px; font-weight: 700; color: #1a1a2e; letter-spacing: 0.5px;">TimrX</span>
    """

    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                max-width: 600px; margin: 0 auto; color: #1a1a2e; padding: 0 16px;">

        <!-- Logo -->
        <div style="padding: 24px 0 20px;">
            {header_html}
        </div>

        <!-- Amount section -->
        <div style="padding: 0 0 28px; border-bottom: 1px solid #e8e8e8;">
            <p style="color: #666; font-size: 15px; margin: 0 0 6px;">Receipt from TimrX</p>
            <p style="font-size: 36px; font-weight: 700; margin: 0 0 8px; color: #1a1a2e;">&pound;{amount_gbp:.2f}</p>
            <p style="color: #27ae60; font-size: 14px; font-weight: 600; margin: 0;">Paid {paid_date}</p>
        </div>

        <!-- Summary card -->
        <div style="margin: 28px 0;">
            <div style="border: 1px solid #e2e2e2; border-radius: 8px; overflow: hidden;">
                <div style="padding: 14px 16px; background-color: #f7f7f9; border-bottom: 1px solid #e2e2e2;">
                    <span style="font-weight: 600; font-size: 14px; color: #1a1a2e;">Summary</span>
                </div>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #efefef;">
                        <td style="padding: 14px 16px; font-size: 14px; color: #1a1a2e;">{plan_name}</td>
                        <td style="padding: 14px 16px; text-align: right; font-size: 14px; color: #1a1a2e;">&pound;{amount_gbp:.2f}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #efefef;">
                        <td style="padding: 14px 16px; font-size: 14px; color: #666;">Credits added</td>
                        <td style="padding: 14px 16px; text-align: right; font-size: 14px; color: #1a1a2e;">{credits:,}</td>
                    </tr>
                    <tr style="background-color: #f7f7f9;">
                        <td style="padding: 14px 16px; font-weight: 700; font-size: 14px; color: #1a1a2e;">Amount paid</td>
                        <td style="padding: 14px 16px; text-align: right; font-weight: 700; font-size: 14px; color: #1a1a2e;">&pound;{amount_gbp:.2f}</td>
                    </tr>
                </table>
            </div>
        </div>

        <p style="color: #666; font-size: 14px; line-height: 1.5; margin: 0 0 28px;">
            Your credits are now available in your account.
        </p>

        <!-- Footer -->
        <div style="padding: 24px 0; border-top: 1px solid #e8e8e8; text-align: center;">
            <p style="color: #999; font-size: 12px; margin: 0 0 4px;">
                Questions? Contact us at
                <a href="mailto:support@timrx.live" style="color: #5b7cfa; text-decoration: none;">support@timrx.live</a>
            </p>
            <p style="color: #bbb; font-size: 11px; margin: 8px 0 0;">TimrX &mdash; 3D Print Hub</p>
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

    # If no logo_bytes passed in, try loading it
    if not logo_bytes:
        try:
            from backend.services.invoicing_service import _load_logo
            logo_bytes = _load_logo()
        except Exception:
            pass

    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" '
            'style="height: 22px; width: auto;" />'
        )

    # Build header: logo + TimrX text, or just text
    header_html = f"""
        <table cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td style="vertical-align: middle; padding-right: 8px;">{logo_img_tag}</td>
                <td style="vertical-align: middle;">
                    <span style="font-size: 15px; font-weight: 700; color: #1a1a2e; letter-spacing: 0.5px;">TimrX</span>
                </td>
            </tr>
        </table>
    """ if logo_bytes else """
        <span style="font-size: 15px; font-weight: 700; color: #1a1a2e; letter-spacing: 0.5px;">TimrX</span>
    """

    html_body = f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                max-width: 600px; margin: 0 auto; color: #1a1a2e; padding: 0 16px;">

        <!-- Logo -->
        <div style="padding: 24px 0 20px;">
            {header_html}
        </div>

        <!-- Amount section -->
        <div style="padding: 0 0 28px; border-bottom: 1px solid #e8e8e8;">
            <p style="color: #666; font-size: 15px; margin: 0 0 6px;">Receipt from TimrX</p>
            <p style="font-size: 36px; font-weight: 700; margin: 0 0 8px; color: #1a1a2e;">&pound;{amount_gbp:.2f}</p>
            <p style="color: #27ae60; font-size: 14px; font-weight: 600; margin: 0;">Paid {paid_date}</p>
        </div>

        <!-- Reference numbers -->
        <div style="margin: 24px 0;">
            <div style="border: 1px solid #e2e2e2; border-radius: 8px; overflow: hidden;">
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #efefef;">
                        <td style="padding: 12px 16px; font-size: 14px; color: #666;">Invoice number</td>
                        <td style="padding: 12px 16px; text-align: right; font-size: 14px; color: #1a1a2e;">{invoice_number}</td>
                    </tr>
                    <tr>
                        <td style="padding: 12px 16px; font-size: 14px; color: #666;">Receipt number</td>
                        <td style="padding: 12px 16px; text-align: right; font-size: 14px; color: #1a1a2e;">{receipt_number}</td>
                    </tr>
                </table>
            </div>
        </div>

        <!-- Summary card -->
        <div style="margin: 0 0 24px;">
            <div style="border: 1px solid #e2e2e2; border-radius: 8px; overflow: hidden;">
                <div style="padding: 14px 16px; background-color: #f7f7f9; border-bottom: 1px solid #e2e2e2;">
                    <span style="font-weight: 600; font-size: 14px; color: #1a1a2e;">Summary</span>
                </div>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr style="border-bottom: 1px solid #efefef;">
                        <td style="padding: 14px 16px; font-size: 14px; color: #1a1a2e;">{plan_name}</td>
                        <td style="padding: 14px 16px; text-align: right; font-size: 14px; color: #1a1a2e;">&pound;{amount_gbp:.2f}</td>
                    </tr>
                    <tr style="border-bottom: 1px solid #efefef;">
                        <td style="padding: 14px 16px; font-size: 14px; color: #666;">Credits added</td>
                        <td style="padding: 14px 16px; text-align: right; font-size: 14px; color: #1a1a2e;">{credits:,}</td>
                    </tr>
                    <tr style="background-color: #f7f7f9;">
                        <td style="padding: 14px 16px; font-weight: 700; font-size: 14px; color: #1a1a2e;">Amount paid</td>
                        <td style="padding: 14px 16px; text-align: right; font-weight: 700; font-size: 14px; color: #1a1a2e;">&pound;{amount_gbp:.2f}</td>
                    </tr>
                </table>
            </div>
        </div>

        <p style="color: #666; font-size: 14px; line-height: 1.5; margin: 0 0 6px;">
            Your invoice and receipt PDFs are attached to this email.
        </p>
        <p style="color: #666; font-size: 14px; line-height: 1.5; margin: 0 0 28px;">
            Your credits are now available in your account.
        </p>

        <!-- Footer -->
        <div style="padding: 24px 0; border-top: 1px solid #e8e8e8; text-align: center;">
            <p style="color: #999; font-size: 12px; margin: 0 0 4px;">
                Questions? Contact us at
                <a href="mailto:support@timrx.live" style="color: #5b7cfa; text-decoration: none;">support@timrx.live</a>
            </p>
            <p style="color: #bbb; font-size: 11px; margin: 8px 0 0;">TimrX &mdash; 3D Print Hub</p>
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
# Blog / Updates Email
# ─────────────────────────────────────────────────────────────
def send_blog_update(
    to_email: str,
    title: str,
    summary: str,
    blog_url: Optional[str] = None,
) -> bool:
    """
    Send a blog/updates email to a user.

    Args:
        to_email: Recipient email address
        title: Blog post or update title
        summary: Short summary or excerpt (supports HTML)
        blog_url: Optional link to the full post
    """
    subject = f"TimrX Update — {title}"

    # Load logo + blog icon for inline CID embedding
    try:
        from backend.services.invoicing_service import _load_logo
        logo_bytes = _load_logo()
    except Exception:
        logo_bytes = None

    blog_icon_bytes = None
    try:
        from backend.config import config as _cfg
        for _bp in [
            _cfg.APP_DIR / "backend" / "assets" / "blogs.png",  # server
            _cfg.APP_DIR / "assets" / "blogs.png",               # local alt
        ]:
            resolved = _bp.resolve()
            if resolved.is_file():
                blog_icon_bytes = resolved.read_bytes()
                break
    except Exception:
        pass

    logo_img_tag = ""
    if logo_bytes:
        logo_img_tag = (
            '<img src="cid:timrx_logo" alt="TimrX" '
            'style="height: 22px; width: auto; display: block;" />'
        )

    blog_icon_tag = ""
    if blog_icon_bytes:
        blog_icon_tag = (
            '<img src="cid:blog_icon" alt="" '
            'style="height: 48px; width: auto; display: block; margin: 0 auto 16px;" />'
        )

    # CTA button (only if blog_url provided)
    cta_html = ""
    if blog_url:
        cta_html = f"""
            <div style="text-align: center; margin: 24px 0 0;">
                <a href="{blog_url}"
                   style="display: inline-block; background-color: #5b7cfa; color: #ffffff;
                          font-size: 14px; font-weight: 600; text-decoration: none;
                          padding: 12px 28px; border-radius: 6px;">
                    Read More
                </a>
            </div>
        """

    html_body = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto;
                background-color: #ffffff; border-radius: 12px; overflow: hidden;
                border: 1px solid #e8e8e8;">

        <!-- Header with logo -->
        <div style="background-color: #1a1a2e; padding: 18px 24px;">
            <table cellpadding="0" cellspacing="0" border="0">
                <tr>
                    <td style="vertical-align: middle; padding-right: 8px;">
                        {logo_img_tag}
                    </td>
                    <td style="vertical-align: middle;">
                        <span style="font-size: 15px; font-weight: 700; color: #ffffff;
                                     letter-spacing: 0.5px;">TimrX</span>
                    </td>
                </tr>
            </table>
        </div>

        <!-- Body -->
        <div style="padding: 36px 32px 28px;">
            <div style="text-align: center;">
                {blog_icon_tag}
            </div>
            <h2 style="color: #1a1a2e; margin: 0 0 8px; font-size: 22px; font-weight: 600;">
                {title}
            </h2>
            <div style="color: #555; font-size: 15px; line-height: 1.6; margin: 0 0 8px;">
                {summary}
            </div>
            {cta_html}
        </div>

        <!-- Footer -->
        <div style="background-color: #f9f9fb; border-top: 1px solid #eee; padding: 20px 32px;
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

    # Plain text version
    cta_text = f"\nRead more: {blog_url}\n" if blog_url else ""

    text_body = f"""{title}

{summary}
{cta_text}
---
TimrX - 3D Print Hub
Need help? Contact us at support@timrx.live
"""

    # Build inline images list
    inline_images = []
    if logo_bytes:
        inline_images.append({
            "cid": "timrx_logo",
            "data": logo_bytes,
            "content_type": "image/png",
        })
    if blog_icon_bytes:
        inline_images.append({
            "cid": "blog_icon",
            "data": blog_icon_bytes,
            "content_type": "image/png",
        })

    if inline_images:
        try:
            from backend.services.email_service import EmailService
            result = EmailService.send_raw(
                to=to_email,
                subject=subject,
                html=html_body,
                text=text_body,
                inline_images=inline_images,
            )
            if result.success:
                return True
            print(f"[EMAIL] send_blog_update send_raw failed: {result.message}, falling back to simple send")
        except Exception as e:
            print(f"[EMAIL] send_blog_update send_raw error: {e}, falling back to simple send")

    return send_email(to_email, subject, html_body, text_body)


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
