"""
Email utilities for TimrX Backend.
Handles sending transactional emails via SendGrid or SMTP.
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, Any

from .config import (
    SENDGRID_API_KEY,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASSWORD,
    EMAIL_FROM_ADDRESS,
    EMAIL_FROM_NAME,
    ADMIN_EMAIL,
    NOTIFY_ON_NEW_IDENTITY,
    NOTIFY_ON_PURCHASE,
    NOTIFY_ON_RESTORE_REQUEST,
    IS_DEV,
)

# Check if email is configured
EMAIL_CONFIGURED = bool(SENDGRID_API_KEY or (SMTP_HOST and SMTP_USER and SMTP_PASSWORD))

if EMAIL_CONFIGURED:
    print(f"[EMAIL] Email configured via {'SendGrid' if SENDGRID_API_KEY else 'SMTP'}")
else:
    print("[EMAIL] WARNING: Email not configured - emails will be logged only")


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
    Send an email via SMTP (SendGrid-compatible).
    Returns True on success, False on failure.
    """
    if not EMAIL_CONFIGURED:
        print(f"[EMAIL] Would send to {to_email}: {subject}")
        if IS_DEV:
            print(f"[EMAIL] Body: {text_body or html_body[:200]}...")
        return True  # Pretend success in dev when not configured

    from_addr = from_email or EMAIL_FROM_ADDRESS
    sender_name = from_name or EMAIL_FROM_NAME

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{sender_name} <{from_addr}>"
        msg["To"] = to_email

        # Attach text version if provided
        if text_body:
            msg.attach(MIMEText(text_body, "plain"))

        # Attach HTML version
        msg.attach(MIMEText(html_body, "html"))

        # Send via SMTP
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(from_addr, to_email, msg.as_string())

        print(f"[EMAIL] Sent to {to_email}: {subject}")
        return True

    except Exception as e:
        print(f"[EMAIL] Failed to send to {to_email}: {e}")
        return False


# ─────────────────────────────────────────────────────────────
# Transactional Email Templates
# ─────────────────────────────────────────────────────────────
def send_magic_code(to_email: str, code: str) -> bool:
    """Send a magic login code to the user."""
    subject = "Your TimrX Access Code"

    html_body = f"""
    <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
        <h2 style="color: #333;">Your Access Code</h2>
        <p>Use this code to restore access to your TimrX account:</p>
        <div style="background: #f5f5f5; padding: 20px; text-align: center; margin: 20px 0;">
            <span style="font-size: 32px; font-weight: bold; letter-spacing: 4px; color: #333;">{code}</span>
        </div>
        <p style="color: #666; font-size: 14px;">This code expires in 15 minutes.</p>
        <p style="color: #666; font-size: 14px;">If you didn't request this code, you can safely ignore this email.</p>
        <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
        <p style="color: #999; font-size: 12px;">TimrX - 3D Print Hub</p>
    </div>
    """

    text_body = f"""
Your TimrX Access Code

Use this code to restore access to your account:

{code}

This code expires in 15 minutes.

If you didn't request this code, you can safely ignore this email.

- TimrX
    """

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
# Admin Notifications
# ─────────────────────────────────────────────────────────────
def notify_admin(subject: str, message: str, data: Optional[Dict[str, Any]] = None) -> bool:
    """Send a notification to the admin email."""
    if not ADMIN_EMAIL:
        print(f"[EMAIL] Admin notification (no email configured): {subject}")
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

    return send_email(ADMIN_EMAIL, f"[TimrX Admin] {subject}", html_body)


def notify_new_identity(identity_id: str, email: Optional[str] = None) -> bool:
    """Notify admin when a new identity is created (with email)."""
    if not NOTIFY_ON_NEW_IDENTITY:
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
    if not NOTIFY_ON_PURCHASE:
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
    if not NOTIFY_ON_RESTORE_REQUEST:
        return False

    return notify_admin(
        "Restore Code Requested",
        "A user has requested an account restore code.",
        {"Email": email},
    )
