"""
Contact Form Routes Blueprint
-----------------------------
Handles contact form submissions from the portfolio site.
Sends formatted emails to admin.
"""

from __future__ import annotations

from flask import Blueprint, jsonify, request
from backend.services.email_service import EmailService
from backend.config import config
import re
from datetime import datetime

bp = Blueprint("contact", __name__)

# Email validation regex
EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

# Admin email to receive contact form submissions
ADMIN_EMAIL = "admin@timrx.live"


def sanitize_html(text: str) -> str:
    """Basic HTML escaping to prevent injection."""
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


@bp.route("/contact/submit", methods=["POST", "OPTIONS"])
def submit_contact():
    """
    Handle contact form submission.

    Expected JSON body:
    {
        "name": "John Doe",
        "email": "john@example.com",
        "subject": "Project Inquiry",  (optional)
        "budget": "£1,500–£3,000",
        "message": "Tell me about your project..."
    }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    try:
        data = request.get_json() or {}

        # Extract and validate fields
        name = (data.get("name") or "").strip()
        email = (data.get("email") or "").strip()
        subject = (data.get("subject") or "").strip()
        budget = (data.get("budget") or "").strip()
        message = (data.get("message") or "").strip()

        # Validation
        errors = []

        if not name:
            errors.append("Name is required")
        elif len(name) > 100:
            errors.append("Name is too long (max 100 characters)")

        if not email:
            errors.append("Email is required")
        elif not EMAIL_REGEX.match(email):
            errors.append("Invalid email address")

        if not budget:
            errors.append("Budget selection is required")

        if not message:
            errors.append("Message is required")
        elif len(message) < 10:
            errors.append("Message is too short (min 10 characters)")
        elif len(message) > 5000:
            errors.append("Message is too long (max 5000 characters)")

        if subject and len(subject) > 200:
            errors.append("Subject is too long (max 200 characters)")

        if errors:
            return jsonify({
                "ok": False,
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": errors[0],
                    "details": errors
                }
            }), 400

        # Sanitize for HTML email
        safe_name = sanitize_html(name)
        safe_email = sanitize_html(email)
        safe_subject = sanitize_html(subject) if subject else "(No subject)"
        safe_budget = sanitize_html(budget)
        safe_message = sanitize_html(message)

        # Format timestamp
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        # Build HTML email
        html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,Ubuntu,sans-serif;background:#f5f5f5;">
    <div style="max-width:600px;margin:0 auto;padding:20px;">
        <div style="background:#000000;color:#fff;padding:20px 24px 16px;border-radius:12px 12px 0 0;">
            <table cellpadding="0" cellspacing="0" border="0" width="100%">
                <tr>
                    <td>
                        <table cellpadding="0" cellspacing="0" border="0">
                            <tr>
                                <td style="vertical-align:middle;padding-right:10px;line-height:0;">
                                    <img src="https://timrx.live/img/logo.png" alt="TimrX" height="32"
                                         style="height:32px;width:auto;display:block;" />
                                </td>
                                <td style="vertical-align:middle;">
                                    <span style="font-size:18px;font-weight:800;color:#ffffff;letter-spacing:0.5px;">TimrX</span>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
            <h1 style="margin:16px 0 0;font-size:20px;font-weight:600;color:#ffffff;">New Contact Form Submission</h1>
            <p style="margin:6px 0 0;opacity:0.7;font-size:13px;color:#aaa;">TimrX Portfolio</p>
        </div>

        <div style="background:#fff;padding:30px;border:1px solid #e8e8e8;border-top:none;">
            <table style="width:100%;border-collapse:collapse;">
                <tr>
                    <td style="padding:12px 0;border-bottom:1px solid #f0f0f0;width:120px;color:#666;font-size:14px;vertical-align:top;">From:</td>
                    <td style="padding:12px 0;border-bottom:1px solid #f0f0f0;font-size:14px;font-weight:500;">{safe_name}</td>
                </tr>
                <tr>
                    <td style="padding:12px 0;border-bottom:1px solid #f0f0f0;color:#666;font-size:14px;vertical-align:top;">Email:</td>
                    <td style="padding:12px 0;border-bottom:1px solid #f0f0f0;font-size:14px;">
                        <a href="mailto:{safe_email}" style="color:#0066cc;text-decoration:none;">{safe_email}</a>
                    </td>
                </tr>
                <tr>
                    <td style="padding:12px 0;border-bottom:1px solid #f0f0f0;color:#666;font-size:14px;vertical-align:top;">Budget:</td>
                    <td style="padding:12px 0;border-bottom:1px solid #f0f0f0;font-size:14px;">
                        <span style="background:#0b0b0b;color:#fff;padding:4px 12px;border-radius:20px;font-size:13px;">{safe_budget}</span>
                    </td>
                </tr>
                <tr>
                    <td style="padding:12px 0;border-bottom:1px solid #f0f0f0;color:#666;font-size:14px;vertical-align:top;">Subject:</td>
                    <td style="padding:12px 0;border-bottom:1px solid #f0f0f0;font-size:14px;">{safe_subject}</td>
                </tr>
            </table>

            <div style="margin-top:24px;">
                <h3 style="margin:0 0 12px;font-size:14px;color:#666;font-weight:500;">Message:</h3>
                <div style="background:#f9f9f9;padding:20px;border-radius:8px;font-size:14px;line-height:1.6;white-space:pre-wrap;">{safe_message}</div>
            </div>
        </div>

        <div style="background:#fafafa;padding:20px 30px;border:1px solid #e8e8e8;border-top:none;border-radius:0 0 12px 12px;">
            <p style="margin:0;font-size:12px;color:#888;">
                Received: {timestamp}<br>
                Reply directly to this email to respond to {safe_name}.
            </p>
        </div>
    </div>
</body>
</html>
"""

        # Build plain text version
        text_body = f"""
NEW CONTACT FORM SUBMISSION
===========================

From: {name}
Email: {email}
Budget: {budget}
Subject: {subject if subject else "(No subject)"}

Message:
--------
{message}

---
Received: {timestamp}
Reply to this email to respond to {name}.
"""

        # Send email to admin
        email_subject = f"[TimrX Contact] {subject if subject else f'New inquiry from {name}'}"

        result = EmailService.send(
            to=ADMIN_EMAIL,
            subject=email_subject,
            html=html_body,
            text=text_body,
            reply_to=email,  # Reply-To header for easy response
            reply_to_name=name
        )

        if result.success:
            print(f"[CONTACT] Form submitted successfully from {email}")
            return jsonify({
                "ok": True,
                "message": "Your message has been sent successfully. I'll get back to you within 24-48 hours."
            })
        else:
            # Email failed - inform user to try again or use alternative contact
            print(f"[CONTACT] Email send failed: {result.error}")
            return jsonify({
                "ok": False,
                "error": {
                    "code": "EMAIL_FAILED",
                    "message": "Failed to send message. Please try again or email directly at admin@timrx.live"
                }
            }), 500

    except Exception as e:
        print(f"[CONTACT] Error processing form: {e}")
        return jsonify({
            "ok": False,
            "error": {
                "code": "SERVER_ERROR",
                "message": "Something went wrong. Please try again or email directly."
            }
        }), 500
