"""
Email Service - Hardened email sending with proper error handling.

Features:
- EMAIL_ENABLED toggle (if false, just logs)
- EMAIL_PROVIDER support (neo, ses, sendgrid)
- DNS and TCP health checks
- Never crashes calling endpoints on failure
- Detailed logging for debugging

Usage:
    from email_service import EmailService

    # Send email (never throws)
    success = EmailService.send(to="user@example.com", subject="Hi", html="<p>Hello</p>")

    # Health check
    result = EmailService.healthcheck()
"""

import smtplib
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, Dict, Any, Tuple
from dataclasses import dataclass

from config import config


@dataclass
class EmailResult:
    """Result of an email send attempt."""
    success: bool
    message: str
    error: Optional[str] = None


class EmailService:
    """Hardened email service with proper error handling."""

    _initialized = False
    _config_logged = False

    @classmethod
    def _log_config(cls) -> None:
        """Log email configuration at startup (once)."""
        if cls._config_logged:
            return
        cls._config_logged = True

        if not config.EMAIL_ENABLED:
            print("[EMAIL] Email DISABLED (EMAIL_ENABLED=false) - emails will be logged only")
            return

        if not config.EMAIL_CONFIGURED:
            missing = []
            if not config.SMTP_HOST:
                missing.append("SMTP_HOST")
            if not config.SMTP_USER:
                missing.append("SMTP_USER")
            if not config.SMTP_PASSWORD:
                missing.append("SMTP_PASSWORD")
            print(f"[EMAIL] WARNING: Email not configured - missing: {', '.join(missing)}")
            print("[EMAIL] Emails will be logged only until configured")
            return

        # Mask credentials for logging
        masked_user = config.SMTP_USER[:3] + "***" if len(config.SMTP_USER) > 3 else "(set)"
        masked_pass = "***" + config.SMTP_PASSWORD[-4:] if len(config.SMTP_PASSWORD) > 4 else "(set)"
        from_name, from_addr = config.SMTP_FROM_PARSED

        print(f"[EMAIL] Email ENABLED via {config.EMAIL_PROVIDER.upper()}")
        print(f"[EMAIL] SMTP: {config.SMTP_HOST}:{config.SMTP_PORT} (TLS={config.SMTP_USE_TLS})")
        print(f"[EMAIL] Auth: user={masked_user} pass={masked_pass}")
        print(f"[EMAIL] From: {from_name} <{from_addr}>")

    @classmethod
    def send(
        cls,
        to: str,
        subject: str,
        html: str,
        text: Optional[str] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None,
    ) -> EmailResult:
        """
        Send an email. Never throws - returns EmailResult.

        Args:
            to: Recipient email address
            subject: Email subject
            html: HTML body
            text: Plain text body (optional, auto-generated if not provided)
            from_email: Override from address
            from_name: Override from name

        Returns:
            EmailResult with success status and message
        """
        cls._log_config()

        # If disabled, just log
        if not config.EMAIL_ENABLED:
            print(f"[EMAIL] DISABLED - Would send to {to}: {subject}")
            return EmailResult(success=True, message="Email disabled - logged only")

        # If not configured, log and return success (don't block flows)
        if not config.EMAIL_CONFIGURED:
            print(f"[EMAIL] NOT CONFIGURED - Would send to {to}: {subject}")
            return EmailResult(success=True, message="Email not configured - logged only")

        # Get from address
        default_name, default_addr = config.SMTP_FROM_PARSED
        sender_name = from_name or default_name
        sender_addr = from_email or default_addr

        try:
            # Build message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{sender_name} <{sender_addr}>"
            msg["To"] = to

            # Attach text version
            if text:
                msg.attach(MIMEText(text, "plain"))

            # Attach HTML version
            msg.attach(MIMEText(html, "html"))

            # Log connection attempt
            print(f"[EMAIL] Connecting: {config.SMTP_HOST}:{config.SMTP_PORT} (provider={config.EMAIL_PROVIDER})")

            # Send via SMTP with timeout
            with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=config.SMTP_TIMEOUT) as server:
                if config.SMTP_USE_TLS:
                    server.starttls()
                server.login(config.SMTP_USER, config.SMTP_PASSWORD)
                server.sendmail(sender_addr, to, msg.as_string())

            print(f"[EMAIL] SENT to {to}: {subject}")
            return EmailResult(success=True, message="Email sent successfully")

        except socket.gaierror as e:
            # DNS resolution failed
            error_msg = f"DNS resolution failed for {config.SMTP_HOST}: {e}"
            print(f"[EMAIL] FAILED - {error_msg}")
            return EmailResult(success=False, message="DNS resolution failed", error=str(e))

        except socket.timeout as e:
            error_msg = f"Connection timeout to {config.SMTP_HOST}:{config.SMTP_PORT}"
            print(f"[EMAIL] FAILED - {error_msg}")
            return EmailResult(success=False, message="Connection timeout", error=str(e))

        except smtplib.SMTPAuthenticationError as e:
            error_msg = f"SMTP authentication failed: {e}"
            print(f"[EMAIL] FAILED - {error_msg}")
            return EmailResult(success=False, message="Authentication failed", error=str(e))

        except smtplib.SMTPException as e:
            error_msg = f"SMTP error: {e}"
            print(f"[EMAIL] FAILED - {error_msg}")
            return EmailResult(success=False, message="SMTP error", error=str(e))

        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            print(f"[EMAIL] FAILED - {error_msg}")
            return EmailResult(success=False, message="Unexpected error", error=str(e))

    @classmethod
    def healthcheck(cls) -> Dict[str, Any]:
        """
        Check email service health:
        1. DNS resolution of SMTP_HOST
        2. TCP connection to SMTP_HOST:SMTP_PORT

        Returns dict with status and details.
        """
        cls._log_config()

        result = {
            "enabled": config.EMAIL_ENABLED,
            "configured": config.EMAIL_CONFIGURED,
            "provider": config.EMAIL_PROVIDER,
            "smtp_host": config.SMTP_HOST,
            "smtp_port": config.SMTP_PORT,
            "dns_ok": False,
            "tcp_ok": False,
            "error": None,
        }

        if not config.EMAIL_ENABLED:
            result["status"] = "disabled"
            result["message"] = "Email sending is disabled"
            return result

        if not config.EMAIL_CONFIGURED:
            result["status"] = "not_configured"
            result["message"] = "SMTP credentials not configured"
            return result

        # Step 1: DNS resolution
        print(f"[EMAIL] Healthcheck: Resolving DNS for {config.SMTP_HOST}...")
        try:
            ip_addresses = socket.gethostbyname_ex(config.SMTP_HOST)
            result["dns_ok"] = True
            result["dns_ips"] = ip_addresses[2]
            print(f"[EMAIL] Healthcheck: DNS OK - {config.SMTP_HOST} -> {ip_addresses[2]}")
        except socket.gaierror as e:
            result["error"] = f"DNS resolution failed: {e}"
            result["status"] = "dns_failed"
            result["message"] = f"Cannot resolve {config.SMTP_HOST}"
            print(f"[EMAIL] Healthcheck: DNS FAILED - {e}")
            return result

        # Step 2: TCP connection
        print(f"[EMAIL] Healthcheck: TCP connect to {config.SMTP_HOST}:{config.SMTP_PORT}...")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((config.SMTP_HOST, config.SMTP_PORT))
            sock.close()
            result["tcp_ok"] = True
            print(f"[EMAIL] Healthcheck: TCP OK - connected to {config.SMTP_HOST}:{config.SMTP_PORT}")
        except socket.timeout:
            result["error"] = "TCP connection timeout (5s)"
            result["status"] = "tcp_timeout"
            result["message"] = f"Connection to {config.SMTP_HOST}:{config.SMTP_PORT} timed out"
            print(f"[EMAIL] Healthcheck: TCP TIMEOUT")
            return result
        except socket.error as e:
            result["error"] = f"TCP connection failed: {e}"
            result["status"] = "tcp_failed"
            result["message"] = f"Cannot connect to {config.SMTP_HOST}:{config.SMTP_PORT}"
            print(f"[EMAIL] Healthcheck: TCP FAILED - {e}")
            return result

        result["status"] = "healthy"
        result["message"] = "Email service is healthy"
        print(f"[EMAIL] Healthcheck: HEALTHY")
        return result

    @classmethod
    def send_test(cls, to: str) -> EmailResult:
        """Send a test email to verify configuration."""
        return cls.send(
            to=to,
            subject="TimrX Email Test",
            html="""
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <h2 style="color: #333;">Email Test Successful!</h2>
                <p>This is a test email from TimrX to verify your email configuration is working.</p>
                <p style="color: #666; font-size: 14px;">If you received this email, your SMTP settings are correct.</p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 30px 0;">
                <p style="color: #999; font-size: 12px;">TimrX - 3D Print Hub</p>
            </div>
            """,
            text="Email Test Successful!\n\nThis is a test email from TimrX to verify your email configuration is working.\n\nIf you received this email, your SMTP settings are correct.\n\n- TimrX",
        )


# ─────────────────────────────────────────────────────────────
# Convenience functions (for backward compatibility)
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
    Send an email (backward compatible function).
    Returns True on success, False on failure.
    """
    result = EmailService.send(
        to=to_email,
        subject=subject,
        html=html_body,
        text=text_body,
        from_email=from_email,
        from_name=from_name,
    )
    return result.success


def email_healthcheck() -> Dict[str, Any]:
    """Run email health check and return results."""
    return EmailService.healthcheck()


# Log config on module import
EmailService._log_config()
