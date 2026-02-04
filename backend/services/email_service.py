"""
Email Service - Hardened email sending with proper error handling.

Features:
- EMAIL_ENABLED toggle (if false, just logs)
- EMAIL_PROVIDER support (neo, ses, sendgrid)
- AWS SES via boto3 (when EMAIL_PROVIDER=ses)
- SMTP for other providers (neo, sendgrid, etc.)
- DNS and TCP health checks (for SMTP)
- Never crashes calling endpoints on failure
- Detailed logging for debugging

Usage:
    from backend.services.email_service import EmailService

    # Send email (never throws)
    success = EmailService.send(to="user@example.com", subject="Hi", html="<p>Hello</p>")

    # Health check
    result = EmailService.healthcheck()

Environment variables for SES:
    EMAIL_PROVIDER=ses
    AWS_REGION=eu-west-2
    AWS_ACCESS_KEY_ID=xxx
    AWS_SECRET_ACCESS_KEY=xxx
    SES_FROM_EMAIL=noreply@timrx.app (or EMAIL_FROM_ADDRESS)
"""

import smtplib
import socket
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass

from backend.config import config

# Try to import boto3 for SES
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    print("[EMAIL] WARNING: boto3 not available - SES sending disabled")


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

        provider = config.EMAIL_PROVIDER.lower()

        # SES provider
        if provider == "ses":
            if not BOTO3_AVAILABLE:
                print("[EMAIL] WARNING: EMAIL_PROVIDER=ses but boto3 not installed")
                print("[EMAIL] Emails will be logged only until boto3 is available")
                return

            if not config.EMAIL_CONFIGURED:
                missing = []
                if not config.AWS_ACCESS_KEY_ID:
                    missing.append("AWS_ACCESS_KEY_ID")
                if not config.AWS_SECRET_ACCESS_KEY:
                    missing.append("AWS_SECRET_ACCESS_KEY")
                if not (config.SES_FROM_EMAIL or config.EMAIL_FROM_ADDRESS):
                    missing.append("SES_FROM_EMAIL or EMAIL_FROM_ADDRESS")
                print(f"[EMAIL] WARNING: SES not configured - missing: {', '.join(missing)}")
                print("[EMAIL] Emails will be logged only until configured")
                return

            from_email = config.SES_FROM_EMAIL or config.EMAIL_FROM_ADDRESS
            print("[EMAIL] Email ENABLED via AWS SES")
            print(f"[EMAIL] Region: {config.AWS_REGION}")
            print(f"[EMAIL] From: {config.EMAIL_FROM_NAME} <{from_email}>")
            return

        # SMTP providers (neo, sendgrid, etc.)
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
        print(f"[EMAIL] SMTP_HOST={config.SMTP_HOST!r} SMTP_PORT={config.SMTP_PORT} (TLS={config.SMTP_USE_TLS})")
        print(f"[EMAIL] Auth: user={masked_user} pass={masked_pass}")
        print(f"[EMAIL] From: {from_name} <{from_addr}>")

        # Pre-check DNS resolution at startup
        try:
            addr_info = socket.getaddrinfo(config.SMTP_HOST, config.SMTP_PORT, socket.AF_INET, socket.SOCK_STREAM)
            resolved_ips = [info[4][0] for info in addr_info]
            print(f"[EMAIL] DNS check OK: {config.SMTP_HOST!r} -> {resolved_ips}")
        except socket.gaierror as e:
            print(f"[EMAIL] DNS check FAILED: {config.SMTP_HOST!r} -> {e!r}")
            print("[EMAIL] WARNING: Emails will fail until DNS resolves")

    @classmethod
    def send(
        cls,
        to: str,
        subject: str,
        html: str,
        text: Optional[str] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        reply_to_name: Optional[str] = None,
    ) -> EmailResult:
        """
        Send an email. Never throws - returns EmailResult.

        Args:
            to: Recipient email address
            subject: Email subject
            html: HTML body
            text: Plain text body (optional, auto-generated if not provided)
            from_email: Override from address (must be verified/authorized sender)
            from_name: Override from name
            reply_to: Reply-To email address (for contact forms, use this instead of from_email)
            reply_to_name: Reply-To display name

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

        # Route to appropriate provider
        provider = config.EMAIL_PROVIDER.lower()
        if provider == "ses":
            return cls._send_via_ses(to, subject, html, text, from_email, from_name, reply_to, reply_to_name)

        # Default: SMTP providers (neo, sendgrid, etc.)
        return cls._send_via_smtp(to, subject, html, text, from_email, from_name, reply_to, reply_to_name)

    @classmethod
    def _send_via_ses(
        cls,
        to: str,
        subject: str,
        html: str,
        text: Optional[str] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        reply_to_name: Optional[str] = None,
    ) -> EmailResult:
        """Send email via AWS SES using boto3."""
        if not BOTO3_AVAILABLE:
            print(f"[EMAIL] SES ERROR: boto3 not available - cannot send to {to}")
            return EmailResult(success=False, message="boto3 not available", error="ImportError")

        # Get from address (must be verified in SES)
        sender_addr = from_email or config.SES_FROM_EMAIL or config.EMAIL_FROM_ADDRESS
        sender_name = from_name or config.EMAIL_FROM_NAME
        sender = f"{sender_name} <{sender_addr}>" if sender_name else sender_addr

        try:
            # Create SES client
            ses_client = boto3.client(
                "ses",
                region_name=config.AWS_REGION,
                aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            )

            # Build email body
            body = {"Html": {"Charset": "UTF-8", "Data": html}}
            if text:
                body["Text"] = {"Charset": "UTF-8", "Data": text}

            # Build send_email params
            send_params = {
                "Source": sender,
                "Destination": {"ToAddresses": [to]},
                "Message": {
                    "Subject": {"Charset": "UTF-8", "Data": subject},
                    "Body": body,
                },
            }

            # Add Reply-To if provided
            if reply_to:
                reply_to_formatted = f"{reply_to_name} <{reply_to}>" if reply_to_name else reply_to
                send_params["ReplyToAddresses"] = [reply_to_formatted]

            # Send email
            response = ses_client.send_email(**send_params)

            message_id = response.get("MessageId", "unknown")
            print(f"[EMAIL] SES SENT to {to}: {subject} (MessageId: {message_id})")
            return EmailResult(success=True, message=f"Email sent via SES (MessageId: {message_id})")

        except NoCredentialsError as e:
            print(f"[EMAIL] SES ERROR: No credentials - {e}")
            return EmailResult(success=False, message="AWS credentials not configured", error=str(e))

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = e.response.get("Error", {}).get("Message", str(e))
            print(f"[EMAIL] SES ERROR ({error_code}): {error_msg}")
            return EmailResult(success=False, message=f"SES error: {error_code}", error=error_msg)

        except Exception as e:
            print(f"[EMAIL] SES ERROR: Unexpected - {e}")
            return EmailResult(success=False, message="Unexpected SES error", error=str(e))

    @classmethod
    def send_raw(
        cls,
        to: str,
        subject: str,
        html: str,
        text: Optional[str] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None,
        attachments: Optional[List[Dict[str, Any]]] = None,
        inline_images: Optional[List[Dict[str, Any]]] = None,
    ) -> EmailResult:
        """
        Send an email with attachments and/or inline images via SES send_raw_email.

        attachments: [{"filename": "invoice.pdf", "data": bytes, "content_type": "application/pdf"}]
        inline_images: [{"cid": "logo", "data": bytes, "content_type": "image/png"}]

        Falls back to simple send() if no attachments/inline_images provided.
        Never throws - returns EmailResult.
        """
        cls._log_config()

        # If no attachments, use the simpler send() path
        if not attachments and not inline_images:
            return cls.send(to=to, subject=subject, html=html, text=text,
                            from_email=from_email, from_name=from_name)

        if not config.EMAIL_ENABLED:
            print(f"[EMAIL] DISABLED - Would send raw to {to}: {subject}")
            return EmailResult(success=True, message="Email disabled - logged only")

        if not config.EMAIL_CONFIGURED:
            print(f"[EMAIL] NOT CONFIGURED - Would send raw to {to}: {subject}")
            return EmailResult(success=True, message="Email not configured - logged only")

        provider = config.EMAIL_PROVIDER.lower()

        if provider == "ses":
            if not BOTO3_AVAILABLE:
                print(f"[EMAIL] send_raw ERROR: boto3 not available for SES provider")
                return EmailResult(success=False, message="boto3 not available", error="ImportError")
            sender_addr = from_email or config.SES_FROM_EMAIL or config.EMAIL_FROM_ADDRESS
        else:
            default_name, default_addr = config.SMTP_FROM_PARSED
            sender_addr = default_addr

        sender_name = from_name or config.EMAIL_FROM_NAME
        sender = f"{sender_name} <{sender_addr}>" if sender_name else sender_addr

        try:
            # Build MIME structure:
            # mixed
            #   related
            #     alternative
            #       text/plain
            #       text/html
            #     inline images (cid)
            #   attachments (pdf, etc.)

            msg_mixed = MIMEMultipart("mixed")
            msg_mixed["Subject"] = subject
            msg_mixed["From"] = sender
            msg_mixed["To"] = to

            msg_related = MIMEMultipart("related")

            msg_alt = MIMEMultipart("alternative")
            if text:
                msg_alt.attach(MIMEText(text, "plain", "utf-8"))
            msg_alt.attach(MIMEText(html, "html", "utf-8"))

            msg_related.attach(msg_alt)

            # Inline images (CID)
            for img in (inline_images or []):
                mime_img = MIMEImage(img["data"], _subtype=img.get("content_type", "image/png").split("/")[-1])
                mime_img.add_header("Content-ID", f"<{img['cid']}>")
                mime_img.add_header("Content-Disposition", "inline", filename=f"{img['cid']}.png")
                msg_related.attach(mime_img)

            msg_mixed.attach(msg_related)

            # File attachments
            for att in (attachments or []):
                part = MIMEBase("application", "octet-stream")
                part.set_payload(att["data"])
                encoders.encode_base64(part)
                ct = att.get("content_type", "application/octet-stream")
                maintype, subtype = ct.split("/", 1) if "/" in ct else ("application", "octet-stream")
                part.replace_header("Content-Type", ct)
                part.add_header("Content-Disposition", "attachment", filename=att["filename"])
                msg_mixed.attach(part)

            att_count = len(attachments or [])
            img_count = len(inline_images or [])

            # Route to provider
            if provider == "ses":
                ses_client = boto3.client(
                    "ses",
                    region_name=config.AWS_REGION,
                    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
                )
                response = ses_client.send_raw_email(
                    Source=sender,
                    Destinations=[to],
                    RawMessage={"Data": msg_mixed.as_string()},
                )
                message_id = response.get("MessageId", "unknown")
                print(
                    f"[EMAIL] SES RAW SENT to {to}: {subject} "
                    f"({att_count} attachment(s), {img_count} inline image(s), "
                    f"MessageId: {message_id})"
                )
                return EmailResult(success=True, message=f"Email sent via SES raw (MessageId: {message_id})")
            else:
                # Send via SMTP (neo, sendgrid, etc.)
                smtp_host = config.SMTP_HOST
                smtp_port = config.SMTP_PORT
                print(f"[EMAIL] send_raw SMTP connecting: host={smtp_host!r} port={smtp_port}")
                with smtplib.SMTP(smtp_host, smtp_port, timeout=config.SMTP_TIMEOUT) as server:
                    if config.SMTP_USE_TLS:
                        server.starttls()
                    server.login(config.SMTP_USER, config.SMTP_PASSWORD)
                    server.sendmail(sender_addr, to, msg_mixed.as_string())
                print(
                    f"[EMAIL] SMTP RAW SENT to {to}: {subject} "
                    f"({att_count} attachment(s), {img_count} inline image(s))"
                )
                return EmailResult(success=True, message="Email sent via SMTP raw")

        except Exception as e:
            print(f"[EMAIL] RAW SEND ERROR ({provider}): {e}")
            return EmailResult(success=False, message=f"{provider} raw send error", error=str(e))

    @classmethod
    def _send_via_smtp(
        cls,
        to: str,
        subject: str,
        html: str,
        text: Optional[str] = None,
        from_email: Optional[str] = None,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        reply_to_name: Optional[str] = None,
    ) -> EmailResult:
        """Send email via SMTP."""
        # Get from address (use configured sender, not contact form sender)
        default_name, default_addr = config.SMTP_FROM_PARSED
        sender_name = from_name or default_name
        sender_addr = default_addr  # Always use configured sender for SMTP auth

        try:
            # Build message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"{sender_name} <{sender_addr}>"
            msg["To"] = to

            # Add Reply-To header if provided (for contact forms)
            if reply_to:
                reply_to_formatted = f"{reply_to_name} <{reply_to}>" if reply_to_name else reply_to
                msg["Reply-To"] = reply_to_formatted

            # Attach text version
            if text:
                msg.attach(MIMEText(text, "plain"))

            # Attach HTML version
            msg.attach(MIMEText(html, "html"))

            # Log connection attempt with debug info
            smtp_host = config.SMTP_HOST
            smtp_port = config.SMTP_PORT
            print(f"[EMAIL] Connecting: host={smtp_host!r} port={smtp_port} (provider={config.EMAIL_PROVIDER})")

            # Pre-resolve DNS to catch issues early and log IPs
            try:
                addr_info = socket.getaddrinfo(smtp_host, smtp_port, socket.AF_INET, socket.SOCK_STREAM)
                resolved_ips = [info[4][0] for info in addr_info]
                print(f"[EMAIL] DNS resolved {smtp_host!r} -> {resolved_ips}")
            except socket.gaierror as dns_err:
                print(f"[EMAIL] WARNING: DNS failed for {smtp_host!r}: {dns_err!r} (email not sent, request continues)")
                return EmailResult(
                    success=False,
                    message=f"DNS resolution failed for {smtp_host!r}",
                    error=f"{type(dns_err).__name__}: {dns_err}"
                )

            # Send via SMTP with timeout
            with smtplib.SMTP(smtp_host, smtp_port, timeout=config.SMTP_TIMEOUT) as server:
                if config.SMTP_USE_TLS:
                    server.starttls()
                server.login(config.SMTP_USER, config.SMTP_PASSWORD)
                server.sendmail(sender_addr, to, msg.as_string())

            print(f"[EMAIL] SENT to {to}: {subject}")
            return EmailResult(success=True, message="Email sent successfully")

        except socket.gaierror as e:
            # DNS resolution failed (fallback - should be caught by pre-resolve above)
            print(f"[EMAIL] WARNING: DNS error for {smtp_host!r}: {e!r} (email not sent, request continues)")
            return EmailResult(success=False, message=f"DNS resolution failed for {smtp_host!r}", error=f"{type(e).__name__}: {e}")

        except socket.timeout as e:
            print(f"[EMAIL] WARNING: Connection timeout to {smtp_host!r}:{smtp_port}: {e!r} (email not sent, request continues)")
            return EmailResult(success=False, message="Connection timeout", error=f"{type(e).__name__}: {e}")

        except smtplib.SMTPAuthenticationError as e:
            print(f"[EMAIL] WARNING: SMTP auth error: {e!r} (email not sent, request continues)")
            return EmailResult(success=False, message="Authentication failed", error=f"{type(e).__name__}: {e}")

        except smtplib.SMTPException as e:
            print(f"[EMAIL] WARNING: SMTP error: {e!r} (email not sent, request continues)")
            return EmailResult(success=False, message="SMTP error", error=f"{type(e).__name__}: {e}")

        except Exception as e:
            print(f"[EMAIL] WARNING: Unexpected error: {e!r} (email not sent, request continues)")
            return EmailResult(success=False, message="Unexpected error", error=f"{type(e).__name__}: {e}")

    @classmethod
    def healthcheck(cls) -> Dict[str, Any]:
        """
        Check email service health:
        1. DNS resolution of SMTP_HOST
        2. TCP connection to SMTP_HOST:SMTP_PORT

        Returns dict with status and details.
        """
        cls._log_config()

        smtp_host = config.SMTP_HOST
        smtp_port = config.SMTP_PORT

        result = {
            "enabled": config.EMAIL_ENABLED,
            "configured": config.EMAIL_CONFIGURED,
            "provider": config.EMAIL_PROVIDER,
            "smtp_host": smtp_host,
            "smtp_host_repr": repr(smtp_host),  # Debug: show exact value with quotes
            "smtp_port": smtp_port,
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
        print(f"[EMAIL] Healthcheck: Resolving DNS for {smtp_host!r}...")
        try:
            ip_addresses = socket.gethostbyname_ex(smtp_host)
            result["dns_ok"] = True
            result["dns_ips"] = ip_addresses[2]
            print(f"[EMAIL] Healthcheck: DNS OK - {smtp_host!r} -> {ip_addresses[2]}")
        except socket.gaierror as e:
            result["error"] = f"DNS resolution failed for {smtp_host!r}: {e!r}"
            result["status"] = "dns_failed"
            result["message"] = f"Cannot resolve {smtp_host!r}"
            print(f"[EMAIL] Healthcheck: DNS FAILED for {smtp_host!r}: {e!r}")
            return result

        # Step 2: TCP connection
        print(f"[EMAIL] Healthcheck: TCP connect to {smtp_host!r}:{smtp_port}...")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((smtp_host, smtp_port))
            sock.close()
            result["tcp_ok"] = True
            print(f"[EMAIL] Healthcheck: TCP OK - connected to {smtp_host!r}:{smtp_port}")
        except socket.timeout:
            result["error"] = f"TCP connection timeout (5s) to {smtp_host!r}:{smtp_port}"
            result["status"] = "tcp_timeout"
            result["message"] = f"Connection to {smtp_host!r}:{smtp_port} timed out"
            print(f"[EMAIL] Healthcheck: TCP TIMEOUT to {smtp_host!r}:{smtp_port}")
            return result
        except socket.error as e:
            result["error"] = f"TCP connection failed to {smtp_host!r}:{smtp_port}: {e!r}"
            result["status"] = "tcp_failed"
            result["message"] = f"Cannot connect to {smtp_host!r}:{smtp_port}"
            print(f"[EMAIL] Healthcheck: TCP FAILED to {smtp_host!r}:{smtp_port}: {e!r}")
            return result

        result["status"] = "healthy"
        result["message"] = "Email service is healthy"
        print("[EMAIL] Healthcheck: HEALTHY")
        return result

    @classmethod
    def send_test(cls, to: str) -> EmailResult:
        """Send a test email to verify configuration."""
        return cls.send(
            to=to,
            subject="TimrX Email Test",
            html="""
            <div style="background-color: #000000; width: 100%; padding: 0; margin: 0;">
            <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
                        max-width: 600px; margin: 0 auto; background-color: #000000; border-radius: 12px; overflow: hidden;">
                <div style="padding: 20px 24px 16px;">
                    <table cellpadding="0" cellspacing="0" border="0" width="100%">
                        <tr>
                            <td>
                                <table cellpadding="0" cellspacing="0" border="0">
                                    <tr>
                                        <td style="vertical-align: middle; padding-right: 10px; line-height: 0;">
                                            <img src="https://timrx.live/img/logo.png" alt="TimrX" height="32"
                                                 style="height:32px; width:auto; display:block;" />
                                        </td>
                                        <td style="vertical-align: middle;">
                                            <span style="font-size: 18px; font-weight: 800; color: #ffffff;
                                                         letter-spacing: 0.5px;">TimrX</span>
                                        </td>
                                    </tr>
                                </table>
                            </td>
                        </tr>
                    </table>
                </div>
                <div style="padding: 24px 24px 28px;">
                    <h2 style="color: #ffffff; margin: 0 0 12px; font-size: 20px; font-weight: 600;">Email Test Successful!</h2>
                    <p style="color: #aaa; font-size: 14px; line-height: 1.5; margin: 0 0 8px;">This is a test email from TimrX to verify your email configuration is working.</p>
                    <p style="color: #888; font-size: 13px; line-height: 1.5; margin: 0;">If you received this email, your settings are correct.</p>
                </div>
                <div style="border-top: 1px solid #222; padding: 16px 24px; text-align: center;">
                    <p style="color: #555; font-size: 11px; margin: 0;">TimrX &mdash; 3D Print Hub</p>
                </div>
            </div>
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
    reply_to: Optional[str] = None,
    reply_to_name: Optional[str] = None,
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
        reply_to=reply_to,
        reply_to_name=reply_to_name,
    )
    return result.success


def email_healthcheck() -> Dict[str, Any]:
    """Run email health check and return results."""
    return EmailService.healthcheck()


# Log config on module import
EmailService._log_config()
