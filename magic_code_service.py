"""
Magic Code Service - Handles passwordless authentication via email codes.

Flow:
1. request_restore(email) -> Generate code, store hash, send email
2. User receives email with 6-digit code
3. redeem_restore(email, code, session_id) -> Verify, link session to identity

Security:
- Codes are 6-digit numeric (100000-999999)
- Only hash stored in DB (SHA256)
- Codes expire (default 15 minutes)
- Max attempts per code (default 5)
- Rate limiting: max 3 active codes per email, 60s cooldown
- IP tracking for abuse detection
"""

import secrets
import hashlib
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone

from db import (
    transaction,
    fetch_one,
    fetch_all,
    query_one,
    query_all,
    execute,
    Tables,
    hash_string,
)
from config import config
from emailer import send_magic_code, notify_restore_request


class MagicCodeService:
    """Service for magic code authentication."""

    # Rate limiting constants
    MAX_ACTIVE_CODES_PER_EMAIL = 3
    COOLDOWN_SECONDS = config.MAGIC_CODE_COOLDOWN_SECONDS  # Default: 60

    # ─────────────────────────────────────────────────────────────
    # Code Generation
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def generate_code() -> str:
        """Generate a 6-digit numeric code (100000-999999)."""
        return str(secrets.randbelow(900000) + 100000)

    @staticmethod
    def hash_code(code: str) -> str:
        """Hash a code for secure storage using SHA256."""
        return hashlib.sha256(code.encode()).hexdigest()

    # ─────────────────────────────────────────────────────────────
    # Request Code (Step 1)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def request_restore(
        email: str,
        ip_address: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """
        Request a magic code for account restore.

        Args:
            email: Email address to send code to
            ip_address: Client IP for rate limiting (optional)

        Returns:
            Tuple of (success: bool, message: str)

        Rate limits:
        - Max 3 active codes per email
        - 60 second cooldown between requests
        """
        email = email.strip().lower()

        # Validate email format (basic)
        if not email or "@" not in email or "." not in email:
            return (False, "Invalid email format")

        # Check if email exists in the system
        identity = query_one(
            f"SELECT id, email FROM {Tables.IDENTITIES} WHERE email = %s",
            (email,),
        )
        if not identity:
            # Don't reveal if email exists - still return success to prevent enumeration
            # But don't actually send the email
            print(f"[MAGIC_CODE] Request for unknown email: {email}")
            return (True, "If this email is registered, a code has been sent")

        # Check rate limits
        rate_limit_ok, rate_limit_msg = MagicCodeService._check_rate_limits(email)
        if not rate_limit_ok:
            return (False, rate_limit_msg)

        # Generate code
        plain_code = MagicCodeService.generate_code()
        code_hash = MagicCodeService.hash_code(plain_code)

        # Hash IP for storage (privacy)
        ip_hash = hash_string(ip_address) if ip_address else None

        # Store in database
        expiry_minutes = config.MAGIC_CODE_EXPIRY_MINUTES
        with transaction() as cur:
            cur.execute(
                f"""
                INSERT INTO {Tables.MAGIC_CODES}
                (email, code_hash, expires_at, attempts, consumed, ip_hash, created_at)
                VALUES (%s, %s, NOW() + INTERVAL '%s minutes', 0, FALSE, %s, NOW())
                RETURNING id
                """,
                (email, code_hash, expiry_minutes, ip_hash),
            )
            code_record = fetch_one(cur)

        if not code_record:
            print(f"[MAGIC_CODE] Failed to create code record for {email}")
            return (False, "Failed to create code")

        # Send email
        email_sent = send_magic_code(email, plain_code)
        if not email_sent:
            print(f"[MAGIC_CODE] Failed to send email to {email}")
            # Still return success to not reveal email issues
            return (True, "If this email is registered, a code has been sent")

        # Notify admin (optional)
        notify_restore_request(email)

        print(f"[MAGIC_CODE] Code sent to {email}, expires in {expiry_minutes} minutes")
        return (True, "Code sent to your email")

    @staticmethod
    def _check_rate_limits(email: str) -> Tuple[bool, str]:
        """
        Check rate limits for an email.
        Returns (ok: bool, message: str).
        """
        # Check cooldown (time since last request)
        last_code_time = MagicCodeService.get_last_code_time(email)
        if last_code_time:
            cooldown_seconds = MagicCodeService.COOLDOWN_SECONDS
            elapsed = (datetime.now(timezone.utc) - last_code_time).total_seconds()
            if elapsed < cooldown_seconds:
                remaining = int(cooldown_seconds - elapsed)
                return (False, f"Please wait {remaining} seconds before requesting another code")

        # Check active codes count
        active_count = MagicCodeService.get_active_codes_count(email)
        if active_count >= MagicCodeService.MAX_ACTIVE_CODES_PER_EMAIL:
            return (False, "Too many pending codes. Please use an existing code or wait for them to expire")

        return (True, "")

    # ─────────────────────────────────────────────────────────────
    # Redeem Code (Step 2)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def redeem_restore(
        email: str,
        code: str,
        session_id: str,
    ) -> Tuple[bool, Optional[str], str]:
        """
        Verify a magic code and link the session to the identity.

        Args:
            email: Email address the code was sent to
            code: The 6-digit code entered by user
            session_id: Current session ID to link to the identity

        Returns:
            Tuple of (success: bool, identity_id: Optional[str], message: str)
        """
        email = email.strip().lower()
        code = code.strip()

        # Validate inputs
        if not email or not code or not session_id:
            return (False, None, "Missing required fields")

        if len(code) != 6 or not code.isdigit():
            return (False, None, "Invalid code format")

        # Get identity for this email
        identity = query_one(
            f"SELECT id FROM {Tables.IDENTITIES} WHERE email = %s",
            (email,),
        )
        if not identity:
            return (False, None, "Invalid email or code")

        identity_id = str(identity["id"])

        # Hash the provided code
        code_hash = MagicCodeService.hash_code(code)

        # Find matching active code
        code_record = query_one(
            f"""
            SELECT id, attempts
            FROM {Tables.MAGIC_CODES}
            WHERE email = %s
              AND code_hash = %s
              AND consumed = FALSE
              AND expires_at > NOW()
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (email, code_hash),
        )

        if not code_record:
            # Code not found - increment attempts on most recent code for this email
            MagicCodeService._increment_attempts(email)
            return (False, None, "Invalid or expired code")

        # Check max attempts on this specific code
        if code_record["attempts"] >= config.MAGIC_CODE_MAX_ATTEMPTS:
            return (False, None, "Too many failed attempts. Please request a new code")

        code_id = str(code_record["id"])

        # Mark code as consumed and link session to identity
        with transaction() as cur:
            # Mark code as consumed
            cur.execute(
                f"""
                UPDATE {Tables.MAGIC_CODES}
                SET consumed = TRUE, consumed_at = NOW()
                WHERE id = %s
                """,
                (code_id,),
            )

            # Update session to point to this identity
            cur.execute(
                f"""
                UPDATE {Tables.SESSIONS}
                SET identity_id = %s, updated_at = NOW()
                WHERE id = %s
                RETURNING id
                """,
                (identity_id, session_id),
            )
            updated_session = fetch_one(cur)

            if not updated_session:
                # Session doesn't exist - this is an error
                raise ValueError(f"Session {session_id} not found")

            # Update identity's last_seen_at
            cur.execute(
                f"""
                UPDATE {Tables.IDENTITIES}
                SET last_seen_at = NOW(), email_verified = TRUE
                WHERE id = %s
                """,
                (identity_id,),
            )

        # Invalidate other pending codes for this email
        MagicCodeService.invalidate_codes_for_email(email)

        print(f"[MAGIC_CODE] Successfully restored session for {email}, identity={identity_id}")
        return (True, identity_id, "Account restored successfully")

    @staticmethod
    def _increment_attempts(email: str) -> None:
        """Increment attempt counter on the most recent active code for email."""
        execute(
            f"""
            UPDATE {Tables.MAGIC_CODES}
            SET attempts = attempts + 1
            WHERE id = (
                SELECT id FROM {Tables.MAGIC_CODES}
                WHERE email = %s
                  AND consumed = FALSE
                  AND expires_at > NOW()
                ORDER BY created_at DESC
                LIMIT 1
            )
            """,
            (email,),
        )

    # ─────────────────────────────────────────────────────────────
    # Rate Limiting Helpers
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_active_codes_count(email: str) -> int:
        """
        Get count of active (non-expired, non-consumed) codes for email.
        Used for rate limiting.
        """
        email = email.strip().lower()
        row = query_one(
            f"""
            SELECT COUNT(*) as count
            FROM {Tables.MAGIC_CODES}
            WHERE email = %s
              AND consumed = FALSE
              AND expires_at > NOW()
            """,
            (email,),
        )
        return int(row["count"]) if row else 0

    @staticmethod
    def get_last_code_time(email: str) -> Optional[datetime]:
        """
        Get timestamp of the most recent code for email.
        Used for cooldown enforcement.
        """
        email = email.strip().lower()
        row = query_one(
            f"""
            SELECT created_at
            FROM {Tables.MAGIC_CODES}
            WHERE email = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (email,),
        )
        return row["created_at"] if row else None

    # ─────────────────────────────────────────────────────────────
    # Cleanup / Maintenance
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def cleanup_expired_codes() -> int:
        """
        Delete expired codes from the database.
        Returns count of codes deleted.
        Should be called periodically.
        """
        # Keep codes for audit trail, but mark them as expired
        # Or delete codes older than 24 hours
        result = execute(
            f"""
            DELETE FROM {Tables.MAGIC_CODES}
            WHERE expires_at < NOW() - INTERVAL '24 hours'
            """,
        )
        count = result if isinstance(result, int) else 0
        if count > 0:
            print(f"[MAGIC_CODE] Cleaned up {count} expired codes")
        return count

    @staticmethod
    def invalidate_codes_for_email(email: str) -> int:
        """
        Invalidate all pending codes for an email.
        Called after successful verification.
        Returns count of codes invalidated.
        """
        email = email.strip().lower()
        result = execute(
            f"""
            UPDATE {Tables.MAGIC_CODES}
            SET consumed = TRUE, consumed_at = NOW()
            WHERE email = %s
              AND consumed = FALSE
            """,
            (email,),
        )
        count = result if isinstance(result, int) else 0
        if count > 0:
            print(f"[MAGIC_CODE] Invalidated {count} pending codes for {email}")
        return count

    # ─────────────────────────────────────────────────────────────
    # Backward Compatibility Aliases
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def request_code(email: str, ip_address: Optional[str] = None) -> Tuple[bool, str]:
        """Alias for request_restore (backward compatibility)."""
        return MagicCodeService.request_restore(email, ip_address)

    @staticmethod
    def verify_code(email: str, code: str, session_id: str) -> Tuple[bool, Optional[str], str]:
        """Alias for redeem_restore (backward compatibility)."""
        return MagicCodeService.redeem_restore(email, code, session_id)
