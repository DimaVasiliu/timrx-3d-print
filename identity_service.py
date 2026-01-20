"""
Identity Service - Manages user identities and sessions.

Anonymous-first identity system:
- Users get an identity + session on first visit (no signup required)
- Email can be attached later (for purchases/recovery)
- Sessions are stored in HttpOnly cookies

Usage:
    from backend.services.identity_service import IdentityService

    # In a route:
    session_id, identity_id = IdentityService.get_or_create_session(request, response)
    identity = IdentityService.get_identity(identity_id)
"""

from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import uuid
import hashlib

from db import (
    transaction,
    fetch_one,
    fetch_all,
    query_one,
    execute_returning,
    execute,
    Tables,
    now_utc,
    DatabaseError,
    DatabaseIntegrityError,
)
from config import config
from emailer import notify_new_identity


class IdentityService:
    """Service for managing identities and sessions."""

    # ─────────────────────────────────────────────────────────────
    # Cookie Management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_session_id_from_request(request) -> Optional[str]:
        """Extract session ID from request cookies."""
        return request.cookies.get(config.SESSION_COOKIE_NAME)

    @staticmethod
    def set_session_cookie(response, session_id: str) -> None:
        """
        Set the session cookie on the response.
        Uses SESSION_TTL_SECONDS for max_age (must match DB session expiry).

        Cookie settings (production):
        - Domain: .timrx.live (allows timrx.live + 3d.timrx.live + www.timrx.live)
        - Secure: True (HTTPS only)
        - SameSite: None (required for cross-subdomain with credentials)
        - HttpOnly: True (not accessible via JavaScript)
        - Path: /
        """
        # Build cookie kwargs - only include domain if set (None omits it)
        cookie_kwargs = {
            "max_age": config.SESSION_TTL_SECONDS,
            "httponly": config.SESSION_COOKIE_HTTPONLY,
            "secure": config.SESSION_COOKIE_SECURE,
            "samesite": config.SESSION_COOKIE_SAMESITE,
            "path": config.SESSION_COOKIE_PATH,
        }

        # Add domain for cross-subdomain sharing (e.g., ".timrx.live")
        domain = config.SESSION_COOKIE_DOMAIN
        if domain:
            cookie_kwargs["domain"] = domain

        response.set_cookie(config.SESSION_COOKIE_NAME, session_id, **cookie_kwargs)

        print(
            f"[SESSION] Cookie set: name={config.SESSION_COOKIE_NAME}, "
            f"domain={domain!r}, secure={config.SESSION_COOKIE_SECURE}, "
            f"samesite={config.SESSION_COOKIE_SAMESITE}"
        )

    @staticmethod
    def clear_session_cookie(response) -> None:
        """Clear the session cookie from the response."""
        # Must include domain to properly clear cross-subdomain cookie
        delete_kwargs = {"path": config.SESSION_COOKIE_PATH}

        domain = config.SESSION_COOKIE_DOMAIN
        if domain:
            delete_kwargs["domain"] = domain

        response.delete_cookie(config.SESSION_COOKIE_NAME, **delete_kwargs)

    # ─────────────────────────────────────────────────────────────
    # Identity CRUD
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def create_identity(email: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new identity with an associated wallet.
        Email is optional (anonymous-first).
        Returns the created identity record.

        Raises:
            DatabaseIntegrityError: If email already exists
            DatabaseError: On database failure
        """
        normalized_email = email.strip().lower() if email else None

        with transaction() as cur:
            # Create identity
            cur.execute(
                f"""
                INSERT INTO {Tables.IDENTITIES} (email, email_verified, created_at, last_seen_at)
                VALUES (%s, %s, NOW(), NOW())
                RETURNING *
                """,
                (normalized_email, False),
            )
            identity = fetch_one(cur)

            if not identity:
                raise DatabaseError("Failed to create identity")

            identity_id = str(identity["id"])

            # Create wallet with initial balance
            initial_balance = config.FREE_CREDITS_ON_SIGNUP
            cur.execute(
                f"""
                INSERT INTO {Tables.WALLETS} (identity_id, balance_credits, updated_at)
                VALUES (%s, %s, NOW())
                """,
                (identity_id, initial_balance),
            )

            # If initial balance > 0, create ledger entry
            if initial_balance > 0:
                cur.execute(
                    f"""
                    INSERT INTO {Tables.LEDGER_ENTRIES}
                    (identity_id, entry_type, amount_credits, ref_type, meta, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    """,
                    (identity_id, "grant", initial_balance, "signup", '{"reason": "welcome_credits"}'),
                )

        # Send admin notification if email was attached (non-blocking)
        if normalized_email:
            try:
                notify_new_identity(identity_id, normalized_email)
            except Exception as email_err:
                print(f"[IDENTITY] WARNING: Admin notification failed: {email_err}")

        print(f"[IDENTITY] Created new identity: {identity_id} (email: {normalized_email or 'anonymous'})")
        return identity

    @staticmethod
    def get_identity(identity_id: str) -> Optional[Dict[str, Any]]:
        """
        Get an identity by ID.
        Returns None if not found.
        """
        return query_one(
            f"SELECT * FROM {Tables.IDENTITIES} WHERE id = %s",
            (identity_id,),
        )

    @staticmethod
    def get_identity_by_email(email: str) -> Optional[Dict[str, Any]]:
        """
        Get an identity by email address.
        Returns None if not found.
        """
        normalized_email = email.strip().lower()
        return query_one(
            f"SELECT * FROM {Tables.IDENTITIES} WHERE email = %s",
            (normalized_email,),
        )

    @staticmethod
    def get_identity_with_wallet(identity_id: str) -> Optional[Dict[str, Any]]:
        """
        Get identity with wallet balance.
        Returns combined dict with identity and wallet fields.
        """
        return query_one(
            f"""
            SELECT i.*, w.balance_credits
            FROM {Tables.IDENTITIES} i
            LEFT JOIN {Tables.WALLETS} w ON w.identity_id = i.id
            WHERE i.id = %s
            """,
            (identity_id,),
        )

    # Reasons for email attachment failure (internal use only - never expose to users)
    ATTACH_REASON_SUCCESS = "success"
    ATTACH_REASON_ALREADY_ATTACHED = "already_attached"
    ATTACH_REASON_BELONGS_TO_OTHER = "belongs_to_other"  # Anti-enumeration: logged only

    @staticmethod
    def attach_email(identity_id: str, email: str) -> Tuple[Dict[str, Any], bool, str]:
        """
        Attach email to an identity. Non-enumeration safe.

        Rules:
        - Normalizes email to lower/trim
        - If email belongs to a DIFFERENT identity: returns OK without attaching
          (user must use restore flow - but we don't tell them that explicitly)
        - If same identity already has that email: returns (identity, False) - no change
        - Otherwise attaches email and returns (identity, True) - changed

        IMPORTANT: This method NEVER raises errors for email conflicts to prevent
        user enumeration. The reason is returned but should only be logged internally,
        never exposed to users.

        Does NOT verify email. Verification is handled via magic codes separately.

        Returns:
            Tuple of (identity_dict, was_changed, reason)
            - reason is for internal logging only, never expose to users

        Raises:
            ValueError: If identity not found or email empty
        """
        normalized_email = email.strip().lower()

        if not normalized_email:
            raise ValueError("Email cannot be empty")

        # Check current identity first
        current = IdentityService.get_identity(identity_id)
        if not current:
            raise ValueError(f"Identity {identity_id} not found")

        # If same email already attached, return idempotently (no change)
        if current.get("email") == normalized_email:
            print(f"[IDENTITY] Email already attached to identity {identity_id}: {normalized_email}")
            return current, False, IdentityService.ATTACH_REASON_ALREADY_ATTACHED

        # Check if this email is already used by another identity
        existing = IdentityService.get_identity_by_email(normalized_email)
        if existing and str(existing["id"]) != identity_id:
            # ANTI-ENUMERATION: Do NOT raise an error or reveal this to the user.
            # Just log internally and return as if successful.
            # User must use restore/redeem flow to take over the account.
            print(
                f"[IDENTITY] Email attach silently blocked (anti-enumeration): "
                f"{normalized_email} belongs to identity {existing['id']}, "
                f"requested by identity {identity_id}"
            )
            return current, False, IdentityService.ATTACH_REASON_BELONGS_TO_OTHER

        result = execute_returning(
            f"""
            UPDATE {Tables.IDENTITIES}
            SET email = %s, last_seen_at = NOW()
            WHERE id = %s
            RETURNING *
            """,
            (normalized_email, identity_id),
        )

        if not result:
            raise ValueError(f"Identity {identity_id} not found")

        # Send admin notification only when email is newly attached (non-blocking)
        try:
            notify_new_identity(identity_id, normalized_email)
        except Exception as email_err:
            print(f"[IDENTITY] WARNING: Admin notification failed: {email_err}")

        print(f"[IDENTITY] Attached email to identity {identity_id}: {normalized_email}")
        return result, True, IdentityService.ATTACH_REASON_SUCCESS

    @staticmethod
    def touch_identity(identity_id: str) -> bool:
        """
        Update last_seen_at for an identity.
        Called on each authenticated request.
        """
        try:
            count = execute(
                f"UPDATE {Tables.IDENTITIES} SET last_seen_at = NOW() WHERE id = %s",
                (identity_id,),
            )
            return count > 0
        except DatabaseError:
            return False

    # ─────────────────────────────────────────────────────────────
    # Session Management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def hash_for_storage(value: str) -> str:
        """Hash a value (IP, user agent) for privacy-safe storage."""
        if not value:
            return ""
        return hashlib.sha256(value.encode()).hexdigest()

    @staticmethod
    def create_session(
        identity_id: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new session for an identity.
        Returns the session record with ID.

        Note: DB expiry uses SESSION_TTL_DAYS (must match cookie max_age).

        Raises:
            DatabaseError: On database failure
        """
        session_id = str(uuid.uuid4())
        expires_at = now_utc() + timedelta(days=config.SESSION_TTL_DAYS)

        ip_hash = IdentityService.hash_for_storage(ip_address) if ip_address else None
        ua_hash = IdentityService.hash_for_storage(user_agent) if user_agent else None

        session = execute_returning(
            f"""
            INSERT INTO {Tables.SESSIONS}
            (id, identity_id, created_at, expires_at, ip_hash, user_agent_hash)
            VALUES (%s, %s, NOW(), %s, %s, %s)
            RETURNING *
            """,
            (session_id, identity_id, expires_at, ip_hash, ua_hash),
        )

        if not session:
            raise DatabaseError("Failed to create session")

        print(f"[SESSION] Created session {session_id} for identity {identity_id}")
        return session

    @staticmethod
    def validate_session(session_id: str) -> Optional[Dict[str, Any]]:
        """
        Validate a session ID and return the associated identity.
        Returns None if session is invalid, expired, or revoked.
        Also updates identity's last_seen_at.
        """
        if not session_id:
            return None

        result = query_one(
            f"""
            SELECT i.*, s.id as session_id, s.expires_at as session_expires_at
            FROM {Tables.SESSIONS} s
            JOIN {Tables.IDENTITIES} i ON i.id = s.identity_id
            WHERE s.id = %s
              AND s.revoked_at IS NULL
              AND s.expires_at > NOW()
            """,
            (session_id,),
        )

        if result:
            # Touch identity in background (don't fail if this fails)
            try:
                IdentityService.touch_identity(str(result["id"]))
            except Exception:
                pass

        return result

    @staticmethod
    def revoke_session(session_id: str) -> bool:
        """
        Revoke a session by ID.
        Returns True on success.
        """
        try:
            count = execute(
                f"UPDATE {Tables.SESSIONS} SET revoked_at = NOW() WHERE id = %s AND revoked_at IS NULL",
                (session_id,),
            )
            if count > 0:
                print(f"[SESSION] Revoked session {session_id}")
            return count > 0
        except DatabaseError:
            return False

    @staticmethod
    def revoke_all_sessions(identity_id: str, except_session_id: Optional[str] = None) -> int:
        """
        Revoke all sessions for an identity.
        Optionally keep one session active (current session).
        Returns count of revoked sessions.
        """
        try:
            if except_session_id:
                count = execute(
                    f"""
                    UPDATE {Tables.SESSIONS}
                    SET revoked_at = NOW()
                    WHERE identity_id = %s AND id != %s AND revoked_at IS NULL
                    """,
                    (identity_id, except_session_id),
                )
            else:
                count = execute(
                    f"UPDATE {Tables.SESSIONS} SET revoked_at = NOW() WHERE identity_id = %s AND revoked_at IS NULL",
                    (identity_id,),
                )
            print(f"[SESSION] Revoked {count} sessions for identity {identity_id}")
            return count
        except DatabaseError:
            return 0

    # ─────────────────────────────────────────────────────────────
    # Main Entry Point: get_or_create_session
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _create_anonymous_session_atomic(
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> Tuple[str, str, Dict[str, Any]]:
        """
        Create identity + wallet + session in ONE atomic transaction.
        This prevents race conditions where concurrent requests could create
        multiple identities or orphaned records.

        Returns (session_id, identity_id, identity_dict).

        Raises:
            DatabaseError: On database failure
        """
        session_id = str(uuid.uuid4())
        expires_at = now_utc() + timedelta(days=config.SESSION_TTL_DAYS)
        initial_balance = config.FREE_CREDITS_ON_SIGNUP

        ip_hash = IdentityService.hash_for_storage(ip_address) if ip_address else None
        ua_hash = IdentityService.hash_for_storage(user_agent) if user_agent else None

        with transaction() as cur:
            # 1. Create identity
            cur.execute(
                f"""
                INSERT INTO {Tables.IDENTITIES} (email, email_verified, created_at, last_seen_at)
                VALUES (NULL, FALSE, NOW(), NOW())
                RETURNING *
                """,
            )
            identity = fetch_one(cur)

            if not identity:
                raise DatabaseError("Failed to create identity")

            identity_id = str(identity["id"])

            # 2. Create wallet with initial balance
            cur.execute(
                f"""
                INSERT INTO {Tables.WALLETS} (identity_id, balance_credits, updated_at)
                VALUES (%s, %s, NOW())
                """,
                (identity_id, initial_balance),
            )

            # 3. Create ledger entry for welcome credits (if any)
            if initial_balance > 0:
                cur.execute(
                    f"""
                    INSERT INTO {Tables.LEDGER_ENTRIES}
                    (identity_id, entry_type, amount_credits, ref_type, meta, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    """,
                    (identity_id, "grant", initial_balance, "signup", '{"reason": "welcome_credits"}'),
                )

            # 4. Create session
            cur.execute(
                f"""
                INSERT INTO {Tables.SESSIONS}
                (id, identity_id, created_at, expires_at, ip_hash, user_agent_hash)
                VALUES (%s, %s, NOW(), %s, %s, %s)
                RETURNING *
                """,
                (session_id, identity_id, expires_at, ip_hash, ua_hash),
            )
            session = fetch_one(cur)

            if not session:
                raise DatabaseError("Failed to create session")

        print(f"[SESSION] Created anonymous session {session_id} for new identity {identity_id}")
        return session_id, identity_id, identity

    @staticmethod
    def get_or_create_session(request, response) -> Tuple[str, str]:
        """
        Get existing session or create new anonymous identity + session.
        Sets the session cookie on the response if new.

        Concurrency-safe: creates identity + wallet + session in one transaction.

        Returns (session_id, identity_id).

        Usage in a route:
            @app.route("/api/me")
            def get_me():
                session_id, identity_id = IdentityService.get_or_create_session(request, response)
                identity = IdentityService.get_identity(identity_id)
                return jsonify(identity)
        """
        # Try to get existing session from cookie
        cookie_session_id = IdentityService.get_session_id_from_request(request)

        if cookie_session_id:
            # Validate existing session
            identity = IdentityService.validate_session(cookie_session_id)
            if identity:
                # Session is valid, return existing
                return cookie_session_id, str(identity["id"])

            # Session invalid/expired - do NOT reuse cookie value, create fresh
            print(f"[SESSION] Invalid/expired session {cookie_session_id}, creating new")

        # Create new anonymous identity + session atomically
        ip_address = request.remote_addr
        user_agent = request.headers.get("User-Agent", "")

        session_id, identity_id, _ = IdentityService._create_anonymous_session_atomic(
            ip_address, user_agent
        )

        # Set cookie on response
        IdentityService.set_session_cookie(response, session_id)

        return session_id, identity_id

    @staticmethod
    def get_current_identity(request) -> Optional[Dict[str, Any]]:
        """
        Get the current identity from the request session.
        Returns None if no valid session.
        Does NOT create a new identity.
        """
        session_id = IdentityService.get_session_id_from_request(request)
        if not session_id:
            return None
        return IdentityService.validate_session(session_id)

    @staticmethod
    def require_identity(request) -> Dict[str, Any]:
        """
        Get the current identity or raise ValueError.
        Use this when an endpoint requires authentication.

        Raises:
            ValueError: If no valid session
        """
        identity = IdentityService.get_current_identity(request)
        if not identity:
            raise ValueError("No valid session")
        return identity

    # ────────────────────────────────────────────────────────────��
    # Session Linking (for magic code restore)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def link_session_to_identity(
        session_id: str,
        new_identity_id: str,
        request,
    ) -> Dict[str, Any]:
        """
        Link an existing session to a different identity.
        Used when restoring access via magic code.

        The old identity's session is revoked and a new session is created
        for the restored identity.

        Returns the new session record.
        """
        # Revoke the old session
        IdentityService.revoke_session(session_id)

        # Create new session for the restored identity
        ip_address = request.remote_addr
        user_agent = request.headers.get("User-Agent", "")

        new_session = IdentityService.create_session(new_identity_id, ip_address, user_agent)

        print(f"[SESSION] Linked session to identity {new_identity_id} (old session: {session_id})")
        return new_session

    # ─────────────────────────────────────────────────────────────
    # Cleanup
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def cleanup_expired_sessions() -> int:
        """
        Delete expired sessions from the database.
        Should be called periodically.
        Returns count of deleted sessions.
        """
        try:
            count = execute(
                f"DELETE FROM {Tables.SESSIONS} WHERE expires_at < NOW()"
            )
            if count > 0:
                print(f"[SESSION] Cleaned up {count} expired sessions")
            return count
        except DatabaseError:
            return 0
