"""
Identity Service - Manages user identities and sessions.

Anonymous-first identity system:
- Users get an identity + session on first visit (no signup required)
- Email can be attached later (for purchases/recovery)
- Sessions are stored in HttpOnly cookies

Cookie Collision Handling:
- Browsers may have multiple timrx_sid cookies (host-only vs domain cookie)
- We parse ALL values and select the one with an ACTIVE session in DB
- Legacy/invalid cookies are expired in responses

Usage:
    from backend.services.identity_service import IdentityService

    # In a route:
    session_id, identity_id = IdentityService.get_or_create_session(request, response)
    identity = IdentityService.get_identity(identity_id)
"""

from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime, timedelta
import uuid
import hashlib
import os
import re

from flask import Response, jsonify, g

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
import config as cfg
from emailer import notify_new_identity

# Debug flag for verbose cookie logging (set SESSION_DEBUG=1 to enable)
SESSION_DEBUG = os.getenv("SESSION_DEBUG", "").lower() in ("1", "true", "yes")

# Production safety warning (one-time at startup)
if SESSION_DEBUG and cfg.config.IS_PROD:
    print(
        "[WARN] SESSION_DEBUG enabled in production - "
        "disable after troubleshooting (set SESSION_DEBUG=0 or remove env var)"
    )


class IdentityService:
    """Service for managing identities and sessions."""

    # ─────────────────────────────────────────────────────────────
    # Cookie Parsing & Collision Resolution
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_all_session_ids_from_header(request) -> List[str]:
        """
        Parse ALL timrx_sid values from the raw Cookie header.

        When browsers have cookie collisions (multiple cookies with same name
        but different Domain attributes), Flask's request.cookies only returns
        one value. We need to parse the raw header to get all of them.

        Returns: List of session ID strings (may be empty, may have duplicates removed)
        """
        raw_cookie = request.headers.get("Cookie", "")
        if not raw_cookie:
            return []

        cookie_name = cfg.config.SESSION_COOKIE_NAME
        # Pattern: timrx_sid=<value> where value continues until ; or end
        # Cookie values are typically alphanumeric + hyphens (UUIDs)
        pattern = rf'{re.escape(cookie_name)}=([a-fA-F0-9\-]+)'
        matches = re.findall(pattern, raw_cookie)

        # Remove duplicates while preserving order
        seen = set()
        unique = []
        for sid in matches:
            if sid not in seen:
                seen.add(sid)
                unique.append(sid)

        return unique

    @staticmethod
    def _check_session_active(session_id: str) -> bool:
        """
        Check if a session ID corresponds to an active session in the database.
        Active = exists, not revoked, not expired.

        This is a lightweight check (no identity join) for collision resolution.
        """
        if not session_id:
            return False

        try:
            result = query_one(
                f"""
                SELECT 1 FROM {Tables.SESSIONS}
                WHERE id = %s
                  AND revoked_at IS NULL
                  AND expires_at > NOW()
                """,
                (session_id,),
            )
            return result is not None
        except Exception:
            return False

    @staticmethod
    def resolve_session_id(request) -> Tuple[Optional[str], List[str], str]:
        """
        Resolve the correct session ID from potentially multiple cookie values.

        Strategy:
        1. Parse all timrx_sid values from raw Cookie header
        2. Check each against DB for active session
        3. Return the first active one, or None if none active

        Returns:
            Tuple of (selected_session_id, all_candidates, reason)
            - selected_session_id: The session ID to use, or None
            - all_candidates: List of all session IDs found in cookies
            - reason: Why this session was selected (for logging)
        """
        candidates = IdentityService._parse_all_session_ids_from_header(request)

        if not candidates:
            # Fallback to Flask's parsed value (in case our regex missed something)
            flask_sid = request.cookies.get(cfg.config.SESSION_COOKIE_NAME)
            if flask_sid:
                candidates = [flask_sid]

        if not candidates:
            return (None, [], "no_cookies")

        if len(candidates) == 1:
            # Single cookie - just use it (will be validated later)
            return (candidates[0], candidates, "single_cookie")

        # Multiple cookies detected - check which is active
        if SESSION_DEBUG:
            print(f"[SESSION] Cookie collision: {len(candidates)} timrx_sid values found")

        active_sessions = []
        for sid in candidates:
            if IdentityService._check_session_active(sid):
                active_sessions.append(sid)

        if len(active_sessions) == 1:
            selected = active_sessions[0]
            reason = "single_active_from_collision"
            if SESSION_DEBUG:
                print(f"[SESSION] Resolved collision: selected {selected[:8]}... (only active session)")
            return (selected, candidates, reason)

        if len(active_sessions) > 1:
            # Multiple active sessions - pick the first one (arbitrary but deterministic)
            selected = active_sessions[0]
            reason = "first_active_from_multiple"
            if SESSION_DEBUG:
                print(f"[SESSION] Multiple active sessions in collision, picking first: {selected[:8]}...")
            return (selected, candidates, reason)

        # No active sessions found - return first candidate (will fail validation, new session created)
        reason = "no_active_sessions"
        if SESSION_DEBUG:
            print(f"[SESSION] No active sessions among {len(candidates)} candidates")
        return (candidates[0], candidates, reason)

    @staticmethod
    def get_session_id_from_request(request) -> Optional[str]:
        """
        Extract session ID from request cookies.

        Handles cookie collisions by checking which session is actually active.
        """
        selected, candidates, reason = IdentityService.resolve_session_id(request)

        # Log collision detection (gated by SESSION_DEBUG)
        if SESSION_DEBUG:
            if len(candidates) > 1:
                print(
                    f"[SESSION] COLLISION RESOLVED: {len(candidates)} cookies -> "
                    f"selected={selected[:8] + '...' if selected else 'None'}, reason={reason}"
                )
            elif selected:
                print(f"[SESSION] Single cookie: {selected[:8]}...")

        return selected

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

        IMPORTANT: In production, we ALWAYS set domain=.timrx.live to avoid
        host-only cookie collisions (e.g., 3d.timrx.live vs .timrx.live).
        """
        # Determine if we're in production mode
        # Force prod detection: if we see timrx.live in any form, treat as prod
        is_prod = cfg.config.IS_PROD

        # PRODUCTION: Canonical cookie settings (NEVER deviate)
        if is_prod:
            # Always use .timrx.live domain in production, regardless of config
            canonical_domain = cfg.config.SESSION_COOKIE_DOMAIN or ".timrx.live"
            canonical_samesite = "None"
            canonical_secure = True
        else:
            # Development: no domain (host-only), Lax samesite
            canonical_domain = None
            canonical_samesite = "Lax"
            canonical_secure = False

        # ─────────────────────────────────────────────────────────────
        # LEGACY COOKIE KILLER: Expire host-only cookie from current host
        #
        # Cookie Domain rules (RFC 6265):
        # - A response from 3d.timrx.live can only set/delete cookies where
        #   the Domain attribute is a suffix-match of the response host.
        # - We CAN manage: no-domain (host-only), Domain=.timrx.live, Domain=3d.timrx.live
        # - We CANNOT manage: Domain=www.timrx.live (sibling host, browser ignores)
        #
        # Strategy:
        # 1. Delete host-only cookie (no Domain attr) - clears 3d.timrx.live-scoped cookie
        # 2. Set canonical cookie with Domain=.timrx.live - shared across all subdomains
        # ─────────────────────────────────────────────────────────────
        if is_prod and canonical_domain:
            # Expire host-only cookie (no domain attribute = scoped to current host only)
            # This removes any cookie that was set without a Domain attribute,
            # which would be scoped to exactly the response host (e.g., 3d.timrx.live)
            response.set_cookie(
                cfg.config.SESSION_COOKIE_NAME,
                "",  # Empty value
                max_age=0,
                expires=0,
                path="/",
                secure=True,
                httponly=True,
                samesite="Lax",
                # NO domain attribute = targets host-only cookie on current host
            )

            if SESSION_DEBUG:
                print("[SESSION] Legacy cookie killer: expired host-only cookie (current host)")

        # ─────────────────────────────────────────────────────────────
        # SET CANONICAL COOKIE with proper domain
        # ─────────────────────────────────────────────────────────────
        cookie_kwargs = {
            "max_age": cfg.config.SESSION_TTL_SECONDS,
            "httponly": True,  # Always httponly
            "secure": canonical_secure,
            "samesite": canonical_samesite,
            "path": "/",
        }

        if canonical_domain:
            cookie_kwargs["domain"] = canonical_domain

        response.set_cookie(cfg.config.SESSION_COOKIE_NAME, session_id, **cookie_kwargs)

        # Debug logging for Set-Cookie header inspection (only when SESSION_DEBUG=1)
        if SESSION_DEBUG:
            set_cookie_headers = response.headers.getlist('Set-Cookie')
            print(
                f"[SESSION] Cookie set: name={cfg.config.SESSION_COOKIE_NAME}, "
                f"session_id={session_id[:8]}..., "
                f"domain={canonical_domain!r}, secure={canonical_secure}, "
                f"samesite={canonical_samesite}, is_prod={is_prod}"
            )
            print(f"[SESSION] Set-Cookie headers ({len(set_cookie_headers)}): {set_cookie_headers}")

    @staticmethod
    def clear_session_cookie(response) -> None:
        """
        Clear session cookies from the response.

        Cookie Domain rules (RFC 6265):
        - A response can only delete cookies where Domain is a suffix-match of response host.
        - From 3d.timrx.live: can delete host-only and Domain=.timrx.live
        - From 3d.timrx.live: CANNOT delete Domain=www.timrx.live (sibling, browser ignores)

        In production, clears:
        1. Host-only cookie (scoped to current host)
        2. Canonical cookie (Domain=.timrx.live, shared across subdomains)
        """
        is_prod = cfg.config.IS_PROD
        cookie_name = cfg.config.SESSION_COOKIE_NAME
        canonical_domain = cfg.config.SESSION_COOKIE_DOMAIN or (".timrx.live" if is_prod else None)

        if is_prod:
            # 1. Clear host-only cookie (no domain = current host only)
            response.delete_cookie(cookie_name, path="/")

        # 2. Clear canonical cookie with proper domain
        if canonical_domain:
            response.delete_cookie(cookie_name, path="/", domain=canonical_domain)
        else:
            response.delete_cookie(cookie_name, path="/")

        if SESSION_DEBUG:
            print(f"[SESSION] Cookies cleared: host-only + domain={canonical_domain!r}")

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
            initial_balance = cfg.config.FREE_CREDITS_ON_SIGNUP
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
        expires_at = now_utc() + timedelta(days=cfg.config.SESSION_TTL_DAYS)

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

        if SESSION_DEBUG:
            print(f"[SESSION] Created session {session_id[:8]}... for identity {identity_id[:8]}...")
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
            if count > 0 and SESSION_DEBUG:
                print(f"[SESSION] Revoked session {session_id[:8]}...")
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
            if SESSION_DEBUG:
                print(f"[SESSION] Revoked {count} sessions for identity {identity_id[:8]}...")
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
        expires_at = now_utc() + timedelta(days=cfg.config.SESSION_TTL_DAYS)
        initial_balance = cfg.config.FREE_CREDITS_ON_SIGNUP

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

        if SESSION_DEBUG:
            print(f"[SESSION] Created anonymous session {session_id[:8]}... for new identity {identity_id[:8]}...")
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
            if SESSION_DEBUG:
                print(f"[SESSION] Invalid/expired session {cookie_session_id[:8]}..., creating new")

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

        if SESSION_DEBUG:
            print(f"[SESSION] Linked session to identity {new_identity_id[:8]}... (old session: {session_id[:8]}...)")
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
            if count > 0 and SESSION_DEBUG:
                print(f"[SESSION] Cleaned up {count} expired sessions")
            return count
        except DatabaseError:
            return 0

# --- Phase 8: identity guard helper for modular routes ---


def require_identity() -> tuple[str | None, Response | None]:
    """
    Return the active identity_id from request context or an error response.

    Contract:
    - (identity_id, None) on success
    - (None, error_response) on failure
    """
    identity_id = getattr(g, "identity_id", None)
    if identity_id:
        return identity_id, None

    return None, (
        jsonify({
            "ok": False,
            "error": {"code": "NO_SESSION", "message": "A valid session is required."},
        }),
        401,
    )
