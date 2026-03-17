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

from backend.db import (
    transaction,
    fetch_one,
    fetch_all,
    query_one,
    query_all,
    execute,
    Tables,
    hash_string,
)
from backend.config import config
from backend.emailer import send_magic_code, notify_restore_request


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
    # Restore Safety Check (IDENT-2 hotfix)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def check_restore_safety(
        session_id: str, target_identity_id: str
    ) -> Dict[str, Any]:
        """
        Determine whether switching this session to target_identity_id is safe.

        Returns dict:
            safe: bool — True if restore can proceed
            source_id: str or None — current session's identity
            reason: str — human-readable explanation
            stats: dict or None — source identity data stats (when relevant)

        A restore is BLOCKED when the source identity has meaningful data
        (history, credits, jobs, or purchases) that would be silently
        abandoned. This prevents data loss until a proper merge service
        exists (IDENT-1).
        """
        try:
            current_session = query_one(
                f"SELECT identity_id FROM {Tables.SESSIONS} WHERE id = %s",
                (session_id,),
            )
            if not current_session:
                # No source session — shouldn't happen, but allow (middleware
                # will catch downstream)
                return {"safe": True, "source_id": None, "reason": "no_source_session", "stats": None}

            source_id = str(current_session["identity_id"])

            if source_id == target_identity_id:
                print(
                    f"[RESTORE] Same identity — source=target={source_id[:8]}..., safe"
                )
                return {"safe": True, "source_id": source_id, "reason": "same_identity", "stats": None}

            # Query source identity data breadth
            source_stats = query_one(
                f"""
                SELECT
                    COALESCE(w.balance_credits, 0) as balance,
                    (SELECT COUNT(*) FROM {Tables.HISTORY_ITEMS}
                     WHERE identity_id = %s AND deleted_at IS NULL) as history_count,
                    (SELECT COUNT(*) FROM {Tables.JOBS}
                     WHERE identity_id = %s) as job_count,
                    (SELECT COUNT(*) FROM {Tables.PURCHASES}
                     WHERE identity_id = %s AND status = 'paid') as purchase_count,
                    (SELECT COUNT(*) FROM {Tables.SUBSCRIPTIONS}
                     WHERE identity_id::text = %s
                       AND status IN ('active', 'paused')) as active_sub_count
                FROM {Tables.WALLETS} w
                WHERE w.identity_id = %s
                """,
                (source_id, source_id, source_id, source_id, source_id),
            )

            if not source_stats:
                # No wallet row ⇒ identity was never used, safe to abandon
                print(
                    f"[RESTORE] No wallet for source {source_id[:8]}..., safe"
                )
                return {"safe": True, "source_id": source_id, "reason": "no_wallet", "stats": None}

            stats = {
                "history": int(source_stats["history_count"]),
                "jobs": int(source_stats["job_count"]),
                "balance": int(source_stats["balance"]),
                "purchases": int(source_stats["purchase_count"]),
                "active_subscriptions": int(source_stats["active_sub_count"]),
            }

            has_data = (
                stats["history"] > 0
                or stats["jobs"] > 0
                or stats["balance"] > 0
                or stats["purchases"] > 0
                or stats["active_subscriptions"] > 0
            )

            if has_data:
                print(
                    f"[RESTORE] BLOCKED: source {source_id[:8]}... → target "
                    f"{target_identity_id[:8]}... — source has "
                    f"history={stats['history']}, jobs={stats['jobs']}, "
                    f"balance={stats['balance']}, purchases={stats['purchases']}, "
                    f"subs={stats['active_subscriptions']}"
                )
                return {
                    "safe": False,
                    "source_id": source_id,
                    "reason": "source_has_data",
                    "stats": stats,
                }
            else:
                print(
                    f"[RESTORE] Safe: source {source_id[:8]}... has no data, "
                    f"proceeding to target {target_identity_id[:8]}..."
                )
                return {"safe": True, "source_id": source_id, "reason": "source_empty", "stats": stats}

        except Exception as err:
            # Safety check failure must NOT silently allow data loss.
            # Block the restore and log the error.
            print(f"[RESTORE] Safety check ERROR — blocking restore as precaution: {err}")
            return {
                "safe": False,
                "source_id": None,
                "reason": "safety_check_error",
                "stats": None,
            }

    # ─────────────────────────────────────────────────────────────
    # Request Code (Step 1)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def request_restore(
        email: str,
        ip_address: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Request a magic code for account restore.

        Args:
            email: Email address entered by user (may be a merged alias)
            ip_address: Client IP for rate limiting (optional)

        Returns:
            Dict with keys:
              ok: bool
              message: str
              resolved_email: str or None (canonical email code was sent to, if different)
              merge_redirected: bool

        Rate limits:
        - Max 3 active codes per email
        - 60 second cooldown between requests
        """
        input_email = email.strip().lower()

        # Validate email format (basic)
        if not input_email or "@" not in input_email or "." not in input_email:
            return {"ok": False, "message": "Invalid email format"}

        # Check if email exists in the system
        identity = query_one(
            f"SELECT id, email, merged_into_id FROM {Tables.IDENTITIES} WHERE email = %s",
            (input_email,),
        )
        if not identity:
            # Anti-enumeration: don't reveal if email exists
            print(f"[MAGIC_CODE] Request for unknown email: {input_email}")
            return {
                "ok": True,
                "message": "If this email is registered, a code has been sent",
            }

        # Follow merge chain — resolve to canonical identity
        delivery_email = input_email
        merge_redirected = False
        if identity.get("merged_into_id"):
            from backend.services.identity_service import IdentityService
            canonical_id = IdentityService.resolve_canonical_id(str(identity["id"]))
            canonical = query_one(
                f"SELECT id, email FROM {Tables.IDENTITIES} WHERE id = %s",
                (canonical_id,),
            )
            if canonical and canonical.get("email"):
                delivery_email = canonical["email"]
                merge_redirected = True
                print(
                    f"[MAGIC_CODE] Restore alias resolved: "
                    f"{input_email} → {delivery_email} "
                    f"(source {str(identity['id'])[:8]}... → canonical {canonical_id[:8]}...)"
                )

        # Check rate limits (against input email to prevent abuse via aliases)
        rate_limit_ok, rate_limit_msg = MagicCodeService._check_rate_limits(input_email)
        if not rate_limit_ok:
            return {"ok": False, "message": rate_limit_msg}

        # Generate code
        plain_code = MagicCodeService.generate_code()
        code_hash = MagicCodeService.hash_code(plain_code)

        # Hash IP for storage (privacy)
        ip_hash = hash_string(ip_address) if ip_address else None

        # Store code under the INPUT email so redeem can find it by what the user typed
        expiry_minutes = config.MAGIC_CODE_EXPIRY_MINUTES
        with transaction() as cur:
            cur.execute(
                f"""
                INSERT INTO {Tables.MAGIC_CODES}
                (email, code_hash, expires_at, attempts, consumed, ip_hash, created_at)
                VALUES (%s, %s, NOW() + %s * INTERVAL '1 minute', 0, FALSE, %s, NOW())
                RETURNING id
                """,
                (input_email, code_hash, expiry_minutes, ip_hash),
            )
            code_record = fetch_one(cur)

        if not code_record:
            print(f"[MAGIC_CODE] Failed to create code record for {input_email}")
            return {"ok": False, "message": "Failed to create code"}

        # Send email to the DELIVERY address (canonical email)
        email_sent = send_magic_code(delivery_email, plain_code)
        if not email_sent:
            print(f"[MAGIC_CODE] Failed to send email to {delivery_email}")
            return {
                "ok": True,
                "message": "If this email is registered, a code has been sent",
            }

        # Notify admin (optional, non-blocking)
        try:
            notify_restore_request(delivery_email)
        except Exception as email_err:
            print(f"[MAGIC_CODE] WARNING: Admin notification failed: {email_err}")

        print(
            f"[MAGIC_CODE] Code sent to {delivery_email} "
            f"(requested via {input_email}), expires in {expiry_minutes} minutes"
        )

        result = {
            "ok": True,
            "message": "Code sent to your email",
        }
        if merge_redirected:
            result["resolved_email"] = delivery_email
            result["merge_redirected"] = True
        return result

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

    # Return type for redeem_restore: 4-tuple adding optional structured detail
    # (success, identity_id, message, detail_or_none)

    @staticmethod
    def redeem_restore(
        email: str,
        code: str,
        session_id: str,
    ) -> Tuple[bool, Optional[str], str, Optional[Dict[str, Any]]]:
        """
        Verify a magic code and link the session to the identity.

        Safety (IDENT-2 + IDENT-1): If the current session's identity has
        meaningful data, an automatic merge is attempted. If the merge is
        blocked (subscription conflict, in-flight jobs, etc.), the code is
        NOT consumed so the user can retry later.

        Args:
            email: Email address the code was sent to
            code: The 6-digit code entered by user
            session_id: Current session ID to link to the identity

        Returns:
            Tuple of (success, identity_id, message, detail):
              - success: bool
              - identity_id: str or None
              - message: str
              - detail: dict or None — structured info when restore is blocked
        """
        email = email.strip().lower()
        code = code.strip()

        # Validate inputs
        if not email or not code or not session_id:
            return (False, None, "Missing required fields", None)

        if len(code) != 6 or not code.isdigit():
            return (False, None, "Invalid code format", None)

        # Get identity for this email
        identity = query_one(
            f"SELECT id, merged_into_id FROM {Tables.IDENTITIES} WHERE email = %s",
            (email,),
        )
        if not identity:
            return (False, None, "Invalid email or code", None)

        # Follow merge chain — always link session to canonical identity
        identity_id = str(identity["id"])
        if identity.get("merged_into_id"):
            from backend.services.identity_service import IdentityService
            canonical_id = IdentityService.resolve_canonical_id(identity_id)
            print(
                f"[MAGIC_CODE] Redeem using canonical target: {canonical_id[:8]}... "
                f"(input email: {email}, source identity: {identity_id[:8]}...)"
            )
            identity_id = canonical_id

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
            return (False, None, "Invalid or expired code", None)

        # Check max attempts on this specific code
        if code_record["attempts"] >= config.MAGIC_CODE_MAX_ATTEMPTS:
            return (False, None, "Too many failed attempts. Please request a new code", None)

        code_id = str(code_record["id"])

        # ── IDENT-2 safety gate + IDENT-1 auto-merge ────────────
        # Check BEFORE consuming the code so a blocked restore
        # preserves the code for a future retry.
        safety = MagicCodeService.check_restore_safety(session_id, identity_id)

        if not safety["safe"]:
            source_id = safety.get("source_id")
            reason = safety.get("reason")

            # Non-data reasons (e.g. safety_check_error) → block immediately
            if reason != "source_has_data" or not source_id:
                print(
                    f"[RESTORE] Restore DENIED (non-mergeable): session {session_id[:8]}...: "
                    f"reason={reason}, "
                    f"source={source_id[:8] + '...' if source_id else 'unknown'}, "
                    f"target={identity_id[:8]}..."
                )
                return (
                    False,
                    None,
                    "Restore cannot complete automatically. Please contact support.",
                    {
                        "error_code": "RESTORE_BLOCKED_DATA_CONFLICT",
                        "reason": reason,
                        "next_step": "contact_support",
                    },
                )

            # Source has data → attempt automatic merge (IDENT-1)
            from backend.services.merge_service import MergeService

            print(
                f"[RESTORE] Source {source_id[:8]}... has data, "
                f"attempting auto-merge → target {identity_id[:8]}..."
            )

            merge_result = MergeService.execute_merge(
                source_id=source_id,
                target_id=identity_id,
                merged_by="system",
                reason="restore",
                metadata={
                    "trigger": "redeem_restore",
                    "session_id": session_id,
                    "email": email,
                    "source_stats": safety.get("stats"),
                },
                skip_session_id=session_id,
            )

            if not merge_result["success"]:
                blocked = merge_result.get("blocked_reason", "unknown")
                print(
                    f"[RESTORE] Auto-merge BLOCKED: {source_id[:8]}... → "
                    f"{identity_id[:8]}...: {blocked}"
                )
                # Do NOT consume the code — user can retry later
                return (
                    False,
                    None,
                    "Your accounts could not be merged automatically. "
                    "Please wait for any active jobs to finish or contact support.",
                    {
                        "error_code": "RESTORE_MERGE_BLOCKED",
                        "blocked_reason": blocked,
                        "next_step": MergeService._suggest_next_step(blocked),
                    },
                )

            # Merge succeeded — log summary and continue to session swing
            print(
                f"[RESTORE] Auto-merge OK: {source_id[:8]}... → "
                f"{identity_id[:8]}... | "
                f"tables={merge_result.get('tables_migrated', {})}, "
                f"sessions_revoked={merge_result.get('sessions_revoked', 0)}"
            )
        # ── End IDENT-2 safety gate + IDENT-1 auto-merge ──────

        # Mark code as consumed and (if session exists) swing session to identity
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

            # AUTH-3: Only swing session if one exists.
            # When called without a session (no cookie), the route handler
            # creates a fresh session for the target identity directly.
            if session_id is not None:
                try:
                    cur.execute(
                        f"""
                        UPDATE {Tables.SESSIONS}
                        SET identity_id = %s, updated_at = NOW()
                        WHERE id = %s
                        RETURNING id
                        """,
                        (identity_id, session_id),
                    )
                except Exception as e:
                    # Fallback if updated_at column doesn't exist yet (pre-migration)
                    if "updated_at" in str(e):
                        print(f"[MAGIC_CODE] Warning: updated_at column missing, using fallback query")
                        cur.execute(
                            f"""
                            UPDATE {Tables.SESSIONS}
                            SET identity_id = %s
                            WHERE id = %s
                            RETURNING id
                            """,
                            (identity_id, session_id),
                        )
                    else:
                        raise
                updated_session = fetch_one(cur)

                if not updated_session:
                    raise ValueError(f"Session {session_id} not found")

            # NEW-4: Ensure email + email_verified are consistent.
            # The code was issued for `email`, so the canonical identity
            # must end with that exact email and email_verified = TRUE.
            cur.execute(
                f"""
                UPDATE {Tables.IDENTITIES}
                SET email = %s, email_verified = TRUE, last_seen_at = NOW()
                WHERE id = %s
                """,
                (email, identity_id),
            )

        # Invalidate other pending codes for this email
        MagicCodeService.invalidate_codes_for_email(email)

        print(f"[MAGIC_CODE] Successfully restored session for {email}, identity={identity_id}")
        return (True, identity_id, "Account restored successfully", None)

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
    def request_code(email: str, ip_address: Optional[str] = None) -> Dict[str, Any]:
        """Alias for request_restore (backward compatibility)."""
        return MagicCodeService.request_restore(email, ip_address)

    @staticmethod
    def verify_code(email: str, code: str, session_id: str) -> Tuple[bool, Optional[str], str, Optional[Dict[str, Any]]]:
        """Alias for redeem_restore (backward compatibility)."""
        return MagicCodeService.redeem_restore(email, code, session_id)
