"""
Notification Service - In-app + email notification system for TimrX.

Provides:
- Creating notifications for any event (tip, job complete, credit grant, etc.)
- Fetching paginated notifications with filtering
- Unread count for badge display
- Mark read (single / all)
- User preference management (muted categories, email frequency)
- Broadcast messages (admin → all users)
- Optional email delivery via EmailOutboxService

Usage:
    from backend.services.notification_service import NotificationService

    # Fire a notification (e.g. after a tip is received)
    NotificationService.create(
        identity_id=recipient_id,
        category="tip",
        notif_type="tip_received",
        title="You received a tip!",
        body=f"Someone tipped your post {amount} credits",
        icon="fa-hand-holding-dollar",
        link="/3dprint#community",
        meta={"amount": amount, "post_id": post_id, "tipper_id": tipper_id},
        send_email=True,
    )

    # Get unread count for badge
    count = NotificationService.get_unread_count(identity_id)

    # Fetch paginated list
    notifs = NotificationService.get_notifications(identity_id, limit=20, offset=0)
"""

import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from backend.db import (
    fetch_one, fetch_all, transaction, query_one, query_all, execute,
    Tables, get_conn,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Table references (matches 054_notification_center.sql)
# ============================================================================

# Use the billing schema directly since Tables class may not have these yet
_BILLING = "timrx_billing"
T_NOTIFICATIONS = f"{_BILLING}.notifications"
T_PREFERENCES = f"{_BILLING}.notification_preferences"
T_BROADCASTS = f"{_BILLING}.notification_broadcasts"
T_BROADCAST_DISMISSALS = f"{_BILLING}.notification_broadcast_dismissals"


# ============================================================================
# Valid notification types per category
# ============================================================================

VALID_NOTIF_TYPES = {
    "credit": [
        "welcome_bonus",           # First-time free credits
        "free_credits_granted",    # Promo / admin grant
        "low_balance_warning",     # Balance < threshold
        "credits_purchased",       # Purchase confirmed
        "subscription_renewed",    # Recurring sub billed
        "subscription_expiring",   # Expiry approaching
        "refund_approved",         # Refund processed
    ],
    "tip": [
        "tip_received",            # Someone tipped your post
    ],
    "community": [
        "reactions_milestone",     # N+ reactions on a post
        "post_featured",           # Admin featured your post
        "community_milestone",     # Achievement unlocked
    ],
    "job": [
        "job_complete",            # Generation succeeded
        "job_failed",              # Generation failed (credits refunded)
        "texture_complete",        # Texture/remesh done
    ],
    "account": [
        "email_verified",          # Email verification confirmed
        "email_attached",          # Email first linked
        "new_login",               # Session from new context
        "account_merged",          # Identity merge completed
    ],
    "system": [
        "feature_launched",        # New feature announcement
        "maintenance_scheduled",   # Downtime notice
        "tutorial_available",      # New tutorial published
        "platform_tip",            # Tip of the day
    ],
    "promo": [
        "daily_streak",            # Consecutive-day usage
        "seasonal_promo",          # Seasonal credit promotion
    ],
}

# Flatten for quick validation
ALL_NOTIF_TYPES = set()
for types in VALID_NOTIF_TYPES.values():
    ALL_NOTIF_TYPES.update(types)


# ============================================================================
# Service
# ============================================================================

class NotificationService:
    """Core notification operations."""

    # ─── Create ────────────────────────────────────────────────────────────

    @staticmethod
    def create(
        *,
        identity_id: str,
        category: str,
        notif_type: str,
        title: str,
        body: Optional[str] = None,
        icon: Optional[str] = None,
        link: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        send_email: bool = False,
        cur=None,
    ) -> Optional[Dict[str, Any]]:
        """
        Create a notification for a user.

        Can be called inside an existing transaction (pass cur) or standalone.
        If send_email=True AND the user has email + email notifications enabled,
        also queues an email via EmailOutboxService.

        Args:
            identity_id: Target user UUID
            category: One of notification_category enum values
            notif_type: Specific type string (e.g. 'tip_received')
            title: Short notification title (max 256 chars)
            body: Optional longer description
            icon: Font Awesome icon class (e.g. 'fa-hand-holding-dollar')
            link: Deep-link path (e.g. '/3dprint#community')
            meta: Extra JSON data (amount, job_id, sender, etc.)
            send_email: Whether to also queue an email notification
            cur: Optional existing DB cursor (for transactional use)

        Returns:
            The created notification row as dict, or None on failure
        """
        try:
            # Check user preferences — skip if category is muted
            prefs = NotificationService.get_preferences(identity_id)
            if prefs and not prefs.get("in_app_enabled", True):
                logger.debug("[NOTIF] In-app disabled for %s, skipping", identity_id)
                return None
            if prefs and category in (prefs.get("muted_categories") or []):
                logger.debug("[NOTIF] Category '%s' muted for %s", category, identity_id)
                return None

            meta_json = json.dumps(meta or {})

            def _insert(cursor):
                cursor.execute(
                    f"""
                    INSERT INTO {T_NOTIFICATIONS}
                    (identity_id, category, notif_type, title, body, icon, link, meta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING *
                    """,
                    (identity_id, category, notif_type, title, body, icon, link, meta_json),
                )
                return fetch_one(cursor)

            if cur:
                row = _insert(cur)
            else:
                with transaction("notif_create") as txn_cur:
                    row = _insert(txn_cur)

            if row:
                logger.info(
                    "[NOTIF] Created: id=%s type=%s for=%s",
                    row["id"], notif_type, identity_id,
                )

            # Optionally queue email
            if send_email and row:
                NotificationService._maybe_queue_email(
                    identity_id=identity_id,
                    category=category,
                    notif_type=notif_type,
                    title=title,
                    body=body,
                    meta=meta or {},
                    notification_id=str(row["id"]),
                )

            return row

        except Exception as e:
            logger.error("[NOTIF] Failed to create notification: %s", e)
            return None

    # ─── Read ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_unread_count(identity_id: str) -> int:
        """Fast unread count for badge display. Uses partial index."""
        try:
            row = query_one(
                f"""
                SELECT COUNT(*) AS cnt
                FROM {T_NOTIFICATIONS}
                WHERE identity_id = %s AND NOT is_read
                """,
                (identity_id,),
            )
            return row["cnt"] if row else 0
        except Exception as e:
            logger.error("[NOTIF] get_unread_count failed: %s", e)
            return 0

    @staticmethod
    def get_notifications(
        identity_id: str,
        *,
        limit: int = 20,
        offset: int = 0,
        category: Optional[str] = None,
        unread_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Paginated notification fetch.

        Args:
            identity_id: User UUID
            limit: Max items (default 20, max 50)
            offset: Skip N items
            category: Optional category filter
            unread_only: If True, only return unread items

        Returns:
            List of notification dicts, newest first
        """
        try:
            limit = min(max(limit, 1), 50)
            offset = max(offset, 0)

            conditions = ["identity_id = %s"]
            params: list = [identity_id]

            if category:
                conditions.append("category = %s")
                params.append(category)
            if unread_only:
                conditions.append("NOT is_read")

            where_clause = " AND ".join(conditions)
            params.extend([limit, offset])

            rows = query_all(
                f"""
                SELECT id::text, identity_id::text, category::text, notif_type,
                       title, body, icon, link, meta,
                       is_read, is_email_sent,
                       created_at, read_at
                FROM {T_NOTIFICATIONS}
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params),
            )
            return rows or []
        except Exception as e:
            logger.error("[NOTIF] get_notifications failed: %s", e)
            return []

    # ─── Mark read ────────────────────────────────────────────────────────

    @staticmethod
    def mark_read(identity_id: str, notification_id: str) -> bool:
        """Mark a single notification as read. Returns True if updated."""
        try:
            with transaction("notif_mark_read") as cur:
                cur.execute(
                    f"""
                    UPDATE {T_NOTIFICATIONS}
                    SET is_read = TRUE, read_at = NOW()
                    WHERE id = %s AND identity_id = %s AND NOT is_read
                    RETURNING id
                    """,
                    (notification_id, identity_id),
                )
                row = fetch_one(cur)
                return row is not None
        except Exception as e:
            logger.error("[NOTIF] mark_read failed: %s", e)
            return False

    @staticmethod
    def mark_all_read(identity_id: str) -> int:
        """Mark all unread notifications as read. Returns count updated."""
        try:
            with transaction("notif_mark_all_read") as cur:
                cur.execute(
                    f"""
                    UPDATE {T_NOTIFICATIONS}
                    SET is_read = TRUE, read_at = NOW()
                    WHERE identity_id = %s AND NOT is_read
                    """,
                    (identity_id,),
                )
                return cur.rowcount
        except Exception as e:
            logger.error("[NOTIF] mark_all_read failed: %s", e)
            return 0

    # ─── Preferences ──────────────────────────────────────────────────────

    @staticmethod
    def get_preferences(identity_id: str) -> Optional[Dict[str, Any]]:
        """Get user notification preferences. Returns None if no row (use defaults)."""
        try:
            return query_one(
                f"""
                SELECT identity_id::text, in_app_enabled, email_enabled,
                       email_frequency, muted_categories, updated_at
                FROM {T_PREFERENCES}
                WHERE identity_id = %s
                """,
                (identity_id,),
            )
        except Exception:
            return None

    @staticmethod
    def update_preferences(
        identity_id: str,
        *,
        in_app_enabled: Optional[bool] = None,
        email_enabled: Optional[bool] = None,
        email_frequency: Optional[str] = None,
        muted_categories: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Upsert user notification preferences.
        Only provided fields are updated.
        """
        try:
            # Build SET clause dynamically
            sets = ["updated_at = NOW()"]
            params: list = []

            if in_app_enabled is not None:
                sets.append("in_app_enabled = %s")
                params.append(in_app_enabled)
            if email_enabled is not None:
                sets.append("email_enabled = %s")
                params.append(email_enabled)
            if email_frequency is not None:
                if email_frequency not in ("instant", "daily", "weekly", "none"):
                    raise ValueError(f"Invalid email_frequency: {email_frequency}")
                sets.append("email_frequency = %s")
                params.append(email_frequency)
            if muted_categories is not None:
                sets.append("muted_categories = %s")
                params.append(muted_categories)

            set_clause = ", ".join(sets)
            params.append(identity_id)  # For WHERE / ON CONFLICT

            with transaction("notif_update_prefs") as cur:
                cur.execute(
                    f"""
                    INSERT INTO {T_PREFERENCES} (identity_id, {", ".join(
                        f.split(" = ")[0] for f in sets if f != "updated_at = NOW()"
                    )}, updated_at)
                    VALUES (%s, {", ".join(["%s"] * (len(params) - 1))}, NOW())
                    ON CONFLICT (identity_id)
                    DO UPDATE SET {set_clause}
                    RETURNING *
                    """,
                    (identity_id, *params[:-1]),  # identity_id first, then field values
                )
                row = fetch_one(cur)
                return row or {}
        except Exception as e:
            logger.error("[NOTIF] update_preferences failed: %s", e)
            # Simpler upsert fallback
            try:
                with transaction("notif_update_prefs_fallback") as cur:
                    # Try update first
                    cur.execute(
                        f"""
                        UPDATE {T_PREFERENCES}
                        SET {set_clause}
                        WHERE identity_id = %s
                        RETURNING *
                        """,
                        (*params[:-1], identity_id),
                    )
                    row = fetch_one(cur)
                    if row:
                        return row
                    # Insert with defaults
                    cur.execute(
                        f"""
                        INSERT INTO {T_PREFERENCES} (identity_id)
                        VALUES (%s)
                        RETURNING *
                        """,
                        (identity_id,),
                    )
                    return fetch_one(cur) or {}
            except Exception as e2:
                logger.error("[NOTIF] update_preferences fallback failed: %s", e2)
                return {}

    # ─── Broadcasts ───────────────────────────────────────────────────────

    @staticmethod
    def get_active_broadcasts(identity_id: str) -> List[Dict[str, Any]]:
        """
        Get broadcasts the user hasn't dismissed, within active date range.
        Merged into the notification dropdown alongside personal notifications.
        """
        try:
            rows = query_all(
                f"""
                SELECT b.id::text, b.category::text, b.notif_type, b.title, b.body,
                       b.icon, b.link, b.meta, b.starts_at, b.expires_at
                FROM {T_BROADCASTS} b
                WHERE b.is_active = TRUE
                  AND b.starts_at <= NOW()
                  AND (b.expires_at IS NULL OR b.expires_at > NOW())
                  AND NOT EXISTS (
                    SELECT 1 FROM {T_BROADCAST_DISMISSALS} d
                    WHERE d.broadcast_id = b.id AND d.identity_id = %s
                  )
                ORDER BY b.created_at DESC
                LIMIT 10
                """,
                (identity_id,),
            )
            return rows or []
        except Exception as e:
            logger.error("[NOTIF] get_active_broadcasts failed: %s", e)
            return []

    @staticmethod
    def dismiss_broadcast(identity_id: str, broadcast_id: str) -> bool:
        """Dismiss a broadcast for a user."""
        try:
            with transaction("notif_dismiss_broadcast") as cur:
                cur.execute(
                    f"""
                    INSERT INTO {T_BROADCAST_DISMISSALS} (identity_id, broadcast_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (identity_id, broadcast_id),
                )
                return True
        except Exception as e:
            logger.error("[NOTIF] dismiss_broadcast failed: %s", e)
            return False

    # ─── Email integration ────────────────────────────────────────────────

    @staticmethod
    def _maybe_queue_email(
        *,
        identity_id: str,
        category: str,
        notif_type: str,
        title: str,
        body: Optional[str],
        meta: Dict[str, Any],
        notification_id: str,
    ):
        """
        Queue an email notification if the user has email + email enabled.
        Uses the existing EmailOutboxService durable queue.
        """
        try:
            # Check if user has email and email notifications enabled
            prefs = NotificationService.get_preferences(identity_id)
            if prefs and not prefs.get("email_enabled", True):
                return
            if prefs and prefs.get("email_frequency") == "none":
                return

            # Look up user email
            identity = query_one(
                f"SELECT email FROM {Tables.IDENTITIES} WHERE id = %s",
                (identity_id,),
            )
            if not identity or not identity.get("email"):
                return  # No email attached — in-app only

            email = identity["email"]

            # Map notif_type to email template
            template = NotificationService._get_email_template(notif_type)
            if not template:
                return  # No email template for this type

            subject = f"TimrX — {title}"

            from backend.services.email_outbox_service import EmailOutboxService

            with transaction("notif_queue_email") as cur:
                EmailOutboxService.queue_email(
                    cur,
                    to_email=email,
                    template=template,
                    payload={
                        "title": title,
                        "body": body or "",
                        "category": category,
                        "notif_type": notif_type,
                        "notification_id": notification_id,
                        **meta,
                    },
                    subject=subject,
                    identity_id=identity_id,
                )

            # Mark email as sent on the notification
            try:
                execute(
                    f"UPDATE {T_NOTIFICATIONS} SET is_email_sent = TRUE WHERE id = %s",
                    (notification_id,),
                )
            except Exception:
                pass  # Non-critical

            logger.info("[NOTIF] Queued email: type=%s to=%s", notif_type, email)

        except Exception as e:
            # Email failure must not break the notification flow
            logger.error("[NOTIF] Email queue failed (non-fatal): %s", e)

    @staticmethod
    def _get_email_template(notif_type: str) -> Optional[str]:
        """Map notification type to email template identifier."""
        TEMPLATE_MAP = {
            "tip_received": "notification_tip_received",
            "job_complete": "notification_job_complete",
            "job_failed": "notification_job_failed",
            "low_balance_warning": "notification_low_balance",
            "welcome_bonus": "notification_welcome_bonus",
            "credits_purchased": "notification_credits_purchased",
            "refund_approved": "notification_refund_approved",
            "email_verified": "notification_email_verified",
            "feature_launched": "notification_feature_announcement",
            "subscription_renewed": "notification_subscription_renewed",
            "subscription_expiring": "notification_subscription_expiring",
        }
        return TEMPLATE_MAP.get(notif_type)

    # ─── Cleanup ──────────────────────────────────────────────────────────

    @staticmethod
    def cleanup_old_notifications(days: int = 90) -> int:
        """Delete read notifications older than N days. Call from cron."""
        try:
            with transaction("notif_cleanup") as cur:
                cur.execute(
                    f"""
                    DELETE FROM {T_NOTIFICATIONS}
                    WHERE is_read = TRUE
                      AND created_at < NOW() - INTERVAL '%s days'
                    """,
                    (days,),
                )
                count = cur.rowcount
                if count > 0:
                    logger.info("[NOTIF] Cleaned up %d old notifications", count)
                return count
        except Exception as e:
            logger.error("[NOTIF] cleanup failed: %s", e)
            return 0
