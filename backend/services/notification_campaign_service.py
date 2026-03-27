"""
Notification Campaign Service - Admin broadcast campaigns for TimrX.

Provides:
- Campaign CRUD (create, update, list, get, archive, duplicate)
- Audience resolution (query matching identities from rules)
- Safe campaign publishing (deduplicated per-user delivery)
- Credit grants tied to campaigns (ledger-backed, idempotent)
- Campaign analytics (delivered, read, clicked, dismissed, credits granted)
- Direct user notification + credit actions from admin

Usage:
    from backend.services.notification_campaign_service import NotificationCampaignService

    # Create campaign
    campaign = NotificationCampaignService.create_campaign(
        internal_name="Launch Promo",
        title="Try our new video tool!",
        body="Generate stunning videos with AI",
        grant_general_credits=50,
        grant_mode="on_delivery",
        audience_rules={"target": "all"},
        created_by="admin@timrx.com",
    )

    # Publish campaign
    result = NotificationCampaignService.publish_campaign(campaign["id"])
"""

import json
import logging
import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from backend.db import (
    fetch_one, fetch_all, transaction, query_one, query_all, execute,
    Tables,
)

logger = logging.getLogger(__name__)

# ============================================================================
# Table references (matches 056_notification_campaigns.sql)
# ============================================================================

_BILLING = "timrx_billing"
T_CAMPAIGNS = f"{_BILLING}.notification_campaigns"
T_DELIVERIES = f"{_BILLING}.notification_deliveries"
T_NOTIFICATIONS = f"{_BILLING}.notifications"
T_IDENTITIES = f"{_BILLING}.identities"
T_WALLETS = f"{_BILLING}.wallets"
T_SUBSCRIPTIONS = f"{_BILLING}.subscriptions"
T_PURCHASES = f"{_BILLING}.purchases"
T_JOBS = f"{_BILLING}.jobs"


# ============================================================================
# Campaign fields for insert/update
# ============================================================================

CAMPAIGN_CONTENT_FIELDS = [
    "internal_name", "title", "body", "rich_body", "emoji", "badge", "icon",
    "media_type", "media_url", "thumbnail_url",
    "action_label", "action_link", "secondary_action_label", "secondary_action_link",
    "link", "category", "delivery_mode",
]

CAMPAIGN_TARGETING_FIELDS = [
    "audience_rules", "grant_mode", "grant_general_credits", "grant_video_credits",
    "claim_expires_at",
]

CAMPAIGN_LIFECYCLE_FIELDS = [
    "status", "scheduled_at", "expires_at",
]


# ============================================================================
# Service
# ============================================================================

class NotificationCampaignService:
    """Admin notification campaign operations."""

    # ─── Campaign CRUD ─────────────────────────────────────────────────────

    @staticmethod
    def create_campaign(**kwargs) -> Dict[str, Any]:
        """
        Create a new campaign (starts as draft).

        Args:
            internal_name: Internal reference name (required)
            title: Notification title (required)
            body: Short body text
            rich_body: Optional rich/long body
            emoji: Optional emoji
            badge: Optional badge label (New, Bonus, etc.)
            icon: Font Awesome icon class
            media_type: none/image/video
            media_url: Media URL
            thumbnail_url: Thumbnail URL
            action_label: Primary CTA label
            action_link: Primary CTA link
            secondary_action_label: Secondary CTA label
            secondary_action_link: Secondary CTA link
            link: Deep-link path
            category: Notification category (default: system)
            delivery_mode: standard/rich_card/pinned/reward/direct
            audience_rules: JSON rules for targeting
            grant_mode: none/on_delivery/on_click
            grant_general_credits: General credits to grant
            grant_video_credits: Video credits to grant
            claim_expires_at: When credit claim expires
            scheduled_at: When to auto-publish
            expires_at: When campaign expires
            created_by: Admin identifier

        Returns:
            The created campaign dict
        """
        internal_name = kwargs.get("internal_name")
        title = kwargs.get("title")
        if not internal_name or not title:
            raise ValueError("internal_name and title are required")

        # Build columns and values
        columns = ["internal_name", "title"]
        values = [internal_name, title]
        placeholders = ["%s", "%s"]

        all_fields = CAMPAIGN_CONTENT_FIELDS + CAMPAIGN_TARGETING_FIELDS + CAMPAIGN_LIFECYCLE_FIELDS + ["created_by"]

        for field in all_fields:
            if field in ("internal_name", "title"):
                continue
            val = kwargs.get(field)
            if val is not None:
                columns.append(field)
                if field == "audience_rules" and isinstance(val, dict):
                    values.append(json.dumps(val))
                    placeholders.append("%s::jsonb")
                elif field in ("scheduled_at", "expires_at", "claim_expires_at") and isinstance(val, str):
                    values.append(val)
                    placeholders.append("%s::timestamptz")
                else:
                    values.append(val)
                    placeholders.append("%s")

        col_str = ", ".join(columns)
        ph_str = ", ".join(placeholders)

        with transaction("campaign_create") as cur:
            cur.execute(
                f"""
                INSERT INTO {T_CAMPAIGNS} ({col_str})
                VALUES ({ph_str})
                RETURNING *
                """,
                tuple(values),
            )
            row = fetch_one(cur)

        logger.info("[CAMPAIGN] Created: id=%s name=%s", row["id"], internal_name)
        return _serialize_campaign(row)

    @staticmethod
    def update_campaign(campaign_id: str, **kwargs) -> Dict[str, Any]:
        """
        Update a campaign. Only draft/scheduled campaigns can be edited.
        """
        with transaction("campaign_update") as cur:
            # Lock and verify status
            cur.execute(
                f"SELECT * FROM {T_CAMPAIGNS} WHERE id = %s FOR UPDATE",
                (campaign_id,),
            )
            campaign = fetch_one(cur)
            if not campaign:
                raise ValueError(f"Campaign not found: {campaign_id}")
            if campaign["status"] not in ("draft", "scheduled"):
                raise ValueError(f"Cannot edit campaign in status: {campaign['status']}")

            # Build SET clause
            sets = ["updated_at = NOW()"]
            params = []
            all_fields = CAMPAIGN_CONTENT_FIELDS + CAMPAIGN_TARGETING_FIELDS + CAMPAIGN_LIFECYCLE_FIELDS

            for field in all_fields:
                if field in kwargs:
                    val = kwargs[field]
                    if field == "audience_rules" and isinstance(val, dict):
                        sets.append(f"{field} = %s::jsonb")
                        params.append(json.dumps(val))
                    elif field in ("scheduled_at", "expires_at", "claim_expires_at"):
                        sets.append(f"{field} = %s::timestamptz")
                        params.append(val)
                    else:
                        sets.append(f"{field} = %s")
                        params.append(val)

            if len(sets) == 1:
                return _serialize_campaign(campaign)

            params.append(campaign_id)
            cur.execute(
                f"""
                UPDATE {T_CAMPAIGNS}
                SET {", ".join(sets)}
                WHERE id = %s
                RETURNING *
                """,
                tuple(params),
            )
            row = fetch_one(cur)
            logger.info("[CAMPAIGN] Updated: id=%s", campaign_id)
            return _serialize_campaign(row)

    @staticmethod
    def get_campaign(campaign_id: str) -> Optional[Dict[str, Any]]:
        """Get campaign by ID with analytics summary."""
        row = query_one(
            f"SELECT * FROM {T_CAMPAIGNS} WHERE id = %s",
            (campaign_id,),
        )
        if not row:
            return None
        result = _serialize_campaign(row)

        # Attach analytics
        stats = query_one(
            f"""
            SELECT
                COUNT(*) AS total_deliveries,
                COUNT(*) FILTER (WHERE delivery_status = 'delivered') AS delivered,
                COUNT(*) FILTER (WHERE delivery_status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE delivery_status = 'skipped') AS skipped,
                COUNT(*) FILTER (WHERE read_at IS NOT NULL) AS read_count,
                COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) AS click_count,
                COUNT(*) FILTER (WHERE dismissed_at IS NOT NULL) AS dismiss_count,
                COALESCE(SUM(credits_granted_general), 0) AS total_general_credits,
                COALESCE(SUM(credits_granted_video), 0) AS total_video_credits
            FROM {T_DELIVERIES}
            WHERE campaign_id = %s
            """,
            (campaign_id,),
        )
        result["analytics"] = dict(stats) if stats else {}
        return result

    @staticmethod
    def list_campaigns(
        *,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List campaigns with optional status filter."""
        limit = min(max(1, limit), 100)
        offset = max(0, offset)

        conditions = []
        params: list = []

        if status:
            conditions.append("c.status = %s")
            params.append(status)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.extend([limit, offset])

        rows = query_all(
            f"""
            SELECT c.*,
                COALESCE(d.delivered, 0) AS delivered_count,
                COALESCE(d.read_count, 0) AS read_count,
                COALESCE(d.click_count, 0) AS click_count,
                COALESCE(d.total_general, 0) AS credits_granted
            FROM {T_CAMPAIGNS} c
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) FILTER (WHERE delivery_status = 'delivered') AS delivered,
                    COUNT(*) FILTER (WHERE read_at IS NOT NULL) AS read_count,
                    COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) AS click_count,
                    COALESCE(SUM(credits_granted_general), 0) AS total_general
                FROM {T_DELIVERIES}
                WHERE campaign_id = c.id
            ) d ON TRUE
            {where}
            ORDER BY c.created_at DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params),
        )

        total = query_one(
            f"SELECT COUNT(*) AS cnt FROM {T_CAMPAIGNS} {where}",
            tuple(params[:-2]) if params[:-2] else (),
        )

        return {
            "campaigns": [_serialize_campaign(r) for r in (rows or [])],
            "total": total["cnt"] if total else 0,
            "limit": limit,
            "offset": offset,
        }

    @staticmethod
    def duplicate_campaign(campaign_id: str, created_by: Optional[str] = None) -> Dict[str, Any]:
        """Duplicate a campaign as a new draft."""
        original = query_one(
            f"SELECT * FROM {T_CAMPAIGNS} WHERE id = %s",
            (campaign_id,),
        )
        if not original:
            raise ValueError(f"Campaign not found: {campaign_id}")

        # Copy content fields
        kwargs = {}
        for field in CAMPAIGN_CONTENT_FIELDS + CAMPAIGN_TARGETING_FIELDS:
            val = original.get(field)
            if val is not None:
                kwargs[field] = val
        kwargs["internal_name"] = f"{original['internal_name']} (copy)"
        kwargs["created_by"] = created_by or original.get("created_by")

        return NotificationCampaignService.create_campaign(**kwargs)

    @staticmethod
    def archive_campaign(campaign_id: str) -> Dict[str, Any]:
        """Archive a campaign."""
        with transaction("campaign_archive") as cur:
            cur.execute(
                f"""
                UPDATE {T_CAMPAIGNS}
                SET status = 'archived', updated_at = NOW()
                WHERE id = %s AND status != 'archived'
                RETURNING *
                """,
                (campaign_id,),
            )
            row = fetch_one(cur)
            if not row:
                raise ValueError(f"Campaign not found or already archived: {campaign_id}")
            return _serialize_campaign(row)

    # ─── Audience Resolution ───────────────────────────────────────────────

    @staticmethod
    def resolve_audience(audience_rules: Dict[str, Any]) -> List[str]:
        """
        Resolve audience rules to a list of identity_ids.

        Supported rules:
            {"target": "all"}                        → All identities
            {"target": "verified_email"}             → Verified email users
            {"target": "unverified_email"}           → Unverified email users
            {"target": "low_credits", "threshold": N} → Users with balance ≤ N
            {"target": "zero_credits"}               → Users with 0 credits
            {"target": "active", "days": N}          → Seen in last N days
            {"target": "inactive", "days": N}        → Not seen in N days
            {"target": "paid_users"}                 → Users with paid purchases
            {"target": "subscription_users"}         → Active subscribers
            {"target": "free_only"}                  → Never purchased
            {"target": "generated_video"}            → Used video generation
            {"target": "generated_3d"}               → Used 3D generation
            {"target": "generated_image"}            → Used image generation
            {"target": "manual", "identity_ids": [...]} → Specific identities

        Returns:
            List of identity_id strings
        """
        target = audience_rules.get("target", "all")

        if target == "manual":
            ids = audience_rules.get("identity_ids", [])
            if not ids:
                raise ValueError("Manual target requires identity_ids list")
            return [str(i) for i in ids]

        # Build query based on target
        query, params = _build_audience_query(target, audience_rules)

        rows = query_all(query, params)
        return [str(r["identity_id"]) for r in (rows or [])]

    @staticmethod
    def preview_audience_count(audience_rules: Dict[str, Any]) -> int:
        """Count matching identities without fetching all IDs."""
        target = audience_rules.get("target", "all")

        if target == "manual":
            return len(audience_rules.get("identity_ids", []))

        query, params = _build_audience_query(target, audience_rules, count_only=True)
        row = query_one(query, params)
        return row["cnt"] if row else 0

    # ─── Campaign Publishing ──────────────────────────────────────────────

    @staticmethod
    def publish_campaign(campaign_id: str) -> Dict[str, Any]:
        """
        Publish a campaign: resolve audience, create deliveries + notifications.

        This is idempotent — already-delivered users are skipped via
        UNIQUE(campaign_id, identity_id) on notification_deliveries.

        Credit grants (if grant_mode='on_delivery') are applied atomically
        per user with stable ledger refs to prevent double-grants.

        Returns:
            Summary with counts of delivered, skipped, failed, credits_granted
        """
        with transaction("campaign_publish_lock") as cur:
            cur.execute(
                f"SELECT * FROM {T_CAMPAIGNS} WHERE id = %s FOR UPDATE",
                (campaign_id,),
            )
            campaign = fetch_one(cur)
            if not campaign:
                raise ValueError(f"Campaign not found: {campaign_id}")
            if campaign["status"] not in ("draft", "scheduled", "publishing"):
                raise ValueError(f"Cannot publish campaign in status: {campaign['status']}")

            # Mark as publishing
            cur.execute(
                f"""
                UPDATE {T_CAMPAIGNS}
                SET status = 'publishing', updated_at = NOW()
                WHERE id = %s
                """,
                (campaign_id,),
            )

        # Resolve audience (outside lock to avoid long hold)
        audience_rules = campaign["audience_rules"]
        if isinstance(audience_rules, str):
            audience_rules = json.loads(audience_rules)
        identity_ids = NotificationCampaignService.resolve_audience(audience_rules)

        delivered = 0
        skipped = 0
        failed = 0
        credits_granted_total = 0

        for identity_id in identity_ids:
            try:
                result = _deliver_to_user(campaign, identity_id)
                if result == "delivered":
                    delivered += 1
                    if campaign["grant_mode"] == "on_delivery":
                        granted = _grant_campaign_credits(campaign, identity_id)
                        credits_granted_total += granted
                elif result == "skipped":
                    skipped += 1
            except Exception as e:
                logger.error(
                    "[CAMPAIGN] Delivery failed: campaign=%s user=%s error=%s",
                    campaign_id, identity_id, e,
                )
                failed += 1

        # Mark as published
        with transaction("campaign_mark_published") as cur:
            cur.execute(
                f"""
                UPDATE {T_CAMPAIGNS}
                SET status = 'published', published_at = NOW(), updated_at = NOW()
                WHERE id = %s
                """,
                (campaign_id,),
            )

        logger.info(
            "[CAMPAIGN] Published: id=%s delivered=%d skipped=%d failed=%d credits=%d",
            campaign_id, delivered, skipped, failed, credits_granted_total,
        )

        return {
            "campaign_id": str(campaign_id),
            "delivered": delivered,
            "skipped": skipped,
            "failed": failed,
            "credits_granted": credits_granted_total,
            "total_targeted": len(identity_ids),
        }

    @staticmethod
    def test_send(campaign_id: str, identity_id: str) -> Dict[str, Any]:
        """Send a test notification to a specific user (does not change campaign status)."""
        campaign = query_one(
            f"SELECT * FROM {T_CAMPAIGNS} WHERE id = %s",
            (campaign_id,),
        )
        if not campaign:
            raise ValueError(f"Campaign not found: {campaign_id}")

        result = _deliver_to_user(campaign, identity_id, is_test=True)
        return {"status": result, "identity_id": str(identity_id)}

    # ─── Click/Claim Tracking ─────────────────────────────────────────────

    @staticmethod
    def record_click(notification_id: str, identity_id: str) -> Dict[str, Any]:
        """Record a click on a campaign notification. Grants credits if grant_mode='on_click'."""
        result = {"clicked": False, "credits_granted": 0}

        with transaction("campaign_click") as cur:
            # Update notification click timestamp
            cur.execute(
                f"""
                UPDATE {T_NOTIFICATIONS}
                SET clicked_at = COALESCE(clicked_at, NOW())
                WHERE id = %s AND identity_id = %s
                RETURNING campaign_id
                """,
                (notification_id, identity_id),
            )
            notif = fetch_one(cur)
            if not notif or not notif.get("campaign_id"):
                return result

            campaign_id = notif["campaign_id"]

            # Update delivery record
            cur.execute(
                f"""
                UPDATE {T_DELIVERIES}
                SET clicked_at = COALESCE(clicked_at, NOW())
                WHERE campaign_id = %s AND identity_id = %s
                """,
                (campaign_id, identity_id),
            )

            result["clicked"] = True

        # Check if we need to grant credits on click
        campaign = query_one(
            f"SELECT * FROM {T_CAMPAIGNS} WHERE id = %s",
            (campaign_id,),
        )
        if campaign and campaign["grant_mode"] == "on_click":
            # Check expiry
            if campaign.get("claim_expires_at"):
                now = datetime.now(timezone.utc)
                expires = campaign["claim_expires_at"]
                if hasattr(expires, 'tzinfo') and expires.tzinfo is None:
                    expires = expires.replace(tzinfo=timezone.utc)
                if now > expires:
                    return result

            granted = _grant_campaign_credits(campaign, identity_id)
            result["credits_granted"] = granted

        return result

    @staticmethod
    def record_dismiss(notification_id: str, identity_id: str) -> bool:
        """Record dismissal of a campaign notification."""
        try:
            with transaction("campaign_dismiss") as cur:
                cur.execute(
                    f"""
                    UPDATE {T_NOTIFICATIONS}
                    SET is_dismissed = TRUE, dismissed_at = COALESCE(dismissed_at, NOW())
                    WHERE id = %s AND identity_id = %s
                    RETURNING campaign_id
                    """,
                    (notification_id, identity_id),
                )
                notif = fetch_one(cur)
                if notif and notif.get("campaign_id"):
                    cur.execute(
                        f"""
                        UPDATE {T_DELIVERIES}
                        SET dismissed_at = COALESCE(dismissed_at, NOW())
                        WHERE campaign_id = %s AND identity_id = %s
                        """,
                        (notif["campaign_id"], identity_id),
                    )
                return True
        except Exception as e:
            logger.error("[CAMPAIGN] record_dismiss failed: %s", e)
            return False

    # ─── Analytics ─────────────────────────────────────────────────────────

    @staticmethod
    def get_overview_stats() -> Dict[str, Any]:
        """Get aggregate stats for the notification center overview."""
        stats = query_one(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE status = 'draft') AS drafts,
                COUNT(*) FILTER (WHERE status = 'scheduled') AS scheduled,
                COUNT(*) FILTER (WHERE status = 'published') AS published,
                COUNT(*) FILTER (WHERE status = 'expired') AS expired,
                COUNT(*) FILTER (WHERE status = 'archived') AS archived
            FROM {T_CAMPAIGNS}
            """,
        )

        delivery_stats = query_one(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE delivery_status = 'delivered') AS delivered_7d,
                COUNT(*) FILTER (WHERE read_at IS NOT NULL) AS read_7d,
                COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) AS clicked_7d,
                COUNT(*) FILTER (WHERE dismissed_at IS NOT NULL) AS dismissed_7d,
                COALESCE(SUM(credits_granted_general), 0) AS credits_general_7d,
                COALESCE(SUM(credits_granted_video), 0) AS credits_video_7d
            FROM {T_DELIVERIES} d
            JOIN {T_CAMPAIGNS} c ON c.id = d.campaign_id
            WHERE d.created_at >= NOW() - INTERVAL '7 days'
            """,
        )

        result = dict(stats) if stats else {}
        if delivery_stats:
            result.update(dict(delivery_stats))
            d7 = delivery_stats.get("delivered_7d", 0) or 0
            r7 = delivery_stats.get("read_7d", 0) or 0
            c7 = delivery_stats.get("clicked_7d", 0) or 0
            dis7 = delivery_stats.get("dismissed_7d", 0) or 0
            result["read_rate"] = round(r7 / d7 * 100, 1) if d7 > 0 else 0
            result["click_rate"] = round(c7 / d7 * 100, 1) if d7 > 0 else 0
            result["dismiss_rate"] = round(dis7 / d7 * 100, 1) if d7 > 0 else 0

        return result

    @staticmethod
    def get_campaign_analytics(campaign_id: str) -> Dict[str, Any]:
        """Detailed analytics for a single campaign."""
        stats = query_one(
            f"""
            SELECT
                COUNT(*) AS total_deliveries,
                COUNT(*) FILTER (WHERE delivery_status = 'delivered') AS delivered,
                COUNT(*) FILTER (WHERE delivery_status = 'failed') AS failed,
                COUNT(*) FILTER (WHERE delivery_status = 'skipped') AS skipped,
                COUNT(*) FILTER (WHERE read_at IS NOT NULL) AS read_count,
                COUNT(*) FILTER (WHERE clicked_at IS NOT NULL) AS click_count,
                COUNT(*) FILTER (WHERE dismissed_at IS NOT NULL) AS dismiss_count,
                COALESCE(SUM(credits_granted_general), 0) AS total_general_credits,
                COALESCE(SUM(credits_granted_video), 0) AS total_video_credits,
                MIN(delivered_at) AS first_delivered,
                MAX(delivered_at) AS last_delivered
            FROM {T_DELIVERIES}
            WHERE campaign_id = %s
            """,
            (campaign_id,),
        )

        result = dict(stats) if stats else {}
        delivered = result.get("delivered", 0) or 0
        read = result.get("read_count", 0) or 0
        clicked = result.get("click_count", 0) or 0
        dismissed = result.get("dismiss_count", 0) or 0

        result["read_rate"] = round(read / delivered * 100, 1) if delivered > 0 else 0
        result["click_rate"] = round(clicked / delivered * 100, 1) if delivered > 0 else 0
        result["dismiss_rate"] = round(dismissed / delivered * 100, 1) if delivered > 0 else 0

        return result

    # ─── Direct User Actions ──────────────────────────────────────────────

    @staticmethod
    def send_direct_notification(
        *,
        identity_id: str,
        title: str,
        body: Optional[str] = None,
        rich_body: Optional[str] = None,
        emoji: Optional[str] = None,
        badge: Optional[str] = None,
        icon: Optional[str] = None,
        media_type: str = "none",
        media_url: Optional[str] = None,
        thumbnail_url: Optional[str] = None,
        action_label: Optional[str] = None,
        action_link: Optional[str] = None,
        link: Optional[str] = None,
        category: str = "system",
        admin_email: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send a one-off notification to a specific user from admin."""
        ref_id = str(uuid.uuid4())

        with transaction("direct_notif") as cur:
            cur.execute(
                f"""
                INSERT INTO {T_NOTIFICATIONS}
                (identity_id, category, notif_type, title, body, rich_body,
                 icon, link, emoji, badge,
                 media_type, media_url, thumbnail_url,
                 action_label, action_link,
                 source_kind, ref_type, ref_id,
                 meta)
                VALUES (%s, %s, 'admin_direct', %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        'admin_direct', 'admin_direct', %s,
                        %s)
                RETURNING *
                """,
                (identity_id, category, title, body, rich_body,
                 icon, link, emoji, badge,
                 media_type, media_url, thumbnail_url,
                 action_label, action_link,
                 ref_id,
                 json.dumps({"admin_email": admin_email})),
            )
            row = fetch_one(cur)

        logger.info("[CAMPAIGN] Direct notification sent to %s by %s", identity_id, admin_email)
        return _serialize_notification(row)

    @staticmethod
    def grant_credits_direct(
        *,
        identity_id: str,
        general_credits: int = 0,
        video_credits: int = 0,
        reason: str = "admin_grant",
        admin_email: Optional[str] = None,
        send_notification: bool = True,
        notification_title: Optional[str] = None,
        notification_body: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Grant credits to a user from admin. Optionally sends a notification.

        Uses stable ref keys for idempotency:
            admin_direct_grant:<uuid>:identity:<identity_id>
        """
        from backend.services.wallet_service import WalletService, CreditType

        grant_ref = str(uuid.uuid4())
        results = {"general_granted": 0, "video_granted": 0, "notification_id": None}

        meta = {
            "reason": reason,
            "admin_email": admin_email,
            "grant_ref": grant_ref,
        }

        if general_credits > 0:
            WalletService.add_ledger_entry(
                identity_id=identity_id,
                entry_type="admin_adjust",
                delta=general_credits,
                ref_type="admin_direct_grant",
                ref_id=f"{grant_ref}:general:{identity_id}",
                meta=meta,
                credit_type=CreditType.GENERAL,
            )
            results["general_granted"] = general_credits

        if video_credits > 0:
            WalletService.add_ledger_entry(
                identity_id=identity_id,
                entry_type="admin_adjust",
                delta=video_credits,
                ref_type="admin_direct_grant",
                ref_id=f"{grant_ref}:video:{identity_id}",
                meta=meta,
                credit_type=CreditType.VIDEO,
            )
            results["video_granted"] = video_credits

        # Send notification about the grant
        if send_notification and (general_credits > 0 or video_credits > 0):
            total = general_credits + video_credits
            n_title = notification_title or f"You received {total} free credits!"
            parts = []
            if general_credits > 0:
                parts.append(f"{general_credits} general credits")
            if video_credits > 0:
                parts.append(f"{video_credits} video credits")
            n_body = notification_body or f"You've been gifted {' and '.join(parts)}. Enjoy creating!"

            from backend.services.notification_service import NotificationService
            notif = NotificationService.create(
                identity_id=identity_id,
                category="credit",
                notif_type="free_credits_granted",
                title=n_title,
                body=n_body,
                icon="fa-gift",
                link="/3dprint",
                meta={"general_credits": general_credits, "video_credits": video_credits, "admin_email": admin_email},
                ref_type="admin_direct_grant_notif",
                ref_id=f"{grant_ref}:{identity_id}",
            )
            if notif:
                results["notification_id"] = str(notif["id"])

        logger.info(
            "[CAMPAIGN] Direct grant to %s: general=%d video=%d by %s",
            identity_id, general_credits, video_credits, admin_email,
        )
        return results

    @staticmethod
    def get_user_notifications(
        identity_id: str,
        limit: int = 20,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Fetch recent notifications for a user (admin view)."""
        rows = query_all(
            f"""
            SELECT n.id::text, n.identity_id::text, n.category::text, n.notif_type,
                   n.title, n.body, n.icon, n.link, n.meta,
                   n.is_read, n.is_dismissed, n.source_kind,
                   n.emoji, n.badge, n.media_type, n.media_url,
                   n.action_label, n.action_link,
                   n.campaign_id::text,
                   n.created_at, n.read_at, n.clicked_at, n.dismissed_at
            FROM {T_NOTIFICATIONS} n
            WHERE n.identity_id = %s
            ORDER BY n.created_at DESC
            LIMIT %s OFFSET %s
            """,
            (identity_id, min(limit, 100), max(offset, 0)),
        )
        return rows or []


# ============================================================================
# Internal helpers
# ============================================================================

def _build_audience_query(
    target: str,
    rules: Dict[str, Any],
    count_only: bool = False,
) -> tuple:
    """Build SQL query for audience resolution."""
    select = "COUNT(*) AS cnt" if count_only else "i.id AS identity_id"

    base = f"SELECT {select} FROM {T_IDENTITIES} i WHERE i.merged_into_id IS NULL"
    params: list = []

    if target == "all":
        pass  # no extra filter

    elif target == "verified_email":
        base += " AND i.email IS NOT NULL AND i.email_verified = TRUE"

    elif target == "unverified_email":
        base += " AND (i.email IS NULL OR i.email_verified = FALSE)"

    elif target == "low_credits":
        threshold = rules.get("threshold", 10)
        base += f"""
            AND EXISTS (
                SELECT 1 FROM {T_WALLETS} w
                WHERE w.identity_id = i.id AND w.balance_credits <= %s
            )
        """
        params.append(threshold)

    elif target == "zero_credits":
        base += f"""
            AND EXISTS (
                SELECT 1 FROM {T_WALLETS} w
                WHERE w.identity_id = i.id AND w.balance_credits = 0
            )
        """

    elif target == "active":
        days = rules.get("days", 7)
        base += f" AND i.last_seen_at >= NOW() - INTERVAL '%s days'"
        params.append(days)

    elif target == "inactive":
        days = rules.get("days", 30)
        base += f" AND (i.last_seen_at IS NULL OR i.last_seen_at < NOW() - INTERVAL '%s days')"
        params.append(days)

    elif target == "paid_users":
        base += f"""
            AND EXISTS (
                SELECT 1 FROM {T_PURCHASES} p
                WHERE p.identity_id = i.id AND p.status = 'paid'
            )
        """

    elif target == "subscription_users":
        base += f"""
            AND EXISTS (
                SELECT 1 FROM {T_SUBSCRIPTIONS} s
                WHERE s.identity_id = i.id AND s.status = 'active'
            )
        """

    elif target == "free_only":
        base += f"""
            AND NOT EXISTS (
                SELECT 1 FROM {T_PURCHASES} p
                WHERE p.identity_id = i.id AND p.status = 'paid'
            )
        """

    elif target == "generated_video":
        base += f"""
            AND EXISTS (
                SELECT 1 FROM {T_JOBS} j
                WHERE j.identity_id = i.id AND j.action_code LIKE 'VIDEO_%'
                AND j.status = 'completed'
            )
        """

    elif target == "generated_3d":
        base += f"""
            AND EXISTS (
                SELECT 1 FROM {T_JOBS} j
                WHERE j.identity_id = i.id
                AND j.action_code IN ('MESHY_TEXT_TO_3D', 'MESHY_IMAGE_TO_3D')
                AND j.status = 'completed'
            )
        """

    elif target == "generated_image":
        base += f"""
            AND EXISTS (
                SELECT 1 FROM {T_JOBS} j
                WHERE j.identity_id = i.id AND j.action_code LIKE 'IMAGE_%'
                AND j.status = 'completed'
            )
        """

    else:
        raise ValueError(f"Unsupported audience target: {target}")

    return base, tuple(params)


def _deliver_to_user(
    campaign: Dict[str, Any],
    identity_id: str,
    is_test: bool = False,
) -> str:
    """
    Deliver a campaign notification to one user.

    Returns 'delivered', 'skipped' (duplicate), or raises on failure.
    """
    campaign_id = str(campaign["id"])
    notif_type = "campaign_broadcast"
    if campaign.get("delivery_mode") == "reward":
        notif_type = "free_credits_granted"
    elif campaign.get("delivery_mode") == "pinned":
        notif_type = "feature_launched"

    category = campaign.get("category", "system")
    meta = {"campaign_id": campaign_id, "is_test": is_test}
    if campaign.get("grant_general_credits", 0) > 0 or campaign.get("grant_video_credits", 0) > 0:
        meta["has_credits"] = True
        meta["general_credits"] = campaign.get("grant_general_credits", 0)
        meta["video_credits"] = campaign.get("grant_video_credits", 0)
        meta["grant_mode"] = campaign.get("grant_mode", "none")

    with transaction("campaign_deliver") as cur:
        # Create notification
        ref_suffix = f"test_{uuid.uuid4().hex[:8]}" if is_test else campaign_id
        cur.execute(
            f"""
            INSERT INTO {T_NOTIFICATIONS}
            (identity_id, category, notif_type, title, body, rich_body,
             icon, link, emoji, badge,
             media_type, media_url, thumbnail_url,
             action_label, action_link, secondary_action_label, secondary_action_link,
             campaign_id, source_kind,
             ref_type, ref_id,
             meta)
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, 'campaign',
                    'campaign', %s,
                    %s)
            ON CONFLICT (identity_id, ref_type, ref_id) WHERE ref_type IS NOT NULL
            DO NOTHING
            RETURNING id
            """,
            (identity_id, category, notif_type,
             campaign.get("title"), campaign.get("body"), campaign.get("rich_body"),
             campaign.get("icon"), campaign.get("link") or campaign.get("action_link"),
             campaign.get("emoji"), campaign.get("badge"),
             campaign.get("media_type", "none"), campaign.get("media_url"), campaign.get("thumbnail_url"),
             campaign.get("action_label"), campaign.get("action_link"),
             campaign.get("secondary_action_label"), campaign.get("secondary_action_link"),
             campaign_id,
             f"{ref_suffix}:{identity_id}",
             json.dumps(meta)),
        )
        notif_row = fetch_one(cur)

        if not notif_row:
            return "skipped"  # Duplicate

        notification_id = notif_row["id"]

        # Create delivery record (skip for test sends)
        if not is_test:
            cur.execute(
                f"""
                INSERT INTO {T_DELIVERIES}
                (campaign_id, identity_id, notification_id,
                 delivery_status, delivered_at)
                VALUES (%s, %s, %s, 'delivered', NOW())
                ON CONFLICT (campaign_id, identity_id) DO NOTHING
                RETURNING id
                """,
                (campaign_id, identity_id, notification_id),
            )
            delivery_row = fetch_one(cur)
            if not delivery_row:
                return "skipped"  # Duplicate delivery

    return "delivered"


def _grant_campaign_credits(campaign: Dict[str, Any], identity_id: str) -> int:
    """
    Grant campaign credits to a user. Idempotent via stable ledger refs.

    Ref pattern: notification_campaign:<campaign_id>:identity:<identity_id>

    Returns total credits granted.
    """
    from backend.services.wallet_service import WalletService, CreditType

    campaign_id = str(campaign["id"])
    general = campaign.get("grant_general_credits", 0) or 0
    video = campaign.get("grant_video_credits", 0) or 0
    total_granted = 0

    meta = {
        "campaign_id": campaign_id,
        "campaign_name": campaign.get("internal_name"),
        "grant_mode": campaign.get("grant_mode"),
    }

    if general > 0:
        ref_id = f"notification_campaign:{campaign_id}:general:{identity_id}"
        try:
            WalletService.add_ledger_entry(
                identity_id=identity_id,
                entry_type="admin_adjust",
                delta=general,
                ref_type="campaign_grant",
                ref_id=ref_id,
                meta=meta,
                credit_type=CreditType.GENERAL,
            )
            total_granted += general
        except Exception as e:
            # Check if it's a duplicate (idempotent skip)
            if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                logger.debug("[CAMPAIGN] Duplicate grant skipped: %s", ref_id)
            else:
                logger.error("[CAMPAIGN] Grant failed: %s %s", ref_id, e)
                raise

    if video > 0:
        ref_id = f"notification_campaign:{campaign_id}:video:{identity_id}"
        try:
            WalletService.add_ledger_entry(
                identity_id=identity_id,
                entry_type="admin_adjust",
                delta=video,
                ref_type="campaign_grant",
                ref_id=ref_id,
                meta=meta,
                credit_type=CreditType.VIDEO,
            )
            total_granted += video
        except Exception as e:
            if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                logger.debug("[CAMPAIGN] Duplicate video grant skipped: %s", ref_id)
            else:
                logger.error("[CAMPAIGN] Video grant failed: %s %s", ref_id, e)
                raise

    # Update delivery record with grant info
    if total_granted > 0:
        try:
            execute(
                f"""
                UPDATE {T_DELIVERIES}
                SET credits_granted_general = %s,
                    credits_granted_video = %s,
                    grant_ledger_ref = %s,
                    granted_at = NOW()
                WHERE campaign_id = %s AND identity_id = %s
                """,
                (general, video,
                 f"notification_campaign:{campaign_id}:{identity_id}",
                 campaign_id, identity_id),
            )
        except Exception:
            pass  # Non-fatal: delivery record update is informational

    return total_granted


def _serialize_campaign(row: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize a campaign row for JSON response."""
    if not row:
        return {}
    result = {}
    for key, val in row.items():
        if isinstance(val, datetime):
            result[key] = val.isoformat()
        elif isinstance(val, uuid.UUID):
            result[key] = str(val)
        elif key == "audience_rules" and isinstance(val, str):
            try:
                result[key] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                result[key] = val
        else:
            result[key] = val
    return result


def _serialize_notification(row: Dict[str, Any]) -> Dict[str, Any]:
    """Serialize a notification row for JSON response."""
    if not row:
        return {}
    result = {}
    for key, val in row.items():
        if isinstance(val, datetime):
            result[key] = val.isoformat()
        elif isinstance(val, uuid.UUID):
            result[key] = str(val)
        else:
            result[key] = val
    return result
