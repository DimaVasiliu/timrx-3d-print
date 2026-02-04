"""
Subscription Service — manages recurring credit plans.

Plan codes:
    starter_monthly, creator_monthly, studio_monthly,
    creator_yearly, studio_yearly

Credits are granted once per billing period (monthly).
Yearly plans bill annually but grant credits monthly.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from backend.db import USE_DB, get_conn, Tables


# ── Plan config (credits granted per month) ──────────────────
SUBSCRIPTION_PLANS: Dict[str, Dict[str, Any]] = {
    "starter_monthly": {
        "name": "Starter",
        "credits_per_month": 120,
        "price_gbp": 5.99,
        "cadence": "monthly",
        "tier": "starter",
    },
    "creator_monthly": {
        "name": "Creator",
        "credits_per_month": 300,
        "price_gbp": 14.99,
        "cadence": "monthly",
        "tier": "creator",
    },
    "studio_monthly": {
        "name": "Studio",
        "credits_per_month": 700,
        "price_gbp": 29.99,
        "cadence": "monthly",
        "tier": "studio",
    },
    "creator_yearly": {
        "name": "Creator",
        "credits_per_month": 300,
        "price_gbp": 149.99,
        "cadence": "yearly",
        "tier": "creator",
    },
    "studio_yearly": {
        "name": "Studio",
        "credits_per_month": 700,
        "price_gbp": 299.99,
        "cadence": "yearly",
        "tier": "studio",
    },
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class SubscriptionService:
    """Service for managing user subscriptions and credit grants."""

    # ── queries ───────────────────────────────────────────────

    @staticmethod
    def get_plan_info(plan_code: str) -> Optional[Dict[str, Any]]:
        """Return config for a subscription plan code, or None."""
        return SUBSCRIPTION_PLANS.get(plan_code)

    @staticmethod
    def list_plans() -> List[Dict[str, Any]]:
        """Return all subscription plans with their codes."""
        return [
            {"plan_code": code, **info}
            for code, info in SUBSCRIPTION_PLANS.items()
        ]

    @staticmethod
    def get_active_subscription(identity_id: str) -> Optional[Dict[str, Any]]:
        """Return the active (or cancelled-but-not-expired) subscription for a user."""
        if not USE_DB:
            return None
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, identity_id, plan_code, status,
                               provider, provider_subscription_id,
                               current_period_start, current_period_end,
                               cancelled_at, created_at, updated_at
                        FROM {Tables.SUBSCRIPTIONS}
                        WHERE identity_id = %s
                          AND status IN ('active', 'cancelled')
                          AND (current_period_end IS NULL OR current_period_end > NOW())
                        ORDER BY created_at DESC
                        LIMIT 1
                        """,
                        (identity_id,),
                    )
                    return cur.fetchone()
        except Exception as e:
            print(f"[SUB] Error fetching subscription for {identity_id}: {e}")
            return None

    # ── mutations ─────────────────────────────────────────────

    @staticmethod
    def create_subscription(
        identity_id: str,
        plan_code: str,
        provider: str = "mollie",
        provider_subscription_id: Optional[str] = None,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        """Create a new subscription row.  Returns the row dict or None."""
        if not USE_DB:
            return None

        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            print(f"[SUB] Unknown plan_code: {plan_code}")
            return None

        now = period_start or _now_utc()
        if period_end is None:
            if plan["cadence"] == "yearly":
                period_end = now + timedelta(days=365)
            else:
                period_end = now + timedelta(days=30)

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.SUBSCRIPTIONS}
                            (identity_id, plan_code, status, provider,
                             provider_subscription_id,
                             current_period_start, current_period_end)
                        VALUES (%s, %s, 'active', %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            identity_id,
                            plan_code,
                            provider,
                            provider_subscription_id,
                            now,
                            period_end,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
                print(f"[SUB] Created subscription {row['id']} for {identity_id} plan={plan_code}")
                return row
        except Exception as e:
            print(f"[SUB] Error creating subscription: {e}")
            return None

    @staticmethod
    def activate_subscription(
        subscription_id: str,
        provider_subscription_id: Optional[str] = None,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None,
    ) -> bool:
        """Mark a pending subscription as active and set period dates."""
        if not USE_DB:
            return False
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'active',
                            provider_subscription_id = COALESCE(%s, provider_subscription_id),
                            current_period_start = COALESCE(%s, current_period_start, NOW()),
                            current_period_end = COALESCE(%s, current_period_end),
                            updated_at = NOW()
                        WHERE id::text = %s
                        RETURNING id
                        """,
                        (provider_subscription_id, period_start, period_end, subscription_id),
                    )
                    row = cur.fetchone()
                conn.commit()
                return row is not None
        except Exception as e:
            print(f"[SUB] Error activating subscription {subscription_id}: {e}")
            return False

    @staticmethod
    def cancel_subscription(subscription_id: str) -> bool:
        """Cancel at period end (don't revoke remaining time)."""
        if not USE_DB:
            return False
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'cancelled',
                            cancelled_at = NOW(),
                            updated_at = NOW()
                        WHERE id::text = %s AND status = 'active'
                        RETURNING id
                        """,
                        (subscription_id,),
                    )
                    row = cur.fetchone()
                conn.commit()
                if row:
                    print(f"[SUB] Cancelled subscription {subscription_id}")
                return row is not None
        except Exception as e:
            print(f"[SUB] Error cancelling subscription {subscription_id}: {e}")
            return False

    # ── credit grants ─────────────────────────────────────────

    @staticmethod
    def grant_subscription_credits(
        subscription_id: str,
        period_start: datetime,
        period_end: datetime,
    ) -> Optional[str]:
        """
        Grant monthly credits for a subscription period.

        Idempotent: uses UNIQUE(subscription_id, period_start) to prevent
        double-grants.  Returns the cycle ID on success, None on skip/error.
        """
        if not USE_DB:
            return None

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Fetch subscription to determine credits
                    cur.execute(
                        f"""
                        SELECT id, identity_id, plan_code, status
                        FROM {Tables.SUBSCRIPTIONS}
                        WHERE id::text = %s
                        """,
                        (subscription_id,),
                    )
                    sub = cur.fetchone()
                    if not sub:
                        print(f"[SUB] Subscription {subscription_id} not found")
                        return None

                    plan = SUBSCRIPTION_PLANS.get(sub["plan_code"])
                    if not plan:
                        print(f"[SUB] Unknown plan for subscription {subscription_id}: {sub['plan_code']}")
                        return None

                    credits = plan["credits_per_month"]
                    identity_id = str(sub["identity_id"])

                    # Insert cycle (idempotent via unique constraint)
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.SUBSCRIPTION_CYCLES}
                            (subscription_id, period_start, period_end, credits_granted)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (subscription_id, period_start) DO NOTHING
                        RETURNING id
                        """,
                        (subscription_id, period_start, period_end, credits),
                    )
                    cycle = cur.fetchone()

                    if not cycle:
                        # Already granted for this period
                        print(f"[SUB] Credits already granted for {subscription_id} period {period_start}")
                        return None

                    cycle_id = str(cycle["id"])

                    # Add credits via ledger
                    from backend.services.wallet_service import WalletService
                    WalletService.add_credits(
                        identity_id,
                        credits,
                        entry_type="subscription_grant",
                        ref_type="subscription_cycle",
                        ref_id=cycle_id,
                        meta={
                            "subscription_id": subscription_id,
                            "plan_code": sub["plan_code"],
                            "period_start": period_start.isoformat(),
                            "period_end": period_end.isoformat(),
                        },
                    )

                conn.commit()
                print(
                    f"[SUB] Granted {credits} credits for subscription {subscription_id} "
                    f"period {period_start.date()} → {period_end.date()}"
                )
                return cycle_id

        except Exception as e:
            print(f"[SUB] Error granting credits for {subscription_id}: {e}")
            return None

    # ── admin / scheduled ─────────────────────────────────────

    @staticmethod
    def run_pending_grants() -> int:
        """
        Find active subscriptions that need a credit grant for the current
        period and grant them.  Returns the number of grants issued.

        Safe to call repeatedly (idempotent per period).
        """
        if not USE_DB:
            return 0

        granted = 0
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Find active subs where the current period hasn't been granted
                    cur.execute(
                        f"""
                        SELECT s.id, s.identity_id, s.plan_code,
                               s.current_period_start, s.current_period_end
                        FROM {Tables.SUBSCRIPTIONS} s
                        WHERE s.status = 'active'
                          AND s.current_period_start IS NOT NULL
                          AND s.current_period_start <= NOW()
                          AND NOT EXISTS (
                              SELECT 1 FROM {Tables.SUBSCRIPTION_CYCLES} c
                              WHERE c.subscription_id = s.id
                                AND c.period_start = s.current_period_start
                          )
                        """,
                    )
                    subs = cur.fetchall()

            for sub in subs or []:
                cycle_id = SubscriptionService.grant_subscription_credits(
                    str(sub["id"]),
                    sub["current_period_start"],
                    sub["current_period_end"],
                )
                if cycle_id:
                    granted += 1

        except Exception as e:
            print(f"[SUB] Error running pending grants: {e}")

        print(f"[SUB] run_pending_grants: issued {granted} grant(s)")
        return granted

    @staticmethod
    def get_subscription_by_provider_id(
        provider: str, provider_subscription_id: str
    ) -> Optional[Dict[str, Any]]:
        """Look up a subscription by its external provider ID."""
        if not USE_DB:
            return None
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT *
                        FROM {Tables.SUBSCRIPTIONS}
                        WHERE provider = %s AND provider_subscription_id = %s
                        LIMIT 1
                        """,
                        (provider, provider_subscription_id),
                    )
                    return cur.fetchone()
        except Exception as e:
            print(f"[SUB] Error fetching by provider id: {e}")
            return None
