"""
Subscription Service — manages recurring credit plans.

Plan codes:
    starter_monthly, creator_monthly, studio_monthly,
    starter_yearly, creator_yearly, studio_yearly

Credits are granted once per billing period (monthly).
Yearly plans bill annually but grant credits monthly.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from backend.db import USE_DB, get_conn, Tables


# ── Plan config (credits granted per month) ──────────────────
# Monthly: Credits granted each billing cycle
# Yearly: Credits granted monthly over 12 months (not all at once)
#
# PRICING (Feb 2026):
#   Monthly: Starter £9.99 (400c), Creator £24.99 (1300c), Studio £49.99 (3200c)
#   Yearly:  Starter £99 (4800c/yr), Creator £249 (15600c/yr), Studio £499 (38400c/yr)
#   (Yearly = ~2 months free equivalent)
#
# TIER PERKS:
#   Free:    2 concurrent jobs, standard queue
#   Starter: 5 concurrent jobs, medium priority
#   Creator: 10 concurrent jobs, high priority
#   Studio:  20 concurrent jobs, pro priority

SUBSCRIPTION_PLANS: Dict[str, Dict[str, Any]] = {
    # ── MONTHLY PLANS ──
    "starter_monthly": {
        "name": "Starter",
        "credits_per_month": 400,
        "price_gbp": 9.99,
        "cadence": "monthly",
        "tier": "starter",
        "max_concurrent_jobs": 5,
        "queue_priority": "medium",
    },
    "creator_monthly": {
        "name": "Creator",
        "credits_per_month": 1300,
        "price_gbp": 24.99,
        "cadence": "monthly",
        "tier": "creator",
        "max_concurrent_jobs": 10,
        "queue_priority": "high",
    },
    "studio_monthly": {
        "name": "Studio",
        "credits_per_month": 3200,
        "price_gbp": 49.99,
        "cadence": "monthly",
        "tier": "studio",
        "max_concurrent_jobs": 20,
        "queue_priority": "pro",
    },
    # ── YEARLY PLANS (monthly credit distribution) ──
    # Credits distributed monthly: 400/1300/3200 per month for 12 months
    "starter_yearly": {
        "name": "Starter",
        "credits_per_month": 400,       # 4800/year ÷ 12 months
        "credits_total_yearly": 4800,   # Total for UI display
        "price_gbp": 99.00,
        "cadence": "yearly",
        "tier": "starter",
        "max_concurrent_jobs": 5,
        "queue_priority": "medium",
    },
    "creator_yearly": {
        "name": "Creator",
        "credits_per_month": 1300,      # 15600/year ÷ 12 months
        "credits_total_yearly": 15600,
        "price_gbp": 249.00,
        "cadence": "yearly",
        "tier": "creator",
        "max_concurrent_jobs": 10,
        "queue_priority": "high",
    },
    "studio_yearly": {
        "name": "Studio",
        "credits_per_month": 3200,      # 38400/year ÷ 12 months
        "credits_total_yearly": 38400,
        "price_gbp": 499.00,
        "cadence": "yearly",
        "tier": "studio",
        "max_concurrent_jobs": 20,
        "queue_priority": "pro",
    },
}

# ── Tier perks lookup (derived from subscription or free tier) ──
TIER_PERKS: Dict[str, Dict[str, Any]] = {
    "free": {
        "max_concurrent_jobs": 2,
        "queue_priority": "standard",
    },
    "starter": {
        "max_concurrent_jobs": 5,
        "queue_priority": "medium",
    },
    "creator": {
        "max_concurrent_jobs": 10,
        "queue_priority": "high",
    },
    "studio": {
        "max_concurrent_jobs": 20,
        "queue_priority": "pro",
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
    def get_tier_perks(identity_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get tier perks for a user based on their active subscription.

        Returns:
            Dict with:
                tier: 'free' | 'starter' | 'creator' | 'studio'
                max_concurrent_jobs: int
                queue_priority: 'standard' | 'medium' | 'high' | 'pro'
                plan_code: str or None
        """
        # Default to free tier
        result = {
            "tier": "free",
            "max_concurrent_jobs": TIER_PERKS["free"]["max_concurrent_jobs"],
            "queue_priority": TIER_PERKS["free"]["queue_priority"],
            "plan_code": None,
        }

        if not identity_id:
            return result

        # Check for active subscription
        sub = SubscriptionService.get_active_subscription(identity_id)
        if not sub or sub.get("status") not in ("active", "cancelled"):
            return result

        # Get plan info
        plan_code = sub.get("plan_code")
        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            return result

        # Return tier perks from plan
        tier = plan.get("tier", "free")
        return {
            "tier": tier,
            "max_concurrent_jobs": plan.get("max_concurrent_jobs", TIER_PERKS.get(tier, TIER_PERKS["free"])["max_concurrent_jobs"]),
            "queue_priority": plan.get("queue_priority", TIER_PERKS.get(tier, TIER_PERKS["free"])["queue_priority"]),
            "plan_code": plan_code,
        }

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
        customer_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Create a new subscription row.  Returns the row dict or None."""
        if not USE_DB:
            return None

        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            print(f"[SUB] Unknown plan_code: {plan_code}")
            return None

        now = period_start or _now_utc()
        billing_day = now.day
        if period_end is None:
            if plan["cadence"] == "yearly":
                period_end = now + timedelta(days=365)
            else:
                period_end = now + timedelta(days=30)

        # Calculate next credit date for monthly allocation tracking
        next_credit_date = SubscriptionService.calculate_next_credit_date(now, billing_day)

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.SUBSCRIPTIONS}
                            (identity_id, plan_code, status, provider,
                             provider_subscription_id,
                             current_period_start, current_period_end,
                             customer_email, billing_day, next_credit_date)
                        VALUES (%s, %s, 'active', %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            identity_id,
                            plan_code,
                            provider,
                            provider_subscription_id,
                            now,
                            period_end,
                            customer_email,
                            billing_day,
                            next_credit_date,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
                print(f"[SUB] Created subscription {row['id']} for {identity_id} plan={plan_code} email={customer_email}")
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
        provider: str = "mollie",
        provider_payment_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Grant monthly credits for a subscription period.

        Idempotent via TWO unique constraints:
        1. UNIQUE(subscription_id, period_start) - one grant per subscription per period
        2. UNIQUE(provider, provider_payment_id) - one grant per Mollie payment

        Args:
            subscription_id: Internal subscription UUID
            period_start: Start of the billing cycle (aligned to billing_day)
            period_end: End of the billing cycle (+1 month from start)
            provider: Payment provider (default 'mollie')
            provider_payment_id: Mollie payment ID (tr_*) that triggered this grant

        Returns:
            Dict with cycle_id and credits_granted on success, None on skip/error
        """
        if not USE_DB:
            return None

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Fetch subscription to determine credits and check yearly remaining
                    cur.execute(
                        f"""
                        SELECT id, identity_id, plan_code, status, credits_remaining_months
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

                    # For yearly plans, check if any months remain
                    is_yearly = "_yearly" in sub["plan_code"]
                    remaining = sub.get("credits_remaining_months")
                    if is_yearly and remaining is not None and remaining <= 0:
                        print(f"[SUB] Yearly subscription {subscription_id} has no remaining months")
                        return None

                    credits_amount = plan["credits_per_month"]
                    identity_id = str(sub["identity_id"])

                    # Insert cycle (idempotent via unique constraints)
                    # Uses ON CONFLICT DO NOTHING for both unique constraints
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.SUBSCRIPTION_CYCLES}
                            (subscription_id, period_start, period_end, credits_granted,
                             provider, provider_payment_id)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (subscription_id, period_start) DO NOTHING
                        RETURNING id
                        """,
                        (subscription_id, period_start, period_end, credits_amount,
                         provider, provider_payment_id),
                    )
                    cycle = cur.fetchone()

                    if not cycle:
                        # Already granted for this period - check if it was this payment
                        if provider_payment_id:
                            cur.execute(
                                f"""
                                SELECT id FROM {Tables.SUBSCRIPTION_CYCLES}
                                WHERE provider = %s AND provider_payment_id = %s
                                """,
                                (provider, provider_payment_id),
                            )
                            existing_by_payment = cur.fetchone()
                            if existing_by_payment:
                                print(f"[SUB] Payment {provider_payment_id} already granted cycle")
                                return None

                        print(f"[SUB] Credits already granted for {subscription_id} period {period_start}")
                        return None

                    cycle_id = str(cycle["id"])

                    # Decrement remaining months for yearly plans
                    if is_yearly and remaining is not None:
                        cur.execute(
                            f"""
                            UPDATE {Tables.SUBSCRIPTIONS}
                            SET credits_remaining_months = credits_remaining_months - 1,
                                updated_at = NOW()
                            WHERE id::text = %s AND credits_remaining_months > 0
                            """,
                            (subscription_id,),
                        )

                    # Add credits via ledger (with credit_type based on plan)
                    from backend.services.wallet_service import WalletService, get_credit_type_for_plan
                    credit_type = get_credit_type_for_plan(sub["plan_code"])
                    WalletService.add_credits(
                        identity_id,
                        credits_amount,
                        entry_type="subscription_grant",
                        ref_type="subscription_cycle",
                        ref_id=cycle_id,
                        meta={
                            "subscription_id": subscription_id,
                            "plan_code": sub["plan_code"],
                            "period_start": period_start.isoformat(),
                            "period_end": period_end.isoformat(),
                            "provider_payment_id": provider_payment_id,
                        },
                        credit_type=credit_type,
                    )

                conn.commit()
                print(
                    f"[SUB] Granted {credits_amount} credits for subscription {subscription_id} "
                    f"period {period_start.date()} → {period_end.date()} "
                    f"(payment: {provider_payment_id or 'N/A'})"
                )
                return {
                    "cycle_id": cycle_id,
                    "credits_granted": credits_amount,
                    "subscription_id": subscription_id,
                    "identity_id": identity_id,
                    "period_start": period_start,
                    "period_end": period_end,
                }

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

    # ══════════════════════════════════════════════════════════════
    # ENHANCED SUBSCRIPTION MANAGEMENT (Monthly Credit Allocation)
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def calculate_next_credit_date(
        from_date: datetime,
        billing_day: int,
    ) -> datetime:
        """
        Calculate the next monthly credit allocation date.

        Handles edge cases like billing_day=31 for months with fewer days.

        Args:
            from_date: The date to calculate from
            billing_day: The day of month for billing (1-31)

        Returns:
            Next credit date as datetime
        """
        import calendar

        # Move to next month
        if from_date.month == 12:
            next_month = 1
            next_year = from_date.year + 1
        else:
            next_month = from_date.month + 1
            next_year = from_date.year

        # Get last day of next month
        _, last_day = calendar.monthrange(next_year, next_month)

        # Use billing_day or last day if billing_day exceeds month length
        target_day = min(billing_day, last_day)

        return datetime(
            year=next_year,
            month=next_month,
            day=target_day,
            hour=from_date.hour,
            minute=from_date.minute,
            second=0,
            tzinfo=timezone.utc,
        )

    @staticmethod
    def calculate_cycle_period(
        payment_ts: datetime,
        billing_day: int,
    ) -> tuple[datetime, datetime]:
        """
        Calculate the monthly cycle period (period_start, period_end) for a payment.

        Given a payment timestamp and the subscription's billing_day, determines
        which monthly cycle this payment belongs to.

        Args:
            payment_ts: Payment timestamp (paidAt or createdAt from Mollie)
            billing_day: The day of month for billing (1-31)

        Returns:
            Tuple of (period_start, period_end) as UTC datetimes

        Examples:
            billing_day=18, payment_ts=2026-02-18 → (2026-02-18, 2026-03-18)
            billing_day=18, payment_ts=2026-02-20 → (2026-02-18, 2026-03-18)
            billing_day=31, payment_ts=2026-02-28 → (2026-02-28, 2026-03-31)
        """
        import calendar

        # Ensure UTC
        if payment_ts.tzinfo is None:
            payment_ts = payment_ts.replace(tzinfo=timezone.utc)

        year = payment_ts.year
        month = payment_ts.month
        day = payment_ts.day

        # Get the actual billing day for this month (handle 31 in Feb, etc.)
        _, last_day_of_month = calendar.monthrange(year, month)
        actual_billing_day = min(billing_day, last_day_of_month)

        # Determine if payment is in current or previous cycle
        if day >= actual_billing_day:
            # Payment is on or after billing day → belongs to cycle starting this month
            period_start_year = year
            period_start_month = month
        else:
            # Payment is before billing day → belongs to cycle starting last month
            if month == 1:
                period_start_year = year - 1
                period_start_month = 12
            else:
                period_start_year = year
                period_start_month = month - 1

        # Calculate period_start with proper billing_day handling
        _, last_day_start_month = calendar.monthrange(period_start_year, period_start_month)
        start_day = min(billing_day, last_day_start_month)
        period_start = datetime(
            year=period_start_year,
            month=period_start_month,
            day=start_day,
            hour=0,
            minute=0,
            second=0,
            tzinfo=timezone.utc,
        )

        # Calculate period_end (next month's billing day)
        if period_start_month == 12:
            end_year = period_start_year + 1
            end_month = 1
        else:
            end_year = period_start_year
            end_month = period_start_month + 1

        _, last_day_end_month = calendar.monthrange(end_year, end_month)
        end_day = min(billing_day, last_day_end_month)
        period_end = datetime(
            year=end_year,
            month=end_month,
            day=end_day,
            hour=0,
            minute=0,
            second=0,
            tzinfo=timezone.utc,
        )

        return period_start, period_end

    @staticmethod
    def find_subscription_for_payment(
        identity_id: str,
        plan_code: str,
        provider: str = "mollie",
    ) -> Optional[Dict[str, Any]]:
        """
        Find the most likely subscription for a payment.

        Used by reconciliation when metadata.subscription_id is not available.
        Returns the most recent active subscription matching identity + plan.

        Args:
            identity_id: User's identity UUID
            plan_code: Subscription plan code
            provider: Payment provider (default 'mollie')

        Returns:
            Subscription row or None
        """
        if not USE_DB:
            return None

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, identity_id, plan_code, status, provider,
                               provider_subscription_id, billing_day,
                               current_period_start, current_period_end,
                               credits_remaining_months, customer_email
                        FROM {Tables.SUBSCRIPTIONS}
                        WHERE identity_id::text = %s
                          AND plan_code = %s
                          AND provider = %s
                          AND status IN ('active', 'past_due', 'cancelled')
                        ORDER BY
                            CASE status
                                WHEN 'active' THEN 1
                                WHEN 'past_due' THEN 2
                                ELSE 3
                            END,
                            created_at DESC
                        LIMIT 1
                        """,
                        (identity_id, plan_code, provider),
                    )
                    return cur.fetchone()
        except Exception as e:
            print(f"[SUB] Error finding subscription for payment: {e}")
            return None

    @staticmethod
    def check_payment_already_granted(
        provider: str,
        provider_payment_id: str,
    ) -> bool:
        """
        Check if a payment has already been used to grant a cycle.

        Args:
            provider: Payment provider
            provider_payment_id: Mollie payment ID (tr_*)

        Returns:
            True if payment already granted a cycle, False otherwise
        """
        if not USE_DB or not provider_payment_id:
            return False

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT 1 FROM {Tables.SUBSCRIPTION_CYCLES}
                        WHERE provider = %s AND provider_payment_id = %s
                        LIMIT 1
                        """,
                        (provider, provider_payment_id),
                    )
                    return cur.fetchone() is not None
        except Exception as e:
            print(f"[SUB] Error checking payment grant: {e}")
            return False

    @staticmethod
    def check_cycle_exists(
        subscription_id: str,
        period_start: datetime,
    ) -> bool:
        """
        Check if a cycle already exists for a subscription and period.

        Args:
            subscription_id: Internal subscription UUID
            period_start: Start of the billing cycle

        Returns:
            True if cycle exists, False otherwise
        """
        if not USE_DB:
            return False

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT 1 FROM {Tables.SUBSCRIPTION_CYCLES}
                        WHERE subscription_id::text = %s AND period_start = %s
                        LIMIT 1
                        """,
                        (subscription_id, period_start),
                    )
                    return cur.fetchone() is not None
        except Exception as e:
            print(f"[SUB] Error checking cycle exists: {e}")
            return False

    @staticmethod
    def initialize_subscription(
        identity_id: str,
        plan_code: str,
        customer_email: str,
        provider: str = "mollie",
        provider_subscription_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Initialize a new subscription with proper credit allocation setup.

        This is called after successful payment. It:
        1. Creates the subscription record
        2. Sets up billing_day based on signup date
        3. Grants first month's credits immediately
        4. Calculates next_credit_date for future allocations
        5. Sends confirmation email

        Returns the subscription dict or None on failure.
        """
        if not USE_DB:
            return None

        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            print(f"[SUB] Unknown plan_code: {plan_code}")
            return None

        now = _now_utc()
        billing_day = now.day
        cadence = plan["cadence"]

        # Calculate period end based on cadence
        if cadence == "yearly":
            period_end = now + timedelta(days=365)
            credits_remaining_months = 12
            renewal_date = period_end
        else:
            period_end = now + timedelta(days=30)
            credits_remaining_months = None
            renewal_date = None

        # Calculate next credit date (one month from now)
        next_credit_date = SubscriptionService.calculate_next_credit_date(now, billing_day)

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Create subscription
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.SUBSCRIPTIONS}
                            (identity_id, plan_code, status, provider,
                             provider_subscription_id,
                             current_period_start, current_period_end,
                             billing_day, next_credit_date,
                             credits_remaining_months, renewal_date,
                             customer_email)
                        VALUES (%s, %s, 'active', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING *
                        """,
                        (
                            identity_id,
                            plan_code,
                            provider,
                            provider_subscription_id,
                            now,
                            period_end,
                            billing_day,
                            next_credit_date,
                            credits_remaining_months,
                            renewal_date,
                            customer_email,
                        ),
                    )
                    sub = cur.fetchone()

                    if not sub:
                        return None

                    sub_id = str(sub["id"])

                    # Log subscription created event
                    cur.execute(
                        f"""
                        INSERT INTO timrx_billing.subscription_events
                            (subscription_id, event_type, event_data)
                        VALUES (%s, 'created', %s)
                        """,
                        (sub_id, json.dumps({
                            "plan_code": plan_code,
                            "cadence": cadence,
                            "billing_day": billing_day,
                            "customer_email": customer_email,
                        })),
                    )

                conn.commit()

            # Grant first month's credits immediately
            first_period_end = SubscriptionService.calculate_next_credit_date(now, billing_day)
            cycle_id = SubscriptionService.grant_subscription_credits(
                sub_id, now, first_period_end
            )

            if cycle_id:
                # Send confirmation email with first credits
                SubscriptionService._send_credits_delivered_email(
                    subscription_id=sub_id,
                    customer_email=customer_email,
                    plan_code=plan_code,
                    credits_granted=plan["credits_per_month"],
                    is_first_grant=True,
                    next_credit_date=next_credit_date,
                )

                # Log credits granted event
                SubscriptionService._log_event(sub_id, "credits_granted", {
                    "cycle_id": cycle_id,
                    "credits": plan["credits_per_month"],
                    "is_first_grant": True,
                })

            print(
                f"[SUB] Initialized subscription {sub_id} for {identity_id}, "
                f"plan={plan_code}, billing_day={billing_day}, next_credit={next_credit_date}"
            )

            return sub

        except Exception as e:
            print(f"[SUB] Error initializing subscription: {e}")
            return None

    @staticmethod
    def process_due_credit_allocations() -> Dict[str, Any]:
        """
        Process all subscriptions with due credit allocations.

        This should be run by a cron job every hour or so.
        It finds subscriptions where next_credit_date <= NOW() and:
        1. Grants the monthly credits
        2. Updates next_credit_date to next month
        3. Sends notification email
        4. For yearly plans, decrements credits_remaining_months

        Returns summary: {processed: N, granted: N, errors: N}
        """
        if not USE_DB:
            return {"processed": 0, "granted": 0, "errors": 0}

        result = {"processed": 0, "granted": 0, "errors": 0, "details": []}

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Find subscriptions with due credit allocations
                    # HARDENED: Only grant credits if subscription is truly valid:
                    #   - status = 'active'
                    #   - next_credit_date is due
                    #   - current_period_end is in the future (not expired)
                    #   - not cancelled
                    #   - not expired
                    cur.execute(
                        f"""
                        SELECT s.*,
                               (SELECT COUNT(*) FROM {Tables.SUBSCRIPTION_CYCLES} c
                                WHERE c.subscription_id = s.id) as cycles_count
                        FROM {Tables.SUBSCRIPTIONS} s
                        WHERE s.status = 'active'
                          AND s.next_credit_date IS NOT NULL
                          AND s.next_credit_date <= NOW()
                          AND (s.current_period_end IS NULL OR s.current_period_end >= NOW())
                          AND s.cancelled_at IS NULL
                          AND s.expired_at IS NULL
                        ORDER BY s.next_credit_date ASC
                        LIMIT 100
                        """,
                    )
                    subs = cur.fetchall() or []

            for sub in subs:
                result["processed"] += 1
                sub_id = str(sub["id"])
                identity_id = str(sub["identity_id"])
                plan_code = sub["plan_code"]
                plan = SUBSCRIPTION_PLANS.get(plan_code)

                if not plan:
                    result["errors"] += 1
                    continue

                # ────────────────────────────────────────────────────────────────
                # EMAIL VERIFICATION GATE: Block credit grants if email unverified
                # ────────────────────────────────────────────────────────────────
                try:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                f"SELECT email, email_verified FROM {Tables.IDENTITIES} WHERE id = %s",
                                (identity_id,),
                            )
                            identity = cur.fetchone()

                    if not identity or not identity.get("email_verified"):
                        # Pause the subscription and skip credit grant
                        print(
                            f"[SUB] Pausing subscription {sub_id}: email not verified for identity {identity_id}"
                        )
                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                cur.execute(
                                    f"""
                                    UPDATE {Tables.SUBSCRIPTIONS}
                                    SET pause_reason = 'email_unverified',
                                        paused_at = NOW(),
                                        updated_at = NOW()
                                    WHERE id::text = %s
                                    """,
                                    (sub_id,),
                                )
                            conn.commit()

                        SubscriptionService._log_event(sub_id, "paused_email_unverified", {
                            "identity_id": identity_id,
                            "email": identity.get("email") if identity else None,
                        })

                        result["details"].append({
                            "subscription_id": sub_id,
                            "status": "paused",
                            "reason": "email_unverified",
                        })
                        continue  # Skip credit grant

                except Exception as e:
                    print(f"[SUB] Error checking email verification for {sub_id}: {e}")
                    result["errors"] += 1
                    continue

                # Check if yearly plan has exhausted credits
                if plan["cadence"] == "yearly":
                    remaining = sub.get("credits_remaining_months", 0)
                    if remaining is not None and remaining <= 0:
                        print(f"[SUB] Yearly subscription {sub_id} has exhausted monthly credits")
                        continue

                try:
                    # Grant credits for this period
                    period_start = sub["next_credit_date"]
                    billing_day = sub.get("billing_day") or period_start.day
                    period_end = SubscriptionService.calculate_next_credit_date(period_start, billing_day)

                    cycle_id = SubscriptionService.grant_subscription_credits(
                        sub_id, period_start, period_end
                    )

                    if cycle_id:
                        result["granted"] += 1

                        # Update subscription: advance next_credit_date
                        next_credit = SubscriptionService.calculate_next_credit_date(period_start, billing_day)

                        with get_conn() as conn:
                            with conn.cursor() as cur:
                                update_sql = f"""
                                    UPDATE {Tables.SUBSCRIPTIONS}
                                    SET next_credit_date = %s,
                                        updated_at = NOW()
                                """
                                params = [next_credit]

                                # Decrement remaining months for yearly plans
                                if plan["cadence"] == "yearly":
                                    update_sql += ", credits_remaining_months = GREATEST(0, credits_remaining_months - 1)"

                                update_sql += " WHERE id::text = %s RETURNING credits_remaining_months"
                                params.append(sub_id)

                                cur.execute(update_sql, params)
                                updated = cur.fetchone()
                            conn.commit()

                        # Send credits delivered email
                        customer_email = sub.get("customer_email")
                        if customer_email:
                            remaining_months = updated.get("credits_remaining_months") if updated else None
                            SubscriptionService._send_credits_delivered_email(
                                subscription_id=sub_id,
                                customer_email=customer_email,
                                plan_code=plan_code,
                                credits_granted=plan["credits_per_month"],
                                is_first_grant=False,
                                next_credit_date=next_credit,
                                remaining_months=remaining_months,
                            )

                        # Log event
                        SubscriptionService._log_event(sub_id, "credits_granted", {
                            "cycle_id": cycle_id,
                            "credits": plan["credits_per_month"],
                            "next_credit_date": next_credit.isoformat(),
                        })

                        result["details"].append({
                            "subscription_id": sub_id,
                            "credits": plan["credits_per_month"],
                            "next_credit_date": next_credit.isoformat(),
                        })

                except Exception as e:
                    print(f"[SUB] Error processing credit allocation for {sub_id}: {e}")
                    result["errors"] += 1

        except Exception as e:
            print(f"[SUB] Error in process_due_credit_allocations: {e}")

        print(
            f"[SUB] Credit allocation run: processed={result['processed']}, "
            f"granted={result['granted']}, errors={result['errors']}"
        )
        return result

    @staticmethod
    def verify_provider_status(
        subscription_id: str,
        force: bool = False,
        stale_hours: int = 24,
    ) -> Dict[str, Any]:
        """
        Verify subscription status with the payment provider (Mollie).

        This is a lightweight check that:
        1. Checks if last_provider_check_at is stale (> stale_hours old)
        2. If stale (or force=True), fetches status from Mollie
        3. Updates last_provider_check_at and last_provider_status
        4. Returns the current provider status

        Args:
            subscription_id: Subscription to check
            force: If True, always check provider (ignore cache)
            stale_hours: Hours before a check is considered stale (default: 24)

        Returns:
            Dict with:
                provider_status: The status from provider
                is_valid: True if subscription is active with provider
                checked: True if we made an API call
                error: Error message if failed
        """
        if not USE_DB:
            return {"provider_status": "unknown", "is_valid": True, "checked": False}

        try:
            # Get current subscription
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, provider, provider_subscription_id,
                               last_provider_check_at, last_provider_status, status
                        FROM {Tables.SUBSCRIPTIONS}
                        WHERE id::text = %s
                        """,
                        (subscription_id,),
                    )
                    sub = cur.fetchone()

            if not sub:
                return {"provider_status": "unknown", "is_valid": False, "error": "Subscription not found"}

            # Check if cache is still valid
            last_check = sub.get("last_provider_check_at")
            if not force and last_check:
                stale_threshold = _now_utc() - timedelta(hours=stale_hours)
                if last_check > stale_threshold:
                    # Use cached status
                    cached_status = sub.get("last_provider_status", "unknown")
                    is_valid = cached_status in ("active", "pending")
                    return {
                        "provider_status": cached_status,
                        "is_valid": is_valid,
                        "checked": False,
                        "cached": True,
                    }

            # Need to check with provider
            provider = sub.get("provider", "mollie")
            provider_sub_id = sub.get("provider_subscription_id")

            if provider != "mollie" or not provider_sub_id:
                # Can't check - trust local status
                return {
                    "provider_status": sub.get("status", "unknown"),
                    "is_valid": sub.get("status") == "active",
                    "checked": False,
                    "reason": "No provider subscription ID",
                }

            # Check with Mollie
            provider_status = SubscriptionService._check_mollie_subscription_status(provider_sub_id)

            # Update subscription with provider status
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET last_provider_check_at = NOW(),
                            last_provider_status = %s,
                            updated_at = NOW()
                        WHERE id::text = %s
                        """,
                        (provider_status, subscription_id),
                    )
                conn.commit()

            # Determine if subscription is valid based on provider status
            # Mollie statuses: pending, active, canceled, suspended, completed
            is_valid = provider_status in ("active", "pending")

            result = {
                "provider_status": provider_status,
                "is_valid": is_valid,
                "checked": True,
            }

            # If provider says cancelled/suspended but we think it's active, flag it
            if not is_valid and sub.get("status") == "active":
                result["status_mismatch"] = True
                print(
                    f"[SUB] Provider status mismatch for {subscription_id}: "
                    f"local=active, provider={provider_status}"
                )

            return result

        except Exception as e:
            print(f"[SUB] Error verifying provider status for {subscription_id}: {e}")
            return {"provider_status": "error", "is_valid": True, "checked": False, "error": str(e)}

    @staticmethod
    def _check_mollie_subscription_status(provider_subscription_id: str) -> str:
        """
        Check subscription status with Mollie API.

        Returns the Mollie status: pending, active, canceled, suspended, completed
        """
        try:
            import os
            from mollie.api.client import Client as MollieClient

            api_key = os.environ.get("MOLLIE_API_KEY")
            if not api_key:
                print("[SUB] MOLLIE_API_KEY not set, cannot verify provider status")
                return "unknown"

            mollie = MollieClient()
            mollie.set_api_key(api_key)

            # Mollie subscription IDs start with 'sub_'
            if provider_subscription_id.startswith("sub_"):
                # This is a recurring subscription
                # We need the customer ID to fetch it, so we use payments instead
                # Try to get status from most recent payment
                try:
                    subscription = mollie.subscriptions.get(provider_subscription_id)
                    return subscription.status
                except Exception:
                    # Subscriptions require customer context, try payment lookup
                    pass

            # Try as payment ID
            if provider_subscription_id.startswith("tr_"):
                try:
                    payment = mollie.payments.get(provider_subscription_id)
                    # Map payment status to subscription-like status
                    if payment.is_paid():
                        return "active"
                    elif payment.is_pending():
                        return "pending"
                    elif payment.is_failed() or payment.is_expired():
                        return "canceled"
                    else:
                        return payment.status
                except Exception as e:
                    print(f"[SUB] Error fetching Mollie payment {provider_subscription_id}: {e}")
                    return "unknown"

            return "unknown"

        except ImportError:
            print("[SUB] Mollie client not available")
            return "unknown"
        except Exception as e:
            print(f"[SUB] Error checking Mollie status: {e}")
            return "unknown"

    @staticmethod
    def check_and_expire_past_due_subscriptions() -> Dict[str, Any]:
        """
        Find subscriptions that are past_due for too long and expire them.

        Also finds active subscriptions past their current_period_end and marks them expired.

        Returns summary of expired subscriptions.
        """
        if not USE_DB:
            return {"expired": 0}

        result = {"expired": 0, "past_due_expired": 0, "period_end_expired": 0, "details": []}

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # 1. Expire active subscriptions past their period_end
                    # (These should have been renewed but weren't - likely payment failed silently)
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'expired',
                            expired_at = NOW(),
                            updated_at = NOW()
                        WHERE status = 'active'
                          AND current_period_end IS NOT NULL
                          AND current_period_end < NOW() - INTERVAL '1 day'
                        RETURNING id, customer_email, plan_code
                        """,
                    )
                    period_end_expired = cur.fetchall() or []

                    # 2. Expire past_due subscriptions that have been past_due for > 7 days
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'expired',
                            expired_at = NOW(),
                            updated_at = NOW()
                        WHERE status = 'past_due'
                          AND failed_at IS NOT NULL
                          AND failed_at < NOW() - INTERVAL '7 days'
                        RETURNING id, customer_email, plan_code
                        """,
                    )
                    past_due_expired = cur.fetchall() or []

                conn.commit()

            # Send expiration emails and log events
            for sub in period_end_expired:
                result["expired"] += 1
                result["period_end_expired"] += 1
                sub_id = str(sub["id"])

                SubscriptionService._log_event(sub_id, "expired", {
                    "reason": "period_end_passed",
                })

                if sub.get("customer_email"):
                    SubscriptionService._send_subscription_expired_email(
                        customer_email=sub["customer_email"],
                        plan_code=sub["plan_code"],
                    )

                result["details"].append({
                    "subscription_id": sub_id,
                    "reason": "period_end_passed",
                })

            for sub in past_due_expired:
                result["expired"] += 1
                result["past_due_expired"] += 1
                sub_id = str(sub["id"])

                SubscriptionService._log_event(sub_id, "expired", {
                    "reason": "past_due_timeout",
                })

                if sub.get("customer_email"):
                    SubscriptionService._send_subscription_expired_email(
                        customer_email=sub["customer_email"],
                        plan_code=sub["plan_code"],
                    )

                result["details"].append({
                    "subscription_id": sub_id,
                    "reason": "past_due_timeout",
                })

            if result["expired"] > 0:
                print(
                    f"[SUB] Expired {result['expired']} subscription(s): "
                    f"{result['period_end_expired']} past period_end, "
                    f"{result['past_due_expired']} past_due timeout"
                )

        except Exception as e:
            print(f"[SUB] Error checking expired subscriptions: {e}")
            result["error"] = str(e)

        return result

    @staticmethod
    def send_past_due_reminders() -> Dict[str, Any]:
        """
        Send reminder emails to users with past_due subscriptions.

        Sends reminders at:
        - Day 1: Initial payment failed notification (already sent by mark_past_due)
        - Day 3: First reminder
        - Day 5: Final warning before expiration

        Uses subscription_notifications table to avoid duplicate emails.

        Returns summary of emails sent.
        """
        if not USE_DB:
            return {"sent": 0}

        result = {"sent": 0, "skipped": 0, "details": []}

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Find past_due subscriptions that need reminders
                    cur.execute(
                        f"""
                        SELECT s.id, s.customer_email, s.plan_code, s.failed_at,
                               EXTRACT(DAY FROM (NOW() - s.failed_at)) as days_past_due
                        FROM {Tables.SUBSCRIPTIONS} s
                        WHERE s.status = 'past_due'
                          AND s.failed_at IS NOT NULL
                          AND s.customer_email IS NOT NULL
                          AND EXTRACT(DAY FROM (NOW() - s.failed_at)) IN (3, 5)
                        """,
                    )
                    subs = cur.fetchall() or []

            for sub in subs:
                sub_id = str(sub["id"])
                days = int(sub["days_past_due"])
                notification_type = f"past_due_reminder_{days}d"

                # Check if we already sent this notification today
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT 1 FROM timrx_billing.subscription_notifications
                            WHERE subscription_id = %s
                              AND notification_type = %s
                              AND sent_date = CURRENT_DATE
                            """,
                            (sub_id, notification_type),
                        )
                        already_sent = cur.fetchone()

                if already_sent:
                    result["skipped"] += 1
                    continue

                # Send reminder email
                is_final = days == 5
                email_sent = SubscriptionService._send_past_due_reminder_email(
                    customer_email=sub["customer_email"],
                    plan_code=sub["plan_code"],
                    days_past_due=days,
                    is_final_warning=is_final,
                )

                if email_sent:
                    # Record the notification
                    # The unique constraint on (subscription_id, notification_type, sent_date)
                    # prevents sending the same notification type twice on the same day
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO timrx_billing.subscription_notifications
                                    (subscription_id, notification_type, email_sent_to, details, sent_date)
                                VALUES (%s, %s, %s, %s, CURRENT_DATE)
                                ON CONFLICT (subscription_id, notification_type, sent_date)
                                DO NOTHING
                                """,
                                (
                                    sub_id,
                                    notification_type,
                                    sub["customer_email"],
                                    json.dumps({"days_past_due": days, "is_final": is_final}),
                                ),
                            )
                        conn.commit()

                    result["sent"] += 1
                    result["details"].append({
                        "subscription_id": sub_id,
                        "notification_type": notification_type,
                    })

            if result["sent"] > 0:
                print(f"[SUB] Sent {result['sent']} past_due reminder(s)")

        except Exception as e:
            print(f"[SUB] Error sending past_due reminders: {e}")
            result["error"] = str(e)

        return result

    @staticmethod
    def _send_past_due_reminder_email(
        customer_email: str,
        plan_code: str,
        days_past_due: int,
        is_final_warning: bool,
    ) -> bool:
        """Send reminder email for past_due subscription."""
        try:
            from backend.emailer import send_past_due_reminder_email
            return send_past_due_reminder_email(
                to_email=customer_email,
                plan_code=plan_code,
                days_past_due=days_past_due,
                is_final_warning=is_final_warning,
            )
        except ImportError:
            print(f"[SUB] send_past_due_reminder_email not available")
            return False
        except Exception as e:
            print(f"[SUB] Error sending past_due reminder email: {e}")
            return False

    @staticmethod
    def mark_past_due(subscription_id: str, reason: str = "payment_failed") -> bool:
        """
        Mark a subscription as past_due due to payment failure.

        This pauses credit allocation until payment is resolved.
        """
        if not USE_DB:
            return False

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'past_due',
                            failed_at = NOW(),
                            failure_count = COALESCE(failure_count, 0) + 1,
                            updated_at = NOW()
                        WHERE id::text = %s AND status = 'active'
                        RETURNING id, customer_email, plan_code, failure_count
                        """,
                        (subscription_id,),
                    )
                    sub = cur.fetchone()
                conn.commit()

            if sub:
                # Send payment failed email
                customer_email = sub.get("customer_email")
                if customer_email:
                    SubscriptionService._send_payment_failed_email(
                        customer_email=customer_email,
                        plan_code=sub["plan_code"],
                        failure_count=sub.get("failure_count", 1),
                    )

                # Log event
                SubscriptionService._log_event(subscription_id, "payment_failed", {
                    "reason": reason,
                    "failure_count": sub.get("failure_count", 1),
                })

                print(f"[SUB] Marked subscription {subscription_id} as past_due")
                return True

            return False

        except Exception as e:
            print(f"[SUB] Error marking past_due: {e}")
            return False

    @staticmethod
    def reactivate_subscription(subscription_id: str) -> bool:
        """
        Reactivate a past_due subscription after payment is resolved.

        Resets failure count and resumes credit allocation.
        """
        if not USE_DB:
            return False

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'active',
                            failed_at = NULL,
                            failure_count = 0,
                            updated_at = NOW()
                        WHERE id::text = %s AND status = 'past_due'
                        RETURNING id, customer_email, plan_code
                        """,
                        (subscription_id,),
                    )
                    sub = cur.fetchone()
                conn.commit()

            if sub:
                # Send reactivation email
                customer_email = sub.get("customer_email")
                if customer_email:
                    SubscriptionService._send_subscription_reactivated_email(
                        customer_email=customer_email,
                        plan_code=sub["plan_code"],
                    )

                # Log event
                SubscriptionService._log_event(subscription_id, "payment_resolved", {})

                print(f"[SUB] Reactivated subscription {subscription_id}")
                return True

            return False

        except Exception as e:
            print(f"[SUB] Error reactivating subscription: {e}")
            return False

    @staticmethod
    def process_renewal(subscription_id: str, payment_successful: bool) -> Dict[str, Any]:
        """
        Process a subscription renewal (monthly or yearly).

        For monthly: Called after each month's payment attempt
        For yearly: Called after the annual payment attempt

        Args:
            subscription_id: The subscription UUID
            payment_successful: Whether the renewal payment succeeded

        Returns:
            Dict with status and details
        """
        if not USE_DB:
            return {"ok": False, "error": "DB not available"}

        try:
            # Fetch subscription
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT * FROM {Tables.SUBSCRIPTIONS}
                        WHERE id::text = %s
                        """,
                        (subscription_id,),
                    )
                    sub = cur.fetchone()

            if not sub:
                return {"ok": False, "error": "Subscription not found"}

            plan_code = sub["plan_code"]
            plan = SUBSCRIPTION_PLANS.get(plan_code)

            if not plan:
                return {"ok": False, "error": "Unknown plan"}

            if not payment_successful:
                # Mark as past_due
                SubscriptionService.mark_past_due(subscription_id, "renewal_failed")
                return {"ok": False, "status": "past_due", "message": "Payment failed"}

            # Payment successful - process renewal
            now = _now_utc()
            cadence = plan["cadence"]

            with get_conn() as conn:
                with conn.cursor() as cur:
                    if cadence == "yearly":
                        # Yearly renewal: reset for another 12 months
                        new_period_end = now + timedelta(days=365)
                        cur.execute(
                            f"""
                            UPDATE {Tables.SUBSCRIPTIONS}
                            SET current_period_start = %s,
                                current_period_end = %s,
                                credits_remaining_months = 12,
                                renewal_date = %s,
                                failed_at = NULL,
                                failure_count = 0,
                                updated_at = NOW()
                            WHERE id::text = %s
                            RETURNING *
                            """,
                            (now, new_period_end, new_period_end, subscription_id),
                        )
                    else:
                        # Monthly renewal: extend period by 30 days
                        new_period_end = now + timedelta(days=30)
                        cur.execute(
                            f"""
                            UPDATE {Tables.SUBSCRIPTIONS}
                            SET current_period_start = %s,
                                current_period_end = %s,
                                failed_at = NULL,
                                failure_count = 0,
                                updated_at = NOW()
                            WHERE id::text = %s
                            RETURNING *
                            """,
                            (now, new_period_end, subscription_id),
                        )

                    updated_sub = cur.fetchone()
                conn.commit()

            if updated_sub:
                # Log renewal event
                SubscriptionService._log_event(subscription_id, "renewed", {
                    "cadence": cadence,
                    "new_period_end": updated_sub["current_period_end"].isoformat(),
                })

                # Send renewal confirmation email
                customer_email = updated_sub.get("customer_email")
                if customer_email:
                    SubscriptionService._send_subscription_renewed_email(
                        customer_email=customer_email,
                        plan_code=plan_code,
                        next_billing_date=updated_sub["current_period_end"],
                    )

                return {
                    "ok": True,
                    "status": "renewed",
                    "new_period_end": updated_sub["current_period_end"].isoformat(),
                }

            return {"ok": False, "error": "Update failed"}

        except Exception as e:
            print(f"[SUB] Error processing renewal: {e}")
            return {"ok": False, "error": str(e)}

    @staticmethod
    def cancel_subscription_with_email(subscription_id: str) -> bool:
        """
        Cancel a subscription and send confirmation email.

        The user can continue using remaining credits until period end.
        """
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
                        WHERE id::text = %s AND status IN ('active', 'past_due')
                        RETURNING id, identity_id, customer_email, plan_code, current_period_end
                        """,
                        (subscription_id,),
                    )
                    sub = cur.fetchone()
                conn.commit()

            if sub:
                # Send cancellation email to customer
                customer_email = sub.get("customer_email")
                plan_code = sub["plan_code"]
                if customer_email:
                    SubscriptionService._send_subscription_cancelled_email(
                        customer_email=customer_email,
                        plan_code=plan_code,
                        access_until=sub.get("current_period_end"),
                    )

                # Notify admin about cancellation
                try:
                    from backend.emailer import notify_admin

                    # Parse plan details for admin notification
                    plan_name = plan_code.replace("_monthly", "").replace("_yearly", "").title()
                    cadence = "yearly" if "_yearly" in plan_code else "monthly"
                    access_until = sub.get("current_period_end")

                    notify_admin(
                        subject="Subscription Cancelled",
                        message=f"A user has cancelled their {plan_name} ({cadence}) subscription.",
                        data={
                            "Identity ID": str(sub.get("identity_id", "N/A")),
                            "Email": customer_email or "N/A",
                            "Plan": plan_name,
                            "Cadence": cadence,
                            "Access Until": access_until.strftime("%Y-%m-%d %H:%M UTC") if access_until else "N/A",
                        },
                    )
                    print("[SUB] Admin notified about cancellation")
                except Exception as admin_err:
                    print(f"[SUB] WARNING: Admin notification failed: {admin_err}")

                # Log event
                SubscriptionService._log_event(subscription_id, "cancelled", {
                    "access_until": sub["current_period_end"].isoformat() if sub.get("current_period_end") else None,
                })

                print(f"[SUB] Cancelled subscription {subscription_id}")
                return True

            return False

        except Exception as e:
            print(f"[SUB] Error cancelling subscription: {e}")
            return False

    @staticmethod
    def check_expired_subscriptions() -> int:
        """
        Find and expire subscriptions past their period_end.

        Returns number of subscriptions expired.
        """
        if not USE_DB:
            return 0

        expired = 0
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Find cancelled subscriptions past their period
                    cur.execute(
                        f"""
                        UPDATE {Tables.SUBSCRIPTIONS}
                        SET status = 'expired',
                            updated_at = NOW()
                        WHERE status = 'cancelled'
                          AND current_period_end IS NOT NULL
                          AND current_period_end < NOW()
                        RETURNING id, customer_email, plan_code
                        """,
                    )
                    rows = cur.fetchall() or []
                conn.commit()

            for sub in rows:
                expired += 1
                # Log event
                SubscriptionService._log_event(str(sub["id"]), "expired", {})

                # Optionally send expiration email
                customer_email = sub.get("customer_email")
                if customer_email:
                    SubscriptionService._send_subscription_expired_email(
                        customer_email=customer_email,
                        plan_code=sub["plan_code"],
                    )

        except Exception as e:
            print(f"[SUB] Error checking expired subscriptions: {e}")

        if expired > 0:
            print(f"[SUB] Expired {expired} subscription(s)")

        return expired

    # ══════════════════════════════════════════════════════════════
    # INTERNAL HELPERS
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def _log_event(subscription_id: str, event_type: str, event_data: Dict[str, Any]) -> None:
        """Log a subscription event to the events table."""
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO timrx_billing.subscription_events
                            (subscription_id, event_type, event_data)
                        VALUES (%s, %s, %s)
                        """,
                        (subscription_id, event_type, json.dumps(event_data)),
                    )
                conn.commit()
        except Exception as e:
            print(f"[SUB] Error logging event: {e}")

    @staticmethod
    def _send_credits_delivered_email(
        subscription_id: str,
        customer_email: str,
        plan_code: str,
        credits_granted: int,
        is_first_grant: bool,
        next_credit_date: datetime,
        remaining_months: Optional[int] = None,
    ) -> bool:
        """Send email notification when credits are delivered."""
        try:
            from backend.emailer import send_credits_delivered_email
            return send_credits_delivered_email(
                to_email=customer_email,
                plan_code=plan_code,
                credits_granted=credits_granted,
                is_first_grant=is_first_grant,
                next_credit_date=next_credit_date,
                remaining_months=remaining_months,
            )
        except ImportError:
            print(f"[SUB] send_credits_delivered_email not available")
            return False
        except Exception as e:
            print(f"[SUB] Error sending credits delivered email: {e}")
            return False

    @staticmethod
    def _send_payment_failed_email(
        customer_email: str,
        plan_code: str,
        failure_count: int,
    ) -> bool:
        """Send email notification when payment fails."""
        try:
            from backend.emailer import send_payment_failed_email
            return send_payment_failed_email(
                to_email=customer_email,
                plan_code=plan_code,
                failure_count=failure_count,
            )
        except ImportError:
            print(f"[SUB] send_payment_failed_email not available")
            return False
        except Exception as e:
            print(f"[SUB] Error sending payment failed email: {e}")
            return False

    @staticmethod
    def _send_subscription_reactivated_email(
        customer_email: str,
        plan_code: str,
    ) -> bool:
        """Send email when subscription is reactivated after payment resolved."""
        try:
            from backend.emailer import send_subscription_reactivated_email
            return send_subscription_reactivated_email(
                to_email=customer_email,
                plan_code=plan_code,
            )
        except ImportError:
            print(f"[SUB] send_subscription_reactivated_email not available")
            return False
        except Exception as e:
            print(f"[SUB] Error sending reactivation email: {e}")
            return False

    @staticmethod
    def _send_subscription_renewed_email(
        customer_email: str,
        plan_code: str,
        next_billing_date: datetime,
    ) -> bool:
        """Send email when subscription is renewed."""
        try:
            from backend.emailer import send_subscription_renewed_email
            return send_subscription_renewed_email(
                to_email=customer_email,
                plan_code=plan_code,
                next_billing_date=next_billing_date,
            )
        except ImportError:
            print(f"[SUB] send_subscription_renewed_email not available")
            return False
        except Exception as e:
            print(f"[SUB] Error sending renewal email: {e}")
            return False

    @staticmethod
    def _send_subscription_cancelled_email(
        customer_email: str,
        plan_code: str,
        access_until: Optional[datetime],
    ) -> bool:
        """Send email when subscription is cancelled."""
        try:
            from backend.emailer import send_subscription_cancelled_email
            return send_subscription_cancelled_email(
                to_email=customer_email,
                plan_code=plan_code,
                access_until=access_until,
            )
        except ImportError:
            print(f"[SUB] send_subscription_cancelled_email not available")
            return False
        except Exception as e:
            print(f"[SUB] Error sending cancellation email: {e}")
            return False

    @staticmethod
    def _send_subscription_expired_email(
        customer_email: str,
        plan_code: str,
    ) -> bool:
        """Send email when subscription expires."""
        try:
            from backend.emailer import send_subscription_expired_email
            return send_subscription_expired_email(
                to_email=customer_email,
                plan_code=plan_code,
            )
        except ImportError:
            print(f"[SUB] send_subscription_expired_email not available")
            return False
        except Exception as e:
            print(f"[SUB] Error sending expiration email: {e}")
            return False

    # ══════════════════════════════════════════════════════════════
    # SUBSCRIPTION STATS & QUERIES
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def get_subscription_history(identity_id: str) -> List[Dict[str, Any]]:
        """Get all subscriptions for a user (current and past)."""
        if not USE_DB:
            return []
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT s.*,
                               (SELECT COUNT(*) FROM {Tables.SUBSCRIPTION_CYCLES} c
                                WHERE c.subscription_id = s.id) as total_credits_granted,
                               (SELECT SUM(credits_granted) FROM {Tables.SUBSCRIPTION_CYCLES} c
                                WHERE c.subscription_id = s.id) as total_credits_amount
                        FROM {Tables.SUBSCRIPTIONS} s
                        WHERE s.identity_id = %s
                        ORDER BY s.created_at DESC
                        """,
                        (identity_id,),
                    )
                    return cur.fetchall() or []
        except Exception as e:
            print(f"[SUB] Error fetching subscription history: {e}")
            return []

    @staticmethod
    def get_subscription_stats() -> Dict[str, Any]:
        """Get overall subscription statistics (for admin dashboard)."""
        if not USE_DB:
            return {}
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            COUNT(*) FILTER (WHERE status = 'active') as active_count,
                            COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled_count,
                            COUNT(*) FILTER (WHERE status = 'past_due') as past_due_count,
                            COUNT(*) FILTER (WHERE status = 'expired') as expired_count,
                            COUNT(*) FILTER (WHERE plan_code LIKE '%_monthly') as monthly_count,
                            COUNT(*) FILTER (WHERE plan_code LIKE '%_yearly') as yearly_count,
                            COUNT(*) as total_count
                        FROM {Tables.SUBSCRIPTIONS}
                        """,
                    )
                    return cur.fetchone() or {}
        except Exception as e:
            print(f"[SUB] Error fetching stats: {e}")
            return {}
