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
    "starter_yearly": {
        "name": "Starter",
        "credits_per_month": 100,
        "price_gbp": 69.99,
        "cadence": "yearly",
        "tier": "starter",
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
        now = _now_utc()

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    # Find subscriptions with due credit allocations
                    cur.execute(
                        f"""
                        SELECT s.*,
                               (SELECT COUNT(*) FROM {Tables.SUBSCRIPTION_CYCLES} c
                                WHERE c.subscription_id = s.id) as cycles_count
                        FROM {Tables.SUBSCRIPTIONS} s
                        WHERE s.status = 'active'
                          AND s.next_credit_date IS NOT NULL
                          AND s.next_credit_date <= NOW()
                        ORDER BY s.next_credit_date ASC
                        LIMIT 100
                        """,
                    )
                    subs = cur.fetchall() or []

            for sub in subs:
                result["processed"] += 1
                sub_id = str(sub["id"])
                plan_code = sub["plan_code"]
                plan = SUBSCRIPTION_PLANS.get(plan_code)

                if not plan:
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
                        RETURNING id, customer_email, plan_code, current_period_end
                        """,
                        (subscription_id,),
                    )
                    sub = cur.fetchone()
                conn.commit()

            if sub:
                # Send cancellation email
                customer_email = sub.get("customer_email")
                if customer_email:
                    SubscriptionService._send_subscription_cancelled_email(
                        customer_email=customer_email,
                        plan_code=sub["plan_code"],
                        access_until=sub.get("current_period_end"),
                    )

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
