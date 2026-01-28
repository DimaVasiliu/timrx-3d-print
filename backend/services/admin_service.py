"""
Admin service for TimrX Backend.

Provides admin-only operations:
- System statistics
- Identity listing and management
- Credit adjustments
- Reservation management

Usage:
    from backend.services.admin_service import AdminService

    stats = AdminService.get_stats()
    identities = AdminService.list_identities(limit=50, offset=0)
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from backend.db import query_one, query_all, execute_returning, transaction, Tables
from backend.services.wallet_service import WalletService


class AdminService:
    """Admin operations service."""

    # ─────────────────────────────────────────────────────────────
    # Statistics
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_stats() -> Dict[str, Any]:
        """
        Get system-wide statistics.
        Returns counts of identities, purchases, credits, jobs.
        """
        # Total identities
        total_identities = query_one(
            f"SELECT COUNT(*) as count FROM {Tables.IDENTITIES}"
        )

        # Identities with email
        identities_with_email = query_one(
            f"SELECT COUNT(*) as count FROM {Tables.IDENTITIES} WHERE email IS NOT NULL"
        )

        # Total purchases completed
        total_purchases = query_one(
            f"SELECT COUNT(*) as count FROM {Tables.PURCHASES} WHERE status = 'completed'"
        )

        # Total credits purchased (sum of completed purchases)
        credits_purchased = query_one(
            f"SELECT COALESCE(SUM(credits_granted), 0) as total FROM {Tables.PURCHASES} WHERE status = 'completed'"
        )

        # Total credits spent (from reservation finalizations)
        credits_spent = query_one(
            f"""
            SELECT COALESCE(SUM(ABS(amount_credits)), 0) as total
            FROM {Tables.LEDGER_ENTRIES}
            WHERE entry_type = 'reservation_finalize'
            """
        )

        # Active reservations (held)
        active_reservations = query_one(
            f"SELECT COUNT(*) as count FROM {Tables.CREDIT_RESERVATIONS} WHERE status = 'held'"
        )

        # Jobs by status
        jobs_stats = query_all(
            f"""
            SELECT status, COUNT(*) as count
            FROM {Tables.JOBS}
            GROUP BY status
            """
        )
        jobs_by_status = {row["status"]: row["count"] for row in jobs_stats}

        # Revenue (sum of completed purchase amounts)
        revenue = query_one(
            f"SELECT COALESCE(SUM(amount_gbp), 0) as total FROM {Tables.PURCHASES} WHERE status = 'completed'"
        )

        # Calculate job success rate
        total_jobs = sum(jobs_by_status.values())
        completed_jobs = jobs_by_status.get("completed", 0)
        failed_jobs = jobs_by_status.get("failed", 0)
        finished_jobs = completed_jobs + failed_jobs

        job_success_rate = 0.0
        if finished_jobs > 0:
            job_success_rate = round((completed_jobs / finished_jobs) * 100, 2)

        return {
            "total_identities": total_identities["count"] if total_identities else 0,
            "identities_with_email": identities_with_email["count"] if identities_with_email else 0,
            "total_purchases": total_purchases["count"] if total_purchases else 0,
            "total_credits_purchased": int(credits_purchased["total"]) if credits_purchased else 0,
            "total_credits_spent": int(credits_spent["total"]) if credits_spent else 0,
            "active_reservations": active_reservations["count"] if active_reservations else 0,
            "jobs_by_status": jobs_by_status,
            "total_jobs": total_jobs,
            "job_success_rate": job_success_rate,
            "total_revenue_gbp": float(revenue["total"]) if revenue else 0.0,
        }

    # ─────────────────────────────────────────────────────────────
    # Identity Management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def list_identities(
        limit: int = 50,
        offset: int = 0,
        email_filter: Optional[str] = None,
        has_email: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        List identities with pagination.

        Args:
            limit: Max results (default 50, max 100)
            offset: Pagination offset
            email_filter: Filter by email contains (case-insensitive)
            has_email: Filter to only identities with/without email
        """
        limit = min(max(1, limit), 100)  # Clamp to 1-100
        offset = max(0, offset)

        # Build WHERE clauses
        conditions = []
        params = []

        if email_filter:
            conditions.append("LOWER(i.email) LIKE LOWER(%s)")
            params.append(f"%{email_filter}%")

        if has_email is True:
            conditions.append("i.email IS NOT NULL")
        elif has_email is False:
            conditions.append("i.email IS NULL")

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Get total count
        count_sql = f"""
            SELECT COUNT(*) as count
            FROM {Tables.IDENTITIES} i
            {where_clause}
        """
        total = query_one(count_sql, params)

        # Get paginated results with wallet balance
        list_sql = f"""
            SELECT
                i.id,
                i.email,
                i.email_verified,
                i.created_at,
                i.last_seen_at,
                COALESCE(w.balance_credits, 0) as balance,
                (
                    SELECT COALESCE(SUM(cost_credits), 0)
                    FROM {Tables.CREDIT_RESERVATIONS}
                    WHERE identity_id = i.id AND status = 'held'
                ) as reserved
            FROM {Tables.IDENTITIES} i
            LEFT JOIN {Tables.WALLETS} w ON w.identity_id = i.id
            {where_clause}
            ORDER BY i.created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        rows = query_all(list_sql, params)

        identities = []
        for row in rows:
            identities.append({
                "id": str(row["id"]),
                "email": row["email"],
                "email_verified": row["email_verified"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "last_seen_at": row["last_seen_at"].isoformat() if row["last_seen_at"] else None,
                "wallet": {
                    "balance": row["balance"],
                    "reserved": row["reserved"],
                    "available": max(0, row["balance"] - row["reserved"]),
                },
            })

        return {
            "identities": identities,
            "total": total["count"] if total else 0,
            "limit": limit,
            "offset": offset,
        }

    @staticmethod
    def get_identity_detail(identity_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a single identity.
        Includes wallet, recent purchases, recent jobs.
        """
        # Get identity with wallet
        identity = query_one(
            f"""
            SELECT
                i.id,
                i.email,
                i.email_verified,
                i.created_at,
                i.last_seen_at,
                COALESCE(w.balance_credits, 0) as balance
            FROM {Tables.IDENTITIES} i
            LEFT JOIN {Tables.WALLETS} w ON w.identity_id = i.id
            WHERE i.id = %s
            """,
            [identity_id]
        )

        if not identity:
            return None

        # Get reserved credits
        reserved = query_one(
            f"""
            SELECT COALESCE(SUM(cost_credits), 0) as total
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE identity_id = %s AND status = 'held'
            """,
            [identity_id]
        )

        # Get recent purchases (last 10)
        purchases = query_all(
            f"""
            SELECT id, plan_id, amount_gbp, credits_granted, status, created_at, paid_at
            FROM {Tables.PURCHASES}
            WHERE identity_id = %s
            ORDER BY created_at DESC
            LIMIT 10
            """,
            [identity_id]
        )

        # Get recent jobs (last 20)
        jobs = query_all(
            f"""
            SELECT id, provider, action_code, status, cost_credits, created_at, updated_at
            FROM {Tables.JOBS}
            WHERE identity_id = %s
            ORDER BY created_at DESC
            LIMIT 20
            """,
            [identity_id]
        )

        # Get active sessions
        sessions = query_all(
            f"""
            SELECT id, created_at, expires_at
            FROM {Tables.SESSIONS}
            WHERE identity_id = %s AND revoked_at IS NULL AND expires_at > NOW()
            ORDER BY created_at DESC
            """,
            [identity_id]
        )

        reserved_amount = int(reserved["total"]) if reserved else 0

        return {
            "id": str(identity["id"]),
            "email": identity["email"],
            "email_verified": identity["email_verified"],
            "created_at": identity["created_at"].isoformat() if identity["created_at"] else None,
            "last_seen_at": identity["last_seen_at"].isoformat() if identity["last_seen_at"] else None,
            "wallet": {
                "balance": identity["balance"],
                "reserved": reserved_amount,
                "available": max(0, identity["balance"] - reserved_amount),
            },
            "purchases": [
                {
                    "id": str(p["id"]),
                    "amount_gbp": float(p["amount_gbp"]) if p["amount_gbp"] else 0,
                    "credits_granted": p["credits_granted"],
                    "status": p["status"],
                    "created_at": p["created_at"].isoformat() if p["created_at"] else None,
                }
                for p in purchases
            ],
            "jobs": [
                {
                    "id": str(j["id"]),
                    "provider": j["provider"],
                    "action_code": j["action_code"],
                    "status": j["status"],
                    "cost_credits": j["cost_credits"],
                    "created_at": j["created_at"].isoformat() if j["created_at"] else None,
                }
                for j in jobs
            ],
            "active_sessions": len(sessions),
        }

    # ─────────────────────────────────────────────────────────────
    # Purchase Management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def list_purchases(
        status: Optional[str] = None,
        identity_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        List purchases with filtering.

        Args:
            status: Filter by status ('pending', 'completed', 'failed', 'refunded')
            identity_id: Filter by identity
            limit: Max results
            offset: Pagination offset
        """
        limit = min(max(1, limit), 100)
        offset = max(0, offset)

        conditions = []
        params = []

        if status:
            conditions.append("p.status = %s")
            params.append(status)

        if identity_id:
            conditions.append("p.identity_id = %s")
            params.append(identity_id)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Get total
        count_sql = f"""
            SELECT COUNT(*) as count
            FROM {Tables.PURCHASES} p
            {where_clause}
        """
        total = query_one(count_sql, params)

        # Get purchases with identity and plan info
        list_sql = f"""
            SELECT
                p.id,
                p.identity_id,
                p.plan_id,
                p.provider,
                p.provider_payment_id,
                p.amount_gbp,
                p.currency,
                p.credits_granted,
                p.status,
                p.created_at,
                p.paid_at,
                i.email,
                pl.code as plan_code,
                pl.name as plan_name
            FROM {Tables.PURCHASES} p
            LEFT JOIN {Tables.IDENTITIES} i ON i.id = p.identity_id
            LEFT JOIN {Tables.PLANS} pl ON pl.id = p.plan_id
            {where_clause}
            ORDER BY p.created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        rows = query_all(list_sql, params)

        purchases = []
        for row in rows:
            purchases.append({
                "id": str(row["id"]),
                "identity_id": str(row["identity_id"]),
                "identity_email": row["email"],
                "plan_code": row["plan_code"],
                "plan_name": row["plan_name"],
                "provider": row["provider"],
                "provider_payment_id": row["provider_payment_id"],
                "amount_gbp": float(row["amount_gbp"]) if row["amount_gbp"] else 0,
                "currency": row["currency"],
                "credits_granted": row["credits_granted"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "paid_at": row["paid_at"].isoformat() if row["paid_at"] else None,
            })

        return {
            "purchases": purchases,
            "total": total["count"] if total else 0,
            "limit": limit,
            "offset": offset,
        }

    # ─────────────────────────────────────────────────────────────
    # Credit Management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def grant_credits(
        identity_id: str,
        amount: int,
        reason: str,
        admin_email: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Grant credits to an identity (admin adjustment).

        Args:
            identity_id: Target identity UUID
            amount: Number of credits (positive to grant, negative to deduct)
            reason: Reason for adjustment
            admin_email: Email of admin making the adjustment

        Returns:
            New wallet state
        """
        if amount == 0:
            raise ValueError("Amount cannot be zero")

        # Use wallet service to add ledger entry
        meta = {
            "reason": reason,
            "admin_email": admin_email,
            "adjusted_at": datetime.now(timezone.utc).isoformat(),
        }

        wallet = WalletService.add_ledger_entry(
            identity_id=identity_id,
            entry_type="admin_adjust",
            amount=amount,
            ref_type="admin",
            ref_id=None,
            meta=meta
        )

        return {
            "identity_id": identity_id,
            "amount_adjusted": amount,
            "reason": reason,
            "wallet": wallet,
        }

    # ─────────────────────────────────────────────────────────────
    # Reservation Management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def list_reservations(
        status: str = "held",
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        List credit reservations.

        Args:
            status: Filter by status ('held', 'released', or 'all')
            limit: Max results
            offset: Pagination offset
        """
        limit = min(max(1, limit), 100)
        offset = max(0, offset)

        conditions = []
        params = []

        if status != "all":
            conditions.append("r.status = %s")
            params.append(status)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Get total
        count_sql = f"""
            SELECT COUNT(*) as count
            FROM {Tables.CREDIT_RESERVATIONS} r
            {where_clause}
        """
        total = query_one(count_sql, params)

        # Get reservations with identity info
        list_sql = f"""
            SELECT
                r.id,
                r.identity_id,
                r.action_code,
                r.cost_credits,
                r.status,
                r.created_at,
                r.expires_at,
                r.ref_job_id,
                i.email
            FROM {Tables.CREDIT_RESERVATIONS} r
            LEFT JOIN {Tables.IDENTITIES} i ON i.id = r.identity_id
            {where_clause}
            ORDER BY r.created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        rows = query_all(list_sql, params)

        reservations = []
        for row in rows:
            reservations.append({
                "id": str(row["id"]),
                "identity_id": str(row["identity_id"]),
                "identity_email": row["email"],
                "action_code": row["action_code"],
                "cost_credits": row["cost_credits"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "expires_at": row["expires_at"].isoformat() if row["expires_at"] else None,
                "job_id": str(row["ref_job_id"]) if row["ref_job_id"] else None,
            })

        return {
            "reservations": reservations,
            "total": total["count"] if total else 0,
            "limit": limit,
            "offset": offset,
        }

    @staticmethod
    def release_reservation(reservation_id: str, reason: str = "admin_release") -> bool:
        """
        Manually release a held reservation (returns credits to user).
        """
        from backend.services.reservation_service import ReservationService
        return ReservationService.release_reservation(reservation_id)

    # ─────────────────────────────────────────────────────────────
    # Job Management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def list_jobs(
        status: Optional[str] = None,
        identity_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        List jobs with filtering.
        """
        limit = min(max(1, limit), 100)
        offset = max(0, offset)

        conditions = []
        params = []

        if status:
            conditions.append("j.status = %s")
            params.append(status)

        if identity_id:
            conditions.append("j.identity_id = %s")
            params.append(identity_id)

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Get total
        count_sql = f"""
            SELECT COUNT(*) as count
            FROM {Tables.JOBS} j
            {where_clause}
        """
        total = query_one(count_sql, params)

        # Get jobs with identity info
        list_sql = f"""
            SELECT
                j.id,
                j.identity_id,
                j.provider,
                j.action_code,
                j.status,
                j.cost_credits,
                j.upstream_job_id,
                j.error_message,
                j.created_at,
                j.updated_at,
                i.email
            FROM {Tables.JOBS} j
            LEFT JOIN {Tables.IDENTITIES} i ON i.id = j.identity_id
            {where_clause}
            ORDER BY j.created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])
        rows = query_all(list_sql, params)

        jobs = []
        for row in rows:
            jobs.append({
                "id": str(row["id"]),
                "identity_id": str(row["identity_id"]),
                "identity_email": row["email"],
                "provider": row["provider"],
                "action_code": row["action_code"],
                "status": row["status"],
                "cost_credits": row["cost_credits"],
                "upstream_job_id": row["upstream_job_id"],
                "error_message": row["error_message"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            })

        return {
            "jobs": jobs,
            "total": total["count"] if total else 0,
            "limit": limit,
            "offset": offset,
        }
