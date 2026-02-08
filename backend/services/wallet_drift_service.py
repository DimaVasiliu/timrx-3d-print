"""
Wallet Drift Service - Detects and repairs wallet balance drift.

The wallet.balance_credits is a cached value that must always equal
SUM(ledger_entries.change_credits). This service ensures consistency
and logs all repairs for audit purposes.

Key principles:
- The ledger is IMMUTABLE and is the source of truth
- The wallet balance is a cached value that can be repaired
- All repairs are logged to wallet_repairs table
- Operations are idempotent and safe to run multiple times

Usage:
    from backend.services.wallet_drift_service import WalletDriftService

    # Find all wallets with drift
    drifts = WalletDriftService.find_drifts(limit=100)

    # Repair a specific wallet
    result = WalletDriftService.repair_wallet(identity_id, reason="manual_repair")

    # Run daily audit (for cron)
    summary = WalletDriftService.run_daily_wallet_audit()
"""

from typing import Optional, Dict, Any, List
from datetime import datetime

from backend.db import (
    transaction,
    query_one,
    query_all,
    fetch_one,
    fetch_all,
    Tables,
    USE_DB,
)


# Schema constant
_BILLING_SCHEMA = "timrx_billing"


class WalletDriftService:
    """
    Service for detecting and repairing wallet balance drift.

    The wallet balance is a cached value for performance. This service
    ensures it stays in sync with the ledger (source of truth).
    """

    # ─────────────────────────────────────────────────────────────
    # Read Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def compute_ledger_sum(identity_id: str) -> Dict[str, Any]:
        """
        Compute the ledger sum for an identity.

        This calls the SQL function timrx_billing.compute_ledger_sum()
        which returns the sum of all ledger entries (source of truth).

        Returns:
            Dict with ledger_sum and entry_count
        """
        row = query_one(
            f"""
            SELECT ledger_sum, entry_count
            FROM {_BILLING_SCHEMA}.compute_ledger_sum(%s)
            """,
            (identity_id,),
        )
        if row:
            return {
                "identity_id": identity_id,
                "ledger_sum": int(row.get("ledger_sum", 0) or 0),
                "entry_count": int(row.get("entry_count", 0) or 0),
            }
        return {
            "identity_id": identity_id,
            "ledger_sum": 0,
            "entry_count": 0,
        }

    @staticmethod
    def get_wallet_comparison(identity_id: str) -> Optional[Dict[str, Any]]:
        """
        Get wallet vs ledger comparison for a specific identity.

        Uses the v_wallet_ledger_comparison view.

        Returns:
            Dict with cached_balance, ledger_sum, drift, has_drift, etc.
            Returns None if identity has no wallet.
        """
        return query_one(
            f"""
            SELECT
                wallet_id,
                identity_id,
                cached_balance,
                ledger_sum,
                entry_count,
                drift,
                has_drift,
                wallet_updated_at,
                last_entry_at
            FROM {_BILLING_SCHEMA}.v_wallet_ledger_comparison
            WHERE identity_id = %s
            """,
            (identity_id,),
        )

    @staticmethod
    def find_drifts(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Find all wallets with balance drift.

        Uses the v_wallet_ledger_comparison view to find wallets where
        the cached balance doesn't match the ledger sum.

        Args:
            limit: Maximum number of drifts to return
            offset: Number of drifts to skip (for pagination)

        Returns:
            List of dicts with drift information
        """
        return query_all(
            f"""
            SELECT
                wallet_id,
                identity_id,
                cached_balance,
                ledger_sum,
                entry_count,
                drift,
                wallet_updated_at,
                last_entry_at
            FROM {_BILLING_SCHEMA}.v_wallet_ledger_comparison
            WHERE has_drift = true
            ORDER BY ABS(drift) DESC, wallet_updated_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )

    @staticmethod
    def count_drifts() -> int:
        """
        Count total number of wallets with drift.

        Returns:
            Number of wallets where balance != ledger_sum
        """
        row = query_one(
            f"""
            SELECT COUNT(*) as count
            FROM {_BILLING_SCHEMA}.v_wallet_ledger_comparison
            WHERE has_drift = true
            """
        )
        return int(row.get("count", 0) or 0) if row else 0

    # ─────────────────────────────────────────────────────────────
    # Repair Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def repair_wallet(
        identity_id: str,
        reason: str = "manual_repair",
        trigger_source: str = "api",
    ) -> Dict[str, Any]:
        """
        Repair a wallet balance to match the ledger sum.

        This is the core repair operation. It:
        1. Computes the ledger sum (source of truth)
        2. Compares to current wallet balance
        3. If different, updates wallet and logs repair
        4. If same, does nothing (idempotent)

        The repair is atomic and logged to wallet_repairs table.

        Args:
            identity_id: Identity whose wallet to repair
            reason: Reason for repair (daily_audit, manual_repair, reconciliation, etc.)
            trigger_source: What triggered the repair (cron, api, admin_endpoint, etc.)

        Returns:
            Dict with:
                repaired: bool - True if repair was applied
                old_balance: int - Balance before repair
                new_balance: int - Balance after repair (ledger sum)
                drift_amount: int - Difference (new - old)
                repair_id: UUID or None - ID of repair record if applied
        """
        row = query_one(
            f"""
            SELECT repaired, old_balance, new_balance, drift_amount, repair_id
            FROM {_BILLING_SCHEMA}.repair_wallet_balance(%s, %s, %s)
            """,
            (identity_id, reason, trigger_source),
        )

        if row:
            repaired = row.get("repaired", False)
            result = {
                "identity_id": identity_id,
                "repaired": repaired,
                "old_balance": row.get("old_balance", 0),
                "new_balance": row.get("new_balance", 0),
                "drift_amount": row.get("drift_amount", 0),
                "repair_id": str(row["repair_id"]) if row.get("repair_id") else None,
            }

            if repaired:
                print(
                    f"[WALLET_DRIFT] Repaired wallet for identity={identity_id}: "
                    f"{result['old_balance']} -> {result['new_balance']} "
                    f"(drift={result['drift_amount']:+d})"
                )
            else:
                print(
                    f"[WALLET_DRIFT] No drift for identity={identity_id}: "
                    f"balance={result['old_balance']}"
                )

            return result

        # No wallet found
        return {
            "identity_id": identity_id,
            "repaired": False,
            "old_balance": 0,
            "new_balance": 0,
            "drift_amount": 0,
            "repair_id": None,
            "error": "Wallet not found",
        }

    @staticmethod
    def repair_all_drifts(
        reason: str = "bulk_repair",
        trigger_source: str = "cron",
        limit: int = 1000,
    ) -> Dict[str, Any]:
        """
        Repair all wallets with drift.

        Iterates through all drifts and repairs them one by one.
        Each repair is atomic and logged separately.

        Args:
            reason: Reason for repairs
            trigger_source: What triggered the repairs
            limit: Maximum number of repairs to attempt

        Returns:
            Summary dict with:
                total_drifts: Number of drifts found
                repaired_count: Number of wallets repaired
                failed_count: Number of repair failures
                total_drift_corrected: Sum of all drift amounts corrected
                repairs: List of individual repair results
        """
        drifts = WalletDriftService.find_drifts(limit=limit)

        results = {
            "total_drifts": len(drifts),
            "repaired_count": 0,
            "failed_count": 0,
            "total_drift_corrected": 0,
            "repairs": [],
        }

        for drift in drifts:
            identity_id = str(drift["identity_id"])
            try:
                repair_result = WalletDriftService.repair_wallet(
                    identity_id=identity_id,
                    reason=reason,
                    trigger_source=trigger_source,
                )

                if repair_result.get("repaired"):
                    results["repaired_count"] += 1
                    results["total_drift_corrected"] += abs(
                        repair_result.get("drift_amount", 0)
                    )
                    results["repairs"].append(repair_result)

            except Exception as e:
                results["failed_count"] += 1
                results["repairs"].append({
                    "identity_id": identity_id,
                    "repaired": False,
                    "error": str(e),
                })
                print(f"[WALLET_DRIFT] Error repairing identity={identity_id}: {e}")

        return results

    # ─────────────────────────────────────────────────────────────
    # Audit Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def run_daily_wallet_audit(dry_run: bool = False) -> Dict[str, Any]:
        """
        Run the daily wallet audit.

        This is the main entry point for the daily cron job.
        It finds all drifts and optionally repairs them.

        Args:
            dry_run: If True, only detect drifts without repairing

        Returns:
            Audit summary with:
                started_at: Timestamp
                completed_at: Timestamp
                dry_run: bool
                total_wallets: Number of wallets checked
                drifts_found: Number of wallets with drift
                repairs_applied: Number of repairs (0 if dry_run)
                total_drift_amount: Sum of absolute drift values
                drifts: List of drift details (if dry_run) or repair results
        """
        started_at = datetime.utcnow()
        print(f"[WALLET_DRIFT] Starting daily audit (dry_run={dry_run})...")

        # Count total wallets
        wallet_count_row = query_one(
            f"SELECT COUNT(*) as count FROM {Tables.WALLETS}"
        )
        total_wallets = int(wallet_count_row.get("count", 0) or 0) if wallet_count_row else 0

        # Find all drifts
        drifts = WalletDriftService.find_drifts(limit=10000)
        drifts_found = len(drifts)

        # Calculate total drift
        total_drift_amount = sum(abs(d.get("drift", 0)) for d in drifts)

        result = {
            "started_at": started_at.isoformat(),
            "dry_run": dry_run,
            "total_wallets": total_wallets,
            "drifts_found": drifts_found,
            "total_drift_amount": total_drift_amount,
        }

        if dry_run:
            # Just report, don't repair
            result["repairs_applied"] = 0
            result["drifts"] = [
                {
                    "identity_id": str(d["identity_id"]),
                    "cached_balance": d.get("cached_balance", 0),
                    "ledger_sum": d.get("ledger_sum", 0),
                    "drift": d.get("drift", 0),
                }
                for d in drifts
            ]
            print(
                f"[WALLET_DRIFT] Dry run complete: "
                f"{drifts_found} drifts found, total={total_drift_amount}"
            )
        else:
            # Actually repair
            repair_results = WalletDriftService.repair_all_drifts(
                reason="daily_audit",
                trigger_source="cron",
                limit=10000,
            )
            result["repairs_applied"] = repair_results["repaired_count"]
            result["repair_failures"] = repair_results["failed_count"]
            result["repairs"] = repair_results["repairs"]

            print(
                f"[WALLET_DRIFT] Audit complete: "
                f"{repair_results['repaired_count']} repaired, "
                f"{repair_results['failed_count']} failed"
            )

        result["completed_at"] = datetime.utcnow().isoformat()
        return result

    # ─────────────────────────────────────────────────────────────
    # History / Reporting
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_recent_repairs(
        identity_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Get recent wallet repairs from the audit log.

        Args:
            identity_id: Filter by identity (optional)
            limit: Maximum number of repairs to return
            offset: Number to skip (for pagination)

        Returns:
            List of repair records
        """
        if identity_id:
            return query_all(
                f"""
                SELECT
                    id, identity_id, wallet_id,
                    old_balance, new_balance, drift_amount,
                    reason, trigger_source,
                    ledger_entry_count, details_json,
                    created_at
                FROM {_BILLING_SCHEMA}.wallet_repairs
                WHERE identity_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (identity_id, limit, offset),
            )
        else:
            return query_all(
                f"""
                SELECT
                    id, identity_id, wallet_id,
                    old_balance, new_balance, drift_amount,
                    reason, trigger_source,
                    ledger_entry_count, details_json,
                    created_at
                FROM {_BILLING_SCHEMA}.wallet_repairs
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )

    @staticmethod
    def get_repair_stats(days: int = 30) -> Dict[str, Any]:
        """
        Get repair statistics for the specified period.

        Args:
            days: Number of days to look back

        Returns:
            Dict with:
                total_repairs: Number of repairs in period
                total_drift_corrected: Sum of absolute drift amounts
                by_reason: Breakdown by repair reason
                by_trigger: Breakdown by trigger source
        """
        # Total repairs and drift
        summary = query_one(
            f"""
            SELECT
                COUNT(*) as total_repairs,
                COALESCE(SUM(ABS(drift_amount)), 0) as total_drift_corrected
            FROM {_BILLING_SCHEMA}.wallet_repairs
            WHERE created_at >= NOW() - INTERVAL '%s days'
            """,
            (days,),
        )

        # By reason
        by_reason = query_all(
            f"""
            SELECT
                reason,
                COUNT(*) as count,
                COALESCE(SUM(ABS(drift_amount)), 0) as drift_total
            FROM {_BILLING_SCHEMA}.wallet_repairs
            WHERE created_at >= NOW() - INTERVAL '%s days'
            GROUP BY reason
            ORDER BY count DESC
            """,
            (days,),
        )

        # By trigger source
        by_trigger = query_all(
            f"""
            SELECT
                trigger_source,
                COUNT(*) as count,
                COALESCE(SUM(ABS(drift_amount)), 0) as drift_total
            FROM {_BILLING_SCHEMA}.wallet_repairs
            WHERE created_at >= NOW() - INTERVAL '%s days'
            GROUP BY trigger_source
            ORDER BY count DESC
            """,
            (days,),
        )

        return {
            "days": days,
            "total_repairs": int(summary.get("total_repairs", 0) or 0) if summary else 0,
            "total_drift_corrected": int(summary.get("total_drift_corrected", 0) or 0) if summary else 0,
            "by_reason": [
                {
                    "reason": r["reason"],
                    "count": r["count"],
                    "drift_total": r["drift_total"],
                }
                for r in by_reason
            ],
            "by_trigger": [
                {
                    "trigger_source": t["trigger_source"],
                    "count": t["count"],
                    "drift_total": t["drift_total"],
                }
                for t in by_trigger
            ],
        }
