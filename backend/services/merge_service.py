"""
Identity Merge Service — IDENT-1

Production-safe automated identity merge engine.
Consolidates two identities by migrating all owned data from source to target,
reconciling wallets from ledger, handling subscriptions safely, and revoking
stale sessions.

Usage:
    from backend.services.merge_service import MergeService

    result = MergeService.execute_merge(
        source_id="...",
        target_id="...",
        merged_by="system",
        reason="restore",
    )

    if result["success"]:
        print(f"Merged {result['source_id']} → {result['target_id']}")
    elif result.get("blocked_reason"):
        print(f"Blocked: {result['blocked_reason']}")
"""

from __future__ import annotations

import json as _json
from typing import Any, Dict, List, Optional, Tuple

from backend.db import Tables, fetch_all, fetch_one, transaction


# ─────────────────────────────────────────────────────────────
# Tables not in the Tables class but containing identity_id
# ─────────────────────────────────────────────────────────────
_T_ADMIN_LOGS = "timrx_billing.admin_logs"
_T_COMMUNITY_POSTS = "timrx_app.community_posts"


class MergeService:
    """Automated identity merge engine (IDENT-1)."""

    # ── Active subscription statuses that indicate conflict ──
    _ACTIVE_SUB_STATUSES = ("active", "pending_payment", "past_due")

    # ── Tables migrated via simple UPDATE identity_id ──
    # Order matters: move content tables first, then billing, then ledger last.
    _SIMPLE_MIGRATE_TABLES = [
        # App content (ON DELETE SET NULL — safe to move)
        Tables.MODELS,
        Tables.IMAGES,
        Tables.VIDEOS,
        Tables.HISTORY_ITEMS,
        Tables.ACTIVE_JOBS,
        _T_COMMUNITY_POSTS,
        Tables.ACTIVITY_LOGS,
        # Billing ownership
        Tables.PURCHASES,
        Tables.INVOICES,
        Tables.RECEIPTS,
        Tables.CREDIT_RESERVATIONS,
        Tables.EMAIL_OUTBOX,
        Tables.REFUNDS,
        Tables.PAYMENT_DISPUTES,
        _T_ADMIN_LOGS,
    ]

    # ─────────────────────────────────────────────────────────
    #  PUBLIC API
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def execute_merge(
        source_id: str,
        target_id: str,
        merged_by: str = "system",
        reason: str = "restore",
        mode: str = "full",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a full identity merge: source → target.

        All identity-owned data migrates to the canonical target.
        Wallets are reconciled from ledger (source of truth).
        Source sessions are revoked. Source is marked merged_into target.

        Args:
            source_id:  Identity being merged away.
            target_id:  Identity receiving all data.
            merged_by:  Who initiated ("system", "admin", admin email).
            reason:     Why ("restore", "fragmentation", "admin_request").
            mode:       "full" (execute) or "dry_run" (validate only).
            metadata:   Optional JSONB blob for audit record.

        Returns:
            Dict with keys:
                success: bool
                source_id: str (canonical)
                target_id: str (canonical)
                tables_migrated: Dict[str, int]
                wallet_result: Dict
                subscription_result: Dict
                sessions_revoked: int
                blocked_reason: Optional[str]
                warnings: List[str]
        """
        result = {
            "success": False,
            "source_id": source_id,
            "target_id": target_id,
            "tables_migrated": {},
            "wallet_result": {},
            "subscription_result": {},
            "sessions_revoked": 0,
            "blocked_reason": None,
            "warnings": [],
        }

        try:
            with transaction() as cur:
                # ── 1. Validate and resolve canonical IDs ──
                valid, block_reason, canon_source, canon_target = (
                    MergeService._validate_merge(source_id, target_id, cur)
                )
                result["source_id"] = canon_source
                result["target_id"] = canon_target

                if not valid:
                    result["blocked_reason"] = block_reason
                    print(
                        f"[MERGE] Blocked: {canon_source[:8]}... → "
                        f"{canon_target[:8]}...: {block_reason}"
                    )
                    return result

                # ── 2. Check subscription conflicts ──
                sub_conflict, sub_details = MergeService._check_subscription_conflict(
                    canon_source, canon_target, cur
                )
                result["subscription_result"] = sub_details

                if sub_conflict:
                    result["blocked_reason"] = "subscription_conflict"
                    print(
                        f"[MERGE] Blocked by subscription conflict: "
                        f"source={canon_source[:8]}... target={canon_target[:8]}..."
                    )
                    return result

                # ── 3. Dry-run stops here ──
                if mode == "dry_run":
                    result["success"] = True
                    result["blocked_reason"] = None
                    print(
                        f"[MERGE] Dry-run OK: {canon_source[:8]}... → "
                        f"{canon_target[:8]}..."
                    )
                    return result

                # ── 4. Migrate simple tables ──
                for table in MergeService._SIMPLE_MIGRATE_TABLES:
                    count = MergeService._migrate_table(
                        cur, table, canon_source, canon_target
                    )
                    if count > 0:
                        result["tables_migrated"][table] = count

                # ── 5. Migrate jobs (special: idempotency key conflicts) ──
                jobs_count = MergeService._migrate_jobs(
                    cur, canon_source, canon_target, result["warnings"]
                )
                if jobs_count > 0:
                    result["tables_migrated"][Tables.JOBS] = jobs_count

                # ── 6. Migrate subscriptions ──
                sub_count = MergeService._migrate_subscriptions(
                    cur, canon_source, canon_target
                )
                if sub_count > 0:
                    result["tables_migrated"][Tables.SUBSCRIPTIONS] = sub_count
                    result["subscription_result"]["migrated"] = sub_count

                # ── 7. Handle mollie_customers ──
                mollie_count = MergeService._migrate_mollie_customers(
                    cur, canon_source, canon_target, result["warnings"]
                )
                if mollie_count > 0:
                    result["tables_migrated"][Tables.MOLLIE_CUSTOMERS] = mollie_count

                # ── 8. Merge daily_limits (sum on conflict) ──
                dl_count = MergeService._merge_daily_limits(
                    cur, canon_source, canon_target
                )
                if dl_count > 0:
                    result["tables_migrated"][Tables.DAILY_LIMITS] = dl_count

                # ── 9. Migrate ledger entries ──
                ledger_count = MergeService._migrate_table(
                    cur, Tables.LEDGER_ENTRIES, canon_source, canon_target
                )
                if ledger_count > 0:
                    result["tables_migrated"][Tables.LEDGER_ENTRIES] = ledger_count

                # ── 10. Reconcile wallets from ledger ──
                result["wallet_result"] = MergeService._reconcile_wallets(
                    cur, canon_source, canon_target
                )

                # ── 11. Revoke source sessions ──
                result["sessions_revoked"] = MergeService._revoke_source_sessions(
                    cur, canon_source
                )

                # ── 12. Mark source as merged ──
                cur.execute(
                    f"""
                    UPDATE {Tables.IDENTITIES}
                    SET merged_into_id = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (canon_target, canon_source),
                )

                # ── 13. Write audit record ──
                total_rows = sum(result["tables_migrated"].values())
                audit_meta = {
                    **(metadata or {}),
                    "tables_migrated": result["tables_migrated"],
                    "total_rows_migrated": total_rows,
                    "sessions_revoked": result["sessions_revoked"],
                    "wallet_result": result["wallet_result"],
                    "subscription_result": result["subscription_result"],
                }
                cur.execute(
                    f"""
                    INSERT INTO {Tables.IDENTITY_MERGES}
                    (source_identity_id, target_identity_id, merged_by,
                     merge_reason, merge_mode, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        canon_source,
                        canon_target,
                        merged_by,
                        reason,
                        mode,
                        _json.dumps(audit_meta),
                    ),
                )

                result["success"] = True
                print(
                    f"[MERGE] Complete: {canon_source[:8]}... → "
                    f"{canon_target[:8]}... "
                    f"({total_rows} rows, "
                    f"{result['sessions_revoked']} sessions revoked)"
                )

        except Exception as e:
            print(f"[MERGE] Failed: {source_id[:8]}... → {target_id[:8]}...: {e}")
            result["blocked_reason"] = f"transaction_error: {type(e).__name__}"
            result["success"] = False

        return result

    # ─────────────────────────────────────────────────────────
    #  VALIDATION
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _validate_merge(
        source_id: str, target_id: str, cur
    ) -> Tuple[bool, Optional[str], str, str]:
        """
        Validate merge preconditions within the transaction.

        Returns:
            (valid, block_reason, canonical_source, canonical_target)
        """
        # Resolve canonical IDs
        canon_source = MergeService._resolve_canonical(source_id, cur)
        canon_target = MergeService._resolve_canonical(target_id, cur)

        # Self-merge check (after canonical resolution)
        if canon_source == canon_target:
            return False, "already_merged_same_canonical", canon_source, canon_target

        # Verify both identities exist
        cur.execute(
            f"SELECT id, merged_into_id FROM {Tables.IDENTITIES} WHERE id = %s",
            (canon_source,),
        )
        source_row = fetch_one(cur)
        if not source_row:
            return False, "source_not_found", canon_source, canon_target

        cur.execute(
            f"SELECT id, merged_into_id FROM {Tables.IDENTITIES} WHERE id = %s",
            (canon_target,),
        )
        target_row = fetch_one(cur)
        if not target_row:
            return False, "target_not_found", canon_source, canon_target

        # Source must be canonical (not already merged elsewhere)
        if source_row.get("merged_into_id"):
            existing_target = str(source_row["merged_into_id"])
            if existing_target == canon_target:
                return (
                    False,
                    "already_merged_into_target",
                    canon_source,
                    canon_target,
                )
            return (
                False,
                f"source_already_merged_into_{existing_target[:8]}",
                canon_source,
                canon_target,
            )

        # Target must be canonical
        if target_row.get("merged_into_id"):
            return False, "target_not_canonical", canon_source, canon_target

        # NEW-2: Cycle prevention — verify target chain doesn't reach source
        if MergeService._would_create_cycle(canon_source, canon_target, cur):
            return False, "would_create_cycle", canon_source, canon_target

        return True, None, canon_source, canon_target

    @staticmethod
    def _resolve_canonical(identity_id: str, cur, max_hops: int = 5) -> str:
        """Follow merged_into_id chain to canonical identity (within transaction cursor)."""
        current = identity_id
        for _ in range(max_hops):
            cur.execute(
                f"SELECT merged_into_id FROM {Tables.IDENTITIES} WHERE id = %s",
                (current,),
            )
            row = fetch_one(cur)
            if not row or not row.get("merged_into_id"):
                break
            current = str(row["merged_into_id"])
        return current

    @staticmethod
    def _would_create_cycle(source_id: str, target_id: str, cur) -> bool:
        """
        NEW-2: Check if setting source.merged_into_id = target would create
        a cycle in the merge chain.

        Walk target's existing chain. If source appears anywhere, it's a cycle.
        """
        visited = {source_id}
        current = target_id
        for _ in range(10):
            if current in visited:
                return True
            visited.add(current)
            cur.execute(
                f"SELECT merged_into_id FROM {Tables.IDENTITIES} WHERE id = %s",
                (current,),
            )
            row = fetch_one(cur)
            if not row or not row.get("merged_into_id"):
                break
            current = str(row["merged_into_id"])
        return False

    # ─────────────────────────────────────────────────────────
    #  SUBSCRIPTION CONFLICT DETECTION
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _check_subscription_conflict(
        source_id: str, target_id: str, cur
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if merging would create a double-entitlement subscription conflict.

        Conflict exists when BOTH identities have active/pending/past_due subscriptions.

        Returns:
            (has_conflict, details_dict)
        """
        placeholders = ", ".join(["%s"] * len(MergeService._ACTIVE_SUB_STATUSES))

        cur.execute(
            f"""
            SELECT id, plan_code, status, created_at
            FROM {Tables.SUBSCRIPTIONS}
            WHERE identity_id = %s AND status IN ({placeholders})
            ORDER BY created_at DESC LIMIT 1
            """,
            (source_id, *MergeService._ACTIVE_SUB_STATUSES),
        )
        source_sub = fetch_one(cur)

        cur.execute(
            f"""
            SELECT id, plan_code, status, created_at
            FROM {Tables.SUBSCRIPTIONS}
            WHERE identity_id = %s AND status IN ({placeholders})
            ORDER BY created_at DESC LIMIT 1
            """,
            (target_id, *MergeService._ACTIVE_SUB_STATUSES),
        )
        target_sub = fetch_one(cur)

        details = {
            "source_active_subscription": _sub_summary(source_sub),
            "target_active_subscription": _sub_summary(target_sub),
            "conflict": False,
        }

        if source_sub and target_sub:
            details["conflict"] = True
            details["conflict_reason"] = (
                f"Both identities have active subscriptions: "
                f"source={source_sub['plan_code']}({source_sub['status']}), "
                f"target={target_sub['plan_code']}({target_sub['status']})"
            )
            print(
                f"[MERGE] Subscription conflict: source has "
                f"{source_sub['plan_code']}({source_sub['status']}), "
                f"target has {target_sub['plan_code']}({target_sub['status']})"
            )
            return True, details

        return False, details

    # ─────────────────────────────────────────────────────────
    #  DATA MIGRATION HELPERS
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _migrate_table(cur, table: str, source_id: str, target_id: str) -> int:
        """Simple UPDATE identity_id migration. Returns row count."""
        cur.execute(
            f"""
            UPDATE {table}
            SET identity_id = %s
            WHERE identity_id = %s
            """,
            (target_id, source_id),
        )
        return cur.rowcount

    @staticmethod
    def _migrate_jobs(
        cur, source_id: str, target_id: str, warnings: List[str]
    ) -> int:
        """
        Migrate jobs with idempotency-key conflict handling.

        Jobs with idempotency_key that would conflict with existing target jobs
        are left under the source identity (accessible via canonical resolution).
        """
        # Move jobs that have no idempotency key (no conflict possible)
        cur.execute(
            f"""
            UPDATE {Tables.JOBS}
            SET identity_id = %s
            WHERE identity_id = %s AND idempotency_key IS NULL
            """,
            (target_id, source_id),
        )
        moved = cur.rowcount

        # Move jobs with idempotency keys that don't conflict
        cur.execute(
            f"""
            UPDATE {Tables.JOBS}
            SET identity_id = %s
            WHERE identity_id = %s
              AND idempotency_key IS NOT NULL
              AND idempotency_key NOT IN (
                  SELECT idempotency_key FROM {Tables.JOBS}
                  WHERE identity_id = %s AND idempotency_key IS NOT NULL
              )
            """,
            (target_id, source_id, target_id),
        )
        moved += cur.rowcount

        # Check if any jobs were skipped due to conflicts
        cur.execute(
            f"SELECT COUNT(*) as cnt FROM {Tables.JOBS} WHERE identity_id = %s",
            (source_id,),
        )
        remaining = fetch_one(cur)
        remaining_count = remaining["cnt"] if remaining else 0
        if remaining_count > 0:
            warnings.append(
                f"{remaining_count} jobs skipped (idempotency key conflict)"
            )
            print(
                f"[MERGE] {remaining_count} jobs left on source "
                f"(idempotency key conflict)"
            )

        return moved

    @staticmethod
    def _migrate_subscriptions(cur, source_id: str, target_id: str) -> int:
        """
        Migrate all source subscriptions to target.

        Called only after _check_subscription_conflict confirms no active conflict.
        Inactive/cancelled/expired subs move as historical records.
        """
        cur.execute(
            f"""
            UPDATE {Tables.SUBSCRIPTIONS}
            SET identity_id = %s
            WHERE identity_id = %s
            """,
            (target_id, source_id),
        )
        count = cur.rowcount
        if count > 0:
            print(f"[MERGE] Migrated {count} subscription(s) to target")
        return count

    @staticmethod
    def _migrate_mollie_customers(
        cur, source_id: str, target_id: str, warnings: List[str]
    ) -> int:
        """
        Migrate mollie_customers record if target doesn't have one.
        If both have one, leave source's in place (merged identity).
        """
        # Check target
        cur.execute(
            f"SELECT id FROM {Tables.MOLLIE_CUSTOMERS} WHERE identity_id = %s",
            (target_id,),
        )
        target_has_mollie = fetch_one(cur) is not None

        if target_has_mollie:
            # Check if source also has one
            cur.execute(
                f"SELECT id FROM {Tables.MOLLIE_CUSTOMERS} WHERE identity_id = %s",
                (source_id,),
            )
            if fetch_one(cur):
                warnings.append(
                    "Source mollie_customer left on merged identity "
                    "(target already has one)"
                )
            return 0

        # Target doesn't have one — move source's
        cur.execute(
            f"""
            UPDATE {Tables.MOLLIE_CUSTOMERS}
            SET identity_id = %s, updated_at = NOW()
            WHERE identity_id = %s
            """,
            (target_id, source_id),
        )
        return cur.rowcount

    @staticmethod
    def _merge_daily_limits(cur, source_id: str, target_id: str) -> int:
        """
        Merge daily_limits: move non-conflicting rows, sum conflicting ones.

        daily_limits has UNIQUE(identity_id, day_utc).
        """
        # Get source limits
        cur.execute(
            f"""
            SELECT id, day_utc, meshy_jobs, openai_images
            FROM {Tables.DAILY_LIMITS}
            WHERE identity_id = %s
            """,
            (source_id,),
        )
        source_limits = fetch_all(cur)

        if not source_limits:
            return 0

        migrated = 0
        for row in source_limits:
            day = row["day_utc"]
            # Check if target already has this day
            cur.execute(
                f"""
                SELECT id, meshy_jobs, openai_images
                FROM {Tables.DAILY_LIMITS}
                WHERE identity_id = %s AND day_utc = %s
                """,
                (target_id, day),
            )
            existing = fetch_one(cur)

            if existing:
                # Sum the counters
                cur.execute(
                    f"""
                    UPDATE {Tables.DAILY_LIMITS}
                    SET meshy_jobs = meshy_jobs + %s,
                        openai_images = openai_images + %s,
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (row["meshy_jobs"], row["openai_images"], existing["id"]),
                )
                # Delete source row
                cur.execute(
                    f"DELETE FROM {Tables.DAILY_LIMITS} WHERE id = %s",
                    (row["id"],),
                )
            else:
                # Move to target
                cur.execute(
                    f"""
                    UPDATE {Tables.DAILY_LIMITS}
                    SET identity_id = %s
                    WHERE id = %s
                    """,
                    (target_id, row["id"]),
                )
            migrated += 1

        return migrated

    # ─────────────────────────────────────────────────────────
    #  WALLET RECONCILIATION
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _reconcile_wallets(
        cur, source_id: str, target_id: str
    ) -> Dict[str, Any]:
        """
        Reconcile wallets after ledger entries have been migrated.

        Strategy:
        1. All ledger entries now live under target_id
        2. Recompute target wallet from ledger (source of truth)
        3. Zero out source wallet (no spendable balance remains)

        Does NOT simply add balances — uses ledger as authority.
        """
        result = {}

        # ── Ensure target wallet exists ──
        cur.execute(
            f"SELECT identity_id FROM {Tables.WALLETS} WHERE identity_id = %s",
            (target_id,),
        )
        if not fetch_one(cur):
            cur.execute(
                f"""
                INSERT INTO {Tables.WALLETS} (identity_id, balance_credits,
                    reserved_credits, balance_video_credits, updated_at)
                VALUES (%s, 0, 0, 0, NOW())
                """,
                (target_id,),
            )

        # ── Lock target wallet ──
        cur.execute(
            f"""
            SELECT balance_credits, balance_video_credits
            FROM {Tables.WALLETS}
            WHERE identity_id = %s FOR UPDATE
            """,
            (target_id,),
        )
        target_wallet = fetch_one(cur)
        old_general = (target_wallet or {}).get("balance_credits", 0) or 0
        old_video = (target_wallet or {}).get("balance_video_credits", 0) or 0

        # ── Recompute from ledger (all entries now under target) ──
        cur.execute(
            f"""
            SELECT credit_type, COALESCE(SUM(amount_credits), 0) as total
            FROM {Tables.LEDGER_ENTRIES}
            WHERE identity_id = %s
            GROUP BY credit_type
            """,
            (target_id,),
        )
        sums = {"general": 0, "video": 0}
        for row in fetch_all(cur):
            ct = row.get("credit_type", "general")
            sums[ct] = int(row.get("total", 0) or 0)

        new_general = sums["general"]
        new_video = sums["video"]

        cur.execute(
            f"""
            UPDATE {Tables.WALLETS}
            SET balance_credits = %s,
                balance_video_credits = %s,
                reserved_credits = 0,
                updated_at = NOW()
            WHERE identity_id = %s
            """,
            (new_general, new_video, target_id),
        )

        result["target"] = {
            "old_general": old_general,
            "new_general": new_general,
            "old_video": old_video,
            "new_video": new_video,
        }

        # ── Zero out source wallet ──
        cur.execute(
            f"""
            UPDATE {Tables.WALLETS}
            SET balance_credits = 0, balance_video_credits = 0,
                reserved_credits = 0, updated_at = NOW()
            WHERE identity_id = %s
            """,
            (source_id,),
        )

        # ── Get source wallet pre-merge snapshot for audit ──
        result["source_zeroed"] = True

        print(
            f"[MERGE] Wallet reconciled: target general "
            f"{old_general}→{new_general}, video {old_video}→{new_video}"
        )

        return result

    # ─────────────────────────────────────────────────────────
    #  SESSION HANDLING
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def _revoke_source_sessions(cur, source_id: str) -> int:
        """
        Revoke all active sessions for the source identity.

        Future session validation will find the identity merged and
        resolve to canonical via merged_into_id chain.
        """
        cur.execute(
            f"""
            UPDATE {Tables.SESSIONS}
            SET revoked_at = NOW()
            WHERE identity_id = %s
              AND revoked_at IS NULL
              AND expires_at > NOW()
            """,
            (source_id,),
        )
        count = cur.rowcount
        if count > 0:
            print(f"[MERGE] Revoked {count} active session(s) for source identity")
        return count

    # ─────────────────────────────────────────────────────────
    #  DRY-RUN / PREVIEW HELPER
    # ─────────────────────────────────────────────────────────

    @staticmethod
    def preview_merge(
        source_id: str, target_id: str
    ) -> Dict[str, Any]:
        """
        Preview what a merge would do without executing it.

        Returns data counts per table and subscription conflict status.
        """
        preview = {
            "valid": False,
            "source_id": source_id,
            "target_id": target_id,
            "blocked_reason": None,
            "row_counts": {},
            "subscription_conflict": False,
            "subscription_details": {},
        }

        try:
            with transaction() as cur:
                # Validate
                valid, block_reason, canon_source, canon_target = (
                    MergeService._validate_merge(source_id, target_id, cur)
                )
                preview["source_id"] = canon_source
                preview["target_id"] = canon_target

                if not valid:
                    preview["blocked_reason"] = block_reason
                    return preview

                # Count rows per table
                for table in MergeService._SIMPLE_MIGRATE_TABLES:
                    cur.execute(
                        f"SELECT COUNT(*) as cnt FROM {table} WHERE identity_id = %s",
                        (canon_source,),
                    )
                    row = fetch_one(cur)
                    count = row["cnt"] if row else 0
                    if count > 0:
                        preview["row_counts"][table] = count

                # Count jobs, ledger, subscriptions
                for table in [Tables.JOBS, Tables.LEDGER_ENTRIES, Tables.SUBSCRIPTIONS]:
                    cur.execute(
                        f"SELECT COUNT(*) as cnt FROM {table} WHERE identity_id = %s",
                        (canon_source,),
                    )
                    row = fetch_one(cur)
                    count = row["cnt"] if row else 0
                    if count > 0:
                        preview["row_counts"][table] = count

                # Subscription conflict check
                conflict, details = MergeService._check_subscription_conflict(
                    canon_source, canon_target, cur
                )
                preview["subscription_conflict"] = conflict
                preview["subscription_details"] = details

                # Wallet preview
                cur.execute(
                    f"""
                    SELECT balance_credits, balance_video_credits
                    FROM {Tables.WALLETS} WHERE identity_id = %s
                    """,
                    (canon_source,),
                )
                source_wallet = fetch_one(cur)
                cur.execute(
                    f"""
                    SELECT balance_credits, balance_video_credits
                    FROM {Tables.WALLETS} WHERE identity_id = %s
                    """,
                    (canon_target,),
                )
                target_wallet = fetch_one(cur)
                preview["wallet_preview"] = {
                    "source_general": (source_wallet or {}).get("balance_credits", 0),
                    "source_video": (source_wallet or {}).get("balance_video_credits", 0),
                    "target_general": (target_wallet or {}).get("balance_credits", 0),
                    "target_video": (target_wallet or {}).get("balance_video_credits", 0),
                }

                preview["valid"] = True

        except Exception as e:
            preview["blocked_reason"] = f"preview_error: {type(e).__name__}: {e}"

        return preview


# ─────────────────────────────────────────────────────────
#  Module-level helpers
# ─────────────────────────────────────────────────────────

def _sub_summary(sub_row: Optional[Dict]) -> Optional[Dict]:
    """Format a subscription row for the result dict."""
    if not sub_row:
        return None
    return {
        "id": str(sub_row["id"]),
        "plan_code": sub_row.get("plan_code"),
        "status": sub_row.get("status"),
    }
