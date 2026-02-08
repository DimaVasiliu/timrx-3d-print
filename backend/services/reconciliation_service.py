"""
Reconciliation Service - Safety job for data consistency.

Runs periodically to detect and fix inconsistencies:
1. Purchases missing ledger entries / wallet mismatch
2. Stale held reservations (job terminal or missing)
3. Completed jobs missing history_items

Usage:
    # Run manually
    from backend.services.reconciliation_service import ReconciliationService
    result = ReconciliationService.reconcile_safety()

    # Via cron endpoint
    POST /internal/reconcile?key=YOUR_SECRET
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone, timedelta
import json

from backend.db import (
    fetch_one, transaction, query_one, query_all, execute, execute_returning, Tables
)
from backend.services.wallet_service import WalletService, LedgerEntryType
from backend.services.reservation_service import ReservationService, ReservationStatus
from backend.config import config


class ReconciliationService:
    """
    Service for detecting and fixing data inconsistencies.

    Safe to run frequently (every 15 minutes recommended).
    All fixes are idempotent and logged.
    """

    # Default settings
    STALE_RESERVATION_MINUTES = 30  # Reservations older than this with terminal job
    MAX_FIXES_PER_RUN = 100  # Limit fixes per run to prevent runaway

    # ─────────────────────────────────────────────────────────────
    # Main Entry Point
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def reconcile_safety(
        dry_run: bool = False,
        send_alert: bool = True,
    ) -> Dict[str, Any]:
        """
        Run all reconciliation checks and apply fixes.

        Args:
            dry_run: If True, detect issues but don't fix them
            send_alert: If True, send admin email if fixes were applied

        Returns:
            Summary of all checks and fixes applied
        """
        start_time = datetime.now(timezone.utc)
        print(f"[RECONCILE] Starting reconciliation run (dry_run={dry_run})")

        results = {
            "run_at": start_time.isoformat(),
            "dry_run": dry_run,
            "purchases_missing_ledger": [],
            "wallet_mismatches_fixed": [],
            "stale_reservations_released": [],
            "missing_history_items_created": [],
            "ready_unbilled_jobs": [],  # Jobs that succeeded without credit deduction
            "errors": [],
        }

        # 1. Check purchases missing ledger entries
        try:
            ledger_fixes = ReconciliationService._fix_purchases_missing_ledger(dry_run)
            results["purchases_missing_ledger"] = ledger_fixes
        except Exception as e:
            print(f"[RECONCILE] ERROR in purchases_missing_ledger: {e}")
            results["errors"].append({"check": "purchases_missing_ledger", "error": str(e)})

        # 2. Check wallet balance mismatches
        try:
            wallet_fixes = ReconciliationService._fix_wallet_mismatches(dry_run)
            results["wallet_mismatches_fixed"] = wallet_fixes
        except Exception as e:
            print(f"[RECONCILE] ERROR in wallet_mismatches: {e}")
            results["errors"].append({"check": "wallet_mismatches", "error": str(e)})

        # 3. Check stale held reservations
        try:
            reservation_fixes = ReconciliationService._fix_stale_reservations(dry_run)
            results["stale_reservations_released"] = reservation_fixes
        except Exception as e:
            print(f"[RECONCILE] ERROR in stale_reservations: {e}")
            results["errors"].append({"check": "stale_reservations", "error": str(e)})

        # 4. Check completed jobs missing history_items
        try:
            history_fixes = ReconciliationService._fix_missing_history_items(dry_run)
            results["missing_history_items_created"] = history_fixes
        except Exception as e:
            print(f"[RECONCILE] ERROR in missing_history_items: {e}")
            results["errors"].append({"check": "missing_history_items", "error": str(e)})

        # 5. Detect ready_unbilled jobs (detection only - no automatic fix)
        try:
            unbilled = ReconciliationService._detect_ready_unbilled_jobs()
            results["ready_unbilled_jobs"] = unbilled
            if unbilled:
                print(f"[RECONCILE] CRITICAL: Found {len(unbilled)} ready_unbilled jobs requiring manual review!")
        except Exception as e:
            print(f"[RECONCILE] ERROR in ready_unbilled: {e}")
            results["errors"].append({"check": "ready_unbilled", "error": str(e)})

        # Calculate totals
        total_fixes = (
            len(results["purchases_missing_ledger"])
            + len(results["wallet_mismatches_fixed"])
            + len(results["stale_reservations_released"])
            + len(results["missing_history_items_created"])
        )
        # ready_unbilled is detection only, count separately
        total_unbilled = len(results["ready_unbilled_jobs"])

        end_time = datetime.now(timezone.utc)
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        results["total_fixes"] = total_fixes
        results["total_unbilled"] = total_unbilled
        results["total_errors"] = len(results["errors"])
        results["duration_ms"] = duration_ms

        print(
            f"[RECONCILE] Completed: fixes={total_fixes}, unbilled={total_unbilled}, "
            f"errors={len(results['errors'])}, duration={duration_ms}ms"
        )

        # Send admin alert if fixes were applied or unbilled jobs detected
        if send_alert and (total_fixes > 0 or total_unbilled > 0) and not dry_run:
            ReconciliationService._send_admin_alert(results)

        return results

    # ─────────────────────────────────────────────────────────────
    # Check 1: Purchases Missing Ledger Entries
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _fix_purchases_missing_ledger(dry_run: bool = False) -> List[Dict[str, Any]]:
        """
        Find paid purchases that don't have a corresponding ledger entry.
        Insert missing ledger entry idempotently.
        """
        # Find purchases with status='paid' or 'complete' missing ledger entry
        missing = query_all(
            f"""
            SELECT p.id, p.identity_id, p.credits_granted, p.amount, p.currency,
                   p.plan_code, p.plan_name, p.status, p.purchased_at
            FROM {Tables.PURCHASES} p
            WHERE p.status IN ('paid', 'complete', 'completed')
              AND p.credits_granted > 0
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.LEDGER_ENTRIES} le
                  WHERE le.ref_type = 'purchase'
                    AND le.ref_id = p.id::text
                    AND le.entry_type = %s
              )
            ORDER BY p.purchased_at ASC
            LIMIT %s
            """,
            (LedgerEntryType.PURCHASE_CREDIT, ReconciliationService.MAX_FIXES_PER_RUN),
        )

        fixes = []
        for purchase in missing:
            purchase_id = str(purchase["id"])
            identity_id = str(purchase["identity_id"])
            credits = purchase["credits_granted"]

            print(
                f"[RECONCILE] Found purchase {purchase_id[:8]}... missing ledger entry: "
                f"identity={identity_id[:8]}..., credits={credits}"
            )

            if dry_run:
                fixes.append({
                    "purchase_id": purchase_id,
                    "identity_id": identity_id,
                    "credits": credits,
                    "action": "would_create_ledger_entry",
                })
                continue

            try:
                # Create ledger entry idempotently
                with transaction() as cur:
                    # Double-check it doesn't exist (race condition guard)
                    cur.execute(
                        f"""
                        SELECT id FROM {Tables.LEDGER_ENTRIES}
                        WHERE ref_type = 'purchase' AND ref_id = %s AND entry_type = %s
                        """,
                        (purchase_id, LedgerEntryType.PURCHASE_CREDIT),
                    )
                    if fetch_one(cur):
                        print(f"[RECONCILE] Ledger entry already exists for purchase {purchase_id[:8]}")
                        continue

                    # Ensure wallet exists
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.WALLETS} (identity_id, balance_credits, updated_at)
                        VALUES (%s, 0, NOW())
                        ON CONFLICT (identity_id) DO NOTHING
                        """,
                        (identity_id,),
                    )

                    # Get current balance
                    cur.execute(
                        f"SELECT balance_credits FROM {Tables.WALLETS} WHERE identity_id = %s FOR UPDATE",
                        (identity_id,),
                    )
                    wallet = fetch_one(cur)
                    old_balance = wallet["balance_credits"] if wallet else 0
                    new_balance = old_balance + credits

                    # Insert ledger entry
                    cur.execute(
                        f"""
                        INSERT INTO {Tables.LEDGER_ENTRIES}
                        (identity_id, entry_type, amount_credits, ref_type, ref_id, meta, created_at)
                        VALUES (%s, %s, %s, 'purchase', %s, %s, NOW())
                        """,
                        (
                            identity_id,
                            LedgerEntryType.PURCHASE_CREDIT,
                            credits,
                            purchase_id,
                            json.dumps({
                                "reconciliation": True,
                                "plan_code": purchase.get("plan_code"),
                                "plan_name": purchase.get("plan_name"),
                            }),
                        ),
                    )

                    # Update wallet balance
                    cur.execute(
                        f"""
                        UPDATE {Tables.WALLETS}
                        SET balance_credits = %s, updated_at = NOW()
                        WHERE identity_id = %s
                        """,
                        (new_balance, identity_id),
                    )

                    print(
                        f"[RECONCILE] Created missing ledger entry for purchase {purchase_id[:8]}: "
                        f"credits={credits}, balance: {old_balance} -> {new_balance}"
                    )

                fixes.append({
                    "purchase_id": purchase_id,
                    "identity_id": identity_id,
                    "credits": credits,
                    "old_balance": old_balance,
                    "new_balance": new_balance,
                    "action": "created_ledger_entry",
                })

            except Exception as e:
                print(f"[RECONCILE] ERROR fixing purchase {purchase_id[:8]}: {e}")
                fixes.append({
                    "purchase_id": purchase_id,
                    "identity_id": identity_id,
                    "credits": credits,
                    "action": "error",
                    "error": str(e),
                })

        return fixes

    # ─────────────────────────────────────────────────────────────
    # Check 2: Wallet Balance Mismatches
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _fix_wallet_mismatches(dry_run: bool = False) -> List[Dict[str, Any]]:
        """
        Find wallets where balance_credits != sum(ledger_entries).
        Recompute and fix the balance.
        """
        # Find wallets with mismatched balance
        mismatched = query_all(
            f"""
            SELECT
                w.identity_id,
                w.balance_credits as wallet_balance,
                COALESCE(SUM(le.amount_credits), 0) as ledger_sum
            FROM {Tables.WALLETS} w
            LEFT JOIN {Tables.LEDGER_ENTRIES} le ON le.identity_id = w.identity_id
            GROUP BY w.identity_id, w.balance_credits
            HAVING w.balance_credits != COALESCE(SUM(le.amount_credits), 0)
            LIMIT %s
            """,
            (ReconciliationService.MAX_FIXES_PER_RUN,),
        )

        fixes = []
        for row in mismatched:
            identity_id = str(row["identity_id"])
            wallet_balance = row["wallet_balance"] or 0
            ledger_sum = int(row["ledger_sum"] or 0)

            print(
                f"[RECONCILE] Found wallet mismatch: identity={identity_id[:8]}..., "
                f"wallet={wallet_balance}, ledger={ledger_sum}"
            )

            if dry_run:
                fixes.append({
                    "identity_id": identity_id,
                    "wallet_balance": wallet_balance,
                    "ledger_sum": ledger_sum,
                    "action": "would_fix_balance",
                })
                continue

            try:
                result = WalletService.recompute_wallet_balance(identity_id)
                fixes.append({
                    "identity_id": identity_id,
                    "old_balance": result["old_balance"],
                    "new_balance": result["new_balance"],
                    "action": "fixed_balance",
                })
            except Exception as e:
                print(f"[RECONCILE] ERROR fixing wallet {identity_id[:8]}: {e}")
                fixes.append({
                    "identity_id": identity_id,
                    "wallet_balance": wallet_balance,
                    "ledger_sum": ledger_sum,
                    "action": "error",
                    "error": str(e),
                })

        return fixes

    # ─────────────────────────────────────────────────────────────
    # Check 3: Stale Held Reservations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _fix_stale_reservations(dry_run: bool = False) -> List[Dict[str, Any]]:
        """
        Find held reservations where:
        - Reservation is older than STALE_RESERVATION_MINUTES
        - AND (linked job is terminal (failed/cancelled) OR job is missing)

        Release these reservations to return credits to available balance.
        """
        stale_threshold = datetime.now(timezone.utc) - timedelta(
            minutes=ReconciliationService.STALE_RESERVATION_MINUTES
        )

        # Find stale reservations
        stale = query_all(
            f"""
            SELECT
                r.id as reservation_id,
                r.identity_id,
                r.ref_job_id,
                r.cost_credits,
                r.action_code,
                r.created_at,
                j.status as job_status
            FROM {Tables.CREDIT_RESERVATIONS} r
            LEFT JOIN {Tables.JOBS} j ON j.id = r.ref_job_id
            WHERE r.status = %s
              AND r.created_at < %s
              AND (
                  j.id IS NULL  -- Job missing
                  OR j.status IN ('failed', 'cancelled', 'error')  -- Job terminal
              )
            ORDER BY r.created_at ASC
            LIMIT %s
            """,
            (ReservationStatus.HELD, stale_threshold, ReconciliationService.MAX_FIXES_PER_RUN),
        )

        fixes = []
        for row in stale:
            reservation_id = str(row["reservation_id"])
            identity_id = str(row["identity_id"])
            job_id = str(row["ref_job_id"]) if row["ref_job_id"] else None
            cost_credits = row["cost_credits"]
            job_status = row["job_status"]

            reason = "job_missing" if job_status is None else f"job_{job_status}"

            print(
                f"[RECONCILE] Found stale reservation {reservation_id[:8]}...: "
                f"job={job_id[:8] if job_id else 'None'}..., status={job_status}, credits={cost_credits}"
            )

            if dry_run:
                fixes.append({
                    "reservation_id": reservation_id,
                    "identity_id": identity_id,
                    "job_id": job_id,
                    "cost_credits": cost_credits,
                    "reason": reason,
                    "action": "would_release",
                })
                continue

            try:
                result = ReservationService.release_reservation(
                    reservation_id,
                    reason=f"reconciliation:{reason}",
                )

                fixes.append({
                    "reservation_id": reservation_id,
                    "identity_id": identity_id,
                    "job_id": job_id,
                    "cost_credits": cost_credits,
                    "reason": reason,
                    "was_already_released": result.get("was_already_released", False),
                    "action": "released",
                })
            except Exception as e:
                print(f"[RECONCILE] ERROR releasing reservation {reservation_id[:8]}: {e}")
                fixes.append({
                    "reservation_id": reservation_id,
                    "identity_id": identity_id,
                    "job_id": job_id,
                    "reason": reason,
                    "action": "error",
                    "error": str(e),
                })

        return fixes

    # ─────────────────────────────────────────────────────────────
    # Check 4: Completed Jobs Missing History Items
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _fix_missing_history_items(dry_run: bool = False) -> List[Dict[str, Any]]:
        """
        Find jobs in terminal success state that have asset records (models/images/videos)
        but no corresponding history_items row.

        Create missing history_items rows.
        """
        # Find successful jobs with models but no history item
        missing_model_history = query_all(
            f"""
            SELECT
                j.id as job_id,
                j.identity_id,
                j.action_code,
                j.prompt,
                j.meta,
                m.id as model_id,
                m.title as model_title,
                m.glb_url,
                m.thumbnail_url,
                m.status as model_status
            FROM {Tables.JOBS} j
            INNER JOIN {Tables.MODELS} m ON m.upstream_job_id = j.id::text
            WHERE j.status IN ('succeeded', 'completed', 'complete')
              AND j.identity_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.HISTORY_ITEMS} h
                  WHERE h.identity_id = j.identity_id
                    AND (h.model_id = m.id OR h.payload->>'original_job_id' = j.id::text)
              )
            LIMIT %s
            """,
            (ReconciliationService.MAX_FIXES_PER_RUN // 3,),
        )

        # Find successful jobs with images but no history item
        missing_image_history = query_all(
            f"""
            SELECT
                j.id as job_id,
                j.identity_id,
                j.action_code,
                j.prompt,
                j.meta,
                i.id as image_id,
                i.title as image_title,
                i.image_url,
                i.thumbnail_url
            FROM {Tables.JOBS} j
            INNER JOIN {Tables.IMAGES} i ON i.upstream_id = j.id::text
            WHERE j.status IN ('succeeded', 'completed', 'complete')
              AND j.identity_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.HISTORY_ITEMS} h
                  WHERE h.identity_id = j.identity_id
                    AND (h.image_id = i.id OR h.payload->>'original_job_id' = j.id::text)
              )
            LIMIT %s
            """,
            (ReconciliationService.MAX_FIXES_PER_RUN // 3,),
        )

        # Find successful jobs with videos but no history item
        missing_video_history = query_all(
            f"""
            SELECT
                j.id as job_id,
                j.identity_id,
                j.action_code,
                j.prompt,
                j.meta,
                v.id as video_id,
                v.title as video_title,
                v.video_url,
                v.thumbnail_url
            FROM {Tables.JOBS} j
            INNER JOIN {Tables.VIDEOS} v ON v.upstream_id = j.id::text
            WHERE j.status IN ('succeeded', 'completed', 'complete')
              AND j.identity_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.HISTORY_ITEMS} h
                  WHERE h.identity_id = j.identity_id
                    AND (h.video_id = v.id OR h.payload->>'original_job_id' = j.id::text)
              )
            LIMIT %s
            """,
            (ReconciliationService.MAX_FIXES_PER_RUN // 3,),
        )

        fixes = []

        # Process models
        for row in missing_model_history:
            fix = ReconciliationService._create_missing_history_item(
                row, "model", dry_run
            )
            if fix:
                fixes.append(fix)

        # Process images
        for row in missing_image_history:
            fix = ReconciliationService._create_missing_history_item(
                row, "image", dry_run
            )
            if fix:
                fixes.append(fix)

        # Process videos
        for row in missing_video_history:
            fix = ReconciliationService._create_missing_history_item(
                row, "video", dry_run
            )
            if fix:
                fixes.append(fix)

        return fixes

    @staticmethod
    def _create_missing_history_item(
        row: Dict[str, Any],
        item_type: str,
        dry_run: bool,
    ) -> Optional[Dict[str, Any]]:
        """Create a missing history item for a completed job."""
        from backend.utils import derive_display_title

        job_id = str(row["job_id"])
        identity_id = str(row["identity_id"])
        prompt = row.get("prompt")
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}

        # Get asset-specific fields
        if item_type == "model":
            asset_id = str(row["model_id"])
            title = row.get("model_title") or derive_display_title(prompt, None)
            thumbnail_url = row.get("thumbnail_url")
            glb_url = row.get("glb_url")
            image_url = None
            video_url = None
            model_id = asset_id
            image_id = None
            video_id = None
        elif item_type == "image":
            asset_id = str(row["image_id"])
            title = row.get("image_title") or derive_display_title(prompt, None)
            thumbnail_url = row.get("thumbnail_url")
            glb_url = None
            image_url = row.get("image_url")
            video_url = None
            model_id = None
            image_id = asset_id
            video_id = None
        else:  # video
            asset_id = str(row["video_id"])
            title = row.get("video_title") or derive_display_title(prompt, None)
            thumbnail_url = row.get("thumbnail_url")
            glb_url = None
            image_url = None
            video_url = row.get("video_url")
            model_id = None
            image_id = None
            video_id = asset_id

        print(
            f"[RECONCILE] Found {item_type} job {job_id[:8]}... missing history item: "
            f"asset={asset_id[:8]}..., title={title[:20] if title else 'None'}..."
        )

        if dry_run:
            return {
                "job_id": job_id,
                "identity_id": identity_id,
                "item_type": item_type,
                "asset_id": asset_id,
                "action": "would_create_history",
            }

        try:
            import uuid

            history_id = str(uuid.uuid4())
            payload = {
                "original_job_id": job_id,
                "reconciliation": True,
                **meta,
            }

            with transaction() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {Tables.HISTORY_ITEMS}
                    (id, identity_id, item_type, status, title, prompt,
                     thumbnail_url, glb_url, image_url, video_url,
                     model_id, image_id, video_id, payload, created_at, updated_at)
                    VALUES (%s, %s, %s, 'succeeded', %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (
                        history_id,
                        identity_id,
                        item_type,
                        title,
                        prompt,
                        thumbnail_url,
                        glb_url,
                        image_url,
                        video_url,
                        model_id,
                        image_id,
                        video_id,
                        json.dumps(payload),
                    ),
                )
                result = fetch_one(cur)

            if result:
                print(f"[RECONCILE] Created history item {history_id[:8]} for {item_type} job {job_id[:8]}")
                return {
                    "job_id": job_id,
                    "identity_id": identity_id,
                    "item_type": item_type,
                    "asset_id": asset_id,
                    "history_id": history_id,
                    "action": "created_history",
                }
            else:
                print(f"[RECONCILE] History item already exists for {item_type} job {job_id[:8]}")
                return None

        except Exception as e:
            print(f"[RECONCILE] ERROR creating history for {item_type} job {job_id[:8]}: {e}")
            return {
                "job_id": job_id,
                "identity_id": identity_id,
                "item_type": item_type,
                "asset_id": asset_id,
                "action": "error",
                "error": str(e),
            }

    # ─────────────────────────────────────────────────────────────
    # Check 5: Ready Unbilled Jobs (Detection Only)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _detect_ready_unbilled_jobs(limit: int = 100) -> List[Dict[str, Any]]:
        """
        Detect jobs that completed successfully but were never billed.

        This is a CRITICAL billing bug detection - these jobs:
        1. Have status='ready_unbilled' (explicitly marked by finalize_job_credits)
        2. OR have status='ready'/'done' but no reservation_id and no ledger entry

        These require manual admin review to determine if billing should be applied
        retroactively or the user should be notified.

        NOTE: This is detection only - no automatic fix is applied because billing
        retroactively requires human judgment.
        """
        unbilled_jobs = []

        # Case 1: Jobs explicitly marked as ready_unbilled
        explicit_unbilled = query_all(
            f"""
            SELECT
                j.id as job_id,
                j.identity_id,
                j.provider,
                j.action_code,
                j.status,
                j.cost_credits,
                j.reservation_id,
                j.prompt,
                j.error_message,
                j.created_at,
                j.updated_at,
                j.meta
            FROM {Tables.JOBS} j
            WHERE j.status = 'ready_unbilled'
            ORDER BY j.created_at DESC
            LIMIT %s
            """,
            (limit,),
        )

        for row in explicit_unbilled:
            meta = row.get("meta") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}

            unbilled_jobs.append({
                "job_id": str(row["job_id"]),
                "identity_id": str(row["identity_id"]) if row.get("identity_id") else None,
                "provider": row.get("provider"),
                "action_code": row.get("action_code"),
                "status": row.get("status"),
                "cost_credits": row.get("cost_credits"),
                "reservation_id": str(row["reservation_id"]) if row.get("reservation_id") else None,
                "prompt": (row.get("prompt") or "")[:50] + "..." if row.get("prompt") and len(row.get("prompt", "")) > 50 else row.get("prompt"),
                "error_message": row.get("error_message"),
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
                "detection_source": "explicit_status",
                "detected_at": meta.get("ready_unbilled_detected_at"),
            })

        # Case 2: Jobs with ready/done status but no reservation and no ledger debit
        # These slipped through without proper credit reservation
        implicit_unbilled = query_all(
            f"""
            SELECT
                j.id as job_id,
                j.identity_id,
                j.provider,
                j.action_code,
                j.status,
                j.cost_credits,
                j.reservation_id,
                j.prompt,
                j.created_at,
                j.updated_at
            FROM {Tables.JOBS} j
            WHERE j.status IN ('ready', 'done', 'succeeded', 'completed')
              AND j.reservation_id IS NULL
              AND j.cost_credits > 0
              AND j.identity_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.LEDGER_ENTRIES} le
                  WHERE le.identity_id = j.identity_id
                    AND le.ref_type = 'job'
                    AND le.ref_id = j.id::text
                    AND le.amount_credits < 0
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.CREDIT_RESERVATIONS} r
                  WHERE r.ref_job_id = j.id
                    AND r.status IN ('finalized', 'held')
              )
            ORDER BY j.created_at DESC
            LIMIT %s
            """,
            (limit - len(unbilled_jobs),),
        )

        for row in implicit_unbilled:
            unbilled_jobs.append({
                "job_id": str(row["job_id"]),
                "identity_id": str(row["identity_id"]) if row.get("identity_id") else None,
                "provider": row.get("provider"),
                "action_code": row.get("action_code"),
                "status": row.get("status"),
                "cost_credits": row.get("cost_credits"),
                "reservation_id": None,
                "prompt": (row.get("prompt") or "")[:50] + "..." if row.get("prompt") and len(row.get("prompt", "")) > 50 else row.get("prompt"),
                "error_message": None,
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
                "detection_source": "implicit_missing_reservation",
                "detected_at": None,
            })

        if unbilled_jobs:
            print(
                f"[RECONCILE] CRITICAL: Found {len(unbilled_jobs)} ready_unbilled jobs "
                f"({len(explicit_unbilled)} explicit, {len(implicit_unbilled)} implicit)"
            )

        return unbilled_jobs

    # ─────────────────────────────────────────────────────────────
    # Admin Notification
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _send_admin_alert(results: Dict[str, Any]):
        """Send admin email summarizing reconciliation fixes."""
        try:
            from backend.emailer import notify_admin

            # Build summary
            fixes_summary = []

            if results["purchases_missing_ledger"]:
                count = len(results["purchases_missing_ledger"])
                fixes_summary.append(f"- {count} purchase(s) missing ledger entries (fixed)")

            if results["wallet_mismatches_fixed"]:
                count = len(results["wallet_mismatches_fixed"])
                fixes_summary.append(f"- {count} wallet balance mismatch(es) (fixed)")

            if results["stale_reservations_released"]:
                count = len(results["stale_reservations_released"])
                fixes_summary.append(f"- {count} stale reservation(s) released")

            if results["missing_history_items_created"]:
                count = len(results["missing_history_items_created"])
                fixes_summary.append(f"- {count} missing history item(s) created")

            if results["ready_unbilled_jobs"]:
                count = len(results["ready_unbilled_jobs"])
                fixes_summary.append(f"- CRITICAL: {count} job(s) completed without billing (requires manual review)")

            if results["errors"]:
                count = len(results["errors"])
                fixes_summary.append(f"- {count} error(s) during reconciliation")

            if not fixes_summary:
                return  # Nothing to report

            message = "The reconciliation job detected and fixed the following issues:\n\n"
            message += "\n".join(fixes_summary)
            message += f"\n\nDuration: {results['duration_ms']}ms"

            # Prepare detailed data for email
            data = {
                "Total Fixes": results["total_fixes"],
                "Total Errors": results["total_errors"],
                "Duration (ms)": results["duration_ms"],
            }

            # Add sample details (first 3 of each type)
            if results["purchases_missing_ledger"]:
                sample = results["purchases_missing_ledger"][:3]
                data["Ledger Fixes (sample)"] = json.dumps(sample, default=str)

            if results["wallet_mismatches_fixed"]:
                sample = results["wallet_mismatches_fixed"][:3]
                data["Wallet Fixes (sample)"] = json.dumps(sample, default=str)

            if results["stale_reservations_released"]:
                sample = results["stale_reservations_released"][:3]
                data["Reservation Releases (sample)"] = json.dumps(sample, default=str)

            if results["ready_unbilled_jobs"]:
                sample = results["ready_unbilled_jobs"][:5]  # More samples for critical issue
                data["CRITICAL: Unbilled Jobs (sample)"] = json.dumps(sample, default=str)

            if results["errors"]:
                data["Errors"] = json.dumps(results["errors"], default=str)

            notify_admin(
                subject="Reconciliation Job: Fixes Applied",
                message=message,
                data=data,
            )

            print("[RECONCILE] Admin alert sent")

        except Exception as e:
            print(f"[RECONCILE] Failed to send admin alert: {e}")

    # ─────────────────────────────────────────────────────────────
    # Utility Methods
    # ─────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────
    # Detection-Only Mode (No Fixes)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def detect_anomalies(
        stale_minutes: int = 30,
        check_s3: bool = False,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Detect data anomalies WITHOUT applying fixes.

        This is an admin-only diagnostic tool for identifying:
        1. Jobs with terminal status but missing history_items
        2. Finalized reservations without corresponding ledger entries
        3. Held reservations older than stale_minutes
        4. (Optional) S3 objects with no DB references

        Args:
            stale_minutes: Threshold for "stale" held reservations
            check_s3: If True, also check for orphan S3 objects (slow)
            limit: Max results per category

        Returns:
            Summary of detected anomalies with details
        """
        from datetime import datetime, timezone, timedelta

        start_time = datetime.now(timezone.utc)
        print(f"[RECONCILE:DETECT] Starting anomaly detection (stale_minutes={stale_minutes}, check_s3={check_s3})")

        results = {
            "run_at": start_time.isoformat(),
            "mode": "detection_only",
            "jobs_missing_history": [],
            "finalized_reservations_missing_ledger": [],
            "stale_held_reservations": [],
            "ready_unbilled_jobs": [],  # CRITICAL: Jobs that succeeded without billing
            "orphan_s3_objects": [],
            "warnings": [],
            "errors": [],
        }

        # 1. Jobs with terminal status but missing history_items
        try:
            jobs_missing = ReconciliationService._detect_jobs_missing_history(limit)
            results["jobs_missing_history"] = jobs_missing
            if jobs_missing:
                print(f"[RECONCILE:DETECT] WARNING: {len(jobs_missing)} jobs missing history items")
        except Exception as e:
            print(f"[RECONCILE:DETECT] ERROR in jobs_missing_history: {e}")
            results["errors"].append({"check": "jobs_missing_history", "error": str(e)})

        # 2. Finalized reservations without ledger entries
        try:
            missing_ledger = ReconciliationService._detect_finalized_without_ledger(limit)
            results["finalized_reservations_missing_ledger"] = missing_ledger
            if missing_ledger:
                print(f"[RECONCILE:DETECT] WARNING: {len(missing_ledger)} finalized reservations missing ledger entries")
        except Exception as e:
            print(f"[RECONCILE:DETECT] ERROR in finalized_without_ledger: {e}")
            results["errors"].append({"check": "finalized_without_ledger", "error": str(e)})

        # 3. Stale held reservations
        try:
            stale = ReconciliationService._detect_stale_reservations(stale_minutes, limit)
            results["stale_held_reservations"] = stale
            if stale:
                print(f"[RECONCILE:DETECT] WARNING: {len(stale)} held reservations older than {stale_minutes} minutes")
        except Exception as e:
            print(f"[RECONCILE:DETECT] ERROR in stale_reservations: {e}")
            results["errors"].append({"check": "stale_reservations", "error": str(e)})

        # 4. Ready unbilled jobs (CRITICAL)
        try:
            unbilled = ReconciliationService._detect_ready_unbilled_jobs(limit)
            results["ready_unbilled_jobs"] = unbilled
            if unbilled:
                print(f"[RECONCILE:DETECT] CRITICAL: {len(unbilled)} jobs completed without billing")
        except Exception as e:
            print(f"[RECONCILE:DETECT] ERROR in ready_unbilled: {e}")
            results["errors"].append({"check": "ready_unbilled", "error": str(e)})

        # 5. Orphan S3 objects (optional, slow)
        if check_s3:
            try:
                orphans = ReconciliationService._detect_orphan_s3_objects(limit)
                results["orphan_s3_objects"] = orphans
                if orphans:
                    print(f"[RECONCILE:DETECT] WARNING: {len(orphans)} S3 objects with no DB references")
            except Exception as e:
                print(f"[RECONCILE:DETECT] ERROR in orphan_s3: {e}")
                results["errors"].append({"check": "orphan_s3", "error": str(e)})
        else:
            results["orphan_s3_objects"] = {"skipped": True, "reason": "check_s3=false"}

        # Calculate totals
        end_time = datetime.now(timezone.utc)
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        results["summary"] = {
            "jobs_missing_history_count": len(results["jobs_missing_history"]) if isinstance(results["jobs_missing_history"], list) else 0,
            "finalized_missing_ledger_count": len(results["finalized_reservations_missing_ledger"]) if isinstance(results["finalized_reservations_missing_ledger"], list) else 0,
            "stale_reservations_count": len(results["stale_held_reservations"]) if isinstance(results["stale_held_reservations"], list) else 0,
            "ready_unbilled_count": len(results["ready_unbilled_jobs"]) if isinstance(results["ready_unbilled_jobs"], list) else 0,
            "orphan_s3_count": len(results["orphan_s3_objects"]) if isinstance(results["orphan_s3_objects"], list) else 0,
            "total_anomalies": 0,
            "errors_count": len(results["errors"]),
        }
        results["summary"]["total_anomalies"] = (
            results["summary"]["jobs_missing_history_count"]
            + results["summary"]["finalized_missing_ledger_count"]
            + results["summary"]["stale_reservations_count"]
            + results["summary"]["ready_unbilled_count"]
            + results["summary"]["orphan_s3_count"]
        )
        results["duration_ms"] = duration_ms

        print(
            f"[RECONCILE:DETECT] Completed: anomalies={results['summary']['total_anomalies']}, "
            f"errors={len(results['errors'])}, duration={duration_ms}ms"
        )

        return results

    @staticmethod
    def _detect_jobs_missing_history(limit: int) -> List[Dict[str, Any]]:
        """
        Detect jobs with terminal success status but no history_items row.
        """
        # Jobs with status indicating success but no history item
        missing = query_all(
            f"""
            SELECT
                j.id as job_id,
                j.identity_id,
                j.provider,
                j.action_code,
                j.status,
                j.prompt,
                j.created_at,
                j.updated_at
            FROM {Tables.JOBS} j
            WHERE j.status IN ('succeeded', 'completed', 'complete', 'ready', 'done')
              AND j.identity_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.HISTORY_ITEMS} h
                  WHERE h.identity_id = j.identity_id
                    AND (
                        h.payload->>'original_job_id' = j.id::text
                        OR h.payload->>'job_id' = j.id::text
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.MODELS} m
                  WHERE m.upstream_job_id = j.id::text
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.IMAGES} i
                  WHERE i.upstream_id = j.id::text
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.VIDEOS} v
                  WHERE v.upstream_id = j.id::text
              )
            ORDER BY j.created_at DESC
            LIMIT %s
            """,
            (limit,),
        )

        return [
            {
                "job_id": str(row["job_id"]),
                "identity_id": str(row["identity_id"]),
                "provider": row["provider"],
                "action_code": row["action_code"],
                "status": row["status"],
                "prompt": (row.get("prompt") or "")[:50] + "..." if row.get("prompt") and len(row.get("prompt", "")) > 50 else row.get("prompt"),
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "age_hours": round((datetime.now(timezone.utc) - row["created_at"].replace(tzinfo=timezone.utc)).total_seconds() / 3600, 1) if row.get("created_at") else None,
            }
            for row in missing
        ]

    @staticmethod
    def _detect_finalized_without_ledger(limit: int) -> List[Dict[str, Any]]:
        """
        Detect finalized reservations that don't have a corresponding ledger entry.

        When a reservation is finalized, there should be a ledger entry with:
        - entry_type = 'RESERVATION_FINALIZE'
        - ref_type = 'reservation'
        - ref_id = reservation.id
        """
        missing = query_all(
            f"""
            SELECT
                r.id as reservation_id,
                r.identity_id,
                r.action_code,
                r.cost_credits,
                r.status,
                r.ref_job_id,
                r.created_at,
                r.captured_at
            FROM {Tables.CREDIT_RESERVATIONS} r
            WHERE r.status = 'finalized'
              AND r.captured_at IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.LEDGER_ENTRIES} le
                  WHERE le.ref_type = 'reservation'
                    AND le.ref_id = r.id::text
                    AND le.entry_type = %s
              )
            ORDER BY r.created_at DESC
            LIMIT %s
            """,
            (LedgerEntryType.RESERVATION_FINALIZE, limit),
        )

        return [
            {
                "reservation_id": str(row["reservation_id"]),
                "identity_id": str(row["identity_id"]),
                "action_code": row["action_code"],
                "cost_credits": row["cost_credits"],
                "job_id": str(row["ref_job_id"]) if row.get("ref_job_id") else None,
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "captured_at": row["captured_at"].isoformat() if row.get("captured_at") else None,
                "issue": "finalized_but_no_ledger_debit",
            }
            for row in missing
        ]

    @staticmethod
    def _detect_stale_reservations(stale_minutes: int, limit: int) -> List[Dict[str, Any]]:
        """
        Detect held reservations older than stale_minutes.

        These may indicate:
        - Jobs that never completed
        - Finalization that failed silently
        - Orphaned reservations
        """
        stale_threshold = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)

        stale = query_all(
            f"""
            SELECT
                r.id as reservation_id,
                r.identity_id,
                r.action_code,
                r.cost_credits,
                r.ref_job_id,
                r.created_at,
                r.expires_at,
                j.status as job_status,
                j.updated_at as job_updated_at
            FROM {Tables.CREDIT_RESERVATIONS} r
            LEFT JOIN {Tables.JOBS} j ON j.id = r.ref_job_id
            WHERE r.status = 'held'
              AND r.created_at < %s
            ORDER BY r.created_at ASC
            LIMIT %s
            """,
            (stale_threshold, limit),
        )

        return [
            {
                "reservation_id": str(row["reservation_id"]),
                "identity_id": str(row["identity_id"]),
                "action_code": row["action_code"],
                "cost_credits": row["cost_credits"],
                "job_id": str(row["ref_job_id"]) if row.get("ref_job_id") else None,
                "job_status": row.get("job_status"),
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                "expires_at": row["expires_at"].isoformat() if row.get("expires_at") else None,
                "age_minutes": round((datetime.now(timezone.utc) - row["created_at"].replace(tzinfo=timezone.utc)).total_seconds() / 60, 1) if row.get("created_at") else None,
                "issue": "job_missing" if row.get("job_status") is None else f"job_status_{row.get('job_status')}",
            }
            for row in stale
        ]

    @staticmethod
    def _detect_orphan_s3_objects(limit: int) -> List[Dict[str, Any]]:
        """
        Detect S3 objects that have no corresponding DB reference.

        This is a best-effort check that samples S3 and looks for unreferenced objects.
        Note: This is slow and may have false positives due to timing issues.
        """
        try:
            from backend.services.s3_service import S3Service

            # Get S3 client and bucket
            s3 = S3Service.get_client()
            bucket = config.S3_BUCKET

            if not s3 or not bucket:
                return [{"skipped": True, "reason": "S3 not configured"}]

            orphans = []
            prefixes_to_check = ["images/", "models/", "videos/", "thumbnails/"]

            for prefix in prefixes_to_check:
                if len(orphans) >= limit:
                    break

                try:
                    # List objects with this prefix (sample first 100)
                    response = s3.list_objects_v2(
                        Bucket=bucket,
                        Prefix=prefix,
                        MaxKeys=min(100, limit - len(orphans)),
                    )

                    for obj in response.get("Contents", []):
                        if len(orphans) >= limit:
                            break

                        s3_key = obj["Key"]

                        # Check if this key is referenced in the DB
                        is_referenced = ReconciliationService._is_s3_key_referenced(s3_key)

                        if not is_referenced:
                            orphans.append({
                                "s3_key": s3_key,
                                "prefix": prefix,
                                "size_bytes": obj.get("Size", 0),
                                "last_modified": obj["LastModified"].isoformat() if obj.get("LastModified") else None,
                            })

                except Exception as e:
                    print(f"[RECONCILE:DETECT] Error listing S3 prefix {prefix}: {e}")

            return orphans

        except ImportError:
            return [{"skipped": True, "reason": "S3Service not available"}]
        except Exception as e:
            return [{"skipped": True, "reason": str(e)}]

    @staticmethod
    def _is_s3_key_referenced(s3_key: str) -> bool:
        """
        Check if an S3 key is referenced in any DB table.
        """
        # Check models
        if query_one(
            f"SELECT 1 FROM {Tables.MODELS} WHERE glb_s3_key = %s OR thumbnail_s3_key = %s LIMIT 1",
            (s3_key, s3_key),
        ):
            return True

        # Check images
        if query_one(
            f"SELECT 1 FROM {Tables.IMAGES} WHERE image_s3_key = %s OR thumbnail_s3_key = %s OR source_s3_key = %s LIMIT 1",
            (s3_key, s3_key, s3_key),
        ):
            return True

        # Check videos
        if query_one(
            f"SELECT 1 FROM {Tables.VIDEOS} WHERE video_s3_key = %s OR thumbnail_s3_key = %s LIMIT 1",
            (s3_key, s3_key),
        ):
            return True

        # Check history_items URLs (less reliable, URL may differ from key)
        # We check if the key appears in any URL field
        if query_one(
            f"""
            SELECT 1 FROM {Tables.HISTORY_ITEMS}
            WHERE thumbnail_url LIKE %s
               OR glb_url LIKE %s
               OR image_url LIKE %s
               OR video_url LIKE %s
            LIMIT 1
            """,
            (f"%{s3_key}%", f"%{s3_key}%", f"%{s3_key}%", f"%{s3_key}%"),
        ):
            return True

        return False

    # ─────────────────────────────────────────────────────────────
    # Stats (Original Method)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_stats() -> Dict[str, Any]:
        """
        Get current stats without applying any fixes.
        Useful for monitoring dashboards.
        """
        # Count purchases missing ledger entries
        purchases_missing = query_one(
            f"""
            SELECT COUNT(*) as count
            FROM {Tables.PURCHASES} p
            WHERE p.status IN ('paid', 'complete', 'completed')
              AND p.credits_granted > 0
              AND NOT EXISTS (
                  SELECT 1 FROM {Tables.LEDGER_ENTRIES} le
                  WHERE le.ref_type = 'purchase'
                    AND le.ref_id = p.id::text
                    AND le.entry_type = %s
              )
            """,
            (LedgerEntryType.PURCHASE_CREDIT,),
        )

        # Count wallet mismatches
        wallet_mismatches = query_one(
            f"""
            SELECT COUNT(*) as count
            FROM (
                SELECT
                    w.identity_id,
                    w.balance_credits as wallet_balance,
                    COALESCE(SUM(le.amount_credits), 0) as ledger_sum
                FROM {Tables.WALLETS} w
                LEFT JOIN {Tables.LEDGER_ENTRIES} le ON le.identity_id = w.identity_id
                GROUP BY w.identity_id, w.balance_credits
                HAVING w.balance_credits != COALESCE(SUM(le.amount_credits), 0)
            ) sub
            """,
        )

        # Count stale reservations
        stale_threshold = datetime.now(timezone.utc) - timedelta(
            minutes=ReconciliationService.STALE_RESERVATION_MINUTES
        )
        stale_reservations = query_one(
            f"""
            SELECT COUNT(*) as count
            FROM {Tables.CREDIT_RESERVATIONS} r
            LEFT JOIN {Tables.JOBS} j ON j.id = r.ref_job_id
            WHERE r.status = %s
              AND r.created_at < %s
              AND (
                  j.id IS NULL
                  OR j.status IN ('failed', 'cancelled', 'error')
              )
            """,
            (ReservationStatus.HELD, stale_threshold),
        )

        # Count ready_unbilled jobs
        ready_unbilled = query_one(
            f"""
            SELECT COUNT(*) as count
            FROM {Tables.JOBS} j
            WHERE (
                j.status = 'ready_unbilled'
                OR (
                    j.status IN ('ready', 'done', 'succeeded', 'completed')
                    AND j.reservation_id IS NULL
                    AND j.cost_credits > 0
                    AND j.identity_id IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM {Tables.CREDIT_RESERVATIONS} r
                        WHERE r.ref_job_id = j.id
                          AND r.status IN ('finalized', 'held')
                    )
                )
            )
            """,
        )

        return {
            "purchases_missing_ledger": purchases_missing["count"] if purchases_missing else 0,
            "wallet_mismatches": wallet_mismatches["count"] if wallet_mismatches else 0,
            "stale_reservations": stale_reservations["count"] if stale_reservations else 0,
            "ready_unbilled_jobs": ready_unbilled["count"] if ready_unbilled else 0,
        }

    # ═══════════════════════════════════════════════════════════════
    # MOLLIE PAYMENT RECONCILIATION
    # Compares Mollie API payments to our DB and fixes discrepancies
    # ═══════════════════════════════════════════════════════════════

    @staticmethod
    def reconcile_mollie_payments(
        days_back: int = 30,
        dry_run: bool = False,
        run_type: str = "full",
    ) -> Dict[str, Any]:
        """
        Reconcile Mollie payments against our database.

        Fetches recent payments from Mollie API and ensures:
        1. Paid one-time purchases have corresponding purchase + ledger + wallet
        2. Paid subscriptions have corresponding subscription + cycles + wallet
        3. Refunded/charged-back payments have proper revocation entries

        Args:
            days_back: How many days of payments to scan (default 30)
            dry_run: If True, detect issues but don't fix them
            run_type: Type of run for logging (full, mollie_only, manual)

        Returns:
            Summary of reconciliation with fixes applied
        """
        import requests
        from backend.config import config

        start_time = datetime.now(timezone.utc)
        run_id = None

        # Create reconciliation run record
        if not dry_run:
            run_id = ReconciliationService._create_reconciliation_run(
                run_type=run_type,
                days_back=days_back,
            )

        results = {
            "run_id": str(run_id) if run_id else None,
            "run_at": start_time.isoformat(),
            "days_back": days_back,
            "dry_run": dry_run,
            "scanned_count": 0,
            "fixed_count": 0,
            "errors_count": 0,
            "purchases_fixed": [],
            "subscriptions_fixed": [],
            "refunds_fixed": [],
            "wallets_fixed": [],
            "errors": [],
        }

        # Check if Mollie is configured
        if not config.MOLLIE_CONFIGURED:
            results["errors"].append({"error": "Mollie not configured"})
            results["errors_count"] = 1
            print("[RECONCILE:MOLLIE] Mollie not configured, skipping")
            return results

        print(f"[RECONCILE:MOLLIE] Starting reconciliation (days_back={days_back}, dry_run={dry_run})")

        try:
            # Fetch payments from Mollie API
            payments = ReconciliationService._fetch_mollie_payments(days_back)
            results["scanned_count"] = len(payments)
            print(f"[RECONCILE:MOLLIE] Fetched {len(payments)} payments from Mollie")

            # Process each payment
            for payment in payments:
                try:
                    fix_result = ReconciliationService._reconcile_mollie_payment(
                        payment=payment,
                        run_id=run_id,
                        dry_run=dry_run,
                    )

                    if fix_result and fix_result.get("fixed"):
                        results["fixed_count"] += 1
                        fix_type = fix_result.get("fix_type", "unknown")

                        if fix_type == "purchase_created":
                            results["purchases_fixed"].append(fix_result)
                        elif fix_type == "subscription_granted":
                            results["subscriptions_fixed"].append(fix_result)
                        elif fix_type in ("refund_applied", "chargeback_applied"):
                            results["refunds_fixed"].append(fix_result)
                        elif fix_type == "ledger_created":
                            results["wallets_fixed"].append(fix_result)

                except Exception as payment_err:
                    print(f"[RECONCILE:MOLLIE] Error processing payment {payment.get('id')}: {payment_err}")
                    results["errors"].append({
                        "payment_id": payment.get("id"),
                        "error": str(payment_err),
                    })
                    results["errors_count"] += 1

        except Exception as e:
            print(f"[RECONCILE:MOLLIE] Fatal error: {e}")
            results["errors"].append({"error": str(e)})
            results["errors_count"] += 1

        # Finalize reconciliation run
        end_time = datetime.now(timezone.utc)
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        results["duration_ms"] = duration_ms

        if run_id and not dry_run:
            ReconciliationService._finalize_reconciliation_run(
                run_id=run_id,
                results=results,
            )

        # Send admin alert if fixes were applied
        if results["fixed_count"] > 0 and not dry_run:
            ReconciliationService._send_mollie_reconciliation_alert(results)

        print(
            f"[RECONCILE:MOLLIE] Completed: scanned={results['scanned_count']}, "
            f"fixed={results['fixed_count']}, errors={results['errors_count']}, "
            f"duration={duration_ms}ms"
        )

        return results

    @staticmethod
    def _fetch_mollie_payments(days_back: int = 30) -> List[Dict[str, Any]]:
        """
        Fetch recent payments from Mollie API.

        Args:
            days_back: Number of days to look back

        Returns:
            List of Mollie payment objects
        """
        import requests
        from backend.config import config

        payments = []
        from_date = datetime.now(timezone.utc) - timedelta(days=days_back)

        headers = {
            "Authorization": f"Bearer {config.MOLLIE_API_KEY}",
            "Content-Type": "application/json",
        }

        # Mollie API pagination
        url = "https://api.mollie.com/v2/payments"
        params = {"limit": 250}  # Max per page

        while url:
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)

                if response.status_code != 200:
                    print(f"[RECONCILE:MOLLIE] API error: {response.status_code} - {response.text}")
                    break

                data = response.json()
                page_payments = data.get("_embedded", {}).get("payments", [])

                for payment in page_payments:
                    # Parse created_at and check if within range
                    created_at_str = payment.get("createdAt", "")
                    if created_at_str:
                        try:
                            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                            if created_at < from_date:
                                # Payments are sorted by date desc, so we can stop here
                                url = None
                                break
                            payments.append(payment)
                        except ValueError:
                            payments.append(payment)  # Include if date parse fails
                    else:
                        payments.append(payment)

                # Get next page URL
                if url:
                    links = data.get("_links", {})
                    next_link = links.get("next", {})
                    url = next_link.get("href") if next_link else None
                    params = None  # URL already has params

            except requests.RequestException as e:
                print(f"[RECONCILE:MOLLIE] Request error: {e}")
                break

        return payments

    @staticmethod
    def _reconcile_mollie_payment(
        payment: Dict[str, Any],
        run_id: Optional[str],
        dry_run: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Reconcile a single Mollie payment against our database.

        Args:
            payment: Mollie payment object
            run_id: Reconciliation run ID for logging
            dry_run: If True, detect but don't fix

        Returns:
            Dict with fix info if fixed, None if no fix needed
        """
        status = payment.get("status")
        metadata = payment.get("metadata", {}) or {}

        # Skip non-actionable statuses
        if status not in ("paid", "refunded", "charged_back"):
            return None

        identity_id = metadata.get("identity_id")
        payment_type = metadata.get("type", "purchase")  # 'purchase' or 'subscription'

        if not identity_id:
            # No identity_id in metadata - can't reconcile
            return None

        # Handle based on status
        if status == "paid":
            if payment_type == "subscription":
                return ReconciliationService._reconcile_subscription_payment(
                    payment=payment,
                    run_id=run_id,
                    dry_run=dry_run,
                )
            else:
                return ReconciliationService._reconcile_purchase_payment(
                    payment=payment,
                    run_id=run_id,
                    dry_run=dry_run,
                )
        elif status in ("refunded", "charged_back"):
            return ReconciliationService._reconcile_refund_payment(
                payment=payment,
                run_id=run_id,
                dry_run=dry_run,
            )

        return None

    @staticmethod
    def _reconcile_purchase_payment(
        payment: Dict[str, Any],
        run_id: Optional[str],
        dry_run: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Reconcile a paid one-time purchase payment.

        Checks if purchase row exists. If not, creates it with ledger entry.
        """
        from backend.services.purchase_service import PurchaseService

        payment_id = payment.get("id")
        metadata = payment.get("metadata", {}) or {}
        identity_id = metadata.get("identity_id")
        plan_code = metadata.get("plan_code")
        plan_id = metadata.get("plan_id")
        credits_str = metadata.get("credits", "0")
        customer_email = metadata.get("email")

        if not identity_id or not plan_code or not payment_id:
            return None

        credits_amount = int(credits_str) if credits_str else 0
        amount_data = payment.get("amount", {})
        amount_gbp = float(amount_data.get("value", 0))

        # Check if purchase already exists
        existing = PurchaseService.get_purchase_by_provider_id(payment_id)
        if existing:
            # Already processed - check if ledger entry exists
            ledger_check = ReconciliationService._check_purchase_has_ledger(str(existing["id"]))
            if ledger_check:
                return None  # All good

            # Purchase exists but no ledger - will be caught by existing reconciliation
            print(f"[RECONCILE:MOLLIE] Purchase {payment_id} exists but missing ledger (will be fixed by safety job)")
            return None

        # Purchase doesn't exist - need to create it
        print(f"[RECONCILE:MOLLIE] Found paid payment {payment_id} without purchase record")

        if dry_run:
            return {
                "fixed": True,
                "dry_run": True,
                "fix_type": "purchase_created",
                "payment_id": payment_id,
                "identity_id": identity_id,
                "plan_code": plan_code,
                "credits": credits_amount,
                "amount_gbp": amount_gbp,
            }

        # Create the purchase
        try:
            from backend.services.mollie_service import MollieService
            result = MollieService._record_mollie_purchase(
                identity_id=identity_id,
                plan_id=plan_id or "",
                plan_code=plan_code,
                provider_payment_id=payment_id,
                amount_gbp=amount_gbp,
                credits_granted=credits_amount,
                customer_email=customer_email,
            )

            if result and not result.get("was_existing"):
                # Log the fix
                ReconciliationService._log_reconciliation_fix(
                    run_id=run_id,
                    provider="mollie",
                    provider_payment_id=payment_id,
                    fix_type="purchase_created",
                    identity_id=identity_id,
                    credits_delta=credits_amount,
                    plan_code=plan_code,
                    amount_gbp=amount_gbp,
                    mollie_status="paid",
                    details={
                        "customer_email": customer_email,
                        "purchase_id": result["purchase"]["id"],
                    },
                )

                print(f"[RECONCILE:MOLLIE] Created missing purchase for payment {payment_id}: credits={credits_amount}")

                return {
                    "fixed": True,
                    "fix_type": "purchase_created",
                    "payment_id": payment_id,
                    "identity_id": identity_id,
                    "plan_code": plan_code,
                    "credits": credits_amount,
                    "purchase_id": result["purchase"]["id"],
                }

        except Exception as e:
            print(f"[RECONCILE:MOLLIE] Error creating purchase for {payment_id}: {e}")
            raise

        return None

    @staticmethod
    def _reconcile_subscription_payment(
        payment: Dict[str, Any],
        run_id: Optional[str],
        dry_run: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Reconcile a paid subscription payment.

        PERIOD-SAFE: Grants credits for the correct monthly cycle based on billing_day.
        ID-CORRECT: Does NOT confuse payment IDs (tr_*) with subscription IDs (sub_*).

        Logic:
        1. Parse payment timestamp (paidAt or createdAt)
        2. Find subscription by metadata.subscription_id or identity_id + plan_code lookup
        3. Calculate correct monthly period using billing_day
        4. Check if this payment already granted a cycle (by provider_payment_id)
        5. Check if this period already has a cycle (by subscription_id + period_start)
        6. Grant monthly credits (never 365-day cycles, even for yearly plans)
        """
        from backend.services.subscription_service import SubscriptionService, SUBSCRIPTION_PLANS

        payment_id = payment.get("id")
        metadata = payment.get("metadata", {}) or {}
        identity_id = metadata.get("identity_id")
        plan_code = metadata.get("plan_code")
        subscription_id_from_meta = metadata.get("subscription_id")  # Internal UUID if present

        if not identity_id or not plan_code or not payment_id:
            return None

        plan = SUBSCRIPTION_PLANS.get(plan_code)
        if not plan:
            print(f"[RECONCILE:SUB] Unknown plan_code: {plan_code}")
            return None

        credits_per_month = plan.get("credits_per_month", 0)

        # ── Step 1: Parse payment timestamp ───────────────────────
        paid_at_str = payment.get("paidAt") or payment.get("createdAt")
        if not paid_at_str:
            print(f"[RECONCILE:SUB] Payment {payment_id} has no timestamp")
            return None

        try:
            payment_ts = datetime.fromisoformat(paid_at_str.replace("Z", "+00:00"))
        except ValueError:
            print(f"[RECONCILE:SUB] Payment {payment_id} has invalid timestamp: {paid_at_str}")
            return None

        # ── Step 2: Find the subscription ─────────────────────────
        subscription = None

        # Priority 1: Use subscription_id from metadata (most reliable)
        if subscription_id_from_meta:
            subscription = query_one(
                f"""
                SELECT id, identity_id, plan_code, status, billing_day,
                       credits_remaining_months, customer_email
                FROM {Tables.SUBSCRIPTIONS}
                WHERE id::text = %s
                """,
                (subscription_id_from_meta,),
            )
            if subscription:
                print(f"[RECONCILE:SUB] Found subscription {subscription_id_from_meta} from metadata")

        # Priority 2: Look up by identity_id + plan_code
        if not subscription:
            subscription = SubscriptionService.find_subscription_for_payment(
                identity_id=identity_id,
                plan_code=plan_code,
                provider="mollie",
            )
            if subscription:
                print(f"[RECONCILE:SUB] Found subscription {subscription['id']} by identity+plan lookup")

        # Priority 3: No subscription found - need to create one
        if not subscription:
            print(f"[RECONCILE:SUB] No subscription found for payment {payment_id}, creating new one")

            if dry_run:
                return {
                    "fixed": True,
                    "dry_run": True,
                    "fix_type": "subscription_cycle_granted",
                    "payment_id": payment_id,
                    "identity_id": identity_id,
                    "plan_code": plan_code,
                    "credits": credits_per_month,
                    "note": "Would create new subscription and grant cycle",
                }

            try:
                from backend.services.mollie_service import MollieService
                result = MollieService._handle_subscription_paid(payment)

                if result and not result.get("was_existing"):
                    ReconciliationService._log_reconciliation_fix(
                        run_id=run_id,
                        provider="mollie",
                        provider_payment_id=payment_id,
                        fix_type="subscription_cycle_granted",
                        identity_id=identity_id,
                        credits_delta=credits_per_month,
                        plan_code=plan_code,
                        amount_gbp=plan.get("price_gbp", 0),
                        mollie_status="paid",
                        details={
                            "subscription_id": result.get("purchase_id"),
                            "note": "Created new subscription",
                        },
                    )

                    return {
                        "fixed": True,
                        "fix_type": "subscription_cycle_granted",
                        "payment_id": payment_id,
                        "identity_id": identity_id,
                        "plan_code": plan_code,
                        "credits": credits_per_month,
                        "subscription_id": result.get("purchase_id"),
                    }

            except Exception as e:
                print(f"[RECONCILE:SUB] Error creating subscription for {payment_id}: {e}")
                raise

            return None

        # ── Step 3: Check if payment already granted a cycle ──────
        subscription_id = str(subscription["id"])

        if SubscriptionService.check_payment_already_granted("mollie", payment_id):
            print(f"[RECONCILE:SUB] Payment {payment_id} already granted a cycle")
            return None

        # ── Step 4: Calculate correct monthly period ──────────────
        billing_day = subscription.get("billing_day") or payment_ts.day

        period_start, period_end = SubscriptionService.calculate_cycle_period(
            payment_ts=payment_ts,
            billing_day=billing_day,
        )

        print(
            f"[RECONCILE:SUB] Payment {payment_id} maps to period "
            f"{period_start.date()} → {period_end.date()} (billing_day={billing_day})"
        )

        # ── Step 5: Check if cycle already exists for this period ─
        if SubscriptionService.check_cycle_exists(subscription_id, period_start):
            print(f"[RECONCILE:SUB] Cycle already exists for {subscription_id} period {period_start.date()}")
            return None

        # ── Step 6: Grant the monthly credits ─────────────────────
        print(
            f"[RECONCILE:SUB] Granting {credits_per_month} credits for subscription "
            f"{subscription_id} period {period_start.date()} → {period_end.date()}"
        )

        if dry_run:
            return {
                "fixed": True,
                "dry_run": True,
                "fix_type": "subscription_cycle_granted",
                "payment_id": payment_id,
                "identity_id": identity_id,
                "plan_code": plan_code,
                "credits": credits_per_month,
                "subscription_id": subscription_id,
                "period_start": period_start.isoformat(),
                "period_end": period_end.isoformat(),
            }

        try:
            grant_result = SubscriptionService.grant_subscription_credits(
                subscription_id=subscription_id,
                period_start=period_start,
                period_end=period_end,
                provider="mollie",
                provider_payment_id=payment_id,
            )

            if grant_result and grant_result.get("credits_granted"):
                ReconciliationService._log_reconciliation_fix(
                    run_id=run_id,
                    provider="mollie",
                    provider_payment_id=payment_id,
                    fix_type="subscription_cycle_granted",
                    identity_id=identity_id,
                    credits_delta=credits_per_month,
                    plan_code=plan_code,
                    amount_gbp=plan.get("price_gbp", 0),
                    mollie_status="paid",
                    details={
                        "subscription_id": subscription_id,
                        "cycle_id": grant_result.get("cycle_id"),
                        "period_start": period_start.isoformat(),
                        "period_end": period_end.isoformat(),
                    },
                )

                return {
                    "fixed": True,
                    "fix_type": "subscription_cycle_granted",
                    "payment_id": payment_id,
                    "identity_id": identity_id,
                    "plan_code": plan_code,
                    "credits": credits_per_month,
                    "subscription_id": subscription_id,
                    "cycle_id": grant_result.get("cycle_id"),
                }

        except Exception as e:
            print(f"[RECONCILE:SUB] Error granting subscription credits for {payment_id}: {e}")
            raise

        return None

    @staticmethod
    def _reconcile_refund_payment(
        payment: Dict[str, Any],
        run_id: Optional[str],
        dry_run: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Reconcile a refunded or charged-back payment.

        Checks if revocation ledger entry exists. If not, applies it.
        """
        from backend.services.mollie_service import MollieService

        payment_id = payment.get("id")
        status = payment.get("status")
        metadata = payment.get("metadata", {}) or {}
        identity_id = metadata.get("identity_id")

        if not identity_id:
            return None

        # Check if refund ledger entry already exists
        entry_type = "chargeback" if status == "charged_back" else "refund"
        existing_refund = query_one(
            f"""
            SELECT id FROM {Tables.LEDGER_ENTRIES}
            WHERE identity_id = %s
              AND entry_type = %s
              AND meta->>'payment_id' = %s
            """,
            (identity_id, entry_type, payment_id),
        )

        if existing_refund:
            return None  # Already processed

        print(f"[RECONCILE:MOLLIE] Found {status} payment {payment_id} without revocation entry")

        if dry_run:
            return {
                "fixed": True,
                "dry_run": True,
                "fix_type": f"{entry_type}_applied",
                "payment_id": payment_id,
                "identity_id": identity_id,
            }

        # Apply the refund
        try:
            result = MollieService._handle_payment_refunded(payment)

            if result and not result.get("was_existing"):
                ReconciliationService._log_reconciliation_fix(
                    run_id=run_id,
                    provider="mollie",
                    provider_payment_id=payment_id,
                    fix_type=f"{entry_type}_applied",
                    identity_id=identity_id,
                    credits_delta=-result.get("credits_revoked", 0),
                    mollie_status=status,
                    details={"new_balance": result.get("new_balance")},
                )

                return {
                    "fixed": True,
                    "fix_type": f"{entry_type}_applied",
                    "payment_id": payment_id,
                    "identity_id": identity_id,
                    "credits_revoked": result.get("credits_revoked", 0),
                }

        except Exception as e:
            print(f"[RECONCILE:MOLLIE] Error applying {status} for {payment_id}: {e}")
            raise

        return None

    @staticmethod
    def _check_purchase_has_ledger(purchase_id: str) -> bool:
        """Check if a purchase has a corresponding ledger entry."""
        result = query_one(
            f"""
            SELECT id FROM {Tables.LEDGER_ENTRIES}
            WHERE ref_type = 'purchase' AND ref_id = %s AND entry_type = %s
            """,
            (purchase_id, LedgerEntryType.PURCHASE_CREDIT),
        )
        return result is not None

    # ────────────────────────────────────────────────��────────────
    # Reconciliation Run Management
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _create_reconciliation_run(run_type: str, days_back: int) -> Optional[str]:
        """Create a new reconciliation run record."""
        try:
            result = execute_returning(
                """
                INSERT INTO timrx_billing.reconciliation_runs
                (run_type, days_back, status, started_at)
                VALUES (%s, %s, 'running', NOW())
                RETURNING id
                """,
                (run_type, days_back),
            )
            if result:
                return str(result["id"])
        except Exception as e:
            print(f"[RECONCILE] Error creating run record: {e}")
        return None

    @staticmethod
    def _finalize_reconciliation_run(run_id: str, results: Dict[str, Any]):
        """Update reconciliation run with final results."""
        try:
            notes = f"Scanned {results['scanned_count']} payments, fixed {results['fixed_count']}"
            if results['errors_count'] > 0:
                notes += f", {results['errors_count']} errors"

            execute(
                """
                UPDATE timrx_billing.reconciliation_runs
                SET finished_at = NOW(),
                    status = %s,
                    scanned_count = %s,
                    fixed_count = %s,
                    errors_count = %s,
                    purchases_fixed = %s,
                    subscriptions_fixed = %s,
                    refunds_fixed = %s,
                    wallets_fixed = %s,
                    notes = %s,
                    error_details = %s
                WHERE id = %s
                """,
                (
                    "failed" if results["errors_count"] > 0 and results["fixed_count"] == 0 else "completed",
                    results["scanned_count"],
                    results["fixed_count"],
                    results["errors_count"],
                    len(results.get("purchases_fixed", [])),
                    len(results.get("subscriptions_fixed", [])),
                    len(results.get("refunds_fixed", [])),
                    len(results.get("wallets_fixed", [])),
                    notes,
                    json.dumps(results.get("errors", [])) if results.get("errors") else None,
                    run_id,
                ),
            )
        except Exception as e:
            print(f"[RECONCILE] Error finalizing run record: {e}")

    @staticmethod
    def _log_reconciliation_fix(
        run_id: Optional[str],
        provider: str,
        provider_payment_id: str,
        fix_type: str,
        identity_id: Optional[str] = None,
        credits_delta: int = 0,
        plan_code: Optional[str] = None,
        amount_gbp: Optional[float] = None,
        mollie_status: Optional[str] = None,
        details: Optional[Dict] = None,
    ):
        """Log a reconciliation fix (idempotent via unique constraint)."""
        try:
            execute(
                """
                INSERT INTO timrx_billing.reconciliation_fixes
                (run_id, provider, provider_payment_id, fix_type,
                 identity_id, credits_delta, plan_code, amount_gbp,
                 mollie_status, details_json, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (provider, provider_payment_id, fix_type) DO NOTHING
                """,
                (
                    run_id,
                    provider,
                    provider_payment_id,
                    fix_type,
                    identity_id,
                    credits_delta,
                    plan_code,
                    amount_gbp,
                    mollie_status,
                    json.dumps(details) if details else None,
                ),
            )
        except Exception as e:
            print(f"[RECONCILE] Error logging fix: {e}")

    @staticmethod
    def _send_mollie_reconciliation_alert(results: Dict[str, Any]):
        """Send admin email about Mollie reconciliation fixes."""
        try:
            from backend.emailer import notify_admin

            fixes_summary = []

            if results.get("purchases_fixed"):
                count = len(results["purchases_fixed"])
                total_credits = sum(f.get("credits", 0) for f in results["purchases_fixed"])
                fixes_summary.append(f"- {count} missing purchase(s) created (+{total_credits:,} credits)")

            if results.get("subscriptions_fixed"):
                count = len(results["subscriptions_fixed"])
                total_credits = sum(f.get("credits", 0) for f in results["subscriptions_fixed"])
                fixes_summary.append(f"- {count} missing subscription credits granted (+{total_credits:,} credits)")

            if results.get("refunds_fixed"):
                count = len(results["refunds_fixed"])
                total_credits = sum(f.get("credits_revoked", 0) for f in results["refunds_fixed"])
                fixes_summary.append(f"- {count} refund/chargeback(s) applied (-{total_credits:,} credits)")

            if results.get("wallets_fixed"):
                count = len(results["wallets_fixed"])
                fixes_summary.append(f"- {count} wallet balance(s) corrected")

            if not fixes_summary:
                return

            message = "The Mollie reconciliation job applied the following fixes:\n\n"
            message += "\n".join(fixes_summary)
            message += f"\n\nPayments scanned: {results['scanned_count']}"
            message += f"\nDuration: {results.get('duration_ms', 0)}ms"

            data = {
                "Run ID": results.get("run_id", "N/A"),
                "Days Back": results.get("days_back", 30),
                "Scanned": results["scanned_count"],
                "Fixed": results["fixed_count"],
                "Errors": results["errors_count"],
            }

            # Add sample fixes
            if results.get("purchases_fixed"):
                sample = results["purchases_fixed"][:3]
                data["Purchases Fixed (sample)"] = json.dumps(sample, default=str)

            if results.get("subscriptions_fixed"):
                sample = results["subscriptions_fixed"][:3]
                data["Subscriptions Fixed (sample)"] = json.dumps(sample, default=str)

            notify_admin(
                subject="Mollie Reconciliation: Fixes Applied",
                message=message,
                data=data,
            )

            print("[RECONCILE:MOLLIE] Admin alert sent")

        except Exception as e:
            print(f"[RECONCILE:MOLLIE] Failed to send admin alert: {e}")

    # ─────────────────────────────────────────────────────────────
    # Full Reconciliation (Safety + Mollie)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def reconcile_full(
        days_back: int = 30,
        dry_run: bool = False,
        send_alert: bool = True,
    ) -> Dict[str, Any]:
        """
        Run full reconciliation: both safety checks AND Mollie API comparison.

        This is the recommended daily job.

        Args:
            days_back: Days of Mollie payments to scan
            dry_run: If True, detect issues but don't fix
            send_alert: If True, send admin email if fixes applied

        Returns:
            Combined results from both reconciliation phases
        """
        print(f"[RECONCILE] Starting full reconciliation (days_back={days_back}, dry_run={dry_run})")

        start_time = datetime.now(timezone.utc)

        results = {
            "run_at": start_time.isoformat(),
            "dry_run": dry_run,
            "safety_results": {},
            "mollie_results": {},
            "total_fixes": 0,
            "total_errors": 0,
        }

        # Phase 1: Safety reconciliation (existing DB checks)
        try:
            safety_results = ReconciliationService.reconcile_safety(
                dry_run=dry_run,
                send_alert=False,  # We'll send combined alert
            )
            results["safety_results"] = safety_results
            results["total_fixes"] += safety_results.get("total_fixes", 0)
            results["total_errors"] += safety_results.get("total_errors", 0)
        except Exception as e:
            print(f"[RECONCILE] Safety reconciliation error: {e}")
            results["safety_results"] = {"error": str(e)}
            results["total_errors"] += 1

        # Phase 2: Mollie API reconciliation
        try:
            mollie_results = ReconciliationService.reconcile_mollie_payments(
                days_back=days_back,
                dry_run=dry_run,
                run_type="full",
            )
            results["mollie_results"] = mollie_results
            results["total_fixes"] += mollie_results.get("fixed_count", 0)
            results["total_errors"] += mollie_results.get("errors_count", 0)
        except Exception as e:
            print(f"[RECONCILE] Mollie reconciliation error: {e}")
            results["mollie_results"] = {"error": str(e)}
            results["total_errors"] += 1

        end_time = datetime.now(timezone.utc)
        results["duration_ms"] = int((end_time - start_time).total_seconds() * 1000)

        print(
            f"[RECONCILE] Full reconciliation completed: fixes={results['total_fixes']}, "
            f"errors={results['total_errors']}, duration={results['duration_ms']}ms"
        )

        # Send combined admin alert if requested and fixes were applied
        if send_alert and results["total_fixes"] > 0 and not dry_run:
            ReconciliationService._send_full_reconciliation_alert(results)

        return results

    @staticmethod
    def _send_full_reconciliation_alert(results: Dict[str, Any]):
        """Send admin email summarizing full reconciliation."""
        try:
            from backend.emailer import notify_admin

            safety = results.get("safety_results", {})
            mollie = results.get("mollie_results", {})

            message = "Full reconciliation completed:\n\n"

            # Safety phase summary
            safety_fixes = safety.get("total_fixes", 0)
            if safety_fixes > 0:
                message += f"Safety Phase: {safety_fixes} fix(es)\n"
                if safety.get("purchases_missing_ledger"):
                    message += f"  - {len(safety['purchases_missing_ledger'])} ledger entries created\n"
                if safety.get("wallet_mismatches_fixed"):
                    message += f"  - {len(safety['wallet_mismatches_fixed'])} wallet balances fixed\n"
                if safety.get("stale_reservations_released"):
                    message += f"  - {len(safety['stale_reservations_released'])} reservations released\n"

            # Mollie phase summary
            mollie_fixes = mollie.get("fixed_count", 0)
            if mollie_fixes > 0:
                message += f"\nMollie Phase: {mollie_fixes} fix(es)\n"
                if mollie.get("purchases_fixed"):
                    message += f"  - {len(mollie['purchases_fixed'])} missing purchases created\n"
                if mollie.get("subscriptions_fixed"):
                    message += f"  - {len(mollie['subscriptions_fixed'])} subscription credits granted\n"
                if mollie.get("refunds_fixed"):
                    message += f"  - {len(mollie['refunds_fixed'])} refunds applied\n"

            message += f"\nTotal fixes: {results['total_fixes']}"
            message += f"\nDuration: {results.get('duration_ms', 0)}ms"

            notify_admin(
                subject="Reconciliation Complete: Fixes Applied",
                message=message,
                data={
                    "Total Fixes": results["total_fixes"],
                    "Safety Fixes": safety_fixes,
                    "Mollie Fixes": mollie_fixes,
                    "Errors": results["total_errors"],
                },
            )

        except Exception as e:
            print(f"[RECONCILE] Failed to send full reconciliation alert: {e}")
