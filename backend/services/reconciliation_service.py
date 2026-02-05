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
    fetch_one, fetch_all, transaction, query_one, query_all, execute, Tables
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

        # Calculate totals
        total_fixes = (
            len(results["purchases_missing_ledger"])
            + len(results["wallet_mismatches_fixed"])
            + len(results["stale_reservations_released"])
            + len(results["missing_history_items_created"])
        )

        end_time = datetime.now(timezone.utc)
        duration_ms = int((end_time - start_time).total_seconds() * 1000)

        results["total_fixes"] = total_fixes
        results["total_errors"] = len(results["errors"])
        results["duration_ms"] = duration_ms

        print(
            f"[RECONCILE] Completed: fixes={total_fixes}, errors={len(results['errors'])}, "
            f"duration={duration_ms}ms"
        )

        # Send admin alert if fixes were applied
        if send_alert and total_fixes > 0 and not dry_run:
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

        # 4. Orphan S3 objects (optional, slow)
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
            "orphan_s3_count": len(results["orphan_s3_objects"]) if isinstance(results["orphan_s3_objects"], list) else 0,
            "total_anomalies": 0,
            "errors_count": len(results["errors"]),
        }
        results["summary"]["total_anomalies"] = (
            results["summary"]["jobs_missing_history_count"]
            + results["summary"]["finalized_missing_ledger_count"]
            + results["summary"]["stale_reservations_count"]
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

        return {
            "purchases_missing_ledger": purchases_missing["count"] if purchases_missing else 0,
            "wallet_mismatches": wallet_mismatches["count"] if wallet_mismatches else 0,
            "stale_reservations": stale_reservations["count"] if stale_reservations else 0,
        }
