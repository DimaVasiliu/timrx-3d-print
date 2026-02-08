"""
Reservation Service - Manages credit reservations for jobs.

Flow:
1. reserve_credits() - Hold credits when job starts
2. finalize_reservation() - Capture credits when job completes (spend from wallet)
3. release_reservation() - Return credits when job fails/cancels

Rules:
- One active reservation per (identity_id, job_id, action_code)
- Idempotent: repeated calls return existing reservation
- wallet.balance >= required credits (checked at reservation time)
- Expired reservations are automatically released

Statuses:
- held: Credits are reserved, job in progress
- finalized: Job completed, credits captured (spent)
- released: Job failed/cancelled, credits returned
"""

from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
import json
import uuid

from backend.db import fetch_one, fetch_all, transaction, query_one, query_all, Tables
from backend.services.wallet_service import WalletService, LedgerEntryType, CreditType, get_credit_type_for_action
from backend.services.pricing_service import PricingService
from backend.config import config


def _derive_provider_from_action_code(action_code: str) -> str:
    """Derive provider name from action_code prefix."""
    if action_code.startswith("MESHY_"):
        return "meshy"
    elif action_code.startswith("OPENAI_"):
        return "openai"
    elif action_code.startswith("VIDEO_") or action_code == "GEMINI_VIDEO":
        return "video"
    else:
        return "unknown"


class ReservationStatus:
    """Valid reservation statuses."""
    HELD = "held"
    FINALIZED = "finalized"
    RELEASED = "released"


class ReservationService:
    """
    Service for managing credit reservations.

    Ensures atomic hold/release of credits to prevent overspend
    during async job processing.
    """

    # Default reservation expiry (can be overridden via config)
    DEFAULT_EXPIRY_MINUTES = 20

    # ─────────────────────────────────────────────────────────────
    # Read Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_reservation(reservation_id: str) -> Optional[Dict[str, Any]]:
        """Get a reservation by ID."""
        return query_one(
            f"""
            SELECT id, identity_id, action_code, cost_credits, status,
                   created_at, expires_at, captured_at, released_at, ref_job_id, meta
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE id = %s
            """,
            (reservation_id,),
        )

    @staticmethod
    def get_reservation_by_job(
        identity_id: str,
        job_id: str,
        action_code: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get a reservation by its associated job ID and action.
        Used for idempotency checks.
        """
        return query_one(
            f"""
            SELECT id, identity_id, action_code, cost_credits, status,
                   created_at, expires_at, captured_at, released_at, ref_job_id, meta
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE identity_id = %s
              AND ref_job_id = %s
              AND action_code = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (identity_id, job_id, action_code),
        )

    @staticmethod
    def get_active_reservation_for_job(
        identity_id: str,
        job_id: str,
        action_code: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Get an active (held) reservation for a specific job.
        Used for idempotency - if reservation already exists, return it.
        """
        return query_one(
            f"""
            SELECT id, identity_id, action_code, cost_credits, status,
                   created_at, expires_at, captured_at, released_at, ref_job_id, meta
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE identity_id = %s
              AND ref_job_id = %s
              AND action_code = %s
              AND status = %s
              AND expires_at > NOW()
            """,
            (identity_id, job_id, action_code, ReservationStatus.HELD),
        )

    @staticmethod
    def get_active_reservations(identity_id: str) -> List[Dict[str, Any]]:
        """Get all active (held) reservations for an identity."""
        return query_all(
            f"""
            SELECT id, identity_id, action_code, cost_credits, status,
                   created_at, expires_at, ref_job_id, meta
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE identity_id = %s
              AND status = %s
              AND expires_at > NOW()
            ORDER BY created_at DESC
            """,
            (identity_id, ReservationStatus.HELD),
        )

    @staticmethod
    def get_total_reserved(identity_id: str) -> int:
        """Get total credits currently reserved for an identity."""
        return WalletService.get_reserved_credits(identity_id)

    # ─────────────────────────────────────────────────────────────
    # Write Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def reserve_credits(
        identity_id: str,
        action_key: str,
        job_id: str,
        amount_override: Optional[int] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Reserve credits for a job. Creates a hold that prevents overspend.

        Args:
            identity_id: The user's identity
            action_key: Frontend action key (e.g., 'text_to_3d_generate')
            job_id: Unique job identifier (for idempotency)
            amount_override: Override the standard action cost (optional)
            meta: Additional metadata

        Returns:
            Dict with reservation details and balance info:
            {
                "reservation": {...},
                "balance": current_balance,
                "reserved": total_reserved,
                "available": available_after_hold,
                "is_existing": True if idempotent return
            }

        Raises:
            ValueError: If insufficient credits or invalid action
        """
        # Convert frontend action_key to DB action_code
        action_code = PricingService.get_db_action_code(action_key)
        if not action_code:
            # Try using action_key directly as action_code
            action_code = action_key

        # Determine credit type based on action (video actions need video credits)
        credit_type = get_credit_type_for_action(action_code)

        # Get the cost for this action
        if amount_override is not None:
            cost_credits = amount_override
        else:
            cost_credits = PricingService.get_action_cost(action_key)
            if cost_credits == 0:
                raise ValueError(f"Unknown action: {action_key}")

        # Idempotency check: if reservation already exists for this job, return it
        existing = ReservationService.get_active_reservation_for_job(
            identity_id, job_id, action_code
        )
        if existing:
            wallet = WalletService.get_wallet(identity_id)
            # Return balance for the relevant credit type
            if credit_type == CreditType.VIDEO:
                balance = wallet.get("balance_video_credits", 0) if wallet else 0
            else:
                balance = wallet.get("balance_credits", 0) if wallet else 0
            reserved = WalletService.get_reserved_credits(identity_id, credit_type)

            return {
                "reservation": ReservationService._format_reservation(existing),
                "balance": balance,
                "reserved": reserved,
                "available": max(0, balance - reserved),
                "is_existing": True,
                "credit_type": credit_type,
            }

        meta_json = json.dumps(meta) if meta else None
        expiry_minutes = getattr(config, 'RESERVATION_EXPIRY_MINUTES', ReservationService.DEFAULT_EXPIRY_MINUTES)

        # Determine which balance column to check
        balance_column = "balance_video_credits" if credit_type == CreditType.VIDEO else "balance_credits"

        with transaction() as cur:
            # 1. Lock wallet and check balance for the correct credit type
            cur.execute(
                f"""
                SELECT identity_id, balance_credits, balance_video_credits
                FROM {Tables.WALLETS}
                WHERE identity_id = %s
                FOR UPDATE
                """,
                (identity_id,),
            )
            wallet = fetch_one(cur)

            if not wallet:
                raise ValueError(f"Wallet not found for identity {identity_id}")

            balance = wallet.get(balance_column, 0) or 0

            # 2. Get current reserved amount for this credit type (excluding expired)
            # CRITICAL: Lock all held reservations for this identity/credit_type to prevent race condition.
            cur.execute(
                f"""
                SELECT id, cost_credits
                FROM {Tables.CREDIT_RESERVATIONS}
                WHERE identity_id = %s
                  AND status = %s
                  AND credit_type = %s
                  AND expires_at > NOW()
                FOR UPDATE
                """,
                (identity_id, ReservationStatus.HELD, credit_type),
            )
            locked_reservations = fetch_all(cur)
            current_reserved = sum(
                int(r.get("cost_credits", 0) or 0)
                for r in locked_reservations
            )

            # 3. Check available balance for this credit type
            available = balance - current_reserved
            if available < cost_credits:
                credit_type_label = "video" if credit_type == CreditType.VIDEO else "general"
                print(f"[RESERVATION] REJECTED: insufficient {credit_type_label} credits for {action_key} (need {cost_credits}, have {available})")
                raise ValueError(
                    f"INSUFFICIENT_{credit_type_label.upper()}_CREDITS:required={cost_credits}:balance={balance}:reserved={current_reserved}:available={available}"
                )

            # 4. Create job row FIRST (to satisfy FK constraint on credit_reservations.ref_job_id)
            provider = _derive_provider_from_action_code(action_code)
            cur.execute(
                f"""
                INSERT INTO {Tables.JOBS}
                (id, identity_id, provider, action_code, status, cost_credits, meta, created_at, updated_at)
                VALUES (%s, %s, %s, %s, 'queued', %s, %s, NOW(), NOW())
                ON CONFLICT (id) DO NOTHING
                RETURNING id
                """,
                (job_id, identity_id, provider, action_code, cost_credits, meta_json),
            )
            job_row = fetch_one(cur)
            if not job_row:
                # Job already existed (idempotent case) - that's fine, FK will be satisfied
                pass

            # 5. Create reservation with ref_job_id pointing to the job, including credit_type
            cur.execute(
                f"""
                INSERT INTO {Tables.CREDIT_RESERVATIONS}
                (identity_id, action_code, cost_credits, status, credit_type, created_at, expires_at, ref_job_id, meta)
                VALUES (%s, %s, %s, %s, %s, NOW(), NOW() + INTERVAL '%s minutes', %s, %s)
                RETURNING *
                """,
                (identity_id, action_code, cost_credits, ReservationStatus.HELD, credit_type, expiry_minutes, job_id, meta_json),
            )
            reservation = fetch_one(cur)
            assert reservation is not None, "Reservation insert failed"

            # 6. Update job with reservation_id for bidirectional link
            cur.execute(
                f"""
                UPDATE {Tables.JOBS}
                SET reservation_id = %s, updated_at = NOW()
                WHERE id = %s
                """,
                (reservation["id"], job_id),
            )

            new_reserved = current_reserved + cost_credits
            new_available = balance - new_reserved

            return {
                "reservation": ReservationService._format_reservation(reservation),
                "balance": balance,
                "reserved": new_reserved,
                "available": new_available,
                "is_existing": False,
                "credit_type": credit_type,
            }

    @staticmethod
    def finalize_reservation(reservation_id: str) -> Dict[str, Any]:
        """
        Finalize a reservation (job completed successfully).
        This captures the held credits by creating a ledger spend entry.

        IDEMPOTENT: Safe to call multiple times. Handles:
        - Already finalized: returns was_already_finalized=True
        - Already released: returns was_already_released=True (job was cancelled/failed, don't charge)
        - Not found: returns not_found=True (reservation expired or never existed)

        Args:
            reservation_id: The reservation to finalize

        Returns:
            Dict with finalized reservation and new balance (never raises for idempotent cases)
        """
        with transaction() as cur:
            # Lock and fetch reservation including credit_type
            cur.execute(
                f"""
                SELECT id, identity_id, action_code, cost_credits, status, ref_job_id, credit_type
                FROM {Tables.CREDIT_RESERVATIONS}
                WHERE id = %s
                FOR UPDATE
                """,
                (reservation_id,),
            )
            reservation = fetch_one(cur)

            if not reservation:
                return {
                    "reservation": None,
                    "not_found": True,
                    "was_already_finalized": False,
                    "was_already_released": False,
                }

            if reservation["status"] == ReservationStatus.FINALIZED:
                return {
                    "reservation": ReservationService._format_reservation(reservation),
                    "was_already_finalized": True,
                    "was_already_released": False,
                    "not_found": False,
                }

            if reservation["status"] == ReservationStatus.RELEASED:
                return {
                    "reservation": ReservationService._format_reservation(reservation),
                    "was_already_released": True,
                    "was_already_finalized": False,
                    "not_found": False,
                }

            identity_id = str(reservation["identity_id"])
            cost_credits = reservation["cost_credits"]
            action_code = reservation["action_code"]
            job_id = str(reservation["ref_job_id"]) if reservation.get("ref_job_id") else None
            credit_type = reservation.get("credit_type", CreditType.GENERAL)

            # Determine which balance column to update
            balance_column = "balance_video_credits" if credit_type == CreditType.VIDEO else "balance_credits"

            # Update reservation status
            cur.execute(
                f"""
                UPDATE {Tables.CREDIT_RESERVATIONS}
                SET status = %s, captured_at = NOW()
                WHERE id = %s
                RETURNING *
                """,
                (ReservationStatus.FINALIZED, reservation_id),
            )
            updated = fetch_one(cur)

            # Create ledger entry (deduct from wallet)
            # Note: We're inside a transaction, so we need to do this manually
            cur.execute(
                f"""
                SELECT balance_credits, balance_video_credits
                FROM {Tables.WALLETS}
                WHERE identity_id = %s
                FOR UPDATE
                """,
                (identity_id,),
            )
            wallet = fetch_one(cur)
            current_balance = wallet.get(balance_column, 0) if wallet else 0
            new_balance = current_balance - cost_credits

            # Insert ledger entry with credit_type
            cur.execute(
                f"""
                INSERT INTO {Tables.LEDGER_ENTRIES}
                (identity_id, entry_type, amount_credits, ref_type, ref_id, meta, credit_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    identity_id,
                    LedgerEntryType.RESERVATION_FINALIZE,
                    -cost_credits,  # Negative because it's a spend
                    "reservations",
                    reservation_id,
                    json.dumps({"action_code": action_code, "job_id": job_id}),
                    credit_type,
                ),
            )

            # Update wallet balance for the correct credit type
            cur.execute(
                f"""
                UPDATE {Tables.WALLETS}
                SET {balance_column} = %s, updated_at = NOW()
                WHERE identity_id = %s
                """,
                (new_balance, identity_id),
            )

            provider = _derive_provider_from_action_code(action_code)

            return {
                "reservation": ReservationService._format_reservation(updated),
                "balance": new_balance,
                "was_already_finalized": False,
                "was_already_released": False,
                "not_found": False,
                "identity_id": identity_id,
                "action_code": action_code,
                "cost": cost_credits,
                "provider": provider,
                "credit_type": credit_type,
            }

    @staticmethod
    def release_reservation(reservation_id: str, reason: str = "cancelled") -> Dict[str, Any]:
        """
        Release a reservation (job failed or cancelled).
        This returns the held credits to available balance.

        IDEMPOTENT: Safe to call multiple times. Handles:
        - Already released: returns was_already_released=True
        - Already finalized: returns was_already_finalized=True (no error - job succeeded)
        - Not found: returns not_found=True (reservation expired or never existed)

        Args:
            reservation_id: The reservation to release
            reason: Reason for release (e.g., 'failed', 'cancelled', 'expired')

        Returns:
            Dict with released reservation info (never raises for idempotent cases)
        """
        with transaction() as cur:
            # Lock and fetch reservation
            cur.execute(
                f"""
                SELECT id, identity_id, action_code, cost_credits, status, ref_job_id
                FROM {Tables.CREDIT_RESERVATIONS}
                WHERE id = %s
                FOR UPDATE
                """,
                (reservation_id,),
            )
            reservation = fetch_one(cur)

            if not reservation:
                # Idempotent: reservation not found (expired, cleaned up, or never existed)
                # print(f"[RESERVATION] Not found (idempotent): {reservation_id}")
                return {
                    "reservation": None,
                    "not_found": True,
                    "was_already_released": False,
                    "was_already_finalized": False,
                }

            if reservation["status"] == ReservationStatus.RELEASED:
                # Idempotent: already released
                # print(f"[RESERVATION] Already released: {reservation_id}")
                return {
                    "reservation": ReservationService._format_reservation(reservation),
                    "was_already_released": True,
                    "was_already_finalized": False,
                    "not_found": False,
                }

            if reservation["status"] == ReservationStatus.FINALIZED:
                # Idempotent: already finalized (job succeeded, credits captured)
                # This is NOT an error - it means the job actually succeeded
                # print(f"[RESERVATION] Already finalized (job succeeded): {reservation_id}")
                return {
                    "reservation": ReservationService._format_reservation(reservation),
                    "was_already_finalized": True,
                    "was_already_released": False,
                    "not_found": False,
                }

            # Update reservation status
            cur.execute(
                f"""
                UPDATE {Tables.CREDIT_RESERVATIONS}
                SET status = %s, released_at = NOW(),
                    meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb
                WHERE id = %s
                RETURNING *
                """,
                (ReservationStatus.RELEASED, json.dumps({"release_reason": reason}), reservation_id),
            )
            updated = fetch_one(cur)

            identity_id = str(reservation["identity_id"])
            action_code = reservation["action_code"]
            cost_credits = reservation["cost_credits"]
            provider = _derive_provider_from_action_code(action_code)

            # print(
            #     f"[RESERVATION] Released: id={reservation_id}, reason={reason}, "
            #     f"credits={cost_credits} returned to available"
            # )

            return {
                "reservation": ReservationService._format_reservation(updated),
                "was_already_released": False,
                "was_already_finalized": False,
                "not_found": False,
                # Additional fields for structured logging
                "identity_id": identity_id,
                "action_code": action_code,
                "cost": cost_credits,
                "provider": provider,
            }

    @staticmethod
    def cleanup_expired_reservations() -> int:
        """
        Release all expired reservations.
        Returns count of reservations released.
        Should be called periodically.
        """
        with transaction() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.CREDIT_RESERVATIONS}
                SET status = %s, released_at = NOW(),
                    meta = COALESCE(meta, '{{}}'::jsonb) || '{{"release_reason": "expired"}}'::jsonb
                WHERE status = %s
                  AND expires_at <= NOW()
                RETURNING id
                """,
                (ReservationStatus.RELEASED, ReservationStatus.HELD),
            )
            released = fetch_all(cur)
            count = len(released)

            if count > 0:
                pass  # print(f"[RESERVATION] Cleaned up {count} expired reservations")

            return count

    # ─────────────────────────────────────────────────────────────
    # Convenience Methods
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def check_can_reserve(
        identity_id: str,
        action_key: str,
    ) -> Tuple[bool, int, int, int]:
        """
        Check if user can reserve credits for an action without creating reservation.

        Returns:
            Tuple of (can_reserve, required_credits, balance, available)
        """
        # Get cost for action
        cost_credits = PricingService.get_action_cost(action_key)
        if cost_credits == 0:
            return False, 0, 0, 0

        # Determine credit type from action
        action_code = PricingService.get_db_action_code(action_key) or action_key
        credit_type = get_credit_type_for_action(action_code)

        # Get balance info for the correct credit type
        balance = WalletService.get_balance(identity_id, credit_type)
        reserved = WalletService.get_reserved_credits(identity_id, credit_type)
        available = max(0, balance - reserved)

        can_reserve = available >= cost_credits
        return can_reserve, cost_credits, balance, available

    # ─────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def _format_reservation(reservation: Dict[str, Any]) -> Dict[str, Any]:
        """Format reservation for API response."""
        return {
            "id": str(reservation["id"]),
            "identity_id": str(reservation["identity_id"]),
            "action_code": reservation["action_code"],
            "cost_credits": reservation["cost_credits"],
            "status": reservation["status"],
            "credit_type": reservation.get("credit_type", "general"),
            "job_id": str(reservation["ref_job_id"]) if reservation.get("ref_job_id") else None,
            "created_at": reservation["created_at"].isoformat() if reservation.get("created_at") else None,
            "expires_at": reservation["expires_at"].isoformat() if reservation.get("expires_at") else None,
            "captured_at": reservation["captured_at"].isoformat() if reservation.get("captured_at") else None,
            "released_at": reservation["released_at"].isoformat() if reservation.get("released_at") else None,
        }
