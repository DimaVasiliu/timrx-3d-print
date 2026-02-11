"""
Wallet Service - Manages credit balances and ledger.

═══════════════════════════════════════════════════════════════════════════════
SOURCE OF TRUTH (critical for audit/debugging)
═══════════════════════════════════════════════════════════════════════════════

  BALANCES:   ledger_entries table (grouped by credit_type)
              → wallets.balance_credits / balance_video_credits are CACHE ONLY
              → Can be repaired from ledger at any time

  RESERVED:   credit_reservations table (WHERE status='held' AND credit_type=X)
              → wallets.reserved_* columns are DEPRECATED/UNUSED
              → Always computed dynamically, never cached

  INVARIANT:  wallet.balance == SUM(ledger_entries.amount WHERE credit_type=X)

═══════════════════════════════════════════════════════════════════════════════

Credit types:
- general: Credits for 3D generation and images (cheaper)
- video: Credits for video generation (more expensive, separate balance)

Ledger entry types:
- purchase_credit: Credits from buying a plan
- reservation_finalize: Credits captured when job completes (negative)
- admin_adjust: Manual admin adjustment (positive or negative)
- refund: Credits returned (positive)
- signup_grant: Initial credits on signup (positive)
"""

from typing import Optional, Dict, Any, List
import json

from backend.db import fetch_one, fetch_all, transaction, query_one, query_all, Tables


class CreditType:
    """Valid credit types for separate accounting."""
    GENERAL = "general"  # 3D generation + images
    VIDEO = "video"      # Video generation only


# ─────────────────────────────────────────────────────────────────────────────
# EXHAUSTIVE ACTION CODE SETS - Fail-closed validation
# ─────────────────────────────────────────────────────────────────────────────
#
# POLICY: Any new action_code MUST be registered here or code will raise.
#         When adding a new action in pricing_service.py, you MUST also add it
#         to VIDEO_ACTION_CODES or GENERAL_ACTION_CODES below.
#
#         This is intentional - fail-closed prevents video actions from
#         accidentally consuming cheaper general credits.
# ─────────────────────────────────────────────────────────────────────────────

# Action codes that require VIDEO credits
VIDEO_ACTION_CODES = {
    "VIDEO_GENERATE",
    "VIDEO_TEXT_GENERATE",
    "VIDEO_IMAGE_ANIMATE",
    "GEMINI_VIDEO",
}

# Action codes that use GENERAL credits (3D + images)
GENERAL_ACTION_CODES = {
    "MESHY_TEXT_TO_3D",
    "MESHY_IMAGE_TO_3D",
    "MESHY_REFINE",
    "MESHY_RETEXTURE",
    "OPENAI_IMAGE",
    "OPENAI_IMAGE_2K",
    "OPENAI_IMAGE_4K",
}

# All known action codes (union of video + general)
KNOWN_ACTION_CODES = VIDEO_ACTION_CODES | GENERAL_ACTION_CODES


# ─────────────────────────────────────────────────────────────────────────────
# EXHAUSTIVE PLAN CODE SETS - Fail-closed validation
# ─────────────────────────────────────────────────────────────────────────────
#
# POLICY: Any new plan_code MUST be registered here or code will raise.
#         When adding a new plan in pricing_service.py, you MUST also add it
#         to VIDEO_PLAN_CODES or GENERAL_PLAN_CODES below.
# ─────────────────────────────────────────────────────────────────────────────

# Plan codes that grant VIDEO credits
VIDEO_PLAN_CODES = {
    "video_starter_250",
    "video_creator_750",
    "video_studio_1600",
}

# Plan codes that grant GENERAL credits (one-time purchases)
GENERAL_PLAN_CODES = {
    "starter_250",
    "creator_900",
    "studio_2200",
}

# Subscription plan codes that grant GENERAL credits (recurring)
# Credit amounts defined in subscription_service.py SUBSCRIPTION_PLANS
SUBSCRIPTION_PLAN_CODES = {
    # Monthly subscriptions
    "starter_monthly",   # 400 credits/month
    "creator_monthly",   # 1300 credits/month
    "studio_monthly",    # 3200 credits/month
    # Yearly subscriptions (credits released monthly)
    "starter_yearly",    # 400 credits/month × 12
    "creator_yearly",    # 1300 credits/month × 12
    "studio_yearly",     # 3200 credits/month × 12
}

# All known plan codes (union of video + general + subscription)
KNOWN_PLAN_CODES = VIDEO_PLAN_CODES | GENERAL_PLAN_CODES | SUBSCRIPTION_PLAN_CODES


def get_credit_type_for_action(action_code: str) -> str:
    """
    Determine credit type based on action code.
    Video actions require video credits, general actions use general credits.

    FAIL-CLOSED: Raises ValueError for unknown action codes to prevent
    accidentally allowing video actions to use general credits.

    Args:
        action_code: DB action code (e.g., "VIDEO_GENERATE", "MESHY_TEXT_TO_3D")

    Returns:
        CreditType.VIDEO or CreditType.GENERAL

    Raises:
        ValueError: If action_code is not in KNOWN_ACTION_CODES
    """
    if action_code in VIDEO_ACTION_CODES:
        return CreditType.VIDEO
    if action_code in GENERAL_ACTION_CODES:
        return CreditType.GENERAL
    raise ValueError(
        f"Unknown action code: {action_code}. "
        f"Add to VIDEO_ACTION_CODES or GENERAL_ACTION_CODES in wallet_service.py"
    )


def get_credit_type_for_plan(plan_code: str) -> str:
    """
    Determine credit type based on plan code.
    Video plans grant video credits, general plans grant general credits.

    FAIL-CLOSED: Raises ValueError for unknown plan codes to prevent
    accidentally granting wrong credit type.

    Args:
        plan_code: Plan code (e.g., "video_starter_250", "starter_80")

    Returns:
        CreditType.VIDEO or CreditType.GENERAL

    Raises:
        ValueError: If plan_code is not in KNOWN_PLAN_CODES
    """
    if plan_code in VIDEO_PLAN_CODES:
        return CreditType.VIDEO
    if plan_code in GENERAL_PLAN_CODES:
        return CreditType.GENERAL
    if plan_code in SUBSCRIPTION_PLAN_CODES:
        return CreditType.GENERAL  # Subscriptions grant general credits
    raise ValueError(
        f"Unknown plan code: {plan_code}. "
        f"Add to VIDEO_PLAN_CODES or GENERAL_PLAN_CODES in wallet_service.py"
    )


class LedgerEntryType:
    """Valid ledger entry types."""
    PURCHASE_CREDIT = "purchase_credit"
    RESERVATION_HOLD = "reservation_hold"
    RESERVATION_FINALIZE = "reservation_finalize"
    RESERVATION_RELEASE = "reservation_release"
    ADMIN_ADJUST = "admin_adjust"
    REFUND = "refund"
    CHARGEBACK = "chargeback"
    SIGNUP_GRANT = "signup_grant"

    # Direct charge (idempotent, used by /api/credits/charge)
    CHARGE = "charge"

    # For backward compatibility with existing code
    GRANT = "grant"
    PURCHASE = "purchase"
    SPEND = "spend"


class WalletService:
    """
    Service for managing wallets and credit balances.

    CRITICAL: All balance mutations MUST go through add_ledger_entry()
    to maintain wallet/ledger consistency.
    """

    # ─────────────────────────────────────────────────────────────
    # Read Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_wallet(identity_id: str) -> Optional[Dict[str, Any]]:
        """
        Get wallet for identity.
        Returns wallet record with balance_credits, balance_video_credits, and updated_at.
        """
        return query_one(
            f"""
            SELECT identity_id, balance_credits, balance_video_credits,
                   reserved_video_credits, updated_at
            FROM {Tables.WALLETS}
            WHERE identity_id = %s
            """,
            (identity_id,),
        )

    @staticmethod
    def get_or_create_wallet(identity_id: str) -> Optional[Dict[str, Any]]:
        """
        Get wallet for identity, creating one if it doesn't exist.
        Returns wallet record with balance_credits and balance_video_credits.

        Note: New wallets start with 0 balance for both types. Use add_ledger_entry()
        to add initial credits (e.g., signup_grant).
        """
        wallet = WalletService.get_wallet(identity_id)
        if wallet:
            return wallet
        # Create wallet with 0 balance for both credit types
        with transaction() as cur:
            cur.execute(
                f"""
                INSERT INTO {Tables.WALLETS}
                    (identity_id, balance_credits, balance_video_credits, reserved_video_credits, updated_at)
                VALUES (%s, 0, 0, 0, NOW())
                ON CONFLICT (identity_id) DO NOTHING
                RETURNING *
                """,
                (identity_id,),
            )
            result = fetch_one(cur)
            if result:
                return result
        # If insert returned nothing (conflict), fetch existing
        return WalletService.get_wallet(identity_id)

    @staticmethod
    def get_balance(identity_id: str, credit_type: str = CreditType.GENERAL) -> int:
        """
        Get current credit balance for an identity.
        Returns 0 if wallet doesn't exist.

        Args:
            identity_id: The identity to check
            credit_type: Type of credits ('general' or 'video')

        Returns:
            Balance for the specified credit type
        """
        wallet = WalletService.get_wallet(identity_id)
        if wallet:
            if credit_type == CreditType.VIDEO:
                return wallet.get("balance_video_credits", 0) or 0
            return wallet.get("balance_credits", 0) or 0
        return 0

    @staticmethod
    def get_all_balances(identity_id: str) -> Dict[str, int]:
        """
        Get all credit balances for an identity.
        Returns dict with general and video balances.
        """
        wallet = WalletService.get_wallet(identity_id)
        if wallet:
            return {
                "general": wallet.get("balance_credits", 0) or 0,
                "video": wallet.get("balance_video_credits", 0) or 0,
            }
        return {"general": 0, "video": 0}

    @staticmethod
    def get_available_balance(identity_id: str, credit_type: str = CreditType.GENERAL) -> int:
        """
        Get available credits (balance minus reserved) for a specific credit type.
        This is what can actually be spent on new jobs.

        Args:
            identity_id: The identity to check
            credit_type: Type of credits ('general' or 'video')

        Returns:
            Available balance for the specified credit type
        """
        balance = WalletService.get_balance(identity_id, credit_type)
        reserved = WalletService.get_reserved_credits(identity_id, credit_type)
        return max(0, balance - reserved)

    @staticmethod
    def get_reserved_credits(identity_id: str, credit_type: str = CreditType.GENERAL) -> int:
        """
        Get total credits currently reserved (held) for pending jobs.
        Only counts reservations with status='held' that haven't expired.

        Args:
            identity_id: The identity to check
            credit_type: Type of credits ('general' or 'video')

        Returns:
            Total reserved credits for the specified type
        """
        row = query_one(
            f"""
            SELECT COALESCE(SUM(cost_credits), 0) as total
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE identity_id = %s
              AND status = 'held'
              AND expires_at > NOW()
              AND credit_type = %s
            """,
            (identity_id, credit_type),
        )
        if row:
            return int(row.get("total", 0) or 0)
        return 0

    @staticmethod
    def get_all_reserved_credits(identity_id: str) -> Dict[str, int]:
        """
        Get all reserved credits for an identity, split by credit type.
        """
        rows = query_all(
            f"""
            SELECT credit_type, COALESCE(SUM(cost_credits), 0) as total
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE identity_id = %s
              AND status = 'held'
              AND expires_at > NOW()
            GROUP BY credit_type
            """,
            (identity_id,),
        )
        result = {"general": 0, "video": 0}
        for row in rows:
            ct = row.get("credit_type", "general")
            result[ct] = int(row.get("total", 0) or 0)
        return result

    # ─────────────────────────────────────────────────────────────
    # Write Operations (Transactional)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def add_ledger_entry(
        identity_id: str,
        entry_type: str,
        delta: int,
        ref_type: Optional[str] = None,
        ref_id: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        credit_type: str = CreditType.GENERAL,
    ) -> Dict[str, Any]:
        """
        Add a ledger entry and update wallet balance atomically.

        This is the ONLY way to modify wallet balance. All balance changes
        MUST go through this method to maintain consistency.

        Args:
            identity_id: The identity to modify
            entry_type: Type of entry (see LedgerEntryType)
            delta: Amount to add (positive) or subtract (negative)
            ref_type: Reference table name (e.g., 'purchase', 'reservations')
            ref_id: Reference row ID
            meta: Additional metadata as JSON
            credit_type: Type of credits ('general' or 'video')

        Returns:
            The created ledger entry dict

        Raises:
            ValueError: If wallet doesn't exist or delta would make balance negative
            DatabaseError: On transaction failure
        """
        meta_json = json.dumps(meta) if meta else None

        # Determine which balance column to update
        balance_column = "balance_video_credits" if credit_type == CreditType.VIDEO else "balance_credits"

        with transaction() as cur:
            # 1. Get current wallet balance (with row lock for update)
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

            current_balance = wallet.get(balance_column, 0) or 0
            new_balance = current_balance + delta

            # 2. Check for negative balance (except for certain types)
            # Allow negative for holds since they're backed by available balance check
            allow_negative_types = {
                LedgerEntryType.RESERVATION_HOLD,
                LedgerEntryType.ADMIN_ADJUST,
            }
            if new_balance < 0 and entry_type not in allow_negative_types:
                raise ValueError(
                    f"Insufficient balance: current={current_balance}, delta={delta}, "
                    f"would result in {new_balance} (credit_type={credit_type})"
                )

            # 3. Insert ledger entry (immutable record) with credit_type
            cur.execute(
                f"""
                INSERT INTO {Tables.LEDGER_ENTRIES}
                (identity_id, entry_type, amount_credits, ref_type, ref_id, meta, credit_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING *
                """,
                (identity_id, entry_type, delta, ref_type, ref_id, meta_json, credit_type),
            )
            ledger_entry = fetch_one(cur)
            assert ledger_entry is not None, "Ledger entry insert failed"

            # 4. Update wallet balance for the correct credit type
            cur.execute(
                f"""
                UPDATE {Tables.WALLETS}
                SET {balance_column} = %s, updated_at = NOW()
                WHERE identity_id = %s
                """,
                (new_balance, identity_id),
            )

            print(
                f"[WALLET] Ledger entry: identity={identity_id}, type={entry_type}, "
                f"credit_type={credit_type}, delta={delta:+d}, balance: {current_balance} -> {new_balance}"
            )

            return ledger_entry

    @staticmethod
    def add_credits(
        identity_id: str,
        amount: int,
        entry_type: str,
        ref_type: Optional[str] = None,
        ref_id: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        credit_type: str = CreditType.GENERAL,
    ) -> Optional[Dict[str, Any]]:
        """
        Add credits to a wallet (convenience wrapper for add_ledger_entry).

        Args:
            amount: Must be positive
            entry_type: Usually 'purchase_credit', 'refund', 'admin_adjust', 'signup_grant'
            credit_type: Type of credits ('general' or 'video')

        Returns:
            The ledger entry created
        """
        if amount <= 0:
            raise ValueError(f"Amount must be positive, got {amount}")

        return WalletService.add_ledger_entry(
            identity_id=identity_id,
            entry_type=entry_type,
            delta=amount,  # Positive delta
            ref_type=ref_type,
            ref_id=ref_id,
            meta=meta,
            credit_type=credit_type,
        )

    @staticmethod
    def deduct_credits(
        identity_id: str,
        amount: int,
        entry_type: str,
        ref_type: Optional[str] = None,
        ref_id: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        credit_type: str = CreditType.GENERAL,
    ) -> Optional[Dict[str, Any]]:
        """
        Deduct credits from a wallet (convenience wrapper for add_ledger_entry).

        Args:
            amount: Must be positive (will be negated internally)
            entry_type: Usually 'reservation_finalize', 'spend'
            credit_type: Type of credits ('general' or 'video')

        Returns:
            The ledger entry created, or None if insufficient balance
        """
        if amount <= 0:
            raise ValueError(f"Amount must be positive, got {amount}")

        try:
            return WalletService.add_ledger_entry(
                identity_id=identity_id,
                entry_type=entry_type,
                delta=-amount,  # Negative delta
                ref_type=ref_type,
                ref_id=ref_id,
                meta=meta,
                credit_type=credit_type,
            )
        except ValueError as e:
            if "Insufficient balance" in str(e):
                return None
            raise

    # ─────────────────────────────────────────────────────────────
    # Ledger Query Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def get_ledger_entries(
        identity_id: str,
        limit: int = 50,
        offset: int = 0,
        credit_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get ledger entries for an identity, most recent first.

        Args:
            identity_id: The identity to query
            limit: Max entries to return
            offset: Pagination offset
            credit_type: Filter by credit type (None = all types)
        """
        if credit_type:
            entries = query_all(
                f"""
                SELECT id, identity_id, entry_type, amount_credits,
                       ref_type, ref_id, meta, credit_type, created_at
                FROM {Tables.LEDGER_ENTRIES}
                WHERE identity_id = %s AND credit_type = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (identity_id, credit_type, limit, offset),
            )
        else:
            entries = query_all(
                f"""
                SELECT id, identity_id, entry_type, amount_credits,
                       ref_type, ref_id, meta, credit_type, created_at
                FROM {Tables.LEDGER_ENTRIES}
                WHERE identity_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (identity_id, limit, offset),
            )

        # Format for API response
        return [
            {
                "id": str(entry["id"]),
                "type": entry["entry_type"],
                "amount": entry["amount_credits"],
                "credit_type": entry.get("credit_type", "general"),
                "ref_type": entry.get("ref_type"),
                "ref_id": entry.get("ref_id"),
                "meta": entry.get("meta"),
                "created_at": entry["created_at"].isoformat() if entry.get("created_at") else None,
            }
            for entry in entries
        ]

    @staticmethod
    def get_ledger_sum(identity_id: str, credit_type: str = CreditType.GENERAL) -> int:
        """
        Calculate the sum of all ledger entries for an identity and credit type.
        This should always equal the wallet balance for that credit type.

        Args:
            identity_id: The identity to query
            credit_type: Type of credits ('general' or 'video')

        Returns:
            Sum of all ledger entries for the specified credit type
        """
        row = query_one(
            f"""
            SELECT COALESCE(SUM(amount_credits), 0) as total
            FROM {Tables.LEDGER_ENTRIES}
            WHERE identity_id = %s AND credit_type = %s
            """,
            (identity_id, credit_type),
        )
        if row:
            return int(row.get("total", 0) or 0)
        return 0

    @staticmethod
    def get_all_ledger_sums(identity_id: str) -> Dict[str, int]:
        """
        Calculate the sum of all ledger entries for an identity, split by credit type.
        Returns dict with general and video sums.
        """
        rows = query_all(
            f"""
            SELECT credit_type, COALESCE(SUM(amount_credits), 0) as total
            FROM {Tables.LEDGER_ENTRIES}
            WHERE identity_id = %s
            GROUP BY credit_type
            """,
            (identity_id,),
        )
        result = {"general": 0, "video": 0}
        for row in rows:
            ct = row.get("credit_type", "general")
            result[ct] = int(row.get("total", 0) or 0)
        return result

    # ─────────────────────────────────────────────────────────────
    # Admin / Maintenance Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def recompute_wallet_balance(identity_id: str) -> Dict[str, Any]:
        """
        Recompute and fix wallet balances (both general and video) from ledger entries.
        Used for consistency checks and repairs.

        Returns dict with:
        - old_balance / new_balance / was_corrected: General credits
        - old_video_balance / new_video_balance / video_was_corrected: Video credits
        """
        with transaction() as cur:
            # Lock wallet row
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

            old_balance = wallet.get("balance_credits", 0) or 0
            old_video_balance = wallet.get("balance_video_credits", 0) or 0

            # Calculate sums from ledger by credit_type
            cur.execute(
                f"""
                SELECT credit_type, COALESCE(SUM(amount_credits), 0) as total
                FROM {Tables.LEDGER_ENTRIES}
                WHERE identity_id = %s
                GROUP BY credit_type
                """,
                (identity_id,),
            )
            ledger_rows = fetch_all(cur)
            ledger_sums = {"general": 0, "video": 0}
            for row in ledger_rows:
                ct = row.get("credit_type", "general")
                ledger_sums[ct] = int(row.get("total", 0) or 0)

            new_balance = ledger_sums["general"]
            new_video_balance = ledger_sums["video"]

            was_corrected = old_balance != new_balance
            video_was_corrected = old_video_balance != new_video_balance

            if was_corrected or video_was_corrected:
                # Update wallet to match ledger sums
                cur.execute(
                    f"""
                    UPDATE {Tables.WALLETS}
                    SET balance_credits = %s, balance_video_credits = %s, updated_at = NOW()
                    WHERE identity_id = %s
                    """,
                    (new_balance, new_video_balance, identity_id),
                )
                print(
                    f"[WALLET] Recomputed balance for identity={identity_id}: "
                    f"general: {old_balance} -> {new_balance}, "
                    f"video: {old_video_balance} -> {new_video_balance}"
                )
            else:
                print(
                    f"[WALLET] Balance verified for identity={identity_id}: "
                    f"general={old_balance}, video={old_video_balance} (OK)"
                )

            return {
                "identity_id": identity_id,
                "old_balance": old_balance,
                "new_balance": new_balance,
                "was_corrected": was_corrected,
                "old_video_balance": old_video_balance,
                "new_video_balance": new_video_balance,
                "video_was_corrected": video_was_corrected,
            }

    @staticmethod
    def verify_wallet_integrity(identity_id: str) -> Dict[str, Any]:
        """
        Check if wallet balances match ledger sums without modifying anything.

        Returns dict with:
        - general: {wallet_balance, ledger_sum, is_consistent}
        - video: {wallet_balance, ledger_sum, is_consistent}
        - is_consistent: True if both types match
        """
        wallet = WalletService.get_wallet(identity_id)
        if not wallet:
            return {
                "identity_id": identity_id,
                "is_consistent": True,
                "general": {"wallet_balance": 0, "ledger_sum": 0, "is_consistent": True},
                "video": {"wallet_balance": 0, "ledger_sum": 0, "is_consistent": True},
            }

        general_balance = wallet.get("balance_credits", 0) or 0
        video_balance = wallet.get("balance_video_credits", 0) or 0

        ledger_sums = WalletService.get_all_ledger_sums(identity_id)
        general_ledger_sum = ledger_sums["general"]
        video_ledger_sum = ledger_sums["video"]

        general_consistent = general_balance == general_ledger_sum
        video_consistent = video_balance == video_ledger_sum
        is_consistent = general_consistent and video_consistent

        if not is_consistent:
            print(
                f"[WALLET] INCONSISTENCY DETECTED for identity={identity_id}: "
                f"general: wallet={general_balance} vs ledger={general_ledger_sum}, "
                f"video: wallet={video_balance} vs ledger={video_ledger_sum}"
            )

        return {
            "identity_id": identity_id,
            "is_consistent": is_consistent,
            "general": {
                "wallet_balance": general_balance,
                "ledger_sum": general_ledger_sum,
                "is_consistent": general_consistent,
            },
            "video": {
                "wallet_balance": video_balance,
                "ledger_sum": video_ledger_sum,
                "is_consistent": video_consistent,
            },
            # Backwards compatibility
            "wallet_balance": general_balance,
            "ledger_sum": general_ledger_sum,
        }

    # ─────────────────────────────────────────────────────────────
    # Deprecated Methods (for backward compatibility)
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def recalculate_balance(identity_id: str) -> int:
        """
        Deprecated: Use recompute_wallet_balance() instead.
        Recalculate wallet balance from ledger entries.
        """
        result = WalletService.recompute_wallet_balance(identity_id)
        return result["new_balance"]
