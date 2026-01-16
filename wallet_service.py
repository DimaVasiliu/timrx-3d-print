"""
Wallet Service - Manages credit balances and ledger.

Authoritative accounting system where:
- Ledger entries are IMMUTABLE after insert
- Wallet balance MUST always equal sum of ledger entries
- All mutations use strict DB transactions

Ledger entry types:
- purchase_credit: Credits from buying a plan
- reservation_hold: Credits held for a pending job (negative)
- reservation_finalize: Credits captured when job completes (negative, releases hold)
- reservation_release: Credits released when job fails/cancelled (positive, releases hold)
- admin_adjust: Manual admin adjustment (positive or negative)
- refund: Credits returned (positive)
- signup_grant: Initial credits on signup (positive)
"""

from typing import Optional, Dict, Any, List
import json

from db import fetch_one, fetch_all, transaction, query_one, query_all, Tables


class LedgerEntryType:
    """Valid ledger entry types."""
    PURCHASE_CREDIT = "purchase_credit"
    RESERVATION_HOLD = "reservation_hold"
    RESERVATION_FINALIZE = "reservation_finalize"
    RESERVATION_RELEASE = "reservation_release"
    ADMIN_ADJUST = "admin_adjust"
    ADMIN_GRANT = "admin_grant"  # Admin backdoor credit grant
    REFUND = "refund"
    SIGNUP_GRANT = "signup_grant"

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
        Returns wallet record with balance_credits and updated_at, or None if not found.
        """
        return query_one(
            f"""
            SELECT identity_id, balance_credits, updated_at
            FROM {Tables.WALLETS}
            WHERE identity_id = %s
            """,
            (identity_id,),
        )

    @staticmethod
    def get_or_create_wallet(identity_id: str) -> Optional[Dict[str, Any]]:
        """
        Get wallet for identity, creating one if it doesn't exist.
        Returns wallet record with balance_credits.

        Note: New wallets start with 0 balance. Use add_ledger_entry()
        to add initial credits (e.g., signup_grant).
        """
        wallet = WalletService.get_wallet(identity_id)
        if wallet:
            return wallet
        # Create wallet with 0 balance
        with transaction() as cur:
            cur.execute(
                f"""
                INSERT INTO {Tables.WALLETS} (identity_id, balance_credits, updated_at)
                VALUES (%s, 0, NOW())
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
    def get_balance(identity_id: str) -> int:
        """
        Get current credit balance for an identity.
        Returns 0 if wallet doesn't exist.
        """
        wallet = WalletService.get_wallet(identity_id)
        if wallet:
            return wallet.get("balance_credits", 0) or 0
        return 0

    @staticmethod
    def get_available_balance(identity_id: str) -> int:
        """
        Get available credits (balance minus reserved).
        This is what can actually be spent on new jobs.
        """
        balance = WalletService.get_balance(identity_id)
        reserved = WalletService.get_reserved_credits(identity_id)
        return max(0, balance - reserved)

    @staticmethod
    def get_reserved_credits(identity_id: str) -> int:
        """
        Get total credits currently reserved (held) for pending jobs.
        Only counts reservations with status='held' that haven't expired.
        """
        row = query_one(
            f"""
            SELECT COALESCE(SUM(cost_credits), 0) as total
            FROM {Tables.CREDIT_RESERVATIONS}
            WHERE identity_id = %s
              AND status = 'held'
              AND expires_at > NOW()
            """,
            (identity_id,),
        )
        if row:
            return int(row.get("total", 0) or 0)
        return 0

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
    ) -> Dict[str, Any]:
        """
        Add a ledger entry and update wallet balance atomically.

        This is the ONLY way to modify wallet balance. All balance changes
        MUST go through this method to maintain consistency.

        Args:
            identity_id: The identity to modify
            entry_type: Type of entry (see LedgerEntryType)
            delta: Amount to add (positive) or subtract (negative)
            ref_type: Reference table name (e.g., 'purchases', 'reservations')
            ref_id: Reference row ID
            meta: Additional metadata as JSON

        Returns:
            The created ledger entry dict

        Raises:
            ValueError: If wallet doesn't exist or delta would make balance negative
            DatabaseError: On transaction failure
        """
        meta_json = json.dumps(meta) if meta else None

        with transaction() as cur:
            # 1. Get current wallet balance (with row lock for update)
            cur.execute(
                f"""
                SELECT identity_id, balance_credits
                FROM {Tables.WALLETS}
                WHERE identity_id = %s
                FOR UPDATE
                """,
                (identity_id,),
            )
            wallet = fetch_one(cur)

            if not wallet:
                raise ValueError(f"Wallet not found for identity {identity_id}")

            current_balance = wallet.get("balance_credits", 0) or 0
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
                    f"would result in {new_balance}"
                )

            # 3. Insert ledger entry (immutable record)
            cur.execute(
                f"""
                INSERT INTO {Tables.LEDGER_ENTRIES}
                (identity_id, entry_type, amount_credits, ref_type, ref_id, meta, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                RETURNING *
                """,
                (identity_id, entry_type, delta, ref_type, ref_id, meta_json),
            )
            ledger_entry = fetch_one(cur)

            # 4. Update wallet balance
            cur.execute(
                f"""
                UPDATE {Tables.WALLETS}
                SET balance_credits = %s, updated_at = NOW()
                WHERE identity_id = %s
                """,
                (new_balance, identity_id),
            )

            print(
                f"[WALLET] Ledger entry: identity={identity_id}, type={entry_type}, "
                f"delta={delta:+d}, balance: {current_balance} -> {new_balance}"
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
    ) -> Optional[Dict[str, Any]]:
        """
        Add credits to a wallet (convenience wrapper for add_ledger_entry).

        Args:
            amount: Must be positive
            entry_type: Usually 'purchase_credit', 'refund', 'admin_adjust', 'signup_grant'

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
        )

    @staticmethod
    def deduct_credits(
        identity_id: str,
        amount: int,
        entry_type: str,
        ref_type: Optional[str] = None,
        ref_id: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Deduct credits from a wallet (convenience wrapper for add_ledger_entry).

        Args:
            amount: Must be positive (will be negated internally)
            entry_type: Usually 'reservation_finalize', 'spend'

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
    ) -> List[Dict[str, Any]]:
        """
        Get ledger entries for an identity, most recent first.
        """
        entries = query_all(
            f"""
            SELECT id, identity_id, entry_type, amount_credits,
                   ref_type, ref_id, meta, created_at
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
                "ref_type": entry.get("ref_type"),
                "ref_id": entry.get("ref_id"),
                "meta": entry.get("meta"),
                "created_at": entry["created_at"].isoformat() if entry.get("created_at") else None,
            }
            for entry in entries
        ]

    @staticmethod
    def get_ledger_sum(identity_id: str) -> int:
        """
        Calculate the sum of all ledger entries for an identity.
        This should always equal the wallet balance.
        """
        row = query_one(
            f"""
            SELECT COALESCE(SUM(amount_credits), 0) as total
            FROM {Tables.LEDGER_ENTRIES}
            WHERE identity_id = %s
            """,
            (identity_id,),
        )
        if row:
            return int(row.get("total", 0) or 0)
        return 0

    # ─────────────────────────────────────────────────────────────
    # Admin / Maintenance Operations
    # ─────────────────────────────────────────────────────────────

    @staticmethod
    def recompute_wallet_balance(identity_id: str) -> Dict[str, Any]:
        """
        Recompute and fix wallet balance from ledger entries.
        Used for consistency checks and repairs.

        Returns dict with:
        - old_balance: Balance before fix
        - new_balance: Balance after fix (from ledger sum)
        - was_corrected: True if balance was changed
        """
        with transaction() as cur:
            # Lock wallet row
            cur.execute(
                f"""
                SELECT identity_id, balance_credits
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

            # Calculate sum from ledger
            cur.execute(
                f"""
                SELECT COALESCE(SUM(amount_credits), 0) as total
                FROM {Tables.LEDGER_ENTRIES}
                WHERE identity_id = %s
                """,
                (identity_id,),
            )
            ledger_row = fetch_one(cur)
            new_balance = int(ledger_row.get("total", 0) or 0) if ledger_row else 0

            was_corrected = old_balance != new_balance

            if was_corrected:
                # Update wallet to match ledger sum
                cur.execute(
                    f"""
                    UPDATE {Tables.WALLETS}
                    SET balance_credits = %s, updated_at = NOW()
                    WHERE identity_id = %s
                    """,
                    (new_balance, identity_id),
                )
                print(
                    f"[WALLET] Recomputed balance for identity={identity_id}: "
                    f"{old_balance} -> {new_balance} (corrected)"
                )
            else:
                print(
                    f"[WALLET] Balance verified for identity={identity_id}: {old_balance} (OK)"
                )

            return {
                "identity_id": identity_id,
                "old_balance": old_balance,
                "new_balance": new_balance,
                "was_corrected": was_corrected,
            }

    @staticmethod
    def verify_wallet_integrity(identity_id: str) -> Dict[str, Any]:
        """
        Check if wallet balance matches ledger sum without modifying anything.

        Returns dict with:
        - wallet_balance: Current wallet balance
        - ledger_sum: Sum of all ledger entries
        - is_consistent: True if they match
        """
        wallet_balance = WalletService.get_balance(identity_id)
        ledger_sum = WalletService.get_ledger_sum(identity_id)

        is_consistent = wallet_balance == ledger_sum

        if not is_consistent:
            print(
                f"[WALLET] INCONSISTENCY DETECTED for identity={identity_id}: "
                f"wallet={wallet_balance}, ledger_sum={ledger_sum}"
            )

        return {
            "identity_id": identity_id,
            "wallet_balance": wallet_balance,
            "ledger_sum": ledger_sum,
            "is_consistent": is_consistent,
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
