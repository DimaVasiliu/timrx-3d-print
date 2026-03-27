"""
Welcome Bonus Service - One-time +50 credits for first verified paid purchase.

Business rules:
  1. User must have email_verified = TRUE
  2. User must have just completed a real paid credits purchase
  3. Bonus is granted AT MOST ONCE per identity (enforced by DB unique index)
  4. Grant goes through the standard wallet ledger (auditable, admin-visible)
  5. In-app notification is created (also idempotent via ref_type/ref_id)

Idempotency:
  - DB partial unique index uq_ledger_welcome_bonus_once prevents double-grant
  - ON CONFLICT DO NOTHING silently skips if already granted
  - Notification uses ref_type='welcome_bonus' with identity-scoped ref_id
  - Safe under webhook replay, duplicate finalization, race conditions

Usage:
    from backend.services.welcome_bonus_service import try_welcome_bonus

    # Call after successful purchase credit grant
    try_welcome_bonus(identity_id)
"""

import json
import logging
from typing import Optional, Dict, Any

from backend.db import fetch_one, transaction, query_one, Tables

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

WELCOME_BONUS_CREDITS = 50
WELCOME_BONUS_CREDIT_TYPE = "general"


def try_welcome_bonus(identity_id: str) -> Optional[Dict[str, Any]]:
    """
    Attempt to grant the one-time welcome bonus after a successful paid purchase.

    Checks:
      1. Identity has email_verified = TRUE
      2. No welcome bonus has been granted before (DB-enforced uniqueness)

    If both pass, grants 50 general credits and creates an in-app notification.

    Args:
        identity_id: The identity that just completed a purchase

    Returns:
        Dict with grant details if bonus was granted,
        None if already granted or not eligible,
        None on error (non-fatal, logged)
    """
    try:
        # 1. Check verified status
        identity = query_one(
            f"""
            SELECT id, email, email_verified
            FROM {Tables.IDENTITIES}
            WHERE id = %s
            """,
            (identity_id,),
        )
        if not identity:
            logger.debug("[WELCOME_BONUS] Identity not found: %s", identity_id)
            return None

        if not identity.get("email_verified"):
            logger.debug("[WELCOME_BONUS] Not verified, skipping: %s", identity_id)
            return None

        # 2. Attempt idempotent grant inside a transaction
        #    The partial unique index uq_ledger_welcome_bonus_once ensures
        #    at most one row with (identity_id, ref_type='welcome_bonus',
        #    entry_type='signup_grant') exists.
        with transaction("welcome_bonus") as cur:
            # Lock wallet
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
                logger.warning("[WELCOME_BONUS] No wallet for %s", identity_id)
                return None

            current_balance = wallet.get("balance_credits", 0) or 0
            new_balance = current_balance + WELCOME_BONUS_CREDITS

            # Insert ledger entry — ON CONFLICT DO NOTHING if already granted
            cur.execute(
                f"""
                INSERT INTO {Tables.LEDGER_ENTRIES}
                (identity_id, entry_type, amount_credits, ref_type, ref_id,
                 meta, credit_type, created_at)
                VALUES (%s, 'signup_grant', %s, 'welcome_bonus', %s,
                        %s, %s, NOW())
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                (
                    identity_id,
                    WELCOME_BONUS_CREDITS,
                    f"first_verified_purchase:{identity_id}",
                    json.dumps({
                        "reason": "welcome_bonus_first_verified_purchase",
                        "credits": WELCOME_BONUS_CREDITS,
                    }),
                    WELCOME_BONUS_CREDIT_TYPE,
                ),
            )
            ledger_row = fetch_one(cur)

            if not ledger_row:
                # ON CONFLICT fired — bonus was already granted
                logger.debug("[WELCOME_BONUS] Already granted for %s", identity_id)
                return None

            # 3. Update wallet balance
            cur.execute(
                f"""
                UPDATE {Tables.WALLETS}
                SET balance_credits = %s, updated_at = NOW()
                WHERE identity_id = %s
                """,
                (new_balance, identity_id),
            )

            # Invalidate wallet cache
            try:
                from backend.services.wallet_service import invalidate_wallet_cache
                invalidate_wallet_cache(identity_id)
            except Exception:
                pass

            logger.info(
                "[WELCOME_BONUS] Granted %d credits to %s (balance: %d -> %d)",
                WELCOME_BONUS_CREDITS, identity_id, current_balance, new_balance,
            )

        # 4. Create notification (outside main transaction — non-fatal)
        try:
            from backend.services.notification_service import NotificationService
            NotificationService.create(
                identity_id=identity_id,
                category="credit",
                notif_type="welcome_bonus",
                title="Welcome bonus unlocked!",
                body=f"Thanks for verifying your account and making your first purchase "
                     f"— we've added {WELCOME_BONUS_CREDITS} free credits to your wallet.",
                icon="fa-gift",
                link="/3dprint",
                meta={
                    "credits": WELCOME_BONUS_CREDITS,
                    "reason": "first_verified_purchase",
                },
                ref_type="welcome_bonus",
                ref_id=f"first_verified_purchase:{identity_id}",
            )
        except Exception as notif_err:
            # Non-fatal: credits are granted, notification is a bonus
            logger.warning("[WELCOME_BONUS] Notification failed (non-fatal): %s", notif_err)

        return {
            "granted": True,
            "credits": WELCOME_BONUS_CREDITS,
            "identity_id": identity_id,
            "new_balance": new_balance,
        }

    except Exception as e:
        # Entire welcome bonus is non-fatal — purchase must not fail because of it
        logger.error("[WELCOME_BONUS] Error for %s: %s", identity_id, e)
        return None
