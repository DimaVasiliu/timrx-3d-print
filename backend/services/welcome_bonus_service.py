"""
Welcome Bonus Service - One-time +50 credits on email verification.

Business rules:
  1. User must have email_verified = TRUE
  2. Bonus is granted AT MOST ONCE per identity (enforced by DB unique index)
  3. Grant goes through the standard wallet ledger (auditable, admin-visible)
  4. In-app notification is created (also idempotent via ref_type/ref_id)

Idempotency:
  - DB partial unique index uq_ledger_welcome_bonus_once prevents double-grant
  - ON CONFLICT DO NOTHING silently skips if already granted
  - Notification uses ref_type='welcome_bonus' with identity-scoped ref_id
  - Safe under webhook replay, duplicate finalization, race conditions
  - Safe to call from purchase paths too — silently skipped if already granted

Usage:
    from backend.services.welcome_bonus_service import try_welcome_bonus

    # Call after email verification (primary trigger)
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
    Attempt to grant the one-time welcome bonus.

    Checks:
      1. Identity has email_verified = TRUE
      2. No welcome bonus has been granted before (DB-enforced uniqueness)

    If all pass, grants 50 general credits and creates an in-app notification.

    Safe to call from email verification, purchase finalization, or payment webhooks.
    The DB unique index guarantees at-most-once granting regardless of caller.

    Args:
        identity_id: The identity to check

    Returns:
        Dict with grant details if bonus was granted,
        None if already granted or not eligible,
        None on error (non-fatal, logged)
    """
    try:
        # 1. Check verified status
        row = query_one(
            f"""
            SELECT i.email_verified
            FROM {Tables.IDENTITIES} i
            WHERE i.id = %s
            """,
            (identity_id,),
        )
        if not row:
            logger.debug("[WELCOME_BONUS] Identity not found: %s", identity_id)
            return None

        if not row.get("email_verified"):
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
                    f"email_verified:{identity_id}",
                    json.dumps({
                        "reason": "welcome_bonus_email_verified",
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
                body=f"Welcome to TimrX! We've added {WELCOME_BONUS_CREDITS} free credits "
                     f"to your wallet so you can start creating.",
                icon="fa-gift",
                link="/3dprint",
                meta={
                    "credits": WELCOME_BONUS_CREDITS,
                    "reason": "email_verified",
                },
                ref_type="welcome_bonus",
                ref_id=f"email_verified:{identity_id}",
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
