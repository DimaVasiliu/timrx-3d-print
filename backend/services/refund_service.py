"""
Refund Service — safe admin-only refund execution with audit trail.

Handles:
  - Purchase refund eligibility checks
  - Credit reversal safety validation
  - Refund record creation (audit trail)
  - Ledger-based credit deduction (never wallet hacks)
  - Purchase status update
  - Duplicate refund prevention

Design principles:
  - Conservative: blocks execution when safety is unclear
  - Auditable: every attempt is recorded with full context
  - Idempotent: duplicate refund attempts are rejected, not silently applied
  - Transparent: response clearly states what was/wasn't done
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.db import (
    USE_DB, Tables, transaction, fetch_one, query_all, query_one,
)

_TABLE = Tables.REFUNDS
_VALID_REFUND_TYPES = {
    "full_purchase_refund",
    "partial_purchase_refund",
    "subscription_refund",
    "manual_adjustment_refund",
}
_VALID_STATUSES = {"pending", "executed", "failed", "manual_review_required"}


def _iso(val) -> Optional[str]:
    if val and hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val) if val is not None else None


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTE REFUND
# ─────────────────────────────────────────────────────────────────────────────

def execute_refund(
    *,
    purchase_id: str,
    refund_type: str = "full_purchase_refund",
    reason: Optional[str] = None,
    admin_note: Optional[str] = None,
    allow_credit_reversal: bool = True,
    manual_record_only: bool = False,
    executed_by: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute an admin refund for a one-time purchase.

    Flow:
      1. Validate purchase exists and is refundable
      2. Check for duplicate executed refund
      3. Determine credit_type from plan_code
      4. Compute credits_granted, credits_used, credits_remaining
      5. Decide: safe to auto-execute, or manual_review_required
      6. If safe and not manual_record_only:
         a. Create refund ledger entry (negative credits)
         b. Update wallet balance (GREATEST to prevent negative)
         c. Mark purchase as 'refunded'
      7. Record refund in refunds table
      8. Return full audit summary

    Args:
        purchase_id:            UUID of the purchase to refund
        refund_type:            One of _VALID_REFUND_TYPES
        reason:                 Why the refund is being issued
        admin_note:             Optional admin context
        allow_credit_reversal:  If True, reverse granted credits when safe
        manual_record_only:     If True, only record the decision (no credit/status changes)
        executed_by:            Admin email or identifier

    Returns:
        Dict with refund record + summary of what happened
    """
    from backend.services.purchase_service import PurchaseService
    from backend.services.wallet_service import (
        WalletService, CreditType, get_credit_type_for_plan,
        VIDEO_PLAN_CODES, GENERAL_PLAN_CODES,
    )

    # ── Validate inputs ──
    if refund_type not in _VALID_REFUND_TYPES:
        raise ValueError(f"refund_type must be one of: {', '.join(sorted(_VALID_REFUND_TYPES))}")

    if not purchase_id:
        raise ValueError("purchase_id is required")

    # ── 1. Fetch purchase ──
    purchase = PurchaseService.get_purchase(purchase_id)
    if not purchase:
        raise ValueError(f"Purchase not found: {purchase_id}")

    identity_id = purchase["identity_id"]
    credits_granted = purchase.get("credits_granted", 0) or 0
    amount_gbp = purchase.get("amount", 0) or 0
    plan_code = purchase.get("plan_code") or ""
    purchase_status = purchase.get("status", "")
    payment_provider = purchase.get("provider", "stripe")

    # ── 2. Check purchase is refundable ──
    if purchase_status == "refunded":
        return _blocked_response(
            purchase_id=purchase_id,
            identity_id=identity_id,
            reason="purchase_already_refunded",
            message="This purchase has already been marked as refunded.",
            amount_gbp=amount_gbp,
            credits_granted=credits_granted,
            executed_by=executed_by,
            admin_note=admin_note,
        )

    if purchase_status not in ("completed",):
        return _blocked_response(
            purchase_id=purchase_id,
            identity_id=identity_id,
            reason="purchase_not_completed",
            message=f"Purchase status is '{purchase_status}', not 'completed'. Cannot refund.",
            amount_gbp=amount_gbp,
            credits_granted=credits_granted,
            executed_by=executed_by,
            admin_note=admin_note,
        )

    # ── 3. Check for duplicate executed refund ──
    existing_refund = query_one(
        f"""
        SELECT id, refund_status FROM {_TABLE}
        WHERE purchase_id = %s AND refund_status = 'executed'
        LIMIT 1
        """,
        (purchase_id,),
    )
    if existing_refund:
        return _blocked_response(
            purchase_id=purchase_id,
            identity_id=identity_id,
            reason="duplicate_refund",
            message=f"An executed refund already exists for this purchase (refund_id={existing_refund['id']}).",
            amount_gbp=amount_gbp,
            credits_granted=credits_granted,
            executed_by=executed_by,
            admin_note=admin_note,
        )

    # ── 4. Determine credit type ──
    try:
        credit_type = get_credit_type_for_plan(plan_code) if plan_code else CreditType.GENERAL
    except ValueError:
        credit_type = CreditType.GENERAL
    balance_column = "balance_video_credits" if credit_type == CreditType.VIDEO else "balance_credits"

    # ── 5. Compute credit usage ──
    balances = WalletService.get_all_balances(identity_id)
    if credit_type == CreditType.VIDEO:
        credits_remaining = balances.get("video", 0)
    else:
        credits_remaining = balances.get("general", 0)

    credits_used = max(0, credits_granted - credits_remaining) if credits_granted > 0 else 0

    # ── 6. Decide: safe auto-execute or manual review ──
    credits_to_reverse = 0

    if manual_record_only:
        # Admin explicitly chose record-only mode
        refund_status = "manual_review_required"
        credits_to_reverse = 0
    elif not allow_credit_reversal:
        # Admin does not want credit reversal
        refund_status = "executed"
        credits_to_reverse = 0
    elif credits_used > 0:
        # Credits were used — not safe for automatic full reversal
        print(
            f"[ADMIN_REFUND_BLOCKED] purchase_id={purchase_id} "
            f"reason=credits_already_used granted={credits_granted} "
            f"used={credits_used} remaining={credits_remaining}"
        )
        return _blocked_response(
            purchase_id=purchase_id,
            identity_id=identity_id,
            reason="credits_already_used",
            message=(
                f"Credits have already been used ({credits_used} of {credits_granted}). "
                f"Automatic credit reversal is not safe. "
                f"Use manual_record_only=true to record the refund decision without altering credits."
            ),
            amount_gbp=amount_gbp,
            credits_granted=credits_granted,
            credits_used=credits_used,
            credits_remaining=credits_remaining,
            executed_by=executed_by,
            admin_note=admin_note,
        )
    elif credits_remaining < credits_granted:
        # Balance is lower than granted (possible if subscription added credits too)
        # Be conservative — only reverse what's available
        credits_to_reverse = credits_remaining
        refund_status = "executed"
    else:
        # Full safe reversal
        credits_to_reverse = credits_granted
        refund_status = "executed"

    # ── 7. Execute ──
    external_refund_id = None
    external_refund_executed = False
    now = datetime.now(timezone.utc)

    if refund_status == "executed" and credits_to_reverse > 0:
        try:
            with transaction() as cur:
                # Lock wallet
                cur.execute(
                    f"""
                    SELECT {balance_column} AS current_balance
                    FROM {Tables.WALLETS}
                    WHERE identity_id = %s
                    FOR UPDATE
                    """,
                    (identity_id,),
                )
                wallet = fetch_one(cur)
                current_balance = wallet["current_balance"] if wallet else 0

                # Create refund ledger entry
                cur.execute(
                    f"""
                    INSERT INTO {Tables.LEDGER_ENTRIES}
                    (identity_id, entry_type, amount_credits, ref_type, ref_id,
                     meta, credit_type, created_at)
                    VALUES (%s, 'refund', %s, 'purchase', %s, %s, %s, NOW())
                    ON CONFLICT (identity_id, ref_type, ref_id)
                        WHERE entry_type IN ('refund', 'chargeback') AND ref_type = 'purchase'
                    DO NOTHING
                    RETURNING id
                    """,
                    (
                        identity_id,
                        -credits_to_reverse,
                        purchase_id,
                        json.dumps({
                            "refund_type": refund_type,
                            "reason": reason,
                            "admin_note": admin_note,
                            "executed_by": executed_by,
                            "credits_granted": credits_granted,
                            "credits_used": credits_used,
                            "balance_before": current_balance,
                        }),
                        credit_type,
                    ),
                )
                ledger_row = fetch_one(cur)

                if not ledger_row:
                    # ON CONFLICT fired — ledger entry already exists
                    # This means a refund was already processed via Mollie webhook
                    return _blocked_response(
                        purchase_id=purchase_id,
                        identity_id=identity_id,
                        reason="ledger_refund_exists",
                        message="A refund ledger entry already exists for this purchase (likely from payment webhook).",
                        amount_gbp=amount_gbp,
                        credits_granted=credits_granted,
                        credits_used=credits_used,
                        credits_remaining=credits_remaining,
                        executed_by=executed_by,
                        admin_note=admin_note,
                    )

                # Update wallet balance safely
                cur.execute(
                    f"""
                    UPDATE {Tables.WALLETS}
                    SET {balance_column} = GREATEST({balance_column} - %s, 0),
                        updated_at = NOW()
                    WHERE identity_id = %s
                    RETURNING {balance_column} AS new_balance
                    """,
                    (credits_to_reverse, identity_id),
                )
                wallet_result = fetch_one(cur)
                new_balance = wallet_result["new_balance"] if wallet_result else 0

                # Mark purchase as refunded
                cur.execute(
                    f"""
                    UPDATE {Tables.PURCHASES}
                    SET status = 'refunded'
                    WHERE id = %s
                    """,
                    (purchase_id,),
                )

            print(
                f"[ADMIN_REFUND_EXECUTE] purchase_id={purchase_id} "
                f"amount={amount_gbp} status=executed "
                f"credits_reversed={credits_to_reverse} "
                f"balance: {current_balance} -> {new_balance}"
            )

        except Exception as e:
            # Record failed attempt
            _record_refund(
                purchase_id=purchase_id,
                identity_id=identity_id,
                payment_provider=payment_provider,
                refund_type=refund_type,
                refund_status="failed",
                amount_gbp=amount_gbp,
                credits_reversed=0,
                credit_type=credit_type,
                reason=reason,
                admin_note=f"Execution failed: {e}",
                executed_by=executed_by,
            )
            print(f"[ADMIN_REFUND_FAILED] purchase_id={purchase_id} error={e}")
            raise

    # ── 8. Record in refunds table ──
    refund_record = _record_refund(
        purchase_id=purchase_id,
        identity_id=identity_id,
        payment_provider=payment_provider,
        payment_reference=purchase.get("provider_payment_id"),
        refund_type=refund_type,
        refund_status=refund_status,
        amount_gbp=amount_gbp,
        credits_reversed=credits_to_reverse,
        credit_type=credit_type,
        reason=reason,
        admin_note=admin_note,
        executed_by=executed_by,
        external_refund_id=external_refund_id,
        executed_at=now if refund_status == "executed" else None,
        metadata={
            "credits_granted": credits_granted,
            "credits_used": credits_used,
            "credits_remaining_before": credits_remaining,
            "plan_code": plan_code,
        },
    )

    return {
        "ok": True,
        "refund": refund_record,
        "summary": {
            "purchase_id": purchase_id,
            "identity_id": identity_id,
            "amount_gbp": amount_gbp,
            "credits_granted": credits_granted,
            "credits_used": credits_used,
            "credits_remaining_before": credits_remaining,
            "credits_reversed": credits_to_reverse,
            "credit_type": credit_type,
            "external_refund_executed": external_refund_executed,
            "external_refund_note": (
                "External payment refund (Mollie/Stripe) was NOT automatically executed. "
                "Process the payment refund manually in your payment provider dashboard."
            ),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# LIST REFUND HISTORY
# ─────────────────────────────────────────────────────────────────────────────

def list_refunds(
    *,
    status: Optional[str] = None,
    identity_id: Optional[str] = None,
    purchase_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    """List refund records with optional filters."""
    conditions: list = []
    params: list = []

    if status:
        conditions.append("refund_status = %s")
        params.append(status)
    if identity_id:
        conditions.append("identity_id = %s::uuid")
        params.append(identity_id)
    if purchase_id:
        conditions.append("purchase_id = %s::uuid")
        params.append(purchase_id)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    count_row = query_one(
        f"SELECT COUNT(*) AS total FROM {_TABLE} {where}",
        tuple(params),
    )
    total = count_row["total"] if count_row else 0

    params.extend([limit, offset])
    rows = query_all(
        f"""
        SELECT id, purchase_id, subscription_id, identity_id,
               payment_provider, payment_reference, refund_type, refund_status,
               amount_gbp, currency, credits_reversed, credit_type,
               reason, admin_note, executed_by, external_refund_id,
               metadata, created_at, executed_at
        FROM {_TABLE}
        {where}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params),
    )

    refunds = []
    for r in rows:
        refunds.append({
            "id": str(r["id"]),
            "purchase_id": str(r["purchase_id"]) if r["purchase_id"] else None,
            "subscription_id": str(r["subscription_id"]) if r["subscription_id"] else None,
            "identity_id": str(r["identity_id"]) if r["identity_id"] else None,
            "payment_provider": r["payment_provider"],
            "payment_reference": r["payment_reference"],
            "refund_type": r["refund_type"],
            "refund_status": r["refund_status"],
            "amount_gbp": float(r["amount_gbp"]) if r["amount_gbp"] is not None else 0,
            "currency": r["currency"],
            "credits_reversed": r["credits_reversed"],
            "credit_type": r["credit_type"],
            "reason": r["reason"],
            "admin_note": r["admin_note"],
            "executed_by": r["executed_by"],
            "external_refund_id": r["external_refund_id"],
            "metadata": r["metadata"],
            "created_at": _iso(r["created_at"]),
            "executed_at": _iso(r["executed_at"]),
        })

    return {"refunds": refunds, "total": total}


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _record_refund(
    *,
    purchase_id: str,
    identity_id: str,
    payment_provider: str,
    refund_type: str,
    refund_status: str,
    amount_gbp: float,
    credits_reversed: int = 0,
    credit_type: str = "general",
    reason: Optional[str] = None,
    admin_note: Optional[str] = None,
    executed_by: Optional[str] = None,
    payment_reference: Optional[str] = None,
    external_refund_id: Optional[str] = None,
    metadata: Optional[dict] = None,
    executed_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Insert a refund record into the refunds table."""
    meta_json = json.dumps(metadata, default=str) if metadata else None

    row = query_one(
        f"""
        INSERT INTO {_TABLE}
            (purchase_id, identity_id, payment_provider, payment_reference,
             refund_type, refund_status, amount_gbp, currency,
             credits_reversed, credit_type, reason, admin_note,
             executed_by, external_refund_id, metadata, executed_at)
        VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s, 'GBP',
                %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        RETURNING id, created_at
        """,
        (
            purchase_id, identity_id, payment_provider, payment_reference,
            refund_type, refund_status, amount_gbp,
            credits_reversed, credit_type,
            (reason or "")[:2000] if reason else None,
            (admin_note or "")[:2000] if admin_note else None,
            executed_by, external_refund_id, meta_json,
            executed_at,
        ),
    )

    return {
        "id": str(row["id"]) if row else None,
        "refund_status": refund_status,
        "amount_gbp": float(amount_gbp),
        "credits_reversed": credits_reversed,
        "credit_type": credit_type,
        "external_refund_id": external_refund_id,
        "created_at": _iso(row["created_at"]) if row else None,
        "executed_at": _iso(executed_at),
    }


def _blocked_response(
    *,
    purchase_id: str,
    identity_id: str,
    reason: str,
    message: str,
    amount_gbp: float,
    credits_granted: int,
    credits_used: int = 0,
    credits_remaining: int = 0,
    executed_by: Optional[str] = None,
    admin_note: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record a blocked/manual-review refund attempt and return error response.
    The attempt is still persisted for audit trail.
    """
    refund_record = _record_refund(
        purchase_id=purchase_id,
        identity_id=identity_id,
        payment_provider="unknown",
        refund_type="full_purchase_refund",
        refund_status="manual_review_required",
        amount_gbp=amount_gbp,
        credits_reversed=0,
        reason=reason,
        admin_note=admin_note,
        executed_by=executed_by,
        metadata={
            "blocked_reason": reason,
            "credits_granted": credits_granted,
            "credits_used": credits_used,
            "credits_remaining": credits_remaining,
        },
    )

    return {
        "ok": False,
        "error": "manual_review_required",
        "reason": reason,
        "message": message,
        "refund": refund_record,
        "summary": {
            "purchase_id": purchase_id,
            "identity_id": identity_id,
            "amount_gbp": amount_gbp,
            "credits_granted": credits_granted,
            "credits_used": credits_used,
            "credits_remaining": credits_remaining,
        },
    }
