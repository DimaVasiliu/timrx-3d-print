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
from datetime import date, datetime, timezone
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
_VALID_STATUSES = {"pending", "executed", "failed", "manual_review_required", "approved", "denied", "closed"}


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
    execute_external_refund: bool = False,
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
        execute_external_refund: If True, attempt Mollie payment refund (only for safe full refunds via Mollie)
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
        # Balance is lower than granted — ambiguous situation.
        # Could be partial usage, subscription top-ups spent, etc.
        # Block automatic execution: a partial reversal masquerading as
        # a full refund creates accounting discrepancies.
        print(
            f"[ADMIN_REFUND_BLOCKED] purchase_id={purchase_id} "
            f"reason=balance_mismatch granted={credits_granted} "
            f"remaining={credits_remaining}"
        )
        return _blocked_response(
            purchase_id=purchase_id,
            identity_id=identity_id,
            reason="balance_mismatch",
            message=(
                f"User has {credits_remaining} credits but purchase granted {credits_granted}. "
                f"Automatic reversal would only remove {credits_remaining}, creating an accounting "
                f"discrepancy. Use manual_record_only=true to record the decision, then adjust "
                f"credits manually if needed."
            ),
            amount_gbp=amount_gbp,
            credits_granted=credits_granted,
            credits_used=credits_used,
            credits_remaining=credits_remaining,
            executed_by=executed_by,
            admin_note=admin_note,
        )
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

    # ── 7b. Optional external Mollie refund ──
    external_refund_error = None
    payment_reference = purchase.get("provider_payment_id")

    if (
        execute_external_refund
        and refund_status == "executed"
        and payment_provider == "mollie"
        and payment_reference
        and refund_type == "full_purchase_refund"
        and not manual_record_only
    ):
        external_refund_id, external_refund_executed, external_refund_error = (
            _attempt_mollie_refund(
                payment_id=payment_reference,
                amount_gbp=amount_gbp,
                reason=reason or "Admin refund",
                purchase_id=purchase_id,
            )
        )
    elif execute_external_refund:
        # Requested but not eligible — record why
        if payment_provider != "mollie":
            external_refund_error = f"External refund not supported for provider '{payment_provider}'"
        elif not payment_reference:
            external_refund_error = "No payment reference found on purchase"
        elif refund_type != "full_purchase_refund":
            external_refund_error = f"External refund only supported for full_purchase_refund, got '{refund_type}'"
        elif manual_record_only:
            external_refund_error = "External refund skipped because manual_record_only=true"
        elif refund_status != "executed":
            external_refund_error = f"External refund skipped — internal refund status is '{refund_status}'"
        else:
            external_refund_error = "External refund not eligible"
        print(
            f"[ADMIN_REFUND_EXTERNAL_SKIP] purchase_id={purchase_id} "
            f"reason={external_refund_error}"
        )

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
            "external_refund_attempted": execute_external_refund,
            "external_refund_executed": external_refund_executed,
            "external_refund_error": external_refund_error,
        },
    )

    # ── 9. Queue refund confirmation email (executed refunds only) ──
    if refund_status == "executed" and refund_record.get("id"):
        _queue_refund_email(
            identity_id=identity_id,
            refund_id=refund_record["id"],
            purchase_id=purchase_id,
            amount_gbp=amount_gbp,
            credits_reversed=credits_to_reverse,
            credits_granted=credits_granted,
            refund_type=refund_type,
            payment_provider=payment_provider,
            external_refund_executed=external_refund_executed,
            external_refund_id=external_refund_id,
            reason=reason,
            executed_at=now,
        )

    # Build external refund summary
    external_refund_summary = {
        "attempted": execute_external_refund,
        "provider": payment_provider if execute_external_refund else None,
        "executed": external_refund_executed,
        "external_refund_id": external_refund_id,
        "error": external_refund_error,
    }

    if not execute_external_refund:
        external_note = (
            "External payment refund (Mollie/Stripe) was NOT automatically executed. "
            "Process the payment refund manually in your payment provider dashboard."
        )
    elif external_refund_executed:
        external_note = (
            f"Mollie refund executed successfully (refund_id={external_refund_id})."
        )
    else:
        external_note = (
            f"Mollie refund failed: {external_refund_error or 'unknown error'}. "
            "Process the payment refund manually in your Mollie dashboard."
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
            "external_refund_note": external_note,
        },
        "external_refund": external_refund_summary,
    }


# ─────────────────────────────────────────────────────────────────────────────
# LIST REFUND HISTORY
# ─────────────────────────────────────────────────────────────────────────────

def list_refunds(
    *,
    status: Optional[str] = None,
    identity_id: Optional[str] = None,
    purchase_id: Optional[str] = None,
    email: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    List refund records with enriched context.

    Joins identities for email. Derives external_refund state from
    stored metadata + external_refund_id. Adds display_summary for
    human-readable status.
    """
    conditions: list = []
    params: list = []

    if status:
        conditions.append("r.refund_status = %s")
        params.append(status)
    if identity_id:
        conditions.append("r.identity_id = %s::uuid")
        params.append(identity_id)
    if purchase_id:
        conditions.append("r.purchase_id = %s::uuid")
        params.append(purchase_id)

    # Date range filters on r.created_at (inclusive end date)
    if date_from:
        try:
            d = date.fromisoformat(date_from.strip())
            conditions.append("r.created_at >= %s")
            params.append(d)
        except ValueError:
            pass
    if date_to:
        try:
            d = date.fromisoformat(date_to.strip())
            conditions.append("r.created_at < %s + INTERVAL '1 day'")
            params.append(d)
        except ValueError:
            pass

    # Email substring filter (case-insensitive) — requires JOIN
    needs_email_join = False
    if email and email.strip():
        conditions.append("i.email ILIKE %s")
        params.append(f"%{email.strip()}%")
        needs_email_join = True

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    email_join = f"LEFT JOIN {Tables.IDENTITIES} i ON i.id = r.identity_id" if needs_email_join else ""

    count_row = query_one(
        f"SELECT COUNT(*) AS total FROM {_TABLE} r {email_join} {where}",
        tuple(params),
    )
    total = count_row["total"] if count_row else 0

    params.extend([limit, offset])
    rows = query_all(
        f"""
        SELECT r.id, r.purchase_id, r.subscription_id, r.identity_id,
               r.payment_provider, r.payment_reference, r.refund_type, r.refund_status,
               r.amount_gbp, r.currency, r.credits_reversed, r.credit_type,
               r.reason, r.admin_note, r.executed_by, r.external_refund_id,
               r.metadata, r.created_at, r.executed_at,
               i.email AS identity_email,
               EXISTS(
                   SELECT 1 FROM {Tables.EMAIL_OUTBOX} eo
                   WHERE eo.template = 'refund_review'
                     AND eo.payload->>'refund_id' = r.id::text
                     AND eo.status IN ('pending', 'sent')
               ) AS review_email_sent
        FROM {_TABLE} r
        LEFT JOIN {Tables.IDENTITIES} i ON i.id = r.identity_id
        {where}
        ORDER BY r.created_at DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params),
    )

    refunds = []
    for r in rows:
        meta = r["metadata"] or {}
        ext_id = r["external_refund_id"]

        # Build structured external_refund from stored metadata
        external_refund = {
            "provider": r["payment_provider"],
            "attempted": bool(meta.get("external_refund_attempted", False)),
            "executed": bool(meta.get("external_refund_executed", False)) or bool(ext_id),
            "external_refund_id": ext_id,
            "error": meta.get("external_refund_error"),
        }

        # Determine purchase_type
        purchase_type = "unknown"
        if r["purchase_id"] and not r["subscription_id"]:
            purchase_type = "one_time"
        elif r["subscription_id"]:
            purchase_type = "subscription"

        refunds.append({
            "id": str(r["id"]),
            "purchase_id": str(r["purchase_id"]) if r["purchase_id"] else None,
            "subscription_id": str(r["subscription_id"]) if r["subscription_id"] else None,
            "identity_id": str(r["identity_id"]) if r["identity_id"] else None,
            "email": r["identity_email"],
            "purchase_type": purchase_type,
            "payment_provider": r["payment_provider"],
            "payment_reference": r["payment_reference"],
            "refund_type": r["refund_type"],
            "refund_status": r["refund_status"],
            "amount_gbp": float(r["amount_gbp"]) if r["amount_gbp"] is not None else 0,
            "currency": r["currency"],
            "credits_reversed": r["credits_reversed"],
            "credit_type": r["credit_type"],
            "credits_granted": meta.get("credits_granted"),
            "credits_used": meta.get("credits_used"),
            "reason": r["reason"],
            "admin_note": r["admin_note"],
            "executed_by": r["executed_by"],
            "external_refund_id": ext_id,
            "external_refund": external_refund,
            "display_summary": _build_display_summary(r, external_refund),
            "review_email_sent": bool(r.get("review_email_sent", False)),
            "resolved_by": meta.get("resolved_by"),
            "resolved_at": meta.get("resolved_at"),
            "resolution_reason": meta.get("resolution_reason"),
            "follow_up_email_queued": bool(meta.get("follow_up_email_queued", False)),
            "created_at": _iso(r["created_at"]),
            "executed_at": _iso(r["executed_at"]),
        })

    return {"refunds": refunds, "total": total}


# ─────────────────────────────────────────────────────────────────────────────
# RESOLVE MANUAL-REVIEW REFUND
# ─────────────────────────────────────────────────────────────────────────────

_VALID_RESOLUTIONS = {"approved", "denied", "closed"}
_RESOLVABLE_STATUSES = {"manual_review_required", "failed"}


def resolve_refund(
    *,
    refund_id: str,
    resolution: str,
    reason: Optional[str] = None,
    admin_note: Optional[str] = None,
    resolved_by: Optional[str] = None,
    send_email: bool = False,
) -> Dict[str, Any]:
    """
    Resolve a manual-review refund.

    Updates refund_status to the resolution value (approved/denied/closed).
    Stores resolution metadata for audit trail. Optionally queues a
    follow-up email to the user.
    """
    if resolution not in _VALID_RESOLUTIONS:
        return {"ok": False, "error": f"Invalid resolution: {resolution}. Must be one of: {', '.join(sorted(_VALID_RESOLUTIONS))}"}

    # Fetch current refund
    refund = query_one(
        f"SELECT id, identity_id, purchase_id, refund_status, amount_gbp, currency, reason, metadata FROM {_TABLE} WHERE id = %s::uuid",
        (refund_id,),
    )
    if not refund:
        return {"ok": False, "error": "Refund not found"}

    if refund["refund_status"] not in _RESOLVABLE_STATUSES:
        return {"ok": False, "error": f"Cannot resolve: refund status is '{refund['refund_status']}', must be in {', '.join(sorted(_RESOLVABLE_STATUSES))}"}

    now = datetime.now(timezone.utc)

    # Merge resolution metadata into existing metadata
    existing_meta = refund["metadata"] or {}
    existing_meta["resolved_by"] = resolved_by
    existing_meta["resolved_at"] = _iso(now)
    existing_meta["resolution"] = resolution
    existing_meta["resolution_reason"] = (reason or "")[:2000] if reason else None
    existing_meta["resolution_admin_note"] = (admin_note or "")[:2000] if admin_note else None
    existing_meta["follow_up_email_queued"] = False

    meta_json = json.dumps(existing_meta, default=str)

    # Update refund status and metadata
    query_one(
        f"""
        UPDATE {_TABLE}
        SET refund_status = %s,
            reason = COALESCE(%s, reason),
            admin_note = COALESCE(%s, admin_note),
            metadata = %s::jsonb
        WHERE id = %s::uuid
        RETURNING id
        """,
        (
            resolution,
            (reason or "")[:2000] if reason else None,
            (admin_note or "")[:2000] if admin_note else None,
            meta_json,
            refund_id,
        ),
    )

    print(f"[ADMIN_REFUND] Resolved refund {refund_id[:8]} as '{resolution}' by {resolved_by}")

    # Optionally queue follow-up email
    email_result = None
    if send_email and resolution in ("approved", "denied"):
        email_result = _queue_resolution_email(
            identity_id=str(refund["identity_id"]),
            refund_id=str(refund["id"]),
            purchase_id=str(refund["purchase_id"]) if refund["purchase_id"] else None,
            amount_gbp=float(refund["amount_gbp"]) if refund["amount_gbp"] is not None else 0,
            currency=refund["currency"] or "GBP",
            resolution=resolution,
            reason=reason,
        )
        if email_result and email_result.get("queued"):
            # Update metadata to record email was queued
            existing_meta["follow_up_email_queued"] = True
            query_one(
                f"UPDATE {_TABLE} SET metadata = %s::jsonb WHERE id = %s::uuid RETURNING id",
                (json.dumps(existing_meta, default=str), refund_id),
            )

    return {
        "ok": True,
        "refund_id": str(refund["id"]),
        "resolution": resolution,
        "resolved_by": resolved_by,
        "resolved_at": _iso(now),
        "email": email_result,
    }


def _queue_resolution_email(
    *,
    identity_id: str,
    refund_id: str,
    purchase_id: Optional[str],
    amount_gbp: float,
    currency: str,
    resolution: str,
    reason: Optional[str],
) -> Dict[str, Any]:
    """
    Queue a refund resolution follow-up email. Best-effort, never crashes caller.
    Returns dict with queued/already_sent status.
    """
    try:
        email_row = query_one(
            f"SELECT email FROM {Tables.IDENTITIES} WHERE id = %s",
            (identity_id,),
        )
        if not email_row or not email_row.get("email"):
            return {"queued": False, "reason": "no_email"}

        user_email = email_row["email"]
        template_name = f"refund_resolution_{resolution}"

        # Duplicate check
        existing = query_one(
            f"""
            SELECT id FROM {Tables.EMAIL_OUTBOX}
            WHERE template = %s
              AND payload->>'refund_id' = %s
              AND status IN ('pending', 'sent')
            LIMIT 1
            """,
            (template_name, refund_id),
        )
        if existing:
            return {"queued": False, "already_sent": True, "reason": "duplicate"}

        from backend.services.email_outbox_service import EmailOutboxService, EmailTemplate

        payload = {
            "refund_id": refund_id,
            "amount_gbp": amount_gbp,
            "currency": currency,
            "purchase_id": purchase_id,
            "resolution": resolution,
            "reason": reason,
        }

        template = EmailTemplate.REFUND_RESOLUTION_APPROVED if resolution == "approved" else EmailTemplate.REFUND_RESOLUTION_DENIED

        with transaction() as cur:
            EmailOutboxService.queue_email(
                cur,
                to_email=user_email,
                template=template,
                payload=payload,
            )

        print(f"[ADMIN_REFUND_EMAIL] Queued {resolution} follow-up for refund {refund_id[:8]} to {user_email}")
        return {"queued": True, "email": user_email}

    except Exception as e:
        print(f"[ADMIN_REFUND_EMAIL] Failed to queue resolution email: {e}")
        return {"queued": False, "reason": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTE APPROVED REFUND
# ─────────────────────────────────────────────────────────────────────────────

def execute_approved_refund(
    *,
    refund_id: str,
    execute_external_refund: bool = False,
    reason: Optional[str] = None,
    admin_note: Optional[str] = None,
    executed_by: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Execute an already-approved refund.

    Completes the second step of the two-step lifecycle:
      manual_review_required → approved → executed

    Performs credit reversal, wallet update, purchase status change,
    and optional Mollie refund — using the same safe logic as
    execute_refund() but operating on the existing refund record
    rather than creating a new one.
    """
    from backend.services.purchase_service import PurchaseService
    from backend.services.wallet_service import (
        WalletService, CreditType, get_credit_type_for_plan,
    )

    # ── 1. Fetch and validate refund ──
    refund = query_one(
        f"SELECT * FROM {_TABLE} WHERE id = %s::uuid",
        (refund_id,),
    )
    if not refund:
        return {"ok": False, "error": "Refund not found"}

    if refund["refund_status"] != "approved":
        return {"ok": False, "error": f"Refund status is '{refund['refund_status']}', must be 'approved' to execute"}

    purchase_id = str(refund["purchase_id"]) if refund["purchase_id"] else None
    identity_id = str(refund["identity_id"])
    payment_provider = refund["payment_provider"]
    refund_type = refund["refund_type"]
    amount_gbp = float(refund["amount_gbp"]) if refund["amount_gbp"] is not None else 0
    existing_meta = refund["metadata"] or {}

    if not purchase_id:
        return {"ok": False, "error": "No purchase linked to this refund"}

    # ── 2. Fetch purchase ──
    purchase = PurchaseService.get_purchase(purchase_id)
    if not purchase:
        return {"ok": False, "error": f"Linked purchase not found: {purchase_id}"}

    purchase_status = purchase.get("status", "")
    plan_code = purchase.get("plan_code") or ""
    credits_granted = purchase.get("credits_granted", 0) or 0

    # Allow execution for completed OR already-refunded-check
    if purchase_status == "refunded":
        return {"ok": False, "error": "Purchase already marked as refunded"}
    if purchase_status not in ("completed",):
        return {"ok": False, "error": f"Purchase status is '{purchase_status}', not 'completed'"}

    # ── 3. Check no executed refund already exists ──
    existing_executed = query_one(
        f"SELECT id FROM {_TABLE} WHERE purchase_id = %s AND refund_status = 'executed' LIMIT 1",
        (purchase_id,),
    )
    if existing_executed:
        return {"ok": False, "error": f"An executed refund already exists for this purchase (refund_id={existing_executed['id']})"}

    # ── 4. Determine credit type and compute usage ──
    try:
        credit_type = get_credit_type_for_plan(plan_code) if plan_code else CreditType.GENERAL
    except ValueError:
        credit_type = CreditType.GENERAL
    balance_column = "balance_video_credits" if credit_type == CreditType.VIDEO else "balance_credits"

    balances = WalletService.get_all_balances(identity_id)
    credits_remaining = balances.get("video" if credit_type == CreditType.VIDEO else "general", 0)
    credits_used = max(0, credits_granted - credits_remaining) if credits_granted > 0 else 0

    # For approved refunds, reverse what is available (up to granted)
    # The admin already approved knowing the situation
    credits_to_reverse = min(credits_remaining, credits_granted)

    # ── 5. Execute credit reversal ──
    now = datetime.now(timezone.utc)
    external_refund_id = None
    external_refund_executed = False
    external_refund_error = None

    if credits_to_reverse > 0:
        try:
            with transaction() as cur:
                # Lock wallet
                cur.execute(
                    f"SELECT {balance_column} AS current_balance FROM {Tables.WALLETS} WHERE identity_id = %s FOR UPDATE",
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
                            "reason": reason or refund["reason"],
                            "executed_by": executed_by,
                            "approved_refund_id": refund_id,
                            "credits_granted": credits_granted,
                            "credits_used": credits_used,
                            "balance_before": current_balance,
                        }),
                        credit_type,
                    ),
                )
                ledger_row = fetch_one(cur)

                if not ledger_row:
                    return {"ok": False, "error": "Refund ledger entry already exists for this purchase"}

                # Update wallet
                cur.execute(
                    f"""
                    UPDATE {Tables.WALLETS}
                    SET {balance_column} = GREATEST({balance_column} - %s, 0), updated_at = NOW()
                    WHERE identity_id = %s
                    RETURNING {balance_column} AS new_balance
                    """,
                    (credits_to_reverse, identity_id),
                )
                wallet_result = fetch_one(cur)
                new_balance = wallet_result["new_balance"] if wallet_result else 0

                # Mark purchase as refunded
                cur.execute(
                    f"UPDATE {Tables.PURCHASES} SET status = 'refunded' WHERE id = %s",
                    (purchase_id,),
                )

            print(
                f"[ADMIN_REFUND_EXECUTE_APPROVED] refund_id={refund_id[:8]} purchase_id={purchase_id[:8]} "
                f"credits_reversed={credits_to_reverse} balance: {current_balance} -> {new_balance}"
            )
        except Exception as e:
            # Update refund record to reflect failure
            query_one(
                f"UPDATE {_TABLE} SET refund_status = 'failed', admin_note = COALESCE(admin_note || ' | ', '') || %s WHERE id = %s::uuid RETURNING id",
                (f"Execution failed: {e}", refund_id),
            )
            print(f"[ADMIN_REFUND_EXECUTE_APPROVED_FAILED] refund_id={refund_id[:8]} error={e}")
            raise
    else:
        # No credits to reverse — just mark purchase as refunded
        query_one(
            f"UPDATE {Tables.PURCHASES} SET status = 'refunded' WHERE id = %s RETURNING id",
            (purchase_id,),
        )

    # ── 6. Optional external Mollie refund ──
    payment_reference = purchase.get("provider_payment_id")

    if (
        execute_external_refund
        and payment_provider == "mollie"
        and payment_reference
        and refund_type == "full_purchase_refund"
    ):
        external_refund_id, external_refund_executed, external_refund_error = (
            _attempt_mollie_refund(
                payment_id=payment_reference,
                amount_gbp=amount_gbp,
                reason=reason or refund["reason"] or "Admin refund",
                purchase_id=purchase_id,
            )
        )
    elif execute_external_refund:
        if payment_provider != "mollie":
            external_refund_error = f"External refund not supported for provider '{payment_provider}'"
        elif not payment_reference:
            external_refund_error = "No payment reference found on purchase"
        elif refund_type != "full_purchase_refund":
            external_refund_error = f"External refund only supported for full_purchase_refund"

    # ── 7. Update existing refund record to executed ──
    existing_meta["executed_by"] = executed_by
    existing_meta["executed_at"] = _iso(now)
    existing_meta["execution_credits_reversed"] = credits_to_reverse
    existing_meta["execution_credits_remaining_before"] = credits_remaining
    existing_meta["execution_credits_used"] = credits_used
    existing_meta["external_refund_attempted"] = execute_external_refund
    existing_meta["external_refund_executed"] = external_refund_executed
    existing_meta["external_refund_error"] = external_refund_error
    if admin_note:
        existing_meta["execution_admin_note"] = admin_note[:2000]

    meta_json = json.dumps(existing_meta, default=str)

    # CAS guard: only update if still 'approved' — prevents duplicate execution
    updated = query_one(
        f"""
        UPDATE {_TABLE}
        SET refund_status = 'executed',
            credits_reversed = %s,
            executed_by = %s,
            executed_at = %s,
            external_refund_id = %s,
            admin_note = COALESCE(admin_note || ' | ', '') || COALESCE(%s, ''),
            metadata = %s::jsonb
        WHERE id = %s::uuid AND refund_status = 'approved'
        RETURNING id
        """,
        (
            credits_to_reverse,
            executed_by,
            now,
            external_refund_id,
            admin_note[:2000] if admin_note else None,
            meta_json,
            refund_id,
        ),
    )
    if not updated:
        return {"ok": False, "error": "Refund was already executed by another request"}

    print(f"[ADMIN_REFUND_EXECUTE_APPROVED] Refund {refund_id[:8]} executed successfully")

    # ── 8. Queue refund confirmation email ──
    _queue_refund_email(
        identity_id=identity_id,
        refund_id=refund_id,
        purchase_id=purchase_id,
        amount_gbp=amount_gbp,
        credits_reversed=credits_to_reverse,
        credits_granted=credits_granted,
        refund_type=refund_type,
        payment_provider=payment_provider,
        external_refund_executed=external_refund_executed,
        external_refund_id=external_refund_id,
        reason=reason or refund["reason"],
        executed_at=now,
    )

    # ── 9. Build response ──
    external_refund_summary = {
        "attempted": execute_external_refund,
        "provider": payment_provider if execute_external_refund else None,
        "executed": external_refund_executed,
        "external_refund_id": external_refund_id,
        "error": external_refund_error,
    }

    return {
        "ok": True,
        "refund_id": refund_id,
        "summary": {
            "purchase_id": purchase_id,
            "identity_id": identity_id,
            "amount_gbp": amount_gbp,
            "credits_granted": credits_granted,
            "credits_used": credits_used,
            "credits_remaining_before": credits_remaining,
            "credits_reversed": credits_to_reverse,
            "credit_type": credit_type,
        },
        "external_refund": external_refund_summary,
    }


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _build_display_summary(row: Dict, external_refund: Dict) -> str:
    """Build a short human-readable summary of the refund state."""
    refund_st = row["refund_status"]
    credits = row["credits_reversed"] or 0
    meta = row["metadata"] or {}

    _REVIEW_REASONS = {
        "credits_already_used": "Manual review required: credits already used",
        "balance_mismatch": "Manual review required: balance does not match granted credits",
        "duplicate_refund": "Blocked: duplicate refund",
        "purchase_already_refunded": "Blocked: purchase already refunded",
    }

    if refund_st == "executed":
        if external_refund.get("executed"):
            ext_part = "Mollie payment refund sent"
        elif external_refund.get("attempted"):
            ext_part = "Mollie payment refund FAILED — process manually"
        elif external_refund.get("provider") == "mollie":
            ext_part = "payment refund not yet processed — action required"
        else:
            ext_part = "no external payment refund"
        return f"Credits reversed ({credits}), {ext_part}"

    if refund_st == "manual_review_required":
        reason = meta.get("blocked_reason") or row.get("reason") or ""
        return _REVIEW_REASONS.get(reason, f"Manual review required: {reason}" if reason else "Manual review required")

    if refund_st == "failed":
        return "Refund execution failed"

    if refund_st == "pending":
        return "Refund pending"

    if refund_st == "approved":
        resolved_by = meta.get("resolved_by", "admin")
        return f"Refund approved by {resolved_by} — awaiting execution"

    if refund_st == "denied":
        resolution_reason = meta.get("resolution_reason") or ""
        return f"Refund denied{': ' + resolution_reason if resolution_reason else ''}"

    if refund_st == "closed":
        return "Refund case closed"

    return refund_st


def _queue_refund_email(
    *,
    identity_id: str,
    refund_id: str,
    purchase_id: str,
    amount_gbp: float,
    credits_reversed: int,
    credits_granted: int,
    refund_type: str,
    payment_provider: str,
    external_refund_executed: bool,
    external_refund_id: Optional[str],
    reason: Optional[str],
    executed_at: Optional[datetime],
) -> None:
    """
    Queue a refund confirmation email via the email outbox.

    Best-effort: never crashes the caller. Looks up email from identity,
    skips silently if not found or if email already queued for this refund.
    """
    try:
        # Look up user email
        email_row = query_one(
            f"SELECT email FROM {Tables.IDENTITIES} WHERE id = %s",
            (identity_id,),
        )
        if not email_row or not email_row.get("email"):
            print(f"[ADMIN_REFUND_EMAIL] Skipped — no email for identity {identity_id}")
            return

        user_email = email_row["email"]

        # Check for duplicate: don't re-queue if already sent/pending for this refund
        existing = query_one(
            f"""
            SELECT id FROM {Tables.EMAIL_OUTBOX}
            WHERE template = 'refund_confirmation'
              AND payload->>'refund_id' = %s
              AND status IN ('pending', 'sent')
            LIMIT 1
            """,
            (refund_id,),
        )
        if existing:
            print(f"[ADMIN_REFUND_EMAIL] Skipped — already queued/sent for refund {refund_id[:8]}")
            return

        from backend.services.email_outbox_service import EmailOutboxService, EmailTemplate

        payload = {
            "refund_id": refund_id,
            "amount_gbp": amount_gbp,
            "currency": "GBP",
            "credits_reversed": credits_reversed,
            "credits_granted": credits_granted,
            "refund_type": refund_type,
            "payment_provider": payment_provider,
            "external_refund_executed": external_refund_executed,
            "external_refund_id": external_refund_id,
            "reason": reason,
            "executed_at": _iso(executed_at),
        }

        with transaction() as cur:
            EmailOutboxService.queue_email(
                cur,
                to_email=user_email,
                template=EmailTemplate.REFUND_CONFIRMATION,
                payload=payload,
                subject=f"TimrX Refund Confirmation - \u00a3{amount_gbp:.2f}",
                identity_id=identity_id,
                purchase_id=purchase_id,
            )

        # Best-effort immediate send
        try:
            EmailOutboxService.send_pending_emails(limit=1)
        except Exception:
            pass  # Will be picked up by cron retry

        print(f"[ADMIN_REFUND_EMAIL] Queued for {user_email} refund={refund_id[:8]}")

    except Exception as e:
        print(f"[ADMIN_REFUND_EMAIL] Failed to queue (non-fatal): {e}")


def _attempt_mollie_refund(
    *,
    payment_id: str,
    amount_gbp: float,
    reason: str,
    purchase_id: str,
) -> tuple:
    """
    Attempt a Mollie payment refund via their API.

    Returns:
        (external_refund_id, success, error_message)
    """
    import requests
    from backend.config import config

    if not getattr(config, "MOLLIE_CONFIGURED", False):
        return (None, False, "Mollie is not configured")

    try:
        resp = requests.post(
            f"https://api.mollie.com/v2/payments/{payment_id}/refunds",
            headers={
                "Authorization": f"Bearer {config.MOLLIE_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "amount": {
                    "currency": "GBP",
                    "value": f"{amount_gbp:.2f}",
                },
                "description": f"Admin refund for purchase {purchase_id}: {reason}",
            },
            timeout=15,
        )

        if resp.status_code in (200, 201):
            data = resp.json()
            refund_id = data.get("id")
            print(
                f"[ADMIN_REFUND_MOLLIE_OK] purchase_id={purchase_id} "
                f"payment_id={payment_id} mollie_refund_id={refund_id}"
            )
            return (refund_id, True, None)
        else:
            error_detail = resp.text[:500]
            print(
                f"[ADMIN_REFUND_MOLLIE_FAIL] purchase_id={purchase_id} "
                f"payment_id={payment_id} status={resp.status_code} "
                f"body={error_detail}"
            )
            return (None, False, f"Mollie API returned {resp.status_code}: {error_detail}")

    except requests.Timeout:
        print(f"[ADMIN_REFUND_MOLLIE_TIMEOUT] purchase_id={purchase_id} payment_id={payment_id}")
        return (None, False, "Mollie API request timed out")
    except Exception as e:
        print(f"[ADMIN_REFUND_MOLLIE_ERROR] purchase_id={purchase_id} error={e}")
        return (None, False, str(e))


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
