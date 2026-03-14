"""
Dispute Service — internal admin dispute/chargeback tracking.

Manages the payment_disputes table for recording purchase disputes.
Does NOT auto-modify purchases or refunds — purely visibility + audit.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.db import Tables, query_all, query_one, get_conn


_TABLE = Tables.PAYMENT_DISPUTES
_VALID_STATUSES = {"open", "under_review", "won", "lost", "closed"}


def _iso(val) -> Optional[str]:
    if val and hasattr(val, "isoformat"):
        return val.isoformat()
    return str(val) if val is not None else None


def _row_to_dict(r) -> Dict[str, Any]:
    return {
        "id": str(r["id"]),
        "purchase_id": str(r["purchase_id"]) if r["purchase_id"] else None,
        "identity_id": str(r["identity_id"]) if r["identity_id"] else None,
        "payment_provider": r["payment_provider"],
        "payment_reference": r["payment_reference"],
        "dispute_status": r["dispute_status"],
        "dispute_reason": r["dispute_reason"],
        "amount_gbp": float(r["amount_gbp"]) if r["amount_gbp"] is not None else None,
        "currency": r["currency"],
        "admin_note": r["admin_note"],
        "evidence_summary": r["evidence_summary"],
        "metadata": r["metadata"],
        "created_at": _iso(r["created_at"]),
        "updated_at": _iso(r["updated_at"]),
    }


# ─────────────────────────────────────────────────────────────────────────────
# LIST
# ─────────────────────────────────────────────────────────────────────────────

def list_disputes(
    *,
    status: Optional[str] = None,
    purchase_id: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    """List disputes with optional filters."""
    conditions: list = []
    params: list = []

    if status:
        conditions.append("dispute_status = %s")
        params.append(status)
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
        SELECT * FROM {_TABLE}
        {where}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
        """,
        tuple(params),
    )

    disputes = [_row_to_dict(r) for r in rows]
    return {"disputes": disputes, "total": total}


# ─────────────────────────────────────────────────────────────────────────────
# CREATE (mark-dispute)
# ─────────────────────────────────────────────────────────────────────────────

def create_dispute(
    *,
    purchase_id: str,
    dispute_reason: Optional[str] = None,
    admin_note: Optional[str] = None,
    executed_by: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record a dispute/chargeback against a purchase.

    Fetches purchase details to populate fields.
    Does NOT modify the purchase or any refund records.
    """
    # Fetch purchase
    purchase = query_one(
        f"""
        SELECT p.id, p.identity_id, p.amount_gbp, p.currency,
               p.provider, p.provider_payment_id, i.email
        FROM {Tables.PURCHASES} p
        LEFT JOIN {Tables.IDENTITIES} i ON i.id = p.identity_id
        WHERE p.id = %s::uuid
        """,
        (purchase_id,),
    )
    if not purchase:
        return {"ok": False, "error": "Purchase not found"}

    # Check for existing open dispute on this purchase
    existing = query_one(
        f"""
        SELECT id, dispute_status FROM {_TABLE}
        WHERE purchase_id = %s::uuid
          AND dispute_status IN ('open', 'under_review')
        """,
        (purchase_id,),
    )
    if existing:
        return {
            "ok": True,
            "already_exists": True,
            "dispute_id": str(existing["id"]),
            "dispute_status": existing["dispute_status"],
            "message": f"Dispute already exists with status '{existing['dispute_status']}'.",
        }

    meta = {
        "created_by": executed_by,
        "email": purchase["email"],
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_TABLE}
                    (purchase_id, identity_id, payment_provider, payment_reference,
                     dispute_status, dispute_reason, amount_gbp, currency,
                     admin_note, metadata)
                VALUES (%s::uuid, %s::uuid, %s, %s, 'open', %s, %s, %s, %s, %s::jsonb)
                RETURNING id, created_at
                """,
                (
                    purchase_id,
                    str(purchase["identity_id"]),
                    purchase["provider"] or "unknown",
                    purchase["provider_payment_id"],
                    (dispute_reason or "")[:2000] if dispute_reason else None,
                    float(purchase["amount_gbp"]) if purchase["amount_gbp"] else None,
                    purchase["currency"] or "GBP",
                    (admin_note or "")[:2000] if admin_note else None,
                    json.dumps(meta, default=str),
                ),
            )
            row = cur.fetchone()
        conn.commit()

    dispute_id = str(row["id"])
    print(f"[ADMIN_DISPUTE] created purchase_id={purchase_id} dispute_id={dispute_id}")

    return {
        "ok": True,
        "already_exists": False,
        "dispute_id": dispute_id,
        "dispute_status": "open",
        "message": "Dispute recorded.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# UPDATE STATUS
# ─────────────────────────────────────────────────────────────────────────────

def update_dispute_status(
    *,
    dispute_id: str,
    new_status: str,
    admin_note: Optional[str] = None,
) -> Dict[str, Any]:
    """Update a dispute's status."""
    if new_status not in _VALID_STATUSES:
        return {"ok": False, "error": f"Invalid status. Must be one of: {', '.join(sorted(_VALID_STATUSES))}"}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {_TABLE}
                SET dispute_status = %s,
                    admin_note = CASE WHEN %s IS NOT NULL
                        THEN COALESCE(admin_note || ' | ', '') || %s
                        ELSE admin_note END,
                    updated_at = NOW()
                WHERE id = %s::uuid
                RETURNING id
                """,
                (new_status, admin_note, admin_note, dispute_id),
            )
            row = cur.fetchone()
        conn.commit()

    if not row:
        return {"ok": False, "error": "Dispute not found"}

    print(f"[ADMIN_DISPUTE] updated dispute_id={dispute_id} status={new_status}")
    return {"ok": True, "dispute_id": dispute_id, "dispute_status": new_status}


# ─────────────────────────────────────────────────────────────────────────────
# GET BY PURCHASE
# ─────────────────────────────────────────────────────────────────────────────

def get_disputes_for_purchase(purchase_id: str) -> List[Dict[str, Any]]:
    """Return all disputes for a purchase, newest first."""
    rows = query_all(
        f"SELECT * FROM {_TABLE} WHERE purchase_id = %s::uuid ORDER BY created_at DESC",
        (purchase_id,),
    )
    return [_row_to_dict(r) for r in rows]
