"""
Analytics Events Service — server-side queue for dataLayer events the browser
still needs to fire (Google Tag Manager → GA4 + Google Ads).

Why this exists
---------------
Google Ads / GA4 purchase conversions MUST only fire after a payment is truly
finalised: Mollie webhook delivered, credits granted to the user's wallet,
purchase row persisted, refunds branch not taken. None of that is known to the
browser at the moment the user returns from the bank/iDEAL redirect — the
webhook usually lands seconds *after* the redirect.

We solve that by enqueueing the conversion server-side at the exact moment the
finalisation succeeds, and the browser polls + fires + acks on its next visit.

Idempotency (defence in depth)
------------------------------
1. ``event_id`` UNIQUE — same purchase can never enqueue twice.
2. ``fired_at`` — once acked, the row is no longer returned by ``list_pending``.
3. The browser-side helper (``analytics.js``) keeps a localStorage allow-list of
   already-pushed event_ids.
4. GA4 dedups purchase events natively by ``transaction_id``.

Anonymous-first model is preserved: events are scoped to ``identity_id`` (the
anonymous identity is fine — the same identity owns the wallet and the purchase).
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from backend.db import (
    USE_DB,
    Tables,
    get_conn,
    query_all,
    execute,
)


# Canonical event names. Keep aligned with the trigger names used in GTM tags.
EVENT_PURCHASE              = "purchase"
EVENT_CHECKOUT_STARTED      = "begin_checkout"   # client-only, kept for symmetry
EVENT_SIGN_UP               = "sign_up"
EVENT_EMAIL_VERIFIED        = "email_verified"
EVENT_GENERATION_STARTED    = "generation_started"
EVENT_GENERATION_COMPLETED  = "generation_completed"


# Max payload size we'll accept. Keeps the table small and prevents an upstream
# bug from filling rows with megabytes of crud. 8 KB is generous for any
# realistic ecommerce/event payload.
_MAX_PAYLOAD_BYTES = 8 * 1024


def _truncate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Defensive: keep payload under our size limit so we never balloon the row."""
    try:
        serialised = json.dumps(payload, default=str)
        if len(serialised.encode("utf-8")) <= _MAX_PAYLOAD_BYTES:
            return payload
        # Drop optional fields most likely to be bloated; preserve the conversion-critical core.
        slim = {k: payload.get(k) for k in (
            "transaction_id", "value", "currency", "items",
            "plan_code", "credits", "credit_type", "method"
        ) if k in payload}
        return slim
    except Exception:
        return {}


def enqueue(
    identity_id: str,
    event_name: str,
    event_id: str,
    payload: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Idempotently enqueue a server-side conversion event.

    Args:
        identity_id: The TimrX identity that this event belongs to.
        event_name:  Canonical event name (see EVENT_* constants).
        event_id:    Stable idempotency key, e.g. 'purchase:tr_xyz123'.
        payload:     JSON-safe dict that will be passed verbatim to dataLayer.push.

    Returns:
        True if a row was inserted, False if it already existed (dup-safe).
    """
    if not USE_DB or not identity_id or not event_id:
        return False

    payload = _truncate_payload(payload or {})
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {Tables.ANALYTICS_EVENTS}
                        (identity_id, event_name, event_id, payload)
                    VALUES (%s, %s, %s, %s::jsonb)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING id
                    """,
                    (str(identity_id), event_name, event_id, json.dumps(payload)),
                )
                inserted = cur.fetchone() is not None
            conn.commit()
        if inserted:
            print(f"[ANALYTICS] enqueued event_name={event_name} event_id={event_id} identity={identity_id[:8]}...")
        else:
            print(f"[ANALYTICS] duplicate event ignored event_id={event_id}")
        return inserted
    except Exception as e:
        # Never let analytics break a finalisation flow.
        print(f"[ANALYTICS] enqueue failed (non-fatal): event_id={event_id} err={e}")
        return False


def list_pending(identity_id: str, *, limit: int = 25) -> List[Dict[str, Any]]:
    """Return all not-yet-fired events for this identity, oldest first."""
    if not USE_DB or not identity_id:
        return []
    try:
        rows = query_all(
            f"""
            SELECT event_name, event_id, payload, created_at
            FROM {Tables.ANALYTICS_EVENTS}
            WHERE identity_id = %s
              AND fired_at IS NULL
            ORDER BY created_at ASC
            LIMIT %s
            """,
            (str(identity_id), int(limit)),
        )
        out: List[Dict[str, Any]] = []
        for r in rows or []:
            payload = r.get("payload") or {}
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    payload = {}
            out.append({
                "event_name": r["event_name"],
                "event_id": r["event_id"],
                "payload": payload,
                # Browser uses this to forward GA4's `timestamp_micros` if it wants
                "created_at_unix": int(r["created_at"].timestamp()) if r.get("created_at") else int(time.time()),
            })
        return out
    except Exception as e:
        print(f"[ANALYTICS] list_pending failed (non-fatal): identity={identity_id[:8]}... err={e}")
        return []


def ack(identity_id: str, event_ids: List[str]) -> int:
    """
    Mark events as fired. Scoped to identity_id so a hostile client can't ack
    someone else's queue. Returns the number of rows updated.
    """
    if not USE_DB or not identity_id or not event_ids:
        return 0
    # Cap to a sane batch size.
    event_ids = [str(eid) for eid in event_ids if eid][:100]
    if not event_ids:
        return 0
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {Tables.ANALYTICS_EVENTS}
                    SET fired_at = NOW(),
                        fired_count = fired_count + 1
                    WHERE identity_id = %s
                      AND event_id = ANY(%s)
                      AND fired_at IS NULL
                    """,
                    (str(identity_id), event_ids),
                )
                updated = cur.rowcount or 0
            conn.commit()
        if updated:
            print(f"[ANALYTICS] acked {updated}/{len(event_ids)} events for identity={identity_id[:8]}...")
        return updated
    except Exception as e:
        print(f"[ANALYTICS] ack failed (non-fatal): identity={identity_id[:8]}... err={e}")
        return 0
