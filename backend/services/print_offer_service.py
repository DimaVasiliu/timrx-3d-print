"""
Print-order launch offers, referrals and money-denominated print credit.

This service deliberately sits above the base price calculator.  The
calculator answers "what does this order cost"; this module answers "what
should this signed-in customer pay after automatic SaaS-style offers".
"""

from __future__ import annotations

import json
import re
import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from backend.db import Tables, get_conn, hash_string
from backend.services.print_order_pricing import PriceBreakdown


FIRST_SMALL_PRINT_FREE = "FIRST_SMALL_PRINT_FREE"
REFERRAL_REWARD = "REFERRAL_REWARD"
QUALIFYING_PAID_STATUSES = ("paid", "in_production", "shipped", "delivered")
REFERRAL_REWARD_CENTS = {"USD": 500, "EUR": 500, "USD": 500}
MIN_PROVIDER_CHARGE_CENTS = 100


def cents_to_amount(cents: int) -> float:
    return round(int(cents or 0) / 100.0, 2)


def ip_hash(ip_address: Optional[str]) -> Optional[str]:
    ip = (ip_address or "").strip().lower()
    if not ip:
        return None
    return hash_string(f"print-ip:v1:{ip}")


def shipping_address_hash(shipping: Dict[str, Any]) -> Optional[str]:
    parts = [
        shipping.get("email"),
        shipping.get("address"),
        shipping.get("city"),
        shipping.get("postal"),
        shipping.get("country"),
    ]
    normalized = "|".join(_norm(x) for x in parts)
    if not normalized.replace("|", ""):
        return None
    return hash_string(f"print-address:v1:{normalized}")


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _token() -> str:
    return "txr_" + secrets.token_urlsafe(9).replace("-", "").replace("_", "")[:12]


def get_or_create_referral_code(identity_id: str) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            return _get_or_create_referral_code_cur(cur, identity_id)


def _get_or_create_referral_code_cur(cur, identity_id: str) -> Dict[str, Any]:
    cur.execute(
        f"""
        SELECT id, token, status, created_at
        FROM {Tables.PRINT_REFERRAL_CODES}
        WHERE identity_id = %s
        LIMIT 1
        """,
        (identity_id,),
    )
    row = cur.fetchone()
    if row:
        r = dict(row)
        return {"id": str(r["id"]), "token": r["token"], "status": r["status"]}

    for _ in range(5):
        token = _token()
        try:
            cur.execute(
                f"""
                INSERT INTO {Tables.PRINT_REFERRAL_CODES} (identity_id, token)
                VALUES (%s, %s)
                RETURNING id, token, status
                """,
                (identity_id, token),
            )
            r = dict(cur.fetchone())
            return {"id": str(r["id"]), "token": r["token"], "status": r["status"]}
        except Exception:
            # Token collision is extremely unlikely; retry before surfacing.
            continue
    raise RuntimeError("Could not create referral code")


def claim_referral_token(identity_id: str, token: str, request_ip: Optional[str] = None) -> Dict[str, Any]:
    token = (token or "").strip()
    if not token:
        return {"claimed": False, "reason": "missing_token"}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, identity_id
                FROM {Tables.PRINT_REFERRAL_CODES}
                WHERE token = %s AND status = 'active'
                LIMIT 1
                """,
                (token,),
            )
            code = cur.fetchone()
            if not code:
                return {"claimed": False, "reason": "invalid_token"}
            c = dict(code)
            referrer_id = str(c["identity_id"])
            if referrer_id == str(identity_id):
                return {"claimed": False, "reason": "self_referral"}

            cur.execute(
                f"""
                INSERT INTO {Tables.PRINT_REFERRAL_ATTRIBUTIONS} (
                    referrer_identity_id, referred_identity_id, referral_code_id, signup_ip_hash
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (referred_identity_id) DO NOTHING
                RETURNING id
                """,
                (referrer_id, identity_id, c["id"], ip_hash(request_ip)),
            )
            inserted = cur.fetchone()
        conn.commit()
    return {"claimed": bool(inserted), "reason": None if inserted else "already_attributed"}


def referral_summary(identity_id: str, base_url: str = "") -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            code = _get_or_create_referral_code_cur(cur, identity_id)
            cur.execute(
                f"""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                    COUNT(*) FILTER (WHERE status = 'rewarded') AS rewarded
                FROM {Tables.PRINT_REFERRAL_ATTRIBUTIONS}
                WHERE referrer_identity_id = %s
                """,
                (identity_id,),
            )
            stats = dict(cur.fetchone() or {})
            cur.execute(
                f"""
                SELECT currency, COALESCE(SUM(remaining_cents), 0) AS balance_cents
                FROM {Tables.PRINT_CREDIT_LEDGER}
                WHERE identity_id = %s
                  AND remaining_cents > 0
                  AND (expires_at IS NULL OR expires_at > NOW())
                GROUP BY currency
                ORDER BY currency
                """,
                (identity_id,),
            )
            balances = [dict(r) for r in cur.fetchall()]
    root = (base_url or "").rstrip("/")
    return {
        "token": code["token"],
        "url": f"{root}/?ref={code['token']}" if root else f"/?ref={code['token']}",
        "pending": int(stats.get("pending") or 0),
        "rewarded": int(stats.get("rewarded") or 0),
        "balances": balances,
    }


def pending_referral_attribution_id(cur, identity_id: str) -> Optional[str]:
    cur.execute(
        f"""
        SELECT id
        FROM {Tables.PRINT_REFERRAL_ATTRIBUTIONS}
        WHERE referred_identity_id = %s
          AND status = 'pending'
        LIMIT 1
        """,
        (identity_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return str(dict(row)["id"])


def compute_offer_quote(
    identity_id: str,
    base: PriceBreakdown,
    spec: Dict[str, Any],
    shipping: Dict[str, Any],
    request_ip: Optional[str] = None,
    *,
    cur=None,
) -> Dict[str, Any]:
    """
    Return a final payable quote with automatic discounts.

    No rows are mutated here.  Order creation calls reserve_for_order() inside
    a transaction after recomputing this same quote under an identity lock.
    """
    if cur is not None:
        return _compute_offer_quote_cur(cur, identity_id, base, spec, shipping, request_ip)
    with get_conn() as conn:
        with conn.cursor() as cur2:
            return _compute_offer_quote_cur(cur2, identity_id, base, spec, shipping, request_ip)


def _compute_offer_quote_cur(
    cur,
    identity_id: str,
    base: PriceBreakdown,
    spec: Dict[str, Any],
    shipping: Dict[str, Any],
    request_ip: Optional[str],
) -> Dict[str, Any]:
    ip_h = ip_hash(request_ip)
    addr_h = shipping_address_hash(shipping)
    launch = _first_small_print_status(cur, identity_id, base, spec, ip_h, addr_h)
    adjustments: List[Dict[str, Any]] = []

    discount_cents = 0
    offer_code = None
    if launch["eligible"]:
        discount_cents = min(base.subtotal_cents, base.total_cents)
        offer_code = FIRST_SMALL_PRINT_FREE
        launch["applied"] = True
        adjustments.append(_adjustment(FIRST_SMALL_PRINT_FREE, "First small print free", -discount_cents, base.currency))

    credit_available = _credit_balance_cur(cur, identity_id, base.currency)
    print_credit_cents = 0
    if not offer_code and credit_available > 0:
        max_credit = max(0, base.total_cents - discount_cents - MIN_PROVIDER_CHARGE_CENTS)
        print_credit_cents = min(credit_available, max_credit)
        if print_credit_cents > 0:
            adjustments.append(_adjustment("PRINT_CREDIT", "Print credit", -print_credit_cents, base.currency))

    final_total_cents = max(0, base.total_cents - discount_cents - print_credit_cents)
    return {
        **base.to_dict(),
        "base_total_cents": base.total_cents,
        "base_total": cents_to_amount(base.total_cents),
        "discount_cents": discount_cents,
        "discount": cents_to_amount(discount_cents),
        "print_credit_cents": print_credit_cents,
        "print_credit": cents_to_amount(print_credit_cents),
        "total_cents": final_total_cents,
        "total": cents_to_amount(final_total_cents),
        "offer_code": offer_code,
        "offer_snapshot": {
            "launch_offer": launch,
            "adjustments": adjustments,
            "credit_available_cents": credit_available,
            "credit_available": cents_to_amount(credit_available),
            "ip_fingerprint_present": bool(ip_h),
            "shipping_address_fingerprint_present": bool(addr_h),
        },
        "adjustments": adjustments,
        "launch_offer": launch,
        "credit_available_cents": credit_available,
    }


def reserve_for_order(
    cur,
    identity_id: str,
    order_id: str,
    quote: Dict[str, Any],
    request_ip: Optional[str],
    shipping: Dict[str, Any],
) -> None:
    offer_code = quote.get("offer_code")
    if offer_code == FIRST_SMALL_PRINT_FREE and int(quote.get("discount_cents") or 0) > 0:
        cur.execute(
            f"""
            INSERT INTO {Tables.PRINT_OFFER_REDEMPTIONS} (
                identity_id, order_id, offer_code, currency, amount_cents,
                status, ip_hash, shipping_address_hash, meta
            ) VALUES (%s, %s, %s, %s, %s, 'reserved', %s, %s, %s::jsonb)
            """,
            (
                identity_id,
                order_id,
                FIRST_SMALL_PRINT_FREE,
                quote["currency"],
                int(quote["discount_cents"]),
                ip_hash(request_ip),
                shipping_address_hash(shipping),
                json.dumps(quote.get("offer_snapshot") or {}),
            ),
        )

    credit_cents = int(quote.get("print_credit_cents") or 0)
    if credit_cents > 0:
        _reserve_print_credit_cur(cur, identity_id, quote["currency"], credit_cents, order_id)


def finalize_order_benefits(cur, order: Dict[str, Any]) -> None:
    order_id = str(order["id"])
    identity_id = str(order["identity_id"])
    cur.execute(
        f"""
        UPDATE {Tables.PRINT_OFFER_REDEMPTIONS}
        SET status = 'claimed', claimed_at = NOW()
        WHERE order_id = %s AND status = 'reserved'
        """,
        (order_id,),
    )
    cur.execute(
        f"""
        UPDATE {Tables.PRINT_CREDIT_LEDGER}
        SET entry_type = 'debit_spent'
        WHERE order_id = %s AND entry_type = 'debit_reserved'
        """,
        (order_id,),
    )
    _grant_referral_rewards_if_eligible(cur, identity_id, order)


def release_order_reservations(order_id: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {Tables.PRINT_OFFER_REDEMPTIONS}
                SET status = 'released', released_at = NOW()
                WHERE order_id = %s AND status = 'reserved'
                """,
                (order_id,),
            )
            cur.execute(
                f"""
                SELECT id, meta
                FROM {Tables.PRINT_CREDIT_LEDGER}
                WHERE order_id = %s AND entry_type = 'debit_reserved'
                FOR UPDATE
                """,
                (order_id,),
            )
            debits = [dict(r) for r in cur.fetchall()]
            for debit in debits:
                meta = debit.get("meta") or {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except Exception:
                        meta = {}
                for allocation in meta.get("allocations") or []:
                    ledger_id = allocation.get("ledger_id")
                    amount = int(allocation.get("amount_cents") or 0)
                    if ledger_id and amount > 0:
                        cur.execute(
                            f"""
                            UPDATE {Tables.PRINT_CREDIT_LEDGER}
                            SET remaining_cents = remaining_cents + %s
                            WHERE id = %s
                            """,
                            (amount, ledger_id),
                        )
                cur.execute(
                    f"""
                    UPDATE {Tables.PRINT_CREDIT_LEDGER}
                    SET entry_type = 'debit_released'
                    WHERE id = %s
                    """,
                    (debit["id"],),
                )
        conn.commit()


def _first_small_print_status(
    cur,
    identity_id: str,
    base: PriceBreakdown,
    spec: Dict[str, Any],
    ip_h: Optional[str],
    addr_h: Optional[str],
) -> Dict[str, Any]:
    qty = int(spec.get("quantity") or 1)
    dims = spec.get("scaled_dimensions_mm") or []
    target_h = _height_mm(spec, dims)
    eligible_shape = qty == 1 and target_h is not None and target_h <= 50.0

    reason = None
    if qty != 1:
        reason = "Only one print can use the launch free-print offer."
    elif target_h is None:
        reason = "Run Print Check so we can verify the 50 mm launch-offer size limit."
    elif target_h > 50.0:
        reason = "Choose 50 mm height or smaller to use the first-small-print offer."

    cur.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM {Tables.PRINT_ORDERS}
        WHERE identity_id = %s
          AND status = ANY(%s)
        """,
        (identity_id, list(QUALIFYING_PAID_STATUSES)),
    )
    has_paid = int((dict(cur.fetchone() or {}).get("n") or 0)) > 0
    if has_paid and not reason:
        reason = "Launch offer is only available before your first paid print order."

    cur.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM {Tables.PRINT_OFFER_REDEMPTIONS}
        WHERE identity_id = %s
          AND offer_code = %s
          AND status IN ('reserved', 'claimed')
        """,
        (identity_id, FIRST_SMALL_PRINT_FREE),
    )
    already_claimed = int((dict(cur.fetchone() or {}).get("n") or 0)) > 0
    if already_claimed and not reason:
        reason = "Launch offer has already been used on this account."

    fingerprint_seen = False
    if ip_h or addr_h:
        cur.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM {Tables.PRINT_OFFER_REDEMPTIONS}
            WHERE offer_code = %s
              AND status IN ('reserved', 'claimed')
              AND identity_id <> %s
              AND (
                    (%s IS NOT NULL AND ip_hash = %s)
                 OR (%s IS NOT NULL AND shipping_address_hash = %s)
              )
            """,
            (FIRST_SMALL_PRINT_FREE, identity_id, ip_h, ip_h, addr_h, addr_h),
        )
        fingerprint_seen = int((dict(cur.fetchone() or {}).get("n") or 0)) > 0
        if fingerprint_seen and not reason:
            reason = "This launch offer has already been claimed from the same order fingerprint."

    eligible = eligible_shape and not has_paid and not already_claimed and not fingerprint_seen
    return {
        "code": FIRST_SMALL_PRINT_FREE,
        "eligible": eligible,
        "applied": False,
        "reason": None if eligible else reason,
        "max_height_mm": 50,
        "detected_height_mm": target_h,
        "shipping_required": True,
    }


def _height_mm(spec: Dict[str, Any], dims: Any) -> Optional[float]:
    try:
        target = spec.get("target_height_mm")
        if target is not None:
            return float(target)
        if isinstance(dims, (list, tuple)) and len(dims) == 3:
            return float(dims[1])
    except (TypeError, ValueError):
        return None
    return None


def _credit_balance_cur(cur, identity_id: str, currency: str) -> int:
    cur.execute(
        f"""
        SELECT COALESCE(SUM(remaining_cents), 0) AS cents
        FROM {Tables.PRINT_CREDIT_LEDGER}
        WHERE identity_id = %s
          AND currency = %s
          AND remaining_cents > 0
          AND (expires_at IS NULL OR expires_at > NOW())
        """,
        (identity_id, currency),
    )
    return int((dict(cur.fetchone() or {}).get("cents") or 0))


def _reserve_print_credit_cur(cur, identity_id: str, currency: str, amount_cents: int, order_id: str) -> None:
    remaining = int(amount_cents)
    allocations: List[Dict[str, Any]] = []
    cur.execute(
        f"""
        SELECT id, remaining_cents
        FROM {Tables.PRINT_CREDIT_LEDGER}
        WHERE identity_id = %s
          AND currency = %s
          AND remaining_cents > 0
          AND (expires_at IS NULL OR expires_at > NOW())
        ORDER BY expires_at NULLS LAST, created_at ASC
        FOR UPDATE
        """,
        (identity_id, currency),
    )
    for row in cur.fetchall():
        if remaining <= 0:
            break
        r = dict(row)
        take = min(remaining, int(r["remaining_cents"]))
        cur.execute(
            f"""
            UPDATE {Tables.PRINT_CREDIT_LEDGER}
            SET remaining_cents = remaining_cents - %s
            WHERE id = %s
            """,
            (take, r["id"]),
        )
        allocations.append({"ledger_id": str(r["id"]), "amount_cents": take})
        remaining -= take

    if remaining != 0:
        raise RuntimeError("Print credit balance changed while creating checkout")

    cur.execute(
        f"""
        INSERT INTO {Tables.PRINT_CREDIT_LEDGER} (
            identity_id, currency, entry_type, amount_cents, remaining_cents,
            ref_type, ref_id, order_id, meta
        ) VALUES (%s, %s, 'debit_reserved', %s, 0, 'print_order', %s, %s, %s::jsonb)
        """,
        (identity_id, currency, -amount_cents, order_id, order_id, json.dumps({"allocations": allocations})),
    )


def _grant_referral_rewards_if_eligible(cur, identity_id: str, order: Dict[str, Any]) -> None:
    if order.get("offer_code") == FIRST_SMALL_PRINT_FREE:
        return

    order_id = str(order["id"])
    currency = str(order.get("currency") or "USD")
    reward_cents = REFERRAL_REWARD_CENTS.get(currency, 500)

    cur.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM {Tables.PRINT_ORDERS}
        WHERE identity_id = %s
          AND status = ANY(%s)
          AND COALESCE(offer_code, '') <> %s
        """,
        (identity_id, list(QUALIFYING_PAID_STATUSES), FIRST_SMALL_PRINT_FREE),
    )
    if int((dict(cur.fetchone() or {}).get("n") or 0)) != 1:
        return

    cur.execute(
        f"""
        SELECT id, referrer_identity_id
        FROM {Tables.PRINT_REFERRAL_ATTRIBUTIONS}
        WHERE referred_identity_id = %s
          AND status = 'pending'
        LIMIT 1
        FOR UPDATE
        """,
        (identity_id,),
    )
    attr = cur.fetchone()
    if not attr:
        return
    a = dict(attr)
    attribution_id = str(a["id"])
    referrer_id = str(a["referrer_identity_id"])

    cur.execute(
        f"""
        UPDATE {Tables.PRINT_REFERRAL_ATTRIBUTIONS}
        SET status = 'rewarded',
            first_paid_order_id = %s,
            reward_currency = %s,
            reward_cents = %s,
            rewarded_at = NOW()
        WHERE id = %s
        """,
        (order_id, currency, reward_cents, attribution_id),
    )
    for target_identity, side in ((referrer_id, "referrer"), (identity_id, "referred")):
        cur.execute(
            f"""
            INSERT INTO {Tables.PRINT_CREDIT_LEDGER} (
                identity_id, currency, entry_type, amount_cents, remaining_cents,
                ref_type, ref_id, order_id, meta, expires_at
            ) VALUES (%s, %s, 'referral_reward', %s, %s, 'print_referral', %s, %s, %s::jsonb, NOW() + INTERVAL '12 months')
            ON CONFLICT (identity_id, entry_type, ref_type, ref_id)
                WHERE ref_type IS NOT NULL AND ref_id IS NOT NULL
                DO NOTHING
            """,
            (
                target_identity,
                currency,
                reward_cents,
                reward_cents,
                f"{attribution_id}:{side}",
                order_id,
                json.dumps({"side": side, "attribution_id": attribution_id}),
            ),
        )


def _adjustment(code: str, label: str, amount_cents: int, currency: str) -> Dict[str, Any]:
    return {
        "code": code,
        "label": label,
        "amount_cents": int(amount_cents),
        "amount": cents_to_amount(amount_cents),
        "currency": currency,
    }
