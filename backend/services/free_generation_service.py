"""
Homepage free generation gate.

This service exists to prevent abuse of the public homepage generation entry
point while preserving TimrX's normal wallet, reservation, job, history, and
provider pipelines. It stores only hashed visitor signals for abuse prevention
and one-free-generation enforcement; raw IP addresses and raw user agents are
not persisted here.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from flask import g, request

from backend.config import config
from backend.db import Tables, fetch_one, query_one, transaction
from backend.services.wallet_service import (
    CreditType,
    WalletService,
    get_credit_type_for_action,
)


FREE_TRIAL_TABLE = "timrx_billing.free_generation_trials"
TRIAL_ACTIVE_STATUSES = ("reserved", "started")
TRIAL_CONSUMING_STATUSES = ("reserved", "started", "completed")


@dataclass
class TrialDecision:
    allowed: bool
    blocked_reason: Optional[str] = None
    trial: Optional[Dict[str, Any]] = None
    active_job: Optional[Dict[str, Any]] = None


def _hash_value(value: str | None, purpose: str) -> Optional[str]:
    text = (value or "").strip()
    if not text:
        return None
    salt = (
        os.getenv("FREE_GENERATION_HASH_SALT")
        or getattr(config, "CSRF_SECRET", "")
        or os.getenv("DATABASE_URL", "")
        or "timrx-free-generation-v1"
    )
    digest = hashlib.sha256(f"{purpose}:{salt}:{text}".encode("utf-8")).hexdigest()
    return digest


def _prompt_hash(prompt: str) -> str:
    normalized = " ".join((prompt or "").strip().lower().split())
    return _hash_value(normalized, "homepage_prompt") or ""


def _client_ip() -> str:
    # Never trust forwarding headers unless explicitly enabled for a trusted
    # proxy/CDN deployment. The hashed IP is used only for abuse prevention and
    # one-free-generation enforcement; raw IPs are not persisted.
    if getattr(config, "HOMEPAGE_FREE_TRUST_PROXY_HEADERS", False):
        raw = request.headers.get("CF-Connecting-IP")
        if raw:
            return raw.split(",", 1)[0].strip()
    return request.remote_addr or ""


def _fingerprint() -> Dict[str, Optional[str]]:
    return {
        "identity_id": getattr(g, "identity_id", None),
        "anonymous_session_id": getattr(g, "session_id", None),
        "ip_hash": _hash_value(_client_ip(), "homepage_ip"),
        "user_agent_hash": _hash_value(request.headers.get("User-Agent"), "homepage_ua"),
    }


def _credit_type_for_action(action_key: str) -> str:
    try:
        return get_credit_type_for_action(action_key)
    except ValueError:
        try:
            from backend.services.pricing_service import get_db_action_code_from_canonical

            db_code = get_db_action_code_from_canonical(action_key)
            if db_code:
                return get_credit_type_for_action(db_code)
        except Exception:
            pass
    return CreditType.GENERAL


def ensure_free_generation_schema() -> None:
    """DDL is intentionally not allowed in request/runtime code."""
    raise RuntimeError(
        "free_generation_trials schema must be applied via deploy_migrations/075_homepage_free_generation_trials.sql"
    )


def _find_existing_trial(fp: Dict[str, Optional[str]], cur=None, lock: bool = False) -> Optional[Dict[str, Any]]:
    clauses = []
    params = []
    if fp.get("identity_id"):
        clauses.append("identity_id = %s")
        params.append(fp["identity_id"])
    if fp.get("anonymous_session_id"):
        clauses.append("anonymous_session_id = %s")
        params.append(fp["anonymous_session_id"])
    if fp.get("ip_hash") and fp.get("user_agent_hash"):
        clauses.append("(ip_hash = %s AND user_agent_hash = %s)")
        params.extend([fp["ip_hash"], fp["user_agent_hash"]])
    if not clauses:
        return None
    sql = f"""
        SELECT *
        FROM {FREE_TRIAL_TABLE}
        WHERE status IN ('reserved', 'started', 'completed')
          AND ({" OR ".join(clauses)})
        ORDER BY created_at ASC
        LIMIT 1
        {"FOR UPDATE" if lock else ""}
    """
    if cur is not None:
        cur.execute(sql, tuple(params))
        return fetch_one(cur)
    return query_one(sql, tuple(params), source="free_trial_find")


def get_trial_for_job(job_id: str) -> Optional[Dict[str, Any]]:
    return query_one(
        f"SELECT * FROM {FREE_TRIAL_TABLE} WHERE job_id::text = %s LIMIT 1",
        (str(job_id),),
        source="free_trial_job",
    )


def get_current_trial_state() -> Dict[str, Any]:
    existing = _find_existing_trial(_fingerprint())
    if not existing:
        return {"ok": True, "eligible": True, "free_trial_remaining": True}
    status = existing.get("status")
    active = status in TRIAL_ACTIVE_STATUSES and existing.get("job_id")
    return {
        "ok": True,
        "eligible": bool(active),
        "free_trial_remaining": False,
        "status": status,
        "active_job_id": str(existing.get("job_id")) if active else None,
        "generation_type": existing.get("generation_type"),
    }


def get_rate_limit_state() -> Dict[str, int]:
    """Return coarse daily counters for homepage free-generation abuse controls."""
    fp = _fingerprint()
    total_row = query_one(
        f"""
        SELECT COUNT(*)::int AS count
        FROM {FREE_TRIAL_TABLE}
        WHERE created_at >= date_trunc('day', now())
          AND status IN ('reserved', 'started', 'completed')
        """,
        (),
        source="free_trial_rate_total",
    ) or {}
    ip_count = 0
    if fp.get("ip_hash"):
        ip_row = query_one(
            f"""
            SELECT COUNT(*)::int AS count
            FROM {FREE_TRIAL_TABLE}
            WHERE created_at >= date_trunc('day', now())
              AND ip_hash = %s
              AND status IN ('reserved', 'started', 'completed')
            """,
            (fp["ip_hash"],),
            source="free_trial_rate_ip",
        ) or {}
        ip_count = int(ip_row.get("count") or 0)
    return {
        "daily_total": int(total_row.get("count") or 0),
        "daily_ip": ip_count,
    }


def reserve_trial(
    prompt: str,
    generation_type: str,
    idempotency_key: str | None = None,
    max_daily_total: int | None = None,
    max_per_ip_per_day: int | None = None,
) -> TrialDecision:
    fp = _fingerprint()
    idem_hash = _hash_value(idempotency_key, "homepage_idempotency")

    try:
        with transaction("free_trial_reserve") as cur:
            # Serialize daily/IP limit checks with insertion so parallel requests
            # cannot all observe the same pre-limit counters and slip through.
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("homepage_free_generation_daily",))
            if fp.get("ip_hash"):
                cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"homepage_free_generation_ip:{fp['ip_hash']}",))

            existing = _find_existing_trial(fp, cur=cur, lock=True)
            if existing:
                status = existing.get("status")
                active_job = (
                    {
                        "job_id": str(existing["job_id"]),
                        "generation_type": existing.get("generation_type"),
                        "status": status,
                    }
                    if status in TRIAL_ACTIVE_STATUSES and existing.get("job_id")
                    else None
                )
                return TrialDecision(
                    allowed=False,
                    blocked_reason="active_trial" if status in TRIAL_ACTIVE_STATUSES else "free_trial_used",
                    trial=existing,
                    active_job=active_job,
                )

            if max_daily_total is not None and int(max_daily_total) > 0:
                cur.execute(
                    f"""
                    SELECT COUNT(*)::int AS count
                    FROM {FREE_TRIAL_TABLE}
                    WHERE created_at >= date_trunc('day', now())
                      AND status IN ('reserved', 'started', 'completed')
                    """
                )
                total_count = int((fetch_one(cur) or {}).get("count") or 0)
                if total_count >= int(max_daily_total):
                    return TrialDecision(allowed=False, blocked_reason="homepage_free_daily_limit")

            if fp.get("ip_hash") and max_per_ip_per_day is not None and int(max_per_ip_per_day) > 0:
                cur.execute(
                    f"""
                    SELECT COUNT(*)::int AS count
                    FROM {FREE_TRIAL_TABLE}
                    WHERE created_at >= date_trunc('day', now())
                      AND ip_hash = %s
                      AND status IN ('reserved', 'started', 'completed')
                    """,
                    (fp["ip_hash"],),
                )
                ip_count = int((fetch_one(cur) or {}).get("count") or 0)
                if ip_count >= int(max_per_ip_per_day):
                    return TrialDecision(allowed=False, blocked_reason="homepage_free_ip_limit")

            cur.execute(
                f"""
                INSERT INTO {FREE_TRIAL_TABLE}
                    (identity_id, anonymous_session_id, ip_hash, user_agent_hash,
                     generation_type, prompt_hash, status, meta)
                VALUES (%s, %s, %s, %s, %s, %s, 'reserved', %s::jsonb)
                RETURNING *
                """,
                (
                    fp.get("identity_id"),
                    fp.get("anonymous_session_id"),
                    fp.get("ip_hash"),
                    fp.get("user_agent_hash"),
                    generation_type,
                    _prompt_hash(prompt),
                    json.dumps({"source": "homepage_chat", "idempotency_key_hash": idem_hash}),
                ),
            )
            trial = fetch_one(cur)
        return TrialDecision(allowed=True, trial=trial)
    except Exception:
        # A parallel tab/request may have won the unique constraint race.
        existing = _find_existing_trial(fp)
        return TrialDecision(
            allowed=False,
            blocked_reason="active_trial" if existing and existing.get("status") in TRIAL_ACTIVE_STATUSES else "free_trial_used",
            trial=existing,
            active_job=(
                {
                    "job_id": str(existing["job_id"]),
                    "generation_type": existing.get("generation_type"),
                    "status": existing.get("status"),
                }
                if existing and existing.get("job_id") and existing.get("status") in TRIAL_ACTIVE_STATUSES
                else None
            ),
        )


def has_paid_balance(identity_id: str, action_key: str, required_credits: int) -> bool:
    if not identity_id or required_credits <= 0:
        return False
    credit_type = _credit_type_for_action(action_key)
    available = WalletService.get_available_balance(identity_id, credit_type)
    return available >= int(required_credits)


def grant_trial_credits(trial_id: str, identity_id: str, action_key: str, amount: int) -> None:
    """Deprecated no-op.

    Homepage trials must never mint normal spendable wallet credits. The
    gateway now creates a system-funded reservation inside start_paid_job().
    """
    return


def bind_trial_reservation(
    trial_id: str,
    job_id: str,
    reservation_id: str,
    action_key: str,
    amount: int,
    credit_type: str,
) -> None:
    if not trial_id or not job_id or not reservation_id:
        return
    with transaction("free_trial_mark_granted") as cur:
        cur.execute(
            f"""
            UPDATE {FREE_TRIAL_TABLE}
            SET trial_credit_amount = %s,
                trial_credit_type = %s,
                job_id = %s,
                reservation_id = %s,
                status = 'started',
                started_at = COALESCE(started_at, now()),
                meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                updated_at = now()
            WHERE id::text = %s
            """,
            (
                int(amount),
                credit_type,
                str(job_id),
                str(reservation_id),
                json.dumps({
                    "reservation_id": str(reservation_id),
                    "action_key": action_key,
                    "system_funded": True,
                }),
                str(trial_id),
            ),
        )


def mark_started(trial_id: str, job_id: str, generation_type: str, meta: Optional[Dict[str, Any]] = None) -> None:
    if not trial_id or not job_id:
        return
    with transaction("free_trial_started") as cur:
        cur.execute(
            f"""
            UPDATE {FREE_TRIAL_TABLE}
            SET job_id = %s,
                generation_type = %s,
                status = 'started',
                started_at = COALESCE(started_at, now()),
                meta = COALESCE(meta, '{{}}'::jsonb) || %s::jsonb,
                updated_at = now()
            WHERE id::text = %s
            """,
            (str(job_id), generation_type, _json_meta(meta), str(trial_id)),
        )


def mark_completed(job_id: str) -> None:
    with transaction("free_trial_completed") as cur:
        cur.execute(
            f"""
            UPDATE {FREE_TRIAL_TABLE}
            SET status = 'completed',
                completed_at = COALESCE(completed_at, now()),
                updated_at = now()
            WHERE job_id::text = %s
              AND status IN ('reserved', 'started')
            """,
            (str(job_id),),
        )


def mark_completed_by_reservation(reservation_id: str, job_id: str | None = None) -> None:
    if not reservation_id:
        return
    with transaction("free_trial_completed_by_reservation") as cur:
        cur.execute(
            f"""
            UPDATE {FREE_TRIAL_TABLE}
            SET status = 'completed',
                job_id = COALESCE(job_id, %s),
                completed_at = COALESCE(completed_at, now()),
                updated_at = now()
            WHERE reservation_id::text = %s
              AND status IN ('reserved', 'started')
            """,
            (str(job_id) if job_id else None, str(reservation_id)),
        )


def mark_failed(job_id: str, reason: str = "generation_failed") -> None:
    with transaction("free_trial_failed") as cur:
        cur.execute(
            f"""
            UPDATE {FREE_TRIAL_TABLE}
            SET status = 'failed',
                failed_at = COALESCE(failed_at, now()),
                blocked_reason = %s,
                updated_at = now()
            WHERE job_id::text = %s
              AND status IN ('reserved', 'started')
            """,
            (reason[:120], str(job_id)),
        )


def mark_failed_by_reservation(reservation_id: str, reason: str = "generation_failed", job_id: str | None = None) -> None:
    if not reservation_id:
        return
    with transaction("free_trial_failed_by_reservation") as cur:
        cur.execute(
            f"""
            UPDATE {FREE_TRIAL_TABLE}
            SET status = 'failed',
                job_id = COALESCE(job_id, %s),
                failed_at = COALESCE(failed_at, now()),
                blocked_reason = %s,
                updated_at = now()
            WHERE reservation_id::text = %s
              AND status IN ('reserved', 'started')
            """,
            (str(job_id) if job_id else None, reason[:120], str(reservation_id)),
        )


def mark_trial_failed(trial_id: str, reason: str = "generation_failed") -> None:
    with transaction("free_trial_failed_by_id") as cur:
        cur.execute(
            f"""
            UPDATE {FREE_TRIAL_TABLE}
            SET status = 'failed',
                failed_at = COALESCE(failed_at, now()),
                blocked_reason = %s,
                updated_at = now()
            WHERE id::text = %s
              AND status IN ('reserved', 'started')
            """,
            (reason[:120], str(trial_id)),
        )


def reverse_trial_credits_if_failed(job_id: str, reason: str = "generation_failed") -> None:
    """Deprecated no-op: system-funded trials never grant wallet credits."""
    return


def reverse_trial_credits_by_trial_id(trial_id: str, reason: str = "generation_failed") -> None:
    """Deprecated no-op: system-funded trials never grant wallet credits."""
    return


def _json_meta(meta: Optional[Dict[str, Any]]) -> str:
    if not meta:
        return "{}"
    import json

    return json.dumps(meta)
