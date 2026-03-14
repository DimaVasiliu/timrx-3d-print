"""
Alert Service — persistent, deduplicated admin alerting.

Wraps notify_admin() with:
  1. Database persistence (provider_alerts table)
  2. Cooldown-based deduplication (same alert_key suppressed for N minutes)
  3. Occurrence counting (tracks repeated events even when email is suppressed)

Safety guarantees:
  - Alert recording never crashes the calling code (wrapped in try/except)
  - Email send failure still persists the alert row
  - All writes are idempotent on alert_key
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from backend.db import USE_DB, get_conn

_BILLING_SCHEMA = "timrx_billing"
_TABLE = f"{_BILLING_SCHEMA}.provider_alerts"


# ─────────────────────────────────────────────────────────────────────────────
# CORE: send with dedup + persistence
# ─────────────────────────────────────────────────────────────────────────────

def send_admin_alert_once(
    *,
    alert_key: str,
    alert_type: str,
    subject: str,
    message: str,
    severity: str = "warning",
    provider: str | None = None,
    related_job_id: str | None = None,
    related_subscription_id: str | None = None,
    metadata: dict | None = None,
    cooldown_minutes: int = 15,
) -> bool:
    """
    Record an alert and send email only if cooldown has expired.

    Args:
        alert_key:   Stable dedup key (e.g. "provider_wallet_depleted:meshy")
        alert_type:  Category (e.g. "wallet_depleted", "stale_credit_date")
        subject:     Email subject
        message:     Email body
        severity:    "info" | "warning" | "critical"
        provider:    Provider name if applicable
        related_job_id: Job UUID if applicable
        related_subscription_id: Subscription UUID if applicable
        metadata:    Arbitrary JSON data
        cooldown_minutes: Minimum minutes between emails for same alert_key

    Returns:
        True if email was sent, False if suppressed or on error.
    """
    should_send = _record_alert(
        alert_key=alert_key,
        alert_type=alert_type,
        subject=subject,
        message=message,
        severity=severity,
        provider=provider,
        related_job_id=related_job_id,
        related_subscription_id=related_subscription_id,
        metadata=metadata,
        cooldown_minutes=cooldown_minutes,
    )

    if not should_send:
        return False

    # Send the email (best-effort)
    try:
        from backend.emailer import notify_admin

        data = metadata.copy() if metadata else {}
        if provider:
            data["provider"] = provider
        if related_job_id:
            data["job_id"] = related_job_id
        if related_subscription_id:
            data["subscription_id"] = related_subscription_id
        data["severity"] = severity

        sent = notify_admin(subject=subject, message=message, data=data or None, _skip_history=True)

        # Mark that we actually sent
        if sent:
            _mark_sent(alert_key)

        return sent
    except Exception as e:
        print(f"[ALERT] Email send failed for {alert_key}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# RECORD-ONLY: persist without email (for one-off events we just want to log)
# ─────────────────────────────────────────────────────────────────────────────

def record_admin_alert(
    *,
    alert_key: str,
    alert_type: str,
    subject: str,
    message: str = "",
    severity: str = "info",
    provider: str | None = None,
    related_job_id: str | None = None,
    related_subscription_id: str | None = None,
    metadata: dict | None = None,
) -> None:
    """
    Persist an alert event without dedup or email.
    Used for one-off business events (new subscription, cancellation, etc.)
    that should appear in alert history but don't need cooldown.
    """
    if not USE_DB:
        return
    try:
        meta_json = json.dumps(metadata, default=str) if metadata else None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {_TABLE}
                        (alert_key, alert_type, provider, subject, message,
                         severity, related_job_id, related_subscription_id,
                         metadata, last_sent_at)
                    VALUES (%s, %s, %s, %s, %s, %s,
                            %s::uuid, %s::uuid, %s::jsonb, NOW())
                    """,
                    (
                        alert_key,
                        alert_type,
                        provider,
                        subject,
                        (message or "")[:2000],
                        severity,
                        related_job_id,
                        related_subscription_id,
                        meta_json,
                    ),
                )
            conn.commit()
    except Exception as e:
        print(f"[ALERT] Failed to record alert {alert_key}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# RESOLVE: mark an alert as no longer active
# ─────────────────────────────────────────────────────────────────────────────

def mark_alert_resolved(alert_key: str) -> None:
    """Mark an active alert as resolved (is_active = false)."""
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {_TABLE}
                    SET is_active = FALSE
                    WHERE alert_key = %s AND is_active = TRUE
                    """,
                    (alert_key,),
                )
            conn.commit()
    except Exception as e:
        print(f"[ALERT] Failed to resolve alert {alert_key}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _record_alert(
    *,
    alert_key: str,
    alert_type: str,
    subject: str,
    message: str,
    severity: str,
    provider: str | None,
    related_job_id: str | None,
    related_subscription_id: str | None,
    metadata: dict | None,
    cooldown_minutes: int,
) -> bool:
    """
    Insert or update alert row. Returns True if email should be sent.

    Logic:
      - No existing row → INSERT, return True (send email)
      - Existing row, cooldown NOT expired → UPDATE counts, return False
      - Existing row, cooldown expired → UPDATE counts + last_sent_at, return True
    """
    if not USE_DB:
        # No DB — always send to avoid losing alerts
        return True

    try:
        meta_json = json.dumps(metadata, default=str) if metadata else None
        now = datetime.now(timezone.utc)
        cooldown_threshold = now - timedelta(minutes=cooldown_minutes)

        with get_conn() as conn:
            with conn.cursor() as cur:
                # Try to find existing active alert with this key
                cur.execute(
                    f"""
                    SELECT id, last_sent_at, occurrence_count
                    FROM {_TABLE}
                    WHERE alert_key = %s AND is_active = TRUE
                    ORDER BY last_seen_at DESC
                    LIMIT 1
                    """,
                    (alert_key,),
                )
                row = cur.fetchone()

                if row is None:
                    # New alert — insert and send
                    cur.execute(
                        f"""
                        INSERT INTO {_TABLE}
                            (alert_key, alert_type, provider, subject, message,
                             severity, related_job_id, related_subscription_id,
                             metadata, last_sent_at)
                        VALUES (%s, %s, %s, %s, %s, %s,
                                %s::uuid, %s::uuid, %s::jsonb, NOW())
                        """,
                        (
                            alert_key,
                            alert_type,
                            provider,
                            subject,
                            (message or "")[:2000],
                            severity,
                            related_job_id,
                            related_subscription_id,
                            meta_json,
                        ),
                    )
                    conn.commit()
                    return True

                # Existing alert — check cooldown
                alert_id = row["id"]
                last_sent = row["last_sent_at"]
                count = row["occurrence_count"] or 1

                should_send = (last_sent is None) or (last_sent < cooldown_threshold)

                if should_send:
                    # Cooldown expired — update and send
                    cur.execute(
                        f"""
                        UPDATE {_TABLE}
                        SET last_seen_at = NOW(),
                            last_sent_at = NOW(),
                            occurrence_count = %s,
                            message = %s,
                            metadata = COALESCE(%s::jsonb, metadata),
                            severity = %s
                        WHERE id = %s
                        """,
                        (count + 1, (message or "")[:2000], meta_json, severity, alert_id),
                    )
                else:
                    # Cooldown active — update occurrence only, no email
                    cur.execute(
                        f"""
                        UPDATE {_TABLE}
                        SET last_seen_at = NOW(),
                            occurrence_count = %s,
                            metadata = COALESCE(%s::jsonb, metadata)
                        WHERE id = %s
                        """,
                        (count + 1, meta_json, alert_id),
                    )

                conn.commit()
                return should_send

    except Exception as e:
        print(f"[ALERT] DB error recording alert {alert_key}: {e}")
        # On DB error, send email anyway to avoid losing critical alerts
        return True


def _mark_sent(alert_key: str) -> None:
    """Update last_sent_at after successful email delivery."""
    if not USE_DB:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    UPDATE {_TABLE}
                    SET last_sent_at = NOW()
                    WHERE alert_key = %s AND is_active = TRUE
                    """,
                    (alert_key,),
                )
            conn.commit()
    except Exception:
        pass  # Best-effort timestamp update
