"""
Cloudflare Turnstile verification for abuse-sensitive public flows.

This service is intentionally small and fail-closed when enabled:
- missing secret => verification fails
- missing token => verification fails
- Cloudflare/network error => verification fails

Used by the homepage free-generation gateway before any free trial,
reservation, or provider job can be created.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import requests

from backend.config import config


SITEVERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"


@dataclass
class TurnstileResult:
    ok: bool
    reason: str = ""
    errors: list[str] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


def is_turnstile_enabled() -> bool:
    return bool(getattr(config, "TURNSTILE_ENABLED", False))


def verify_turnstile_token(token: str | None) -> TurnstileResult:
    """
    Verify a Cloudflare Turnstile token with Siteverify.

    Cloudflare tokens are short-lived and single-use. Duplicate, expired, fake,
    or missing tokens will return success=false from Siteverify and must block
    free homepage generation.
    """
    if not is_turnstile_enabled():
        return TurnstileResult(ok=True, reason="disabled")

    secret = (getattr(config, "TURNSTILE_SECRET_KEY", "") or "").strip()
    if not secret:
        print("[TURNSTILE][SECURITY] enabled but TURNSTILE_SECRET_KEY is missing")
        return TurnstileResult(ok=False, reason="secret_missing")

    clean_token = (token or "").strip()
    if not clean_token:
        return TurnstileResult(ok=False, reason="token_missing")

    try:
        response = requests.post(
            SITEVERIFY_URL,
            data={"secret": secret, "response": clean_token},
            timeout=6,
        )
    except requests.RequestException as exc:
        print(f"[TURNSTILE][SECURITY] siteverify request failed: {exc}")
        return TurnstileResult(ok=False, reason="siteverify_unavailable")

    try:
        payload = response.json()
    except ValueError:
        print(f"[TURNSTILE][SECURITY] invalid siteverify JSON status={response.status_code}")
        return TurnstileResult(ok=False, reason="siteverify_invalid_response")

    errors = [str(code) for code in (payload.get("error-codes") or [])]
    if response.status_code != 200:
        print(f"[TURNSTILE][SECURITY] siteverify HTTP {response.status_code} errors={errors}")
        return TurnstileResult(ok=False, reason="siteverify_http_error", errors=errors, raw=payload)

    if not payload.get("success"):
        reason = errors[0] if errors else "verification_failed"
        return TurnstileResult(ok=False, reason=reason, errors=errors, raw=payload)

    return TurnstileResult(ok=True, reason="verified", raw=payload)
