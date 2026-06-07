"""HMAC-signed one-click unsubscribe links.

Gmail's bulk-sender requirements (RFC 8058) require marketing emails to
include a ``List-Unsubscribe`` header whose URL accepts a no-auth POST and
unsubscribes the recipient in a single click.

This module mints and verifies short tokens that bind:
  - identity_id  (whose preference to flip)
  - category     ("marketing" | "blog" | "campaign" | ...)
  - exp          (unix timestamp, default +90 days)

Tokens are signed with HMAC-SHA256 keyed off the same admin token used by
``download_link_signer`` so we don't have to introduce a new secret.

Format: ``<base64url(payload)>.<base64url(sig)>``
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Optional, Tuple

from backend.config import config


_SIG_VERSION = "v1"
_DEFAULT_TTL_SECONDS = 90 * 24 * 3600  # 90 days


def _secret() -> bytes:
    raw = (
        getattr(config, "ADMIN_TOKEN", "")
        or getattr(config, "SESSION_COOKIE_SECRET", "")
        or "timrx-unsubscribe-fallback"
    )
    return str(raw).encode("utf-8")


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def issue_token(
    identity_id: str,
    category: str = "marketing",
    ttl_seconds: int = _DEFAULT_TTL_SECONDS,
) -> str:
    """Issue a signed unsubscribe token."""
    payload = {
        "v": _SIG_VERSION,
        "iid": str(identity_id),
        "cat": category,
        "exp": int(time.time()) + int(ttl_seconds),
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    sig = hmac.new(_secret(), payload_bytes, hashlib.sha256).digest()
    return f"{_b64encode(payload_bytes)}.{_b64encode(sig)}"


def verify_token(token: str) -> Optional[Tuple[str, str]]:
    """
    Verify a token and return (identity_id, category) if valid, else None.
    """
    if not token or "." not in token:
        return None
    try:
        payload_b64, sig_b64 = token.split(".", 1)
        payload_bytes = _b64decode(payload_b64)
        provided_sig = _b64decode(sig_b64)
    except Exception:
        return None

    expected_sig = hmac.new(_secret(), payload_bytes, hashlib.sha256).digest()
    if not hmac.compare_digest(provided_sig, expected_sig):
        return None

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        return None

    if payload.get("v") != _SIG_VERSION:
        return None
    if int(payload.get("exp", 0)) < int(time.time()):
        return None

    return str(payload.get("iid", "")), str(payload.get("cat", "marketing"))


def build_unsubscribe_url(identity_id: str, category: str = "marketing") -> str:
    """Build the full unsubscribe URL to embed in the List-Unsubscribe header."""
    base = (
        getattr(config, "PUBLIC_BASE_URL", "")
        or getattr(config, "FRONTEND_BASE_URL", "")
        or "https://3d.timrx.live"
    ).rstrip("/")
    token = issue_token(identity_id, category)
    return f"{base}/api/email/unsubscribe?u={token}"
