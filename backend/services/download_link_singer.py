"""HMAC-signed download links for print-order admin emails.

The admin order email contains "Download GLB / STL" buttons that link to
`/api/print-orders/admin/<order>/download?type=...`. That route is
admin-gated. When the admin clicks the button from a phone or another
device where they are not signed in, the request fails with
`EMAIL_REQUIRED`.

To make those buttons work from anywhere, we sign each URL with HMAC-SHA256
keyed off `config.ADMIN_TOKEN`. The route accepts either an authenticated
admin session (existing behavior) OR a valid signature. The signature
binds the order number, file kind, and an expiry, so a leaked link only
works for that one file until it expires (default 30 days).

Format appended to the URL:  ?type=glb&exp=<unix>&sig=<base64url>
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Optional

from backend.config import config


_SIG_VERSION = "v1"
_DEFAULT_TTL_SECONDS = 30 * 24 * 3600  # 30 days


def _secret() -> Optional[bytes]:
    tok = getattr(config, "ADMIN_TOKEN", "") or ""
    if not tok:
        return None
    return tok.encode("utf-8")


def _payload(order_number: str, kind: str, exp: int) -> bytes:
    return f"{_SIG_VERSION}:{order_number}:{kind}:{exp}".encode("utf-8")


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def sign(order_number: str, kind: str, ttl_seconds: int = _DEFAULT_TTL_SECONDS) -> Optional[dict]:
    """Return {'exp': int, 'sig': str} for a download URL, or None if no secret."""
    secret = _secret()
    if not secret or not order_number or not kind:
        return None
    exp = int(time.time()) + max(60, int(ttl_seconds))
    mac = hmac.new(secret, _payload(order_number, kind, exp), hashlib.sha256).digest()
    return {"exp": exp, "sig": _b64url(mac)}


def verify(order_number: str, kind: str, exp: str, sig: str) -> bool:
    """Return True iff the signature is valid and not expired."""
    secret = _secret()
    if not secret or not order_number or not kind or not exp or not sig:
        return False
    try:
        exp_int = int(exp)
    except (TypeError, ValueError):
        return False
    if exp_int < int(time.time()):
        return False
    expected = hmac.new(secret, _payload(order_number, kind, exp_int), hashlib.sha256).digest()
    try:
        provided = _b64url_decode(sig)
    except Exception:
        return False
    return hmac.compare_digest(expected, provided)
