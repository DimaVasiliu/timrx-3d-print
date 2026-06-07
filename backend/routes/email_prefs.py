"""Email preference / unsubscribe routes.

Implements RFC 8058 one-click unsubscribe so Gmail accepts the
``List-Unsubscribe`` header on our marketing mail. Two surfaces:

  - POST /api/email/unsubscribe?u=<token>   ← Gmail's one-click server call
  - GET  /api/email/unsubscribe?u=<token>   ← browser link from the email

Both accept the same signed token (minted by services.unsubscribe_signer)
and flip ``identities.email_unsubscribed_at`` to NOW(). No authentication
is required — the HMAC signature in the token IS the authentication.
"""

from __future__ import annotations

from flask import Blueprint, request, jsonify

from backend.db import get_conn, USE_DB, Tables
from backend.services.unsubscribe_signer import verify_token


bp = Blueprint("email_prefs", __name__)


_HTML_OK = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Unsubscribed — TimrX</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;
       background:#0b0d12;color:#e7ecf3;margin:0;padding:0;
       min-height:100vh;display:flex;align-items:center;justify-content:center}
  .card{max-width:480px;padding:40px 32px;background:#141822;border-radius:16px;
        box-shadow:0 24px 48px rgba(0,0,0,0.4);text-align:center}
  h1{margin:0 0 12px;font-size:22px;font-weight:600;color:#fff}
  p{margin:0;font-size:15px;line-height:1.55;color:#a0a8b8}
  .ok{display:inline-block;width:48px;height:48px;border-radius:50%;
       background:#1a3e2a;color:#52e09b;font-size:24px;line-height:48px;margin-bottom:16px}
  a{color:#7aa2f7;text-decoration:none}
</style></head>
<body><div class="card">
  <div class="ok">✓</div>
  <h1>You're unsubscribed</h1>
  <p>You won't receive marketing or update emails from TimrX anymore.
  Transactional emails (sign-in codes, receipts) will still arrive so
  your account keeps working.</p>
  <p style="margin-top:20px"><a href="https://timrx.live">Back to TimrX →</a></p>
</div></body></html>"""

_HTML_FAIL = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Link expired — TimrX</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;
       background:#0b0d12;color:#e7ecf3;margin:0;padding:0;
       min-height:100vh;display:flex;align-items:center;justify-content:center}
  .card{max-width:480px;padding:40px 32px;background:#141822;border-radius:16px;
        box-shadow:0 24px 48px rgba(0,0,0,0.4);text-align:center}
  h1{margin:0 0 12px;font-size:22px;font-weight:600;color:#fff}
  p{margin:0;font-size:15px;line-height:1.55;color:#a0a8b8}
  .x{display:inline-block;width:48px;height:48px;border-radius:50%;
     background:#3e1a1a;color:#e07a7a;font-size:24px;line-height:48px;margin-bottom:16px}
  a{color:#7aa2f7;text-decoration:none}
</style></head>
<body><div class="card">
  <div class="x">!</div>
  <h1>This unsubscribe link expired</h1>
  <p>Please email <a href="mailto:hello@timrx.live">hello@timrx.live</a>
  and we'll handle it manually.</p>
</div></body></html>"""


def _set_unsubscribed(identity_id: str, category: str) -> bool:
    """Mark identity as unsubscribed. Returns True on success."""
    if not USE_DB or not identity_id:
        return False
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Single column on identities table — keeps the change small.
                # If you later want per-category granularity, swap to a
                # dedicated email_unsubscribes table.
                cur.execute(
                    f"""
                    UPDATE {Tables.IDENTITIES}
                    SET email_unsubscribed_at = NOW(),
                        email_unsubscribed_category = %s
                    WHERE id::text = %s
                    """,
                    (category, str(identity_id)),
                )
            conn.commit()
        print(f"[UNSUB] identity={identity_id} category={category} unsubscribed")
        return True
    except Exception as e:
        # Column may not exist yet — log but don't crash the endpoint.
        # Migration 074 adds the columns; until it's run, this falls through.
        print(f"[UNSUB] WARNING: update failed (column missing?) — {e}")
        return False


@bp.route("/unsubscribe", methods=["POST", "GET"])
def unsubscribe():
    """
    RFC 8058 one-click unsubscribe.

    Gmail's mail servers issue a POST with body
    ``List-Unsubscribe=One-Click`` to the URL from List-Unsubscribe header.
    Browsers issue a GET when the user clicks the visible "Unsubscribe"
    link in the email. Both are accepted here.

    Returns 200 OK in both cases (idempotent — Gmail may retry).
    """
    token = request.args.get("u") or request.form.get("u")
    if not token:
        return ("Missing token", 400)

    verified = verify_token(token)
    if not verified:
        if request.method == "GET":
            return _HTML_FAIL, 410, {"Content-Type": "text/html; charset=utf-8"}
        # Gmail one-click — return 200 so it doesn't retry on a bad/expired
        # link (idempotent contract). Log for audit.
        print(f"[UNSUB] invalid/expired token: {token[:20]}...")
        return ("", 200)

    identity_id, category = verified
    _set_unsubscribed(identity_id, category)

    if request.method == "GET":
        return _HTML_OK, 200, {"Content-Type": "text/html; charset=utf-8"}
    # POST one-click: just 200, no body needed
    return ("", 200)
