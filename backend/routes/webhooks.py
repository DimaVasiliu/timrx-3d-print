"""
Webhook routes for external service callbacks.

PiAPI account-level notifications (quota, suspension, general alerts).
This does NOT replace job polling — it only handles account notifications.

Render env vars:
    PIAPI_WEBHOOK_ENABLED   — "true" to accept webhooks (default: false)
    PIAPI_WEBHOOK_SECRET    — optional shared secret for verification
    PIAPI_WEBHOOK_LOG_BODY  — "true" to log full payload (default: false)

Webhook URL to register in PiAPI dashboard:
    https://3d.timrx.live/api/webhooks/piapi
"""

from __future__ import annotations

import json

from flask import Blueprint, request, jsonify

from backend.config import config

bp = Blueprint("webhooks", __name__)


# ── Helpers ──────────────────────────────────────────────────

def _safe_get_json():
    """Parse JSON body, return (dict, None) or (None, error_string)."""
    try:
        data = request.get_json(silent=True)
        if data is None:
            return None, "missing or invalid JSON body"
        if not isinstance(data, dict):
            return None, "body must be a JSON object"
        return data, None
    except Exception as e:
        return None, f"JSON parse error: {e}"


def _webhook_secret_is_valid() -> bool:
    """
    Check webhook secret if configured.

    Accepts either:
      - Header: X-Webhook-Secret: <secret>
      - Query param: ?secret=<secret>

    Returns True if secret matches or if no secret is configured.
    """
    secret = config.PIAPI_WEBHOOK_SECRET
    if not secret:
        return True

    header_val = request.headers.get("X-Webhook-Secret", "")
    query_val = request.args.get("secret", "")
    return header_val == secret or query_val == secret


# ── PiAPI Webhook ────────────────────────────────────────────

@bp.route("/webhooks/piapi", methods=["POST"])
def piapi_webhook():
    """
    Receive PiAPI account-level notifications.

    Handles quota alerts, suspension notices, and general account events.
    Does NOT interact with job polling or video generation logic.
    """
    # Gate: check if webhooks are enabled
    if not config.PIAPI_WEBHOOK_ENABLED:
        return jsonify({"error": "webhook_disabled"}), 403

    # Auth: verify shared secret
    if not _webhook_secret_is_valid():
        print("[PIAPI_WEBHOOK] unauthorized")
        return jsonify({"error": "unauthorized"}), 401

    if not config.PIAPI_WEBHOOK_SECRET:
        print("[PIAPI_WEBHOOK] verification disabled (no PIAPI_WEBHOOK_SECRET set)")

    # Parse body
    data, err = _safe_get_json()
    if err:
        print(f"[PIAPI_WEBHOOK] bad request: {err}")
        return jsonify({"error": "bad_request", "message": err}), 400

    try:
        event = data.get("event") or data.get("type") or data.get("action") or "unknown"
        event_type = data.get("type") or data.get("event_type") or ""
        top_keys = sorted(data.keys())

        print(
            f"[PIAPI_WEBHOOK] received event={event} type={event_type} "
            f"keys={top_keys}"
        )

        if config.PIAPI_WEBHOOK_LOG_BODY:
            print(f"[PIAPI_WEBHOOK] body={json.dumps(data, default=str)}")

        return jsonify({"ok": True}), 200

    except Exception as e:
        print(f"[PIAPI_WEBHOOK][ERROR] {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "internal_error"}), 500




