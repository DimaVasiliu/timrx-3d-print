"""
/api/analytics — server-side queue for GTM/GA4/Google Ads conversion events.

Endpoints
---------
GET  /api/analytics/pending  — list events the browser still needs to fire
POST /api/analytics/ack      — mark events as fired so we stop returning them

The browser polls /pending after the wallet is server-confirmed and pushes each
event to dataLayer, then acks. Anonymous identities are fine — the same identity
owns the wallet and the purchase, so the queue scopes naturally.
"""

from __future__ import annotations

from flask import Blueprint, request, jsonify, g

from backend.middleware import with_session, with_session_readonly, no_cache
from backend.services.analytics_events_service import (
    list_pending,
    ack as ack_events,
)


bp = Blueprint("analytics", __name__)


@bp.route("/pending", methods=["GET", "OPTIONS"])
@with_session_readonly
@no_cache
def get_pending():
    """
    Return the list of dataLayer events that this identity still needs to fire.

    Anonymous identities are supported — they may have a server-issued
    `email_verified` / `sign_up` / `purchase` event waiting for them.

    Response:
        {
            "ok": true,
            "events": [
                {
                    "event_name": "purchase",
                    "event_id":   "purchase:tr_xyz",
                    "payload":    { ... },
                    "created_at_unix": 1234567890
                },
                ...
            ]
        }
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id = getattr(g, "identity_id", None)
    if not identity_id:
        # No identity yet — nothing pending by definition.
        return jsonify({"ok": True, "events": []})

    events = list_pending(identity_id)
    return jsonify({"ok": True, "events": events})


@bp.route("/ack", methods=["POST", "OPTIONS"])
@with_session
@no_cache
def post_ack():
    """
    Body:  { "event_ids": ["purchase:tr_xyz", "sign_up:..."] }
    Marks those events as fired (idempotent — already-fired events are no-ops).
    """
    if request.method == "OPTIONS":
        return ("", 204)

    identity_id = getattr(g, "identity_id", None)
    if not identity_id:
        return jsonify({"ok": False, "error": "no_identity", "acked": 0}), 401

    body = request.get_json(silent=True) or {}
    event_ids = body.get("event_ids") or []
    if not isinstance(event_ids, list):
        return jsonify({"ok": False, "error": "invalid_params", "message": "event_ids must be a list"}), 400

    acked = ack_events(identity_id, event_ids)
    return jsonify({"ok": True, "acked": acked})
