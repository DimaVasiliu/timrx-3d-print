"""
Notification Center API Routes.

Endpoints:
  GET  /notifications              — paginated list (filterable by category, unread_only)
  GET  /notifications/unread-count — { count: N } for badge
  POST /notifications/<id>/read    — mark single notification as read
  POST /notifications/read-all     — mark all unread as read
  GET  /notifications/preferences  — get user prefs
  PUT  /notifications/preferences  — update user prefs
  GET  /notifications/broadcasts   — active broadcasts for user
  POST /notifications/broadcasts/<id>/dismiss — dismiss a broadcast
"""

import logging
from flask import Blueprint, jsonify, request

from backend.middleware import with_session
from backend.services.identity_service import require_identity
from backend.services.notification_service import NotificationService

logger = logging.getLogger(__name__)

bp = Blueprint("notifications", __name__)


# ─── List notifications ───────────────────────────────────────────────────────

@bp.route("/notifications", methods=["GET", "OPTIONS"])
def list_notifications():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            limit = min(int(request.args.get("limit", 20)), 50)
            offset = max(int(request.args.get("offset", 0)), 0)
            category = request.args.get("category")  # optional filter
            unread_only = request.args.get("unread_only", "false").lower() == "true"

            notifications = NotificationService.get_notifications(
                identity_id,
                limit=limit,
                offset=offset,
                category=category,
                unread_only=unread_only,
            )

            # Serialize timestamps
            for n in notifications:
                if n.get("created_at"):
                    n["created_at"] = n["created_at"].isoformat()
                if n.get("read_at"):
                    n["read_at"] = n["read_at"].isoformat()

            return jsonify({"ok": True, "notifications": notifications})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] list_notifications error: %s", e)
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Failed to fetch notifications"}}), 500

    return _inner()


# ─── Unread count ─────────────────────────────────────────────────────────────

@bp.route("/notifications/unread-count", methods=["GET", "OPTIONS"])
def unread_count():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            count = NotificationService.get_unread_count(identity_id)
            return jsonify({"ok": True, "count": count})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] unread_count error: %s", e)
            return jsonify({"ok": True, "count": 0})  # Degrade gracefully

    return _inner()


# ─── Mark single as read ─────────────────────────────────────────────────────

@bp.route("/notifications/<notification_id>/read", methods=["POST", "OPTIONS"])
def mark_read(notification_id):
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            success = NotificationService.mark_read(identity_id, notification_id)
            return jsonify({"ok": True, "updated": success})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] mark_read error: %s", e)
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Failed to mark read"}}), 500

    return _inner()


# ─── Mark all as read ─────────────────────────────────────────────────────────

@bp.route("/notifications/read-all", methods=["POST", "OPTIONS"])
def mark_all_read():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            count = NotificationService.mark_all_read(identity_id)
            return jsonify({"ok": True, "updated": count})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] mark_all_read error: %s", e)
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Failed to mark all read"}}), 500

    return _inner()


# ─── Dismiss single notification ──────────────────────────────────────────────

@bp.route("/notifications/<notification_id>", methods=["DELETE", "OPTIONS"])
def dismiss_notification(notification_id):
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            success = NotificationService.dismiss(identity_id, notification_id)
            return jsonify({"ok": True, "deleted": success})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] dismiss error: %s", e)
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Failed to dismiss"}}), 500

    return _inner()


# ─── Dismiss all read notifications ──────────────────────────────────────────

@bp.route("/notifications/dismiss-read", methods=["POST", "OPTIONS"])
def dismiss_all_read():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            count = NotificationService.dismiss_all_read(identity_id)
            return jsonify({"ok": True, "deleted": count})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] dismiss_all_read error: %s", e)
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Failed to dismiss read"}}), 500

    return _inner()


# ─── Preferences ──────────────────────────────────────────────────────────────

@bp.route("/notifications/preferences", methods=["GET", "OPTIONS"])
def get_preferences():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            prefs = NotificationService.get_preferences(identity_id)
            if prefs:
                if prefs.get("updated_at"):
                    prefs["updated_at"] = prefs["updated_at"].isoformat()
                return jsonify({"ok": True, "preferences": prefs})
            else:
                # Return defaults
                return jsonify({"ok": True, "preferences": {
                    "identity_id": identity_id,
                    "in_app_enabled": True,
                    "email_enabled": True,
                    "email_frequency": "instant",
                    "muted_categories": [],
                }})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] get_preferences error: %s", e)
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Failed to fetch preferences"}}), 500

    return _inner()


@bp.route("/notifications/preferences", methods=["PUT", "OPTIONS"])
def update_preferences():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            data = request.get_json(silent=True) or {}

            kwargs = {}
            if "in_app_enabled" in data:
                kwargs["in_app_enabled"] = bool(data["in_app_enabled"])
            if "email_enabled" in data:
                kwargs["email_enabled"] = bool(data["email_enabled"])
            if "email_frequency" in data:
                freq = data["email_frequency"]
                if freq not in ("instant", "daily", "weekly", "none"):
                    return jsonify({"ok": False, "error": {"code": "INVALID_FIELD",
                        "message": "email_frequency must be one of: instant, daily, weekly, none"}}), 400
                kwargs["email_frequency"] = freq
            if "muted_categories" in data:
                cats = data["muted_categories"]
                if not isinstance(cats, list):
                    return jsonify({"ok": False, "error": {"code": "INVALID_FIELD",
                        "message": "muted_categories must be an array"}}), 400
                kwargs["muted_categories"] = cats

            if not kwargs:
                return jsonify({"ok": False, "error": {"code": "NO_CHANGES",
                    "message": "No valid fields to update"}}), 400

            prefs = NotificationService.update_preferences(identity_id, **kwargs)
            if prefs and prefs.get("updated_at"):
                prefs["updated_at"] = prefs["updated_at"].isoformat()

            return jsonify({"ok": True, "preferences": prefs})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] update_preferences error: %s", e)
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Failed to update preferences"}}), 500

    return _inner()


# ─── Broadcasts ───────────────────────────────────────────────────────────────

@bp.route("/notifications/broadcasts", methods=["GET", "OPTIONS"])
def get_broadcasts():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            broadcasts = NotificationService.get_active_broadcasts(identity_id)
            for b in broadcasts:
                if b.get("starts_at"):
                    b["starts_at"] = b["starts_at"].isoformat()
                if b.get("expires_at"):
                    b["expires_at"] = b["expires_at"].isoformat()

            return jsonify({"ok": True, "broadcasts": broadcasts})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] get_broadcasts error: %s", e)
            return jsonify({"ok": True, "broadcasts": []})

    return _inner()


@bp.route("/notifications/broadcasts/<broadcast_id>/dismiss", methods=["POST", "OPTIONS"])
def dismiss_broadcast(broadcast_id):
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            success = NotificationService.dismiss_broadcast(identity_id, broadcast_id)
            return jsonify({"ok": True, "dismissed": success})

        except Exception as e:
            logger.error("[NOTIF_ROUTE] dismiss_broadcast error: %s", e)
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Failed to dismiss"}}), 500

    return _inner()
