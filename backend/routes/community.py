"""
Community Routes Blueprint (Modular, Real Logic)
------------------------------------------------
Registered under /api/_mod to avoid conflicts during migration.
"""

from __future__ import annotations

import logging
import requests as http_requests

from flask import Blueprint, jsonify, request

from backend.config import config
from backend.db import USE_DB, get_conn
from backend.middleware import with_session
from backend.services.identity_service import require_identity

bp = Blueprint("community", __name__)

logger = logging.getLogger(__name__)


@bp.route("/community/feed", methods=["GET", "OPTIONS"])
def community_feed_mod():
    if request.method == "OPTIONS":
        return ("", 204)

    if not USE_DB:
        return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}}), 503

    try:
        limit = min(int(request.args.get("limit", 20)), 100)
        offset = int(request.args.get("offset", 0))
        asset_type = request.args.get("type")

        with get_conn() as conn:
            cursor = conn.cursor()

            type_filter = ""
            if asset_type == "model":
                type_filter = "AND cp.model_id IS NOT NULL"
            elif asset_type == "image":
                type_filter = "AND cp.image_id IS NOT NULL"
            elif asset_type == "history":
                type_filter = "AND cp.history_item_id IS NOT NULL"

            cursor.execute(f"""
                SELECT COUNT(*) FROM timrx_app.community_posts cp
                WHERE cp.status = 'published' AND cp.deleted_at IS NULL {type_filter}
            """)
            total = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT
                    cp.id, cp.display_name, cp.prompt_public, cp.show_prompt, cp.created_at,
                    cp.model_id, cp.image_id, cp.history_item_id,
                    m.title as model_title, m.prompt as model_prompt, m.thumbnail_url as model_thumbnail,
                    m.glb_url as model_glb_url,
                    i.filename as image_filename, i.thumbnail_url as image_thumbnail,
                    h.title as history_title, h.prompt as history_prompt, h.thumbnail_url as history_thumbnail,
                    h.glb_url as history_glb_url, h.image_url as history_image_url
                FROM timrx_app.community_posts cp
                LEFT JOIN timrx_app.models m ON cp.model_id = m.id
                LEFT JOIN timrx_app.images i ON cp.image_id = i.id
                LEFT JOIN timrx_app.history_items h ON cp.history_item_id = h.id
                WHERE cp.status = 'published' AND cp.deleted_at IS NULL {type_filter}
                ORDER BY cp.created_at DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))

            rows = cursor.fetchall()
            cursor.close()

        posts = []

        for row in rows:
            (post_id, display_name, prompt_public, show_prompt, created_at,
             model_id, image_id, history_item_id,
             model_title, model_prompt, model_thumbnail, model_glb_url,
             image_filename, image_thumbnail,
             history_title, history_prompt, history_thumbnail, history_glb_url, history_image_url) = row

            post = {
                "id": str(post_id),
                "display_name": display_name,
                "show_prompt": show_prompt,
                "created_at": created_at.isoformat() if created_at else None,
            }

            if show_prompt and prompt_public:
                post["prompt_public"] = prompt_public

            if model_id:
                post["asset_type"] = "model"
                post["asset"] = {
                    "id": str(model_id),
                    "title": model_title,
                    "thumbnail_url": model_thumbnail,
                }
            elif image_id:
                post["asset_type"] = "image"
                post["asset"] = {
                    "id": str(image_id),
                    "filename": image_filename,
                    "thumbnail_url": image_thumbnail,
                }
            elif history_item_id:
                post["asset_type"] = "history"
                post["asset"] = {
                    "id": str(history_item_id),
                    "title": history_title,
                    "thumbnail_url": history_thumbnail,
                }

            posts.append(post)

        return jsonify({
            "ok": True,
            "posts": posts,
            "total": total,
            "has_more": offset + len(posts) < total,
            "source": "modular",
        })

    except Exception as e:
        print(f"[COMMUNITY][mod] Error in feed: {e}")
        return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Something went wrong. Please try again."}}), 500


@bp.route("/community/share", methods=["POST", "OPTIONS"])
def community_share_mod():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        if not USE_DB:
            return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}}), 503

        try:
            data = request.get_json() or {}
            asset_type = data.get("asset_type")
            asset_id = data.get("asset_id")
            display_name = data.get("display_name")
            prompt_public = data.get("prompt_public")
            show_prompt = bool(data.get("show_prompt", False))

            if asset_type not in ("model", "image", "history"):
                return jsonify({
                    "ok": False,
                    "error": {"code": "INVALID_ASSET_TYPE", "message": "asset_type must be 'model', 'image', or 'history'"}
                }), 400

            if not asset_id:
                return jsonify({
                    "ok": False,
                    "error": {"code": "MISSING_FIELD", "message": "asset_id is required"}
                }), 400

            if not display_name:
                return jsonify({
                    "ok": False,
                    "error": {"code": "MISSING_FIELD", "message": "display_name is required"}
                }), 400

            with get_conn() as conn:
                cursor = conn.cursor()

                if asset_type == "model":
                    cursor.execute("""
                        SELECT id FROM timrx_app.models
                        WHERE id = %s AND identity_id = %s
                    """, (asset_id, identity_id))
                    col_name = "model_id"
                elif asset_type == "image":
                    cursor.execute("""
                        SELECT id FROM timrx_app.images
                        WHERE id = %s AND identity_id = %s
                    """, (asset_id, identity_id))
                    col_name = "image_id"
                else:
                    cursor.execute("""
                        SELECT id FROM timrx_app.history_items
                        WHERE id = %s AND identity_id = %s
                    """, (asset_id, identity_id))
                    col_name = "history_item_id"

                if not cursor.fetchone():
                    cursor.close()
                    return jsonify({
                        "ok": False,
                        "error": {"code": "ASSET_NOT_FOUND", "message": "Asset not found or you don't have permission to share it"}
                    }), 404

                cursor.execute(f"""
                    INSERT INTO timrx_app.community_posts
                    (identity_id, {col_name}, display_name, prompt_public, show_prompt)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (identity_id, asset_id, display_name, prompt_public, show_prompt))

                post_id = cursor.fetchone()[0]
                cursor.close()
                conn.commit()

            return jsonify({"ok": True, "post_id": str(post_id), "source": "modular"})

        except Exception as e:
            print(f"[COMMUNITY][mod] Error in share: {e}")
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Something went wrong. Please try again."}}), 500

    return _inner()


@bp.route("/community/post/<post_id>", methods=["DELETE", "OPTIONS"])
def community_delete_mod(post_id: str):
    @with_session
    def _inner(post_id: str):
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        if not USE_DB:
            return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}}), 503

        try:
            with get_conn() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE timrx_app.community_posts
                    SET status = 'deleted', deleted_at = NOW()
                    WHERE id = %s AND identity_id = %s AND deleted_at IS NULL
                """, (post_id, identity_id))

                if cursor.rowcount == 0:
                    cursor.close()
                    return jsonify({
                        "ok": False,
                        "error": {"code": "POST_NOT_FOUND", "message": "Post not found or you don't have permission to delete it"}
                    }), 404

                cursor.close()
                conn.commit()
            return jsonify({"ok": True, "source": "modular"})

        except Exception as e:
            print(f"[COMMUNITY][mod] Error in delete: {e}")
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Something went wrong. Please try again."}}), 500

    return _inner(post_id)


@bp.route("/community/discord-share", methods=["POST", "OPTIONS"])
def community_discord_share():
    """Post a creation share embed to the Discord webhook."""
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        from backend.services.identity_service import IdentityService

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        webhook_url = config.DISCORD_WEBHOOK_URL
        if not webhook_url:
            return jsonify({"ok": False, "error": {"code": "WEBHOOK_NOT_CONFIGURED"}}), 503

        data = request.get_json() or {}
        asset_type = data.get("type", "creation")
        prompt = data.get("prompt", "")
        thumbnail_url = data.get("thumbnail_url", "")

        # Get user display label for the embed footer (privacy-safe, no full email)
        user_label = ""
        try:
            identity = IdentityService.get_identity(identity_id)
            if identity and identity.get("email"):
                email = identity["email"]
                local = email.split("@")[0] if "@" in email else email
                name = local.replace(".", " ").replace("_", " ").replace("-", " ")
                user_label = " ".join(w.capitalize() for w in name.split() if w)
        except Exception:
            pass

        labels = {"model": "3D Model", "image": "AI Image", "video": "AI Video"}
        label = labels.get(asset_type, "Creation")

        footer_text = f"TimrX 3D Print Hub | {user_label}" if user_label else "TimrX 3D Print Hub"

        embed = {
            "title": f"New {label} on TimrX",
            "description": f"Prompt:\n{prompt[:200]}" if prompt else None,
            "color": 5814783,
            "url": "https://timrx.live/3dprint",
            "footer": {"text": footer_text},
        }

        # Only include image if URL is valid HTTP(S) — data URIs and blob URLs
        # are rejected by Discord's embed validation.
        if isinstance(thumbnail_url, str) and thumbnail_url.startswith(("http://", "https://")):
            embed["image"] = {"url": thumbnail_url}
        elif thumbnail_url:
            logger.info("[Discord] Skipping non-HTTP thumbnail_url: %s", str(thumbnail_url)[:30])

        # Remove None values from embed
        embed = {k: v for k, v in embed.items() if v is not None}

        payload = {
            "username": "TimrX Generator",
            "embeds": [embed],
        }

        try:
            resp = http_requests.post(webhook_url, json=payload, timeout=5)
            if resp.status_code in (200, 204):
                return jsonify({"ok": True})
            logger.warning("[Discord] Webhook returned %s: %s | keys=%s",
                           resp.status_code, resp.text[:500], list(embed.keys()))
            # Fallback: retry with plain content if embed was rejected
            if resp.status_code == 400:
                fallback = {
                    "username": "TimrX Generator",
                    "content": f"New {label} on TimrX" + (f"\nPrompt: {prompt[:200]}" if prompt else ""),
                }
                try:
                    fb_resp = http_requests.post(webhook_url, json=fallback, timeout=5)
                    if fb_resp.status_code in (200, 204):
                        return jsonify({"ok": True})
                except Exception:
                    pass
            return jsonify({"ok": False, "error": {"code": "WEBHOOK_FAILED"}}), 502
        except Exception as e:
            logger.error(f"[Discord] Webhook error: {e}")
            return jsonify({"ok": False, "error": {"code": "WEBHOOK_ERROR"}}), 502

    return _inner()
