"""
Community Routes Blueprint (Modular, Real Logic)
------------------------------------------------
Registered under /api/_mod to avoid conflicts during migration.
"""

from __future__ import annotations

import logging
import traceback
import requests as http_requests

from flask import Blueprint, jsonify, request, g

try:
    from psycopg.rows import tuple_row as _tuple_row
except ImportError:
    _tuple_row = None

from backend.config import config
from backend.db import USE_DB, get_conn
from backend.middleware import with_session
from backend.services.identity_service import require_identity
from backend.services.wallet_service import WalletService, CreditType

bp = Blueprint("community", __name__)
logger = logging.getLogger(__name__)

VALID_REACTIONS = ('heart', 'fire', 'star', 'clap', 'wow')
TIP_AMOUNTS     = (5, 10, 25, 50)


def _cur(conn):
    return conn.cursor(row_factory=_tuple_row) if _tuple_row else conn.cursor()


def _get_gen_type(item_type, glb_url, gen_action, animation_glb_url=None):
    """Map history item fields to a human-readable generation type label."""
    action = (gen_action or '').lower()
    if item_type == 'video':
        if any(k in action for k in ('image', 'animate', 'img2vid', 'image_animate')):
            return 'Image to Video'
        return 'Text to Video'
    elif animation_glb_url or action in ('animate', 'animation', 'meshy_animation'):
        return 'Animated 3D'
    elif action in ('rig', 'rigging', 'meshy_rig', 'meshy_rigging'):
        return 'Rigged 3D'
    elif glb_url:
        if any(k in action for k in ('image', 'img', 'image_to_3d')):
            return 'Image to 3D'
        return 'Text to 3D'
    else:
        return 'AI Image'


# ─── Feed ─────────────────────────────────────────────────────────────────────

@bp.route("/community/feed", methods=["GET", "OPTIONS"])
def community_feed_mod():
    if request.method == "OPTIONS":
        return ("", 204)

    if not USE_DB:
        return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}}), 503

    try:
        limit      = min(int(request.args.get("limit", 20)), 100)
        offset     = int(request.args.get("offset", 0))
        asset_type = request.args.get("type")

        # Build type filter — everything is stored as history_item_id now, so
        # filter based on the joined history_items fields.
        type_filter = ""
        if asset_type == "model":
            # 3D content: history item has a glb_url
            type_filter = (
                "AND cp.history_item_id IS NOT NULL "
                "AND h.glb_url IS NOT NULL AND h.glb_url != '' "
                "AND h.item_type != 'video'"
            )
        elif asset_type == "image":
            # Image content: history item is image type
            type_filter = (
                "AND cp.history_item_id IS NOT NULL "
                "AND h.item_type = 'image'"
            )
        elif asset_type == "video":
            type_filter = (
                "AND cp.history_item_id IS NOT NULL "
                "AND h.item_type = 'video'"
            )
        elif asset_type == "animated":
            type_filter = (
                "AND cp.history_item_id IS NOT NULL "
                "AND (h.payload->>'animation_glb_url') IS NOT NULL "
                "AND (h.payload->>'animation_glb_url') != ''"
            )

        with get_conn("community_feed") as conn:
            cursor = _cur(conn)

            # COUNT — always LEFT JOIN history_items so type_filter can reference h
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM timrx_app.community_posts cp
                LEFT JOIN timrx_app.history_items h ON cp.history_item_id = h.id
                WHERE cp.status = 'published' AND cp.deleted_at IS NULL {type_filter}
            """)
            total = cursor.fetchone()[0]

            # Main SELECT
            cursor.execute(f"""
                SELECT
                    cp.id, cp.display_name, cp.prompt_public, cp.show_prompt, cp.created_at,
                    cp.model_id, cp.image_id, cp.history_item_id,
                    m.title          AS model_title,
                    m.thumbnail_url  AS model_thumbnail,
                    i.image_url      AS image_url,
                    i.thumbnail_url  AS image_thumbnail,
                    h.title          AS history_title,
                    h.thumbnail_url  AS history_thumbnail,
                    h.glb_url        AS history_glb_url,
                    h.image_url      AS history_image_url,
                    h.item_type      AS history_item_type,
                    h.video_url      AS history_video_url,
                    h.payload->>'action' AS gen_action,
                    h.payload->>'animation_glb_url' AS animation_glb_url
                FROM timrx_app.community_posts cp
                LEFT JOIN timrx_app.models        m ON cp.model_id        = m.id
                LEFT JOIN timrx_app.images        i ON cp.image_id        = i.id
                LEFT JOIN timrx_app.history_items h ON cp.history_item_id = h.id
                WHERE cp.status = 'published' AND cp.deleted_at IS NULL {type_filter}
                ORDER BY cp.created_at DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))

            rows = cursor.fetchall()

            posts = []
            for row in rows:
                (post_id, display_name, prompt_public, show_prompt, created_at,
                 model_id, image_id, history_item_id,
                 model_title, model_thumbnail,
                 _image_url, image_thumbnail,
                 history_title, history_thumbnail, history_glb_url, _history_image_url,
                 history_item_type, history_video_url, gen_action,
                 animation_glb_url) = row

                gen_type = _get_gen_type(history_item_type or '', history_glb_url, gen_action, animation_glb_url)

                post = {
                    "id":           str(post_id),
                    "display_name": display_name,
                    "show_prompt":  show_prompt,
                    "created_at":   created_at.isoformat() if created_at else None,
                    "gen_type":     gen_type,
                }

                if show_prompt and prompt_public:
                    post["prompt_public"] = prompt_public

                if model_id:
                    post["asset_type"] = "model"
                    post["asset"] = {"id": str(model_id), "title": model_title, "thumbnail_url": model_thumbnail}
                elif image_id:
                    post["asset_type"] = "image"
                    post["asset"] = {"id": str(image_id), "thumbnail_url": image_thumbnail}
                elif history_item_id:
                    if history_item_type == "video":
                        post["asset_type"] = "video"
                        post["asset"] = {
                            "id":            str(history_item_id),
                            "title":         history_title,
                            "thumbnail_url": history_thumbnail,
                            "video_url":     history_video_url,
                        }
                    else:
                        post["asset_type"] = "model" if history_glb_url else "image"
                        asset = {
                            "id":            str(history_item_id),
                            "title":         history_title,
                            "thumbnail_url": history_thumbnail,
                        }
                        if animation_glb_url:
                            asset["animation_glb_url"] = animation_glb_url
                        elif history_glb_url:
                            asset["glb_url"] = history_glb_url
                        post["asset"] = asset

                posts.append(post)

            # Reactions aggregation (single query for all returned posts)
            if posts:
                post_ids = [p["id"] for p in posts]
                cursor.execute("""
                    SELECT post_id::text, reaction, COUNT(*)::int
                    FROM timrx_app.community_reactions
                    WHERE post_id = ANY(%s::uuid[])
                    GROUP BY post_id, reaction
                """, (post_ids,))
                reactions_map: dict = {}
                for (pid, reaction, cnt) in cursor.fetchall():
                    reactions_map.setdefault(pid, {})[reaction] = cnt

                cursor.execute("""
                    SELECT post_id::text, COALESCE(SUM(amount), 0)::int
                    FROM timrx_app.community_tips
                    WHERE post_id = ANY(%s::uuid[])
                    GROUP BY post_id
                """, (post_ids,))
                tips_map: dict = {pid: total_tips for (pid, total_tips) in cursor.fetchall()}

                for post in posts:
                    post["reactions"]  = reactions_map.get(post["id"], {})
                    post["tip_total"]  = tips_map.get(post["id"], 0)
            else:
                for post in posts:
                    post["reactions"] = {}
                    post["tip_total"] = 0

            cursor.close()

        return jsonify({
            "ok":      True,
            "posts":   posts,
            "total":   total,
            "has_more": offset + len(posts) < total,
            "source":  "modular",
        })

    except Exception as e:
        print(f"[COMMUNITY][mod] Error in feed: {e}")
        print(traceback.format_exc())
        return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Something went wrong. Please try again."}}), 500


# ─── Share ─────────────────────────────────────────────────────────────────────

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
            data         = request.get_json() or {}
            asset_type   = data.get("asset_type")
            asset_id     = data.get("asset_id")
            display_name = data.get("display_name")
            prompt_public = data.get("prompt_public")
            show_prompt  = bool(data.get("show_prompt", False))

            if asset_type not in ("model", "image", "history"):
                return jsonify({"ok": False, "error": {"code": "INVALID_ASSET_TYPE",
                    "message": "asset_type must be 'model', 'image', or 'history'"}}), 400
            if not asset_id:
                return jsonify({"ok": False, "error": {"code": "MISSING_FIELD", "message": "asset_id is required"}}), 400
            if not display_name:
                return jsonify({"ok": False, "error": {"code": "MISSING_FIELD", "message": "display_name is required"}}), 400

            with get_conn("community_share") as conn:
                cursor = _cur(conn)

                if asset_type == "model":
                    cursor.execute("SELECT id FROM timrx_app.models WHERE id = %s AND identity_id = %s",
                                   (asset_id, identity_id))
                    col_name = "model_id"
                elif asset_type == "image":
                    cursor.execute("SELECT id FROM timrx_app.images WHERE id = %s AND identity_id = %s",
                                   (asset_id, identity_id))
                    col_name = "image_id"
                else:
                    cursor.execute("SELECT id FROM timrx_app.history_items WHERE id = %s AND identity_id = %s",
                                   (asset_id, identity_id))
                    col_name = "history_item_id"

                if not cursor.fetchone():
                    cursor.close()
                    return jsonify({"ok": False, "error": {"code": "ASSET_NOT_FOUND",
                        "message": "Asset not found or you don't have permission to share it"}}), 404

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
            print(f"[COMMUNITY][mod] Error in share: {type(e).__name__}: {e}")
            print(traceback.format_exc())
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Something went wrong. Please try again."}}), 500

    return _inner()


# ─── React ─────────────────────────────────────────────────────────────────────

@bp.route("/community/post/<post_id>/react", methods=["POST", "OPTIONS"])
def community_react(post_id: str):
    @with_session
    def _inner(post_id: str):
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        if not USE_DB:
            return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE"}}), 503

        try:
            data     = request.get_json() or {}
            reaction = data.get("reaction")  # None = remove reaction

            if reaction is not None and reaction not in VALID_REACTIONS:
                return jsonify({"ok": False, "error": {"code": "INVALID_REACTION",
                    "message": f"reaction must be one of {VALID_REACTIONS}"}}), 400

            with get_conn("community_react") as conn:
                cursor = _cur(conn)

                # Verify post exists
                cursor.execute("""
                    SELECT id FROM timrx_app.community_posts
                    WHERE id = %s AND status = 'published' AND deleted_at IS NULL
                """, (post_id,))
                if not cursor.fetchone():
                    cursor.close()
                    return jsonify({"ok": False, "error": {"code": "POST_NOT_FOUND"}}), 404

                if reaction is None:
                    # Remove any existing reaction
                    cursor.execute("""
                        DELETE FROM timrx_app.community_reactions
                        WHERE post_id = %s AND identity_id = %s
                    """, (post_id, identity_id))
                else:
                    # Upsert reaction (one per user per post — replace if different)
                    cursor.execute("""
                        INSERT INTO timrx_app.community_reactions (post_id, identity_id, reaction)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (post_id, identity_id)
                        DO UPDATE SET reaction = EXCLUDED.reaction, created_at = now()
                    """, (post_id, identity_id, reaction))

                # Return updated counts
                cursor.execute("""
                    SELECT reaction, COUNT(*)::int
                    FROM timrx_app.community_reactions
                    WHERE post_id = %s
                    GROUP BY reaction
                """, (post_id,))
                reactions = {r: cnt for (r, cnt) in cursor.fetchall()}

                # Return what this user's current reaction is
                cursor.execute("""
                    SELECT reaction FROM timrx_app.community_reactions
                    WHERE post_id = %s AND identity_id = %s
                """, (post_id, identity_id))
                row = cursor.fetchone()
                my_reaction = row[0] if row else None

                cursor.close()
                conn.commit()

            return jsonify({"ok": True, "reactions": reactions, "my_reaction": my_reaction})

        except Exception as e:
            print(f"[COMMUNITY][mod] Error in react: {e}")
            print(traceback.format_exc())
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Something went wrong."}}), 500

    return _inner(post_id)


# ─── Tip ───────────────────────────────────────────────────────────────────────

@bp.route("/community/tip", methods=["POST", "OPTIONS"])
def community_tip():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        tipper_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        if not USE_DB:
            return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE"}}), 503

        try:
            data    = request.get_json() or {}
            post_id = data.get("post_id")
            amount  = data.get("amount")

            if not post_id:
                return jsonify({"ok": False, "error": {"code": "MISSING_FIELD", "message": "post_id is required"}}), 400
            if amount not in TIP_AMOUNTS:
                return jsonify({"ok": False, "error": {"code": "INVALID_AMOUNT",
                    "message": f"amount must be one of {TIP_AMOUNTS}"}}), 400

            with get_conn("community_tip_lookup") as conn:
                cursor = _cur(conn)

                # Look up post and creator
                cursor.execute("""
                    SELECT identity_id FROM timrx_app.community_posts
                    WHERE id = %s AND status = 'published' AND deleted_at IS NULL
                """, (post_id,))
                row = cursor.fetchone()
                cursor.close()

                if not row:
                    return jsonify({"ok": False, "error": {"code": "POST_NOT_FOUND"}}), 404

                recipient_id = str(row[0])

            if recipient_id == tipper_id:
                return jsonify({"ok": False, "error": {"code": "CANNOT_TIP_SELF",
                    "message": "You can't tip your own post."}}), 400

            # Check balance
            available = WalletService.get_available_balance(tipper_id, CreditType.GENERAL)
            if available < amount:
                return jsonify({"ok": False, "error": {"code": "INSUFFICIENT_CREDITS",
                    "message": f"You need {amount} credits to tip. You have {available}."}}), 402

            # Deduct from tipper
            WalletService.deduct_credits(
                identity_id=tipper_id,
                amount=amount,
                entry_type="community_tip_sent",
                ref_type="community_post",
                ref_id=post_id,
                meta={"recipient_id": recipient_id},
            )

            # Credit to creator
            try:
                WalletService.add_credits(
                    identity_id=recipient_id,
                    amount=amount,
                    entry_type="community_tip_received",
                    ref_type="community_post",
                    ref_id=post_id,
                    meta={"tipper_id": tipper_id},
                )
            except Exception as credit_err:
                # Attempt refund if credit fails
                logger.error("[COMMUNITY] Tip credit failed, attempting refund: %s", credit_err)
                try:
                    WalletService.add_credits(tipper_id, amount, "community_tip_refund",
                                              ref_type="community_post", ref_id=post_id)
                except Exception:
                    pass
                return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Tip failed."}}), 500

            # Record in tips table
            with get_conn("community_tip_record") as conn:
                cursor = _cur(conn)
                cursor.execute("""
                    INSERT INTO timrx_app.community_tips
                        (post_id, tipper_identity_id, recipient_identity_id, amount)
                    VALUES (%s, %s, %s, %s)
                """, (post_id, tipper_id, recipient_id, amount))

                cursor.execute("""
                    SELECT COALESCE(SUM(amount), 0)::int
                    FROM timrx_app.community_tips
                    WHERE post_id = %s
                """, (post_id,))
                tip_total = cursor.fetchone()[0]
                cursor.close()
                conn.commit()

            return jsonify({"ok": True, "tip_total": tip_total})

        except Exception as e:
            print(f"[COMMUNITY][mod] Error in tip: {e}")
            print(traceback.format_exc())
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Something went wrong."}}), 500

    return _inner()


# ─── Delete post ───────────────────────────────────────────────────────────────

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
            return jsonify({"ok": False, "error": {"code": "DB_UNAVAILABLE"}}), 503

        try:
            with get_conn("community_delete") as conn:
                cursor = _cur(conn)
                cursor.execute("""
                    UPDATE timrx_app.community_posts
                    SET status = 'deleted', deleted_at = NOW()
                    WHERE id = %s AND identity_id = %s AND deleted_at IS NULL
                """, (post_id, identity_id))

                if cursor.rowcount == 0:
                    cursor.close()
                    return jsonify({"ok": False, "error": {"code": "POST_NOT_FOUND",
                        "message": "Post not found or you don't have permission to delete it"}}), 404

                cursor.close()
                conn.commit()
            return jsonify({"ok": True, "source": "modular"})

        except Exception as e:
            print(f"[COMMUNITY][mod] Error in delete: {e}")
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR", "message": "Something went wrong."}}), 500

    return _inner(post_id)


# ─── Discord Share ─────────────────────────────────────────────────────────────

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

        data          = request.get_json() or {}
        asset_type    = data.get("type", "creation")
        prompt        = data.get("prompt", "")
        thumbnail_url = data.get("thumbnail_url", "")

        user_label = ""
        try:
            identity = IdentityService.get_identity(identity_id)
            if identity and identity.get("email"):
                email = identity["email"]
                local = email.split("@")[0] if "@" in email else email
                name  = local.replace(".", " ").replace("_", " ").replace("-", " ")
                user_label = " ".join(w.capitalize() for w in name.split() if w)
        except Exception:
            pass

        labels      = {"model": "3D Model", "image": "AI Image", "video": "AI Video"}
        label       = labels.get(asset_type, "Creation")
        footer_text = f"TimrX 3D Print Hub | {user_label}" if user_label else "TimrX 3D Print Hub"

        embed = {
            "title":       f"New {label} on TimrX",
            "description": f"Prompt:\n{prompt[:200]}" if prompt else None,
            "color":       5814783,
            "url":         "https://timrx.live/3dprint",
            "footer":      {"text": footer_text},
        }

        if isinstance(thumbnail_url, str) and thumbnail_url.startswith(("http://", "https://")):
            embed["image"] = {"url": thumbnail_url}
        elif thumbnail_url:
            logger.info("[Discord] Skipping non-HTTP thumbnail_url: %s", str(thumbnail_url)[:30])

        embed   = {k: v for k, v in embed.items() if v is not None}
        payload = {"username": "TimrX Generator", "embeds": [embed]}

        try:
            resp = http_requests.post(webhook_url, json=payload, timeout=5)
            if resp.status_code in (200, 204):
                return jsonify({"ok": True})
            logger.warning("[Discord] Webhook returned %s: %s", resp.status_code, resp.text[:500])
            if resp.status_code == 400:
                fallback = {
                    "username": "TimrX Generator",
                    "content":  f"New {label} on TimrX" + (f"\nPrompt: {prompt[:200]}" if prompt else ""),
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