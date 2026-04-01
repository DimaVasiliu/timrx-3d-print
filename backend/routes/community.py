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
from backend.db import USE_DB, get_conn, transaction, query_one
from backend.middleware import with_session
from backend.services.identity_service import require_identity
from backend.services.wallet_service import WalletService, CreditType
from backend.services.notification_service import NotificationService

bp = Blueprint("community", __name__)
logger = logging.getLogger(__name__)

VALID_REACTIONS = ('heart', 'fire', 'star', 'clap', 'wow')
TIP_AMOUNTS     = (5, 10, 25, 50)

# Reaction labels for notification messages
REACTION_LABELS = {'heart': 'liked', 'fire': 'loved', 'star': 'starred', 'clap': 'applauded', 'wow': 'was wowed by'}


def _lookup_actor_name(identity_id: str) -> str:
    """Best-effort lookup of a user's display name from their community posts."""
    try:
        row = query_one(
            "SELECT display_name FROM timrx_app.community_posts "
            "WHERE identity_id = %s AND status = 'published' "
            "ORDER BY created_at DESC LIMIT 1",
            (identity_id,),
        )
        return row["display_name"] if row and row.get("display_name") else "Someone"
    except Exception:
        return "Someone"

# ── Community feed response cache ──
# The feed is public (not per-user), so we can cache aggressively.
# Keyed by (limit, offset, type) → (response_dict, monotonic_timestamp).
import time as _time
_feed_cache: dict = {}
_FEED_CACHE_TTL = 15  # seconds — community content changes slowly


def _get_cached_feed(limit, offset, asset_type):
    key = (limit, offset, asset_type or "all")
    entry = _feed_cache.get(key)
    if entry and (_time.monotonic() - entry[1]) < _FEED_CACHE_TTL:
        return entry[0]
    return None


def _set_cached_feed(limit, offset, asset_type, data):
    key = (limit, offset, asset_type or "all")
    _feed_cache[key] = (data, _time.monotonic())
    # Evict if too many entries
    if len(_feed_cache) > 200:
        cutoff = _time.monotonic() - _FEED_CACHE_TTL
        expired = [k for k, (_, ts) in _feed_cache.items() if ts < cutoff]
        for k in expired:
            del _feed_cache[k]


def invalidate_community_feed_cache():
    """Call after share, delete, reaction, or tip to refresh feed."""
    _feed_cache.clear()


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

        # Short-circuit: return cached response if within TTL
        cached = _get_cached_feed(limit, offset, asset_type)
        if cached is not None:
            return jsonify(cached)

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
                    m.glb_url        AS model_glb_url,
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
                 model_title, model_glb_url, model_thumbnail,
                 image_url, image_thumbnail,
                 history_title, history_thumbnail, history_glb_url, history_image_url,
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
                    asset = {"id": str(model_id), "title": model_title, "thumbnail_url": model_thumbnail}
                    if model_glb_url:
                        asset["glb_url"] = model_glb_url
                    post["asset"] = asset
                elif image_id:
                    post["asset_type"] = "image"
                    asset = {"id": str(image_id), "thumbnail_url": image_thumbnail}
                    if image_url:
                        asset["image_url"] = image_url
                    post["asset"] = asset
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
                        if history_image_url:
                            asset["image_url"] = history_image_url
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

                # Comment counts (efficient batch query)
                cursor.execute("""
                    SELECT post_id::text, COUNT(*)::int
                    FROM timrx_app.community_comments
                    WHERE post_id = ANY(%s::uuid[]) AND status = 'published'
                    GROUP BY post_id
                """, (post_ids,))
                comments_map: dict = {pid: cnt for (pid, cnt) in cursor.fetchall()}

                for post in posts:
                    post["reactions"]      = reactions_map.get(post["id"], {})
                    post["tip_total"]      = tips_map.get(post["id"], 0)
                    post["comment_count"]  = comments_map.get(post["id"], 0)
            else:
                for post in posts:
                    post["reactions"] = {}
                    post["tip_total"] = 0
                    post["comment_count"] = 0

            cursor.close()

        result = {
            "ok":      True,
            "posts":   posts,
            "total":   total,
            "has_more": offset + len(posts) < total,
            "source":  "modular",
        }
        _set_cached_feed(limit, offset, asset_type, result)
        return jsonify(result)

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

            invalidate_community_feed_cache()
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

            with transaction("community_react") as cur:
                # Verify post exists
                cur.execute("""
                    SELECT id FROM timrx_app.community_posts
                    WHERE id = %s AND status = 'published' AND deleted_at IS NULL
                """, (post_id,))
                if not cur.fetchone():
                    return jsonify({"ok": False, "error": {"code": "POST_NOT_FOUND"}}), 404

                if reaction is None:
                    # Remove any existing reaction
                    cur.execute("""
                        DELETE FROM timrx_app.community_reactions
                        WHERE post_id = %s AND identity_id = %s
                    """, (post_id, identity_id))
                else:
                    # Upsert reaction (one per user per post — replace if different)
                    cur.execute("""
                        INSERT INTO timrx_app.community_reactions (post_id, identity_id, reaction)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (post_id, identity_id)
                        DO UPDATE SET reaction = EXCLUDED.reaction, created_at = now()
                    """, (post_id, identity_id, reaction))

                # Return updated counts (dict_row: each row is {"reaction": ..., "count": ...})
                cur.execute("""
                    SELECT reaction, COUNT(*)::int AS count
                    FROM timrx_app.community_reactions
                    WHERE post_id = %s
                    GROUP BY reaction
                """, (post_id,))
                reactions = {row["reaction"]: row["count"] for row in cur.fetchall()}

                # Return what this user's current reaction is
                cur.execute("""
                    SELECT reaction FROM timrx_app.community_reactions
                    WHERE post_id = %s AND identity_id = %s
                """, (post_id, identity_id))
                row = cur.fetchone()
                my_reaction = row["reaction"] if row else None

            # transaction() auto-commits on success.
            invalidate_community_feed_cache()

            # Notify post owner about the reaction (non-blocking, outside txn)
            if reaction is not None:
                try:
                    owner_row = query_one(
                        "SELECT identity_id::text FROM timrx_app.community_posts WHERE id = %s",
                        (post_id,),
                    )
                    owner_id = owner_row["identity_id"] if owner_row else None
                    if owner_id and owner_id != identity_id:
                        actor = _lookup_actor_name(identity_id)
                        verb = REACTION_LABELS.get(reaction, "reacted to")
                        NotificationService.create(
                            identity_id=owner_id,
                            category="community",
                            notif_type="reactions_milestone",
                            title=f"{actor} {verb} your creation!",
                            body=None,
                            icon="fa-heart",
                            link="/community",
                            meta={"post_id": post_id, "reaction": reaction, "reactor_id": identity_id, "actor_name": actor},
                            ref_type="reaction",
                            ref_id=f"{post_id}:{identity_id}",
                        )
                except Exception as notif_err:
                    print(f"[COMMUNITY] Reaction notification failed (non-fatal): {notif_err}")

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
            with transaction("community_tip_record") as cur:
                cur.execute("""
                    INSERT INTO timrx_app.community_tips
                        (post_id, tipper_identity_id, recipient_identity_id, amount)
                    VALUES (%s, %s, %s, %s)
                """, (post_id, tipper_id, recipient_id, amount))

                cur.execute("""
                    SELECT COALESCE(SUM(amount), 0)::int AS total
                    FROM timrx_app.community_tips
                    WHERE post_id = %s
                """, (post_id,))
                row = cur.fetchone()
                tip_total = row["total"] if row else 0
            # transaction() auto-commits on success.

            invalidate_community_feed_cache()

            # ── Notification: tip received ──
            try:
                actor = _lookup_actor_name(tipper_id)
                NotificationService.create(
                    identity_id=recipient_id,
                    category="tip",
                    notif_type="tip_received",
                    title=f"{actor} tipped you {amount} credits!",
                    body=f"Your community post received a tip.",
                    icon="fa-hand-holding-dollar",
                    link="/community",
                    meta={"amount": amount, "post_id": post_id, "tipper_id": tipper_id, "tip_total": tip_total, "actor_name": actor},
                    send_email=True,
                    ref_type="tip",
                    ref_id=f"{post_id}:{tipper_id}:{amount}",
                )
            except Exception as notif_err:
                logger.warning("[COMMUNITY] Tip notification failed (non-fatal): %s", notif_err)

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
            invalidate_community_feed_cache()
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


# ─────────────────────────────────────────────────────────────────────────────
# COMMENTS
# ─────────────────────────────────────────────────────────────────────────────

COMMENT_MAX_LENGTH = 500


@bp.route("/community/post/<post_id>/comments", methods=["GET", "OPTIONS"])
def list_comments(post_id):
    """List published comments for a post, oldest first."""
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        # Current viewer (optional — used for is_mine flags)
        viewer_id = None
        try:
            viewer_id, _ = require_identity()
        except Exception:
            pass

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            c.id::text, c.post_id::text, c.identity_id::text,
                            c.display_name, c.body,
                            c.created_at, c.updated_at
                        FROM timrx_app.community_comments c
                        WHERE c.post_id = %s AND c.status = 'published'
                        ORDER BY c.created_at ASC
                        """,
                        (post_id,),
                    )
                    rows = cur.fetchall() or []

                    cur.execute(
                        "SELECT identity_id::text FROM timrx_app.community_posts WHERE id = %s",
                        (post_id,),
                    )
                    post_row = cur.fetchone()
                    post_owner_id = post_row["identity_id"] if post_row else None

            comments = []
            for r in rows:
                is_mine = viewer_id and r["identity_id"] == viewer_id
                comments.append({
                    "id": r["id"],
                    "post_id": r["post_id"],
                    "display_name": r["display_name"],
                    "body": r["body"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                    "is_mine": bool(is_mine),
                    "is_post_owner": r["identity_id"] == post_owner_id if post_owner_id else False,
                    "can_edit": bool(is_mine),
                    "can_delete": bool(is_mine),
                })

            return jsonify({
                "ok": True,
                "post_id": post_id,
                "comment_count": len(comments),
                "comments": comments,
            })

        except Exception as e:
            logger.error(f"[Community] List comments error: {e}")
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR"}}), 500

    return _inner()


@bp.route("/community/post/<post_id>/comments", methods=["POST"])
def create_comment(post_id):
    """Create a comment on a community post."""
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        data = request.get_json(silent=True) or {}
        body = (data.get("body") or "").strip()
        display_name = (data.get("display_name") or "").strip()

        if not body:
            return jsonify({"ok": False, "error": {"code": "EMPTY_COMMENT", "message": "Comment cannot be empty"}}), 400
        if len(body) > COMMENT_MAX_LENGTH:
            return jsonify({"ok": False, "error": {"code": "COMMENT_TOO_LONG", "message": f"Comment must be {COMMENT_MAX_LENGTH} characters or less"}}), 400
        if not display_name:
            return jsonify({"ok": False, "error": {"code": "NAME_REQUIRED", "message": "Display name is required"}}), 400

        try:
            with transaction() as cur:
                cur.execute(
                    """
                    SELECT id, identity_id::text, display_name
                    FROM timrx_app.community_posts
                    WHERE id = %s AND status = 'published' AND deleted_at IS NULL
                    """,
                    (post_id,),
                )
                post = cur.fetchone()
                if not post:
                    return jsonify({"ok": False, "error": {"code": "POST_NOT_FOUND"}}), 404

                post_owner_id = post["identity_id"]

                cur.execute(
                    """
                    INSERT INTO timrx_app.community_comments
                    (post_id, identity_id, display_name, body)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id::text, created_at, updated_at
                    """,
                    (post_id, identity_id, display_name, body),
                )
                row = cur.fetchone()

            comment_id = row["id"]

            invalidate_community_feed_cache()

            # Notify post owner (not self)
            if post_owner_id != identity_id:
                try:
                    actor = display_name or _lookup_actor_name(identity_id)
                    NotificationService.create(
                        identity_id=post_owner_id,
                        category="community",
                        notif_type="comment_received",
                        title=f"{actor} commented on your creation",
                        body=body[:100] + ("..." if len(body) > 100 else ""),
                        icon="fa-comment",
                        link="/community",
                        meta={
                            "post_id": post_id,
                            "comment_id": comment_id,
                            "commenter_id": identity_id,
                            "actor_name": actor,
                        },
                        ref_type="comment",
                        ref_id=f"{post_id}:{comment_id}",
                    )
                except Exception as notif_err:
                    logger.warning(f"[Community] Comment notification failed: {notif_err}")

            return jsonify({
                "ok": True,
                "comment": {
                    "id": comment_id,
                    "post_id": post_id,
                    "display_name": display_name,
                    "body": body,
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                    "is_mine": True,
                    "is_post_owner": post_owner_id == identity_id,
                    "can_edit": True,
                    "can_delete": True,
                },
            }), 201

        except Exception as e:
            logger.error(f"[Community] Create comment error: {e}")
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR"}}), 500

    return _inner()


@bp.route("/community/comment/<comment_id>", methods=["PATCH", "OPTIONS"])
def edit_comment(comment_id):
    """Edit own comment."""
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        data = request.get_json(silent=True) or {}
        body = (data.get("body") or "").strip()

        if not body:
            return jsonify({"ok": False, "error": {"code": "EMPTY_COMMENT"}}), 400
        if len(body) > COMMENT_MAX_LENGTH:
            return jsonify({"ok": False, "error": {"code": "COMMENT_TOO_LONG"}}), 400

        try:
            with transaction() as cur:
                cur.execute(
                    """
                    UPDATE timrx_app.community_comments
                    SET body = %s
                    WHERE id = %s AND identity_id = %s AND status = 'published'
                    RETURNING id::text, post_id::text, display_name, body, created_at, updated_at
                    """,
                    (body, comment_id, identity_id),
                )
                row = cur.fetchone()

            if not row:
                return jsonify({"ok": False, "error": {"code": "NOT_FOUND_OR_FORBIDDEN"}}), 404

            return jsonify({
                "ok": True,
                "comment": {
                    "id": row["id"],
                    "post_id": row["post_id"],
                    "display_name": row["display_name"],
                    "body": row["body"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                    "is_mine": True,
                    "can_edit": True,
                    "can_delete": True,
                },
            })

        except Exception as e:
            logger.error(f"[Community] Edit comment error: {e}")
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR"}}), 500

    return _inner()


@bp.route("/community/comment/<comment_id>", methods=["DELETE", "OPTIONS"])
def delete_comment(comment_id):
    """Soft-delete own comment."""
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id, auth_error = require_identity()
        if auth_error:
            return auth_error

        try:
            with transaction() as cur:
                cur.execute(
                    """
                    UPDATE timrx_app.community_comments
                    SET status = 'deleted', deleted_at = NOW()
                    WHERE id = %s AND identity_id = %s AND status = 'published'
                    RETURNING id::text
                    """,
                    (comment_id, identity_id),
                )
                row = cur.fetchone()

            if not row:
                return jsonify({"ok": False, "error": {"code": "NOT_FOUND_OR_FORBIDDEN"}}), 404

            invalidate_community_feed_cache()

            return jsonify({"ok": True, "deleted": True})

        except Exception as e:
            logger.error(f"[Community] Delete comment error: {e}")
            return jsonify({"ok": False, "error": {"code": "SERVER_ERROR"}}), 500

    return _inner()
