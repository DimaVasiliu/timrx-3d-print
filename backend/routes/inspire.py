"""
Inspire Routes Blueprint
------------------------
Public endpoint for fetching inspiration content from all user creations.
Returns random models, images, and videos for the Inspire overlay.
"""

from __future__ import annotations

import random
from datetime import datetime
from flask import Blueprint, jsonify, request

from backend.db import USE_DB, get_conn, dict_row

bp = Blueprint("inspire", __name__)


def _get_prompt_of_the_day(cursor) -> dict | None:
    """
    Get a featured prompt of the day.
    Selects a random prompt from recent creations.
    """
    try:
        # Get a random prompt from recent models, images, or videos
        cursor.execute("""
            (
                SELECT prompt, 'model' as category, thumbnail_url
                FROM timrx_app.models
                WHERE prompt IS NOT NULL AND prompt != ''
                  AND thumbnail_url IS NOT NULL
                  AND created_at > NOW() - INTERVAL '30 days'
                ORDER BY RANDOM()
                LIMIT 1
            )
            UNION ALL
            (
                SELECT prompt, 'image' as category, thumbnail_url
                FROM timrx_app.images
                WHERE prompt IS NOT NULL AND prompt != ''
                  AND thumbnail_url IS NOT NULL
                  AND created_at > NOW() - INTERVAL '30 days'
                ORDER BY RANDOM()
                LIMIT 1
            )
            UNION ALL
            (
                SELECT prompt, 'video' as category, thumbnail_url
                FROM timrx_app.videos
                WHERE prompt IS NOT NULL AND prompt != ''
                  AND thumbnail_url IS NOT NULL
                  AND created_at > NOW() - INTERVAL '30 days'
                ORDER BY RANDOM()
                LIMIT 1
            )
            ORDER BY RANDOM()
            LIMIT 1
        """)
        row = cursor.fetchone()

        if row:
            return {
                "prompt": row["prompt"],
                "category": row["category"],
                "thumbnail_url": row["thumbnail_url"]
            }

        # Fallback: return a curated prompt if no content
        return {
            "prompt": "A mystical forest guardian made of twisted ancient vines and glowing mushrooms, ethereal atmosphere",
            "category": "fantasy"
        }
    except Exception as e:
        print(f"[INSPIRE] Error getting prompt of the day: {e}")
        return {
            "prompt": "A sleek futuristic robot assistant with glowing blue accents",
            "category": "sci-fi"
        }


def _determine_card_size(index: int, item_type: str) -> str:
    """Determine card size for masonry layout variety."""
    # Videos get larger sizes
    if item_type == 'video':
        return 'lg'

    # Pattern for visual interest
    pattern = [
        'lg', 'md', 'sm', 'md',
        'sm', 'lg', 'md', 'sm',
        'md', 'sm', 'lg', 'md'
    ]
    return pattern[index % len(pattern)]


@bp.route("/inspire/feed", methods=["GET", "OPTIONS"])
def inspire_feed():
    """
    Get inspiration feed content from all user creations.

    Query params:
    - type: Filter by type (model, image, video, all)
    - limit: Number of items (default 30, max 100)
    - shuffle: Randomize order (default true)
    """
    if request.method == "OPTIONS":
        return ("", 204)

    if not USE_DB:
        return jsonify({
            "ok": False,
            "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}
        }), 503

    try:
        # Parse query parameters
        filter_type = request.args.get("type", "all")
        limit = min(int(request.args.get("limit", 30)), 100)
        shuffle = request.args.get("shuffle", "true").lower() == "true"

        with get_conn() as conn:
            cursor = conn.cursor(row_factory=dict_row)

            # Get prompt of the day
            potd = _get_prompt_of_the_day(cursor)

            # Build the query based on filter type
            # Use UNION ALL to combine results from models, images, and videos
            order_clause = "ORDER BY RANDOM()" if shuffle else "ORDER BY created_at DESC"

            queries = []
            params = []

            # Models query
            if filter_type in ("all", "model", "models"):
                queries.append("""
                    SELECT
                        'model' as asset_type,
                        id::text as asset_id,
                        title,
                        prompt,
                        thumbnail_url,
                        glb_url,
                        NULL as image_url,
                        NULL as video_url,
                        NULL as duration_seconds,
                        created_at
                    FROM timrx_app.models
                    WHERE thumbnail_url IS NOT NULL
                      AND thumbnail_url != ''
                      AND status = 'SUCCEEDED'
                """)

            # Images query
            if filter_type in ("all", "image", "images"):
                queries.append("""
                    SELECT
                        'image' as asset_type,
                        id::text as asset_id,
                        title,
                        prompt,
                        COALESCE(thumbnail_url, image_url) as thumbnail_url,
                        NULL as glb_url,
                        image_url,
                        NULL as video_url,
                        NULL as duration_seconds,
                        created_at
                    FROM timrx_app.images
                    WHERE (thumbnail_url IS NOT NULL AND thumbnail_url != '')
                       OR (image_url IS NOT NULL AND image_url != '')
                """)

            # Videos query
            if filter_type in ("all", "video", "videos"):
                queries.append("""
                    SELECT
                        'video' as asset_type,
                        id::text as asset_id,
                        title,
                        prompt,
                        thumbnail_url,
                        NULL as glb_url,
                        NULL as image_url,
                        video_url,
                        duration_seconds,
                        created_at
                    FROM timrx_app.videos
                    WHERE thumbnail_url IS NOT NULL
                      AND thumbnail_url != ''
                      AND video_url IS NOT NULL
                """)

            if not queries:
                return jsonify({
                    "ok": True,
                    "prompt_of_the_day": potd,
                    "cards": [],
                    "total": 0,
                    "source": "inspire"
                })

            # Combine queries with UNION ALL
            combined_query = f"""
                SELECT * FROM (
                    {' UNION ALL '.join(queries)}
                ) combined
                {order_clause}
                LIMIT %s
            """

            cursor.execute(combined_query, (limit,))
            rows = cursor.fetchall()
            cursor.close()

        # Transform rows into inspire cards
        cards = []
        for idx, row in enumerate(rows):
            asset_type = row["asset_type"]
            thumbnail = row.get("thumbnail_url")

            # Skip if no thumbnail
            if not thumbnail:
                continue

            prompt = row.get("prompt") or row.get("title") or "Untitled creation"

            # Build card object
            card = {
                "id": f"ins-{asset_type[0]}-{row['asset_id']}",
                "type": asset_type,
                "prompt": prompt,
                "thumbnail": thumbnail,
                "size": _determine_card_size(idx, asset_type),
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
            }

            # Add video-specific fields
            if asset_type == "video" and row.get("video_url"):
                card["video_url"] = row["video_url"]
                card["duration"] = row.get("duration_seconds")

            # Generate tags based on recency
            tags = []
            if row.get("created_at"):
                days_old = (datetime.now() - row["created_at"]).days
                if days_old <= 7:
                    tags.append("new")
                if days_old <= 3:
                    tags.append("trending")

            # Randomly assign some tags for variety
            if random.random() > 0.75:
                tags.append("staff-pick")

            card["tags"] = tags if tags else ["community"]

            cards.append(card)

        return jsonify({
            "ok": True,
            "prompt_of_the_day": potd,
            "cards": cards,
            "total": len(cards),
            "source": "inspire"
        })

    except Exception as e:
        print(f"[INSPIRE] Error in feed: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "ok": False,
            "error": {"code": "SERVER_ERROR", "message": str(e)}
        }), 500


@bp.route("/inspire/shuffle", methods=["GET", "OPTIONS"])
def inspire_shuffle():
    """
    Get a fresh shuffled set of inspiration content.
    Alias for /inspire/feed?shuffle=true
    """
    if request.method == "OPTIONS":
        return ("", 204)

    # Forward to feed with shuffle=true
    return inspire_feed()
