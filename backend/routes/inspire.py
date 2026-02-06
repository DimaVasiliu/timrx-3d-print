"""
Inspire Routes Blueprint
------------------------
Production-quality API for the Inspire feed with balanced shuffle.
Returns mixed models, images, and videos from the database.
"""

from __future__ import annotations

import hashlib
import random
from datetime import datetime, timezone
from math import ceil
from typing import List, Dict, Any, Optional

from flask import Blueprint, jsonify, request, Response

from backend.db import USE_DB, get_conn, dict_row

bp = Blueprint("inspire", __name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

MAX_LIMIT = 60
DEFAULT_LIMIT = 24
RECENT_DAYS = 30  # For "new" and "trending" tags


# =============================================================================
# HELPERS
# =============================================================================

def _seeded_shuffle(items: List[Any], seed: str) -> List[Any]:
    """Shuffle list with a deterministic seed for stable results."""
    hash_seed = int(hashlib.md5(seed.encode()).hexdigest(), 16) % (2**32)
    rng = random.Random(hash_seed)
    result = items[:]
    rng.shuffle(result)
    return result


def _interleave_lists(*lists: List[Any]) -> List[Any]:
    """
    Interleave multiple lists: take one from each in round-robin fashion.
    Example: [A1,A2,A3], [B1,B2], [C1] -> [A1,B1,C1,A2,B2,A3]
    """
    result = []
    iterators = [iter(lst) for lst in lists]
    while iterators:
        exhausted = []
        for i, it in enumerate(iterators):
            try:
                result.append(next(it))
            except StopIteration:
                exhausted.append(i)
        # Remove exhausted iterators in reverse order to preserve indices
        for i in reversed(exhausted):
            iterators.pop(i)
    return result


def _compute_aspect(width: Optional[int], height: Optional[int]) -> str:
    """Determine aspect ratio category from dimensions."""
    if not width or not height:
        return "square"
    ratio = width / height
    if ratio > 1.3:
        return "landscape"
    elif ratio < 0.77:
        return "portrait"
    return "square"


def _get_tags(created_at: Optional[datetime], item_id: str) -> List[str]:
    """Generate tags based on recency and stable random assignment."""
    tags = []

    if created_at:
        # Handle both naive and timezone-aware datetimes
        now = datetime.now(timezone.utc) if created_at.tzinfo else datetime.now()
        days_old = (now - created_at).days

        if days_old <= 3:
            tags.append("trending")
        elif days_old <= 7:
            tags.append("new")

    # Stable "staff-pick" assignment based on item ID
    if item_id:
        hash_val = int(hashlib.md5(item_id.encode()).hexdigest(), 16)
        if hash_val % 5 == 0:  # ~20% get staff-pick
            tags.append("staff-pick")

    return tags if tags else ["community"]


def _get_prompt_of_the_day(cursor) -> Dict[str, Any]:
    """
    Get a featured prompt of the day.
    Uses date-based seed for daily consistency.
    """
    try:
        # Use today's date as seed for consistent daily selection
        today_seed = datetime.now().strftime("%Y-%m-%d")

        cursor.execute("""
            SELECT prompt, 'model' as category, thumbnail_url
            FROM timrx_app.models
            WHERE prompt IS NOT NULL AND prompt != ''
              AND thumbnail_url IS NOT NULL AND thumbnail_url != ''
              AND created_at > NOW() - INTERVAL '60 days'
            UNION ALL
            SELECT prompt, 'image' as category, COALESCE(thumbnail_url, image_url) as thumbnail_url
            FROM timrx_app.images
            WHERE prompt IS NOT NULL AND prompt != ''
              AND (thumbnail_url IS NOT NULL OR image_url IS NOT NULL)
              AND created_at > NOW() - INTERVAL '60 days'
            UNION ALL
            SELECT prompt, 'video' as category, thumbnail_url
            FROM timrx_app.videos
            WHERE prompt IS NOT NULL AND prompt != ''
              AND thumbnail_url IS NOT NULL AND thumbnail_url != ''
              AND created_at > NOW() - INTERVAL '60 days'
        """)
        rows = cursor.fetchall()

        if rows:
            # Use seeded shuffle for daily consistency
            shuffled = _seeded_shuffle(rows, today_seed)
            row = shuffled[0]
            return {
                "prompt": row["prompt"],
                "category": row["category"],
                "thumbnail_url": row.get("thumbnail_url")
            }

    except Exception as e:
        print(f"[INSPIRE] Error getting POTD: {e}")

    # Fallback prompts
    fallbacks = [
        ("A mystical forest guardian made of twisted ancient vines and glowing mushrooms, ethereal atmosphere", "fantasy"),
        ("Cyberpunk street food vendor stall with holographic menu, neon signs, steam rising", "sci-fi"),
        ("Crystal dragon with iridescent scales perched on a volcanic rock formation", "fantasy"),
        ("Robot samurai in meditation pose, cherry blossoms, zen garden background", "sci-fi"),
    ]
    # Pick based on day of year for variety
    idx = datetime.now().timetuple().tm_yday % len(fallbacks)
    return {"prompt": fallbacks[idx][0], "category": fallbacks[idx][1]}


def _fetch_models(cursor, limit: int, shuffle: bool, seed: Optional[str]) -> List[Dict]:
    """Fetch models with valid thumbnails."""
    order = "ORDER BY RANDOM()" if shuffle and not seed else "ORDER BY created_at DESC"

    cursor.execute(f"""
        SELECT
            id::text as id,
            'model' as type,
            title,
            prompt,
            thumbnail_url as thumb_url,
            glb_url as asset_url,
            created_at
        FROM timrx_app.models
        WHERE thumbnail_url IS NOT NULL
          AND thumbnail_url != ''
          AND status = 'SUCCEEDED'
        {order}
        LIMIT %s
    """, (limit,))

    rows = cursor.fetchall()
    if seed and shuffle:
        rows = _seeded_shuffle(rows, seed + "_models")
    return [dict(r) for r in rows]


def _fetch_images(cursor, limit: int, shuffle: bool, seed: Optional[str]) -> List[Dict]:
    """Fetch images with valid thumbnails or image URLs."""
    order = "ORDER BY RANDOM()" if shuffle and not seed else "ORDER BY created_at DESC"

    cursor.execute(f"""
        SELECT
            id::text as id,
            'image' as type,
            title,
            prompt,
            COALESCE(thumbnail_url, image_url) as thumb_url,
            image_url as asset_url,
            width,
            height,
            created_at
        FROM timrx_app.images
        WHERE (thumbnail_url IS NOT NULL AND thumbnail_url != '')
           OR (image_url IS NOT NULL AND image_url != '')
        {order}
        LIMIT %s
    """, (limit,))

    rows = cursor.fetchall()
    if seed and shuffle:
        rows = _seeded_shuffle(rows, seed + "_images")
    return [dict(r) for r in rows]


def _fetch_videos(cursor, limit: int, shuffle: bool, seed: Optional[str]) -> List[Dict]:
    """Fetch videos with valid thumbnails."""
    order = "ORDER BY RANDOM()" if shuffle and not seed else "ORDER BY created_at DESC"

    cursor.execute(f"""
        SELECT
            id::text as id,
            'video' as type,
            title,
            prompt,
            thumbnail_url as thumb_url,
            video_url as asset_url,
            duration_seconds,
            created_at
        FROM timrx_app.videos
        WHERE thumbnail_url IS NOT NULL
          AND thumbnail_url != ''
          AND video_url IS NOT NULL
        {order}
        LIMIT %s
    """, (limit,))

    rows = cursor.fetchall()
    if seed and shuffle:
        rows = _seeded_shuffle(rows, seed + "_videos")
    return [dict(r) for r in rows]


def _transform_to_card(item: Dict, index: int) -> Dict[str, Any]:
    """Transform a DB row into an inspire card."""
    item_id = item.get("id", "")
    item_type = item.get("type", "model")
    thumb_url = item.get("thumb_url", "")

    # Skip items without thumbnails
    if not thumb_url:
        return None

    card = {
        "id": f"ins-{item_type[0]}-{item_id}",
        "type": item_type,
        "title": item.get("title") or item.get("prompt") or "Untitled",
        "prompt": item.get("prompt") or item.get("title") or "Untitled creation",
        "thumb_url": thumb_url,
        "created_at": item["created_at"].isoformat() if item.get("created_at") else None,
        "tags": _get_tags(item.get("created_at"), item_id),
    }

    # Add asset URL if available
    if item.get("asset_url"):
        card["asset_url"] = item["asset_url"]

    # Add aspect ratio for images
    if item_type == "image":
        card["aspect"] = _compute_aspect(item.get("width"), item.get("height"))

    # Add duration for videos
    if item_type == "video" and item.get("duration_seconds"):
        card["duration"] = item["duration_seconds"]

    return card


# =============================================================================
# ROUTES
# =============================================================================

@bp.route("/inspire/feed", methods=["GET", "OPTIONS"])
def inspire_feed() -> Response:
    """
    Get balanced inspiration feed with mixed content types.

    Query params:
    - limit: Number of items (default 24, max 60)
    - shuffle: Randomize order (default true)
    - seed: Optional seed for stable shuffle
    - type: Filter by type (model, image, video, all)
    - mix: Mixing strategy (balanced, sequential) - default balanced

    Returns JSON with:
    - ok: boolean
    - prompt_of_the_day: object with prompt and category
    - cards: array of inspire card objects
    - total: number of cards returned
    """
    # Handle CORS preflight
    if request.method == "OPTIONS":
        return Response("", status=204)

    # Check database availability
    if not USE_DB:
        return jsonify({
            "ok": False,
            "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}
        }), 503

    try:
        # Parse query parameters
        limit = min(int(request.args.get("limit", DEFAULT_LIMIT)), MAX_LIMIT)
        shuffle = request.args.get("shuffle", "true").lower() == "true"
        seed = request.args.get("seed")
        filter_type = request.args.get("type", "all").lower()
        mix_strategy = request.args.get("mix", "balanced").lower()

        with get_conn() as conn:
            cursor = conn.cursor(row_factory=dict_row)

            # Get prompt of the day
            potd = _get_prompt_of_the_day(cursor)

            # Determine how many of each type to fetch
            if filter_type == "all":
                # Balanced: fetch ceil(limit/3) of each type
                per_type = ceil(limit / 3)

                models = _fetch_models(cursor, per_type, shuffle, seed)
                images = _fetch_images(cursor, per_type, shuffle, seed)
                videos = _fetch_videos(cursor, per_type, shuffle, seed)

                if mix_strategy == "balanced":
                    # Interleave for even distribution
                    items = _interleave_lists(models, images, videos)
                else:
                    # Sequential: models, then images, then videos
                    items = models + images + videos
                    if shuffle and not seed:
                        random.shuffle(items)
                    elif seed:
                        items = _seeded_shuffle(items, seed)

            elif filter_type in ("model", "models"):
                items = _fetch_models(cursor, limit, shuffle, seed)

            elif filter_type in ("image", "images"):
                items = _fetch_images(cursor, limit, shuffle, seed)

            elif filter_type in ("video", "videos"):
                items = _fetch_videos(cursor, limit, shuffle, seed)

            else:
                items = []

            cursor.close()

        # Transform to cards and trim to limit
        cards = []
        for idx, item in enumerate(items):
            if len(cards) >= limit:
                break
            card = _transform_to_card(item, idx)
            if card:
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
def inspire_shuffle() -> Response:
    """
    Get a fresh shuffled set of inspiration content.
    Shortcut for /inspire/feed?shuffle=true with a random seed.
    """
    if request.method == "OPTIONS":
        return Response("", status=204)

    # Generate a unique seed for this shuffle request
    request.args = request.args.copy()
    request.args["shuffle"] = "true"
    request.args["seed"] = str(random.randint(1, 1000000))

    return inspire_feed()
