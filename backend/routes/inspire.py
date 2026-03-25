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

from backend.db import USE_DB, get_conn, get_conn_resilient, get_conn_direct, dict_row, is_transient_db_error

# Debug: confirm module loads
print("[INSPIRE] Module loaded successfully")

bp = Blueprint("inspire", __name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

MAX_LIMIT = 60
DEFAULT_LIMIT = 24
RECENT_DAYS = 30  # For "new" and "trending" tags

# ── Inspire feed response cache ──
# The feed content changes slowly (admin publishes models/images/videos).
# Seeded requests produce deterministic results, so cache by full param set.
# Non-seeded requests get a short TTL since each random shuffle differs.
import time as _time

_inspire_cache: dict = {}   # cache_key -> (response_dict, monotonic_ts)
_INSPIRE_CACHE_TTL_SEEDED = 60    # 60s for seeded (deterministic) requests
_INSPIRE_CACHE_TTL_RANDOM = 15    # 15s for random (non-seeded) requests
_INSPIRE_CACHE_MAX = 50


def _inspire_cache_key(limit, filter_type, mix_mode, shuffle, seed):
    """Build a cache key from all parameters that affect the response."""
    return (limit, filter_type, mix_mode, shuffle, seed or "__random__")


def _get_cached_inspire(key, seed):
    entry = _inspire_cache.get(key)
    if not entry:
        return None
    ttl = _INSPIRE_CACHE_TTL_SEEDED if seed else _INSPIRE_CACHE_TTL_RANDOM
    if (_time.monotonic() - entry[1]) < ttl:
        return entry[0]
    del _inspire_cache[key]
    return None


def _set_cached_inspire(key, data):
    _inspire_cache[key] = (data, _time.monotonic())
    if len(_inspire_cache) > _INSPIRE_CACHE_MAX:
        cutoff = _time.monotonic() - _INSPIRE_CACHE_TTL_SEEDED
        expired = [k for k, (_, ts) in _inspire_cache.items() if ts < cutoff]
        for k in expired:
            del _inspire_cache[k]


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


def _fetch_models(cursor, limit: Optional[int] = None, debug: bool = False) -> List[Dict]:
    """
    Fetch models with valid thumbnails. Accepts various success status values.
    Includes ALL stages: preview, texture, remesh, refine, etc.
    Returns thumb_preview (always) and thumb_refined (if a related version exists).
    If limit is None, fetches ALL models.
    """
    order = "ORDER BY m.created_at DESC"
    limit_clause = f"LIMIT {limit}" if limit else ""

    # Query ALL models (all stages) with valid thumbnails
    # For preview models: look for textured/refined versions as thumb_refined
    # For texture/remesh/refine models: look for preview version as thumb_refined (shows before/after)
    # Status filter: accept 'ready', 'succeeded', 'success', 'completed', 'done', 'finished' (case-insensitive)
    cursor.execute(f"""
        WITH base_models AS (
            SELECT
                id,
                title,
                prompt,
                root_prompt,
                thumbnail_url,
                glb_url,
                stage,
                upstream_job_id,
                created_at
            FROM timrx_app.models
            WHERE thumbnail_url IS NOT NULL
              AND thumbnail_url != ''
              AND glb_url IS NOT NULL
              AND glb_url != ''
              AND (
                status IS NULL
                OR LOWER(status) IN ('ready', 'succeeded', 'success', 'completed', 'done', 'finished')
              )
        )
        SELECT
            m.id::text as id,
            'model' as type,
            m.title,
            COALESCE(m.prompt, m.root_prompt) as prompt,
            m.thumbnail_url as thumb_preview,
            m.glb_url,
            -- For preview: find textured/refined version; For texture/refine: find preview version
            CASE
                WHEN m.stage IS NULL OR LOWER(m.stage) IN ('preview', 'initial', 'image3d', '') THEN
                    COALESCE(
                        (SELECT thumbnail_url FROM base_models r
                         WHERE r.upstream_job_id = m.upstream_job_id
                           AND r.id != m.id
                           AND LOWER(r.stage) IN ('texture', 'retexture', 'textured')
                         ORDER BY r.created_at DESC LIMIT 1),
                        (SELECT thumbnail_url FROM base_models r
                         WHERE r.upstream_job_id = m.upstream_job_id
                           AND r.id != m.id
                           AND LOWER(r.stage) IN ('refine', 'refined', 'remesh')
                         ORDER BY r.created_at DESC LIMIT 1)
                    )
                ELSE
                    -- For texture/remesh/refine: show preview as "before" on hover
                    (SELECT thumbnail_url FROM base_models r
                     WHERE r.upstream_job_id = m.upstream_job_id
                       AND r.id != m.id
                       AND (r.stage IS NULL OR LOWER(r.stage) IN ('preview', 'initial', 'image3d', ''))
                     ORDER BY r.created_at ASC LIMIT 1)
            END as thumb_refined,
            COALESCE(m.stage, 'preview') as stage,
            m.created_at
        FROM base_models m
        {order}
        {limit_clause}
    """)

    rows = cursor.fetchall()

    # Debug logging
    if debug:
        print(f"[INSPIRE] DEBUG: Fetched {len(rows)} models after filtering")
        if len(rows) == 0:
            # Show what statuses/stages exist in DB to help diagnose
            cursor.execute("""
                SELECT status, stage, COUNT(*) as cnt
                FROM timrx_app.models
                WHERE thumbnail_url IS NOT NULL AND thumbnail_url != ''
                GROUP BY status, stage
                ORDER BY cnt DESC
                LIMIT 10
            """)
            info = cursor.fetchall()
            print(f"[INSPIRE] DEBUG: No models matched. Existing status/stage combos: {[dict(s) for s in info]}")

    return [dict(r) for r in rows]


def _fetch_images(cursor, limit: Optional[int] = None, debug: bool = False) -> List[Dict]:
    """
    Fetch images with valid thumbnails or image URLs.
    Returns thumb_preview (thumbnail or image_url) and thumb_refined (full image_url if different).
    If limit is None, fetches ALL images.
    """
    order = "ORDER BY created_at DESC"
    limit_clause = f"LIMIT {limit}" if limit else ""

    cursor.execute(f"""
        SELECT
            id::text as id,
            'image' as type,
            title,
            prompt,
            COALESCE(thumbnail_url, image_url) as thumb_preview,
            -- For images: use full image_url as "refined" if different from thumbnail
            CASE
                WHEN thumbnail_url IS NOT NULL AND image_url IS NOT NULL AND thumbnail_url != image_url
                THEN image_url
                ELSE NULL
            END as thumb_refined,
            width,
            height,
            created_at
        FROM timrx_app.images
        WHERE (thumbnail_url IS NOT NULL AND thumbnail_url != '')
           OR (image_url IS NOT NULL AND image_url != '')
        {order}
        {limit_clause}
    """)

    rows = cursor.fetchall()
    return [dict(r) for r in rows]


def _fetch_videos(cursor, limit: Optional[int] = None, debug: bool = False) -> List[Dict]:
    """
    Fetch videos with valid thumbnails.
    Videos don't have refined thumbnails, so thumb_refined is always null.
    If limit is None, fetches ALL videos.
    """
    order = "ORDER BY created_at DESC"
    limit_clause = f"LIMIT {limit}" if limit else ""

    cursor.execute(f"""
        SELECT
            id::text as id,
            'video' as type,
            title,
            prompt,
            thumbnail_url as thumb_preview,
            NULL as thumb_refined,
            video_url,
            duration_seconds,
            created_at
        FROM timrx_app.videos
        WHERE thumbnail_url IS NOT NULL
          AND thumbnail_url != ''
          AND video_url IS NOT NULL
        {order}
        {limit_clause}
    """)

    rows = cursor.fetchall()
    return [dict(r) for r in rows]


def _balanced_mix(models: List[Dict], images: List[Dict], videos: List[Dict], target: int, shuffle: bool, seed: Optional[str]) -> List[Any]:
    """
    Create a balanced mix of content types with fallback reallocation.
    Target: ~1/3 each type, reallocating if a type is short.
    """
    per_type = ceil(target / 3)

    # Take what we can from each type
    m_take = models[:per_type]
    i_take = images[:per_type]
    v_take = videos[:per_type]

    # Calculate shortfalls and available extras
    m_short = per_type - len(m_take)
    i_short = per_type - len(i_take)
    v_short = per_type - len(v_take)

    total_short = m_short + i_short + v_short

    # If there's shortfall, reallocate from types with extras
    if total_short > 0:
        m_extra = models[per_type:]
        i_extra = images[per_type:]
        v_extra = videos[per_type:]

        # Fill shortfalls from available extras
        extras = m_extra + i_extra + v_extra
        if shuffle:
            random.shuffle(extras)

        fill_count = min(total_short, len(extras))
        fill_items = extras[:fill_count]

        # Add fill items to the appropriate lists based on type
        for item in fill_items:
            if item.get("type") == "model":
                m_take.append(item)
            elif item.get("type") == "image":
                i_take.append(item)
            else:
                v_take.append(item)

    # Interleave for even distribution
    result = _interleave_lists(m_take, i_take, v_take)

    # Final shuffle if requested
    if shuffle and not seed:
        random.shuffle(result)
    elif seed:
        result = _seeded_shuffle(result, seed + "_mixed")

    return result[:target]


def _transform_to_card(item: Dict, index: int, total_count: int = 24) -> Dict[str, Any]:
    """
    Transform a DB row into an inspire card with size variety.
    Returns normalized thumbnail fields for hover swap functionality.
    """
    item_id = item.get("id", "")
    item_type = item.get("type", "model")

    # Get thumbnail URLs - support both old and new field names
    thumb_preview = item.get("thumb_preview") or item.get("thumb_url") or ""
    thumb_refined = item.get("thumb_refined")  # May be None

    # Skip items without thumbnails
    if not thumb_preview:
        return None

    # Determine card size for visual variety
    # - Videos default to lg
    # - First 1-2 non-video cards can be lg based on hash
    # - Rest are sm or md
    if item_type == "video":
        size = "lg"
    else:
        # Use item hash for stable size assignment
        hash_val = int(hashlib.md5(item_id.encode()).hexdigest(), 16)
        if index < 3 and hash_val % 8 == 0:  # ~12.5% of first 3 items get lg
            size = "lg"
        elif hash_val % 3 == 0:  # ~33% get md
            size = "md"
        else:
            size = "sm"

    card = {
        "id": f"ins-{item_type[0]}-{item_id}",
        "type": item_type,
        "title": item.get("title") or item.get("prompt") or "Untitled",
        "prompt": item.get("prompt") or item.get("title") or "Untitled creation",
        # Normalized thumbnail fields for frontend hover swap
        "thumb_preview": thumb_preview,
        "thumb_refined": thumb_refined,  # null if no refined version exists
        "has_refine": thumb_refined is not None and thumb_refined != "",
        # Legacy field for backwards compatibility
        "thumb_url": thumb_preview,
        "created_at": item["created_at"].isoformat() if item.get("created_at") else None,
        "tags": _get_tags(item.get("created_at"), item_id),
        "size": size,
    }

    # Add glb_url for models (for 3D viewer loading)
    if item_type == "model" and item.get("glb_url"):
        card["glb_url"] = item["glb_url"]

    # Add video URL for videos
    if item_type == "video" and item.get("video_url"):
        card["video_url"] = item["video_url"]

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
        response = Response("", status=204)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response

    # Check database availability
    if not USE_DB:
        response = jsonify({
            "ok": False,
            "error": {"code": "DB_UNAVAILABLE", "message": "Database not configured"}
        })
        response.headers["Content-Type"] = "application/json"
        return response, 503

    try:
        # Parse query parameters
        limit = min(int(request.args.get("limit", DEFAULT_LIMIT)), MAX_LIMIT)
        shuffle = request.args.get("shuffle", "true").lower() == "true"
        seed = request.args.get("seed")
        filter_type = request.args.get("type", "all").lower()
        mix_mode = request.args.get("mix", "balanced").lower()
        if mix_mode not in ("balanced", "sequential"):
            mix_mode = "balanced"

        # Short-circuit: return cached response if within TTL
        _cache_key = _inspire_cache_key(limit, filter_type, mix_mode, shuffle, seed)
        cached = _get_cached_inspire(_cache_key, seed)
        if cached is not None:
            response = jsonify(cached)
            response.headers["Content-Type"] = "application/json"
            return response

        # ── DB fetch with pool→direct fallback ──
        # Cap per-type fetch to 3x the requested limit. This prevents full table
        # scans while leaving enough headroom for balanced mix reallocation.
        _fetch_cap = min(limit * 3, 200)

        def _inspire_db_read(conn_getter):
            """Run all inspire DB reads inside one connection."""
            import time as _itime
            _t_conn = _itime.monotonic()
            with conn_getter as conn:
                _t_start = _itime.monotonic()
                cursor = conn.cursor(row_factory=dict_row)
                potd = _get_prompt_of_the_day(cursor)
                if filter_type == "all":
                    models = _fetch_models(cursor, limit=_fetch_cap, debug=True)
                    images = _fetch_images(cursor, limit=_fetch_cap, debug=True)
                    videos = _fetch_videos(cursor, limit=_fetch_cap, debug=True)
                elif filter_type in ("model", "models"):
                    models = _fetch_models(cursor, limit=_fetch_cap, debug=True)
                    images, videos = [], []
                elif filter_type in ("image", "images"):
                    images = _fetch_images(cursor, limit=_fetch_cap, debug=True)
                    models, videos = [], []
                elif filter_type in ("video", "videos"):
                    videos = _fetch_videos(cursor, limit=_fetch_cap, debug=True)
                    models, images = [], []
                else:
                    models, images, videos = [], [], []
                cursor.close()
                _t_done = _itime.monotonic()
                _ms_conn = int((_t_start - _t_conn) * 1000)
                _ms_query = int((_t_done - _t_start) * 1000)
                print(f"[INSPIRE] DB: conn={_ms_conn}ms query={_ms_query}ms "
                      f"models={len(models)} images={len(images)} videos={len(videos)}")
                return potd, models, images, videos

        try:
            potd, models, images, videos = _inspire_db_read(get_conn_resilient("inspire_feed"))
        except Exception as _e1:
            if is_transient_db_error(_e1):
                print(f"[INSPIRE][FALLBACK] pool query failed, using direct: {type(_e1).__name__}")
                potd, models, images, videos = _inspire_db_read(get_conn_direct("inspire_direct"))
            else:
                raise

        total_available = len(models) + len(images) + len(videos)
        already_shuffled = False

        if filter_type == "all":
            print(
                f"[INSPIRE] Feed counts - models:{len(models)} images:{len(images)} "
                f"videos:{len(videos)} total:{total_available} mix:{mix_mode}"
            )
            if mix_mode == "balanced":
                if shuffle:
                    if seed:
                        models = _seeded_shuffle(models, seed + "_models")
                        images = _seeded_shuffle(images, seed + "_images")
                        videos = _seeded_shuffle(videos, seed + "_videos")
                    else:
                        random.shuffle(models)
                        random.shuffle(images)
                        random.shuffle(videos)

                items = _balanced_mix(
                    models=models,
                    images=images,
                    videos=videos,
                    target=limit,
                    shuffle=shuffle,
                    seed=seed,
                )
                already_shuffled = True
            else:
                items = models + images + videos
        elif filter_type in ("model", "models"):
            items = models
            print(f"[INSPIRE] Models-only feed: {len(items)} items")
        elif filter_type in ("image", "images"):
            items = images
            print(f"[INSPIRE] Images-only feed: {len(items)} items")
        elif filter_type in ("video", "videos"):
            items = videos
            print(f"[INSPIRE] Videos-only feed: {len(items)} items")
        else:
            items = []

        # Shuffle ALL items together (balanced mode already shuffled/mixed above)
        if shuffle and not already_shuffled:
            if seed:
                items = _seeded_shuffle(items, seed)
            else:
                random.shuffle(items)

        # Transform to cards and trim to limit
        cards = []
        seen_ids = set()
        for idx, item in enumerate(items):
            if len(cards) >= limit:
                break
            # Skip duplicates; include type to avoid cross-table id collisions.
            item_id = item.get("id", "")
            item_type = item.get("type", "unknown")
            unique_key = f"{item_type}:{item_id}"
            if unique_key in seen_ids:
                continue
            seen_ids.add(unique_key)

            card = _transform_to_card(item, idx, limit)
            if card:
                cards.append(card)

        result = {
            "ok": True,
            "prompt_of_the_day": potd,
            "cards": cards,
            "total": len(cards),
            "total_available": total_available,
            "source": "inspire"
        }
        _set_cached_inspire(_cache_key, result)
        response = jsonify(result)
        response.headers["Content-Type"] = "application/json"
        return response

    except Exception as e:
        if is_transient_db_error(e):
            # Both pool AND direct failed — truly degraded
            print(f"[INSPIRE][DEGRADED] pool+direct both failed, returning empty: {type(e).__name__}: {e}")
            response = jsonify({
                "ok": True,
                "prompt_of_the_day": None,
                "cards": [],
                "total": 0,
                "total_available": 0,
                "source": "inspire_degraded",
            })
            response.headers["Content-Type"] = "application/json"
            return response
        print(f"[INSPIRE] Error in feed: {e}")
        import traceback
        traceback.print_exc()

        response = jsonify({
            "ok": False,
            "error": {"code": "SERVER_ERROR", "message": str(e)}
        })
        response.headers["Content-Type"] = "application/json"
        return response, 500


@bp.route("/inspire/shuffle", methods=["GET", "OPTIONS"])
def inspire_shuffle() -> Response:
    """
    Get a fresh shuffled set of inspiration content.
    Redirects to /inspire/feed with shuffle params.
    """
    if request.method == "OPTIONS":
        return Response("", status=204)

    from flask import redirect, url_for
    seed = str(random.randint(1, 1000000))
    return redirect(url_for('.inspire_feed', shuffle='true', seed=seed, **request.args))
