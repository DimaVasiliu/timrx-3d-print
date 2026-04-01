"""
History Routes Blueprint (Modular, Real Logic)
---------------------------------------------
Registered under /api/_mod to avoid conflicts during migration.

This mirrors the monolith behavior but lives in the modular route file.
"""

from __future__ import annotations

import json
import uuid

import time as _time
from flask import Blueprint, jsonify, request, g
from psycopg.sql import SQL as _SQL

from backend.db import USE_DB, get_conn, get_conn_resilient, get_conn_direct, transaction, dict_row, Tables, is_transient_db_error

# Short TTL per-identity cache for history GET — avoids repeated heavy JOIN.
# Invalidated on known writes (job finalize, history insert) so completed
# generations appear immediately.
_history_cache = {}  # "identity_id:limit:offset" -> (items_list, monotonic_ts)
_HISTORY_CACHE_TTL = 15  # seconds


def invalidate_history_cache(identity_id: str):
    """Evict all cached history entries for an identity.
    Call this after any write that adds/changes history items for this user
    (job finalize, history POST, video/image/model save to normalized DB).
    Safe to call from any thread. No-op if identity has no cached entries."""
    if not identity_id:
        return
    prefix = f"{identity_id}:"
    keys_to_drop = [k for k in _history_cache if k.startswith(prefix)]
    for k in keys_to_drop:
        _history_cache.pop(k, None)
from backend.middleware import with_session, with_session_readonly
from backend.services.history_service import (
    _local_history_id,
    _lookup_asset_id_for_history,
    _validate_history_item_asset_ids,
    delete_history_local,
    load_history_store,
    save_history_store,
    upsert_history_local,
)
from backend.services.s3_service import (
    collect_all_s3_keys_for_history_item,
    delete_s3_objects_safe,
    ensure_s3_url_for_data_uri,
)
from backend.utils import derive_display_title, is_generic_title, log_db_continue

bp = Blueprint("history", __name__)


@bp.route("/history", methods=["GET", "POST", "OPTIONS"])
def history_mod():
    # GET is read-only: use readonly session (skips touch + renewal = 0 DB writes).
    # POST modifies history: use full session (touch + renewal as normal).
    _session_wrapper = with_session_readonly if request.method == "GET" else with_session

    @_session_wrapper
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id = getattr(g, "identity_id", None)
        if not identity_id:
            return jsonify({
                "ok": False,
                "error": {"code": "NO_SESSION", "message": "A valid session is required to access history."},
            }), 401

        if request.method == "GET":
            import time
            start_time = time.time()

            items = []
            db_source = False

            # Pagination parameters
            import base64 as _b64
            limit = request.args.get("limit", type=int, default=250)
            item_type_filter = request.args.get("type", type=str, default="all").lower().strip()
            cursor_raw = request.args.get("cursor", type=str, default="")
            # Legacy offset support (ignored when cursor is provided)
            offset = request.args.get("offset", type=int, default=0)
            limit = min(max(1, limit), 500)  # Clamp to 1-500
            offset = max(0, offset)
            # Validate type filter
            if item_type_filter not in ("all", "model", "image", "video"):
                item_type_filter = "all"

            # Decode cursor if provided (base64-encoded JSON: {"created_at": ..., "id": ...})
            cursor_created_at = None
            cursor_id = None
            if cursor_raw:
                try:
                    cursor_json = json.loads(_b64.urlsafe_b64decode(cursor_raw + "==").decode("utf-8"))
                    cursor_created_at = cursor_json.get("created_at")
                    cursor_id = cursor_json.get("id")
                except Exception:
                    pass  # Invalid cursor — treat as first page

            use_cursor = cursor_created_at is not None and cursor_id is not None

            print(f"[History][mod] GET: identity_id={identity_id}, USE_DB={USE_DB}, limit={limit}, type={item_type_filter}, cursor={'yes' if use_cursor else 'no'}")

            # Check per-identity cache (avoids repeated 3s+ DB/fallback fetches)
            _hcache_key = f"{identity_id}:{item_type_filter}:{limit}:{cursor_raw or offset}"
            _hcached = _history_cache.get(_hcache_key)
            _cached_has_more = False
            if _hcached:
                _hc_items, _hc_has_more, _hc_ts = _hcached
                if _time.monotonic() - _hc_ts < _HISTORY_CACHE_TTL:
                    items = _hc_items
                    _cached_has_more = _hc_has_more
                    db_source = True
                    print(f"[History][CACHE_HIT] {len(items)} items")

            if USE_DB and not db_source:
                # ── Build type-specific queries to avoid unnecessary JOINs ──
                # For type=image: only LEFT JOIN images (skip models + videos)
                # For type=video: only LEFT JOIN videos (skip models + images)
                # For type=model: only LEFT JOIN models (skip images + videos)
                # For type=all:   3-way LEFT JOIN (needed for mixed results)
                # This eliminates 2 of 3 expensive JSONB-OR joins for filtered tabs.

                _h_base_cols = """h.id, h.item_type, h.status, h.stage, h.title, h.prompt,
                        h.thumbnail_url, h.glb_url, h.image_url, h.video_url, h.payload, h.created_at,
                        h.model_id, h.image_id, h.video_id, h.lineage_origin_id"""

                _model_cols = """,
                        m.id AS m_id, m.title AS m_title, m.glb_url AS m_glb_url,
                        m.thumbnail_url AS m_thumbnail_url, m.meta AS m_meta,
                        m.prompt AS m_prompt, m.status AS m_status"""
                _model_join = f"""LEFT JOIN {Tables.MODELS} m ON (
                        (h.model_id IS NOT NULL AND h.model_id = m.id) OR
                        (h.model_id IS NULL AND m.upstream_job_id = COALESCE(
                            h.payload->>'original_job_id', h.payload->>'preview_task_id', h.payload->>'source_task_id'
                        ) AND COALESCE(
                            h.payload->>'original_job_id', h.payload->>'preview_task_id', h.payload->>'source_task_id'
                        ) IS NOT NULL))"""

                _image_cols = """,
                        i.id AS i_id, i.title AS i_title, i.image_url AS i_image_url,
                        i.thumbnail_url AS i_thumbnail_url, i.prompt AS i_prompt,
                        i.meta AS i_meta"""
                _image_join = f"""LEFT JOIN {Tables.IMAGES} i ON (
                        (h.image_id IS NOT NULL AND h.image_id = i.id) OR
                        (h.image_id IS NULL AND i.upstream_id = COALESCE(
                            h.payload->>'original_job_id', h.payload->>'preview_task_id', h.payload->>'source_task_id'
                        ) AND COALESCE(
                            h.payload->>'original_job_id', h.payload->>'preview_task_id', h.payload->>'source_task_id'
                        ) IS NOT NULL))"""

                _video_cols = """,
                        v.id AS v_id, v.title AS v_title, v.video_url AS v_video_url,
                        v.thumbnail_url AS v_thumbnail_url, v.duration_seconds AS v_duration_seconds,
                        v.resolution AS v_resolution, v.aspect_ratio AS v_aspect_ratio,
                        v.meta AS v_meta, v.prompt AS v_prompt"""
                _video_join = f"""LEFT JOIN {Tables.VIDEOS} v ON (
                        (h.video_id IS NOT NULL AND h.video_id = v.id) OR
                        (h.video_id IS NULL AND v.upstream_id = COALESCE(
                            h.payload->>'original_id', h.payload->>'original_job_id'
                        ) AND COALESCE(
                            h.payload->>'original_id', h.payload->>'original_job_id'
                        ) IS NOT NULL))"""

                # Null placeholders for columns not selected (keeps enrichment code safe)
                _model_nulls = ",\n                        NULL AS m_id, NULL AS m_title, NULL AS m_glb_url, NULL AS m_thumbnail_url, NULL AS m_meta, NULL AS m_prompt, NULL AS m_status"
                _image_nulls = ",\n                        NULL AS i_id, NULL AS i_title, NULL AS i_image_url, NULL AS i_thumbnail_url, NULL AS i_prompt, NULL AS i_meta"
                _video_nulls = ",\n                        NULL AS v_id, NULL AS v_title, NULL AS v_video_url, NULL AS v_thumbnail_url, NULL AS v_duration_seconds, NULL AS v_resolution, NULL AS v_aspect_ratio, NULL AS v_meta, NULL AS v_prompt"

                if item_type_filter == "model":
                    _select_extra = _model_cols + _image_nulls + _video_nulls
                    _joins = _model_join
                elif item_type_filter == "image":
                    _select_extra = _model_nulls + _image_cols + _video_nulls
                    _joins = _image_join
                elif item_type_filter == "video":
                    _select_extra = _model_nulls + _image_nulls + _video_cols
                    _joins = _video_join
                else:
                    # type=all — full 3-way join
                    _select_extra = _model_cols + _image_cols + _video_cols
                    _joins = f"{_model_join}\n                    {_image_join}\n                    {_video_join}"

                _where_type = "AND h.item_type = %s" if item_type_filter != "all" else ""
                _where_cursor = "AND (h.created_at, h.id) < (%s, %s::uuid)" if use_cursor else ""

                _hsql = f"""
                    SELECT
                        {_h_base_cols}{_select_extra}
                    FROM {Tables.HISTORY_ITEMS} h
                    {_joins}
                    WHERE h.identity_id = %s
                      {_where_type}
                      {_where_cursor}
                    ORDER BY h.created_at DESC, h.id DESC
                    LIMIT %s
                """
                # Fetch limit+1 to detect has_more without a COUNT query.
                # Build params dynamically based on which clauses are active.
                _hparams_list = [identity_id]
                if item_type_filter != "all":
                    _hparams_list.append(item_type_filter)
                if use_cursor:
                    _hparams_list.extend([cursor_created_at, cursor_id])
                _hparams_list.append(limit + 1)
                # Fall back to OFFSET only when no cursor is provided and offset > 0
                if not use_cursor and offset > 0:
                    _hsql += " OFFSET %s"
                    _hparams_list.append(offset)
                _hparams = tuple(_hparams_list)

                _hsql_q = _SQL(_hsql)  # Wrap str → SQL for psycopg3 type safety

                def _fetch_history(conn_getter):
                    _t_conn = time.time()
                    with conn_getter as c:
                        _t_query = time.time()
                        with c.cursor(row_factory=dict_row) as cur:
                            cur.execute(_hsql_q, _hparams)
                            rows = cur.fetchall()
                        _t_done = time.time()
                        return rows, int((_t_query - _t_conn) * 1000), int((_t_done - _t_query) * 1000)

                try:
                    _conn_source = "pool"
                    try:
                        rows, _ms_checkout, _ms_query = _fetch_history(get_conn_resilient("history"))
                    except Exception as e1:
                        if is_transient_db_error(e1):
                            print(f"[History][FALLBACK] pool query failed, using direct: {type(e1).__name__}")
                            _conn_source = "direct"
                            rows, _ms_checkout, _ms_query = _fetch_history(get_conn_direct("history_direct"))
                        else:
                            raise
                    _ms_total = int((time.time() - start_time) * 1000)
                    _ms_auth = _ms_total - _ms_checkout - _ms_query
                    print(f"[History][mod] GET: {len(rows)} items type={item_type_filter} "
                          f"total={_ms_total}ms auth={_ms_auth}ms conn={_ms_checkout}ms query={_ms_query}ms "
                          f"src={_conn_source}")
                    db_source = True

                    def _scrub_meshy_urls(value):
                        if isinstance(value, dict):
                            return {k: v for k, v in value.items() if not (isinstance(v, str) and "meshy.ai" in v)}
                        if isinstance(value, list):
                            return [v for v in value if not (isinstance(v, str) and "meshy.ai" in v)]
                        return value

                    def _enrich_model_meta(meta):
                        """Extract model_urls and textured_model_urls from meta JSONB."""
                        if not meta:
                            return {}, {}
                        if isinstance(meta, str):
                            try:
                                meta = json.loads(meta)
                            except Exception:
                                return {}, {}
                        if isinstance(meta, dict):
                            return meta.get("model_urls", {}), meta.get("textured_model_urls", {})
                        return {}, {}

                    def _parse_meta(meta):
                        """Parse meta JSONB to dict."""
                        if not meta:
                            return {}
                        if isinstance(meta, str):
                            try:
                                return json.loads(meta)
                            except Exception:
                                return {}
                        return meta if isinstance(meta, dict) else {}

                    for r in rows:
                        # Build unified response structure with 'kind' field
                        item_type = r["item_type"]
                        item = {
                            "id": str(r["id"]),
                            "kind": item_type,  # unified 'kind' field
                            "type": item_type,  # keep 'type' for backward compatibility
                            "status": r["status"],
                            "created_at": int(r["created_at"].timestamp() * 1000) if r["created_at"] else None,
                        }

                        # Base fields from history_items
                        if r.get("lineage_origin_id"):
                            item["lineage_origin_id"] = str(r["lineage_origin_id"])
                        if r["stage"]:
                            item["stage"] = r["stage"]
                        if r["title"]:
                            item["title"] = r["title"]
                        if r["prompt"]:
                            item["prompt"] = r["prompt"]
                        if r["thumbnail_url"]:
                            item["thumbnail_url"] = r["thumbnail_url"]
                        if r["glb_url"]:
                            item["glb_url"] = r["glb_url"]
                        if r["image_url"]:
                            item["image_url"] = r["image_url"]
                        if r.get("video_url"):
                            item["video_url"] = r["video_url"]

                        # Include original payload for legacy compatibility
                        payload = r["payload"] if r["payload"] else {}
                        if isinstance(payload, str):
                            try:
                                payload = json.loads(payload)
                            except Exception:
                                payload = {}

                        # Hydrate from joined MODEL data
                        if r.get("m_id"):
                            item["model_id"] = str(r["m_id"])
                            if r.get("m_glb_url"):
                                item["glb_url"] = r["m_glb_url"]
                            if r.get("m_thumbnail_url"):
                                item["thumbnail_url"] = r["m_thumbnail_url"]
                            if r.get("m_title") and not item.get("title"):
                                item["title"] = r["m_title"]
                            if r.get("m_prompt") and not item.get("prompt"):
                                item["prompt"] = r["m_prompt"]
                            # Include full model meta
                            model_meta = _parse_meta(r.get("m_meta"))
                            if model_meta:
                                item["meta"] = model_meta
                                # Also extract model_urls for convenience
                                model_urls, textured_model_urls = _enrich_model_meta(r.get("m_meta"))
                                if model_urls:
                                    item["model_urls"] = model_urls
                                if textured_model_urls:
                                    item["textured_model_urls"] = textured_model_urls

                        # Hydrate from joined IMAGE data
                        if r.get("i_id"):
                            item["image_id"] = str(r["i_id"])
                            if r.get("i_image_url"):
                                item["image_url"] = r["i_image_url"]
                            if r.get("i_thumbnail_url"):
                                item["thumbnail_url"] = r["i_thumbnail_url"]
                            if r.get("i_title") and not item.get("title"):
                                item["title"] = r["i_title"]
                            if r.get("i_prompt") and not item.get("prompt"):
                                item["prompt"] = r["i_prompt"]
                            image_meta = _parse_meta(r.get("i_meta"))
                            if image_meta:
                                item["meta"] = image_meta
                                for key in ("artifact_format", "provider_variant", "output_mode", "operation", "upstream_request_id", "upstream_cost", "format"):
                                    if image_meta.get(key) is not None:
                                        item[key] = image_meta.get(key)

                        # Hydrate from joined VIDEO data
                        if r.get("v_id"):
                            item["video_id"] = str(r["v_id"])
                            if r.get("v_video_url"):
                                item["video_url"] = r["v_video_url"]
                            if r.get("v_thumbnail_url"):
                                item["thumbnail_url"] = r["v_thumbnail_url"]
                            if r.get("v_title") and not item.get("title"):
                                item["title"] = r["v_title"]
                            if r.get("v_prompt") and not item.get("prompt"):
                                item["prompt"] = r["v_prompt"]
                            if r.get("v_duration_seconds"):
                                item["duration_seconds"] = r["v_duration_seconds"]
                            if r.get("v_resolution"):
                                item["resolution"] = r["v_resolution"]
                            if r.get("v_aspect_ratio"):
                                item["aspect_ratio"] = r["v_aspect_ratio"]
                            # Include video meta
                            video_meta = _parse_meta(r.get("v_meta"))
                            if video_meta:
                                item["meta"] = video_meta

                        # Scrub any remaining meshy.ai URLs (we only want S3 URLs)
                        for key in ("glb_url", "thumbnail_url", "image_url", "video_url"):
                            val = item.get(key)
                            if isinstance(val, str) and "meshy.ai" in val:
                                item[key] = None

                        for key in ("model_urls", "textured_model_urls", "texture_urls"):
                            if key in item:
                                item[key] = _scrub_meshy_urls(item.get(key))

                        # Derive title if still missing
                        if not item.get("title"):
                            item["title"] = derive_display_title(
                                item.get("prompt"),
                                None,
                                root_prompt=item.get("root_prompt"),
                            )

                        items.append(item)

                    # Trim the extra peek row and derive has_more.
                    # We fetched limit+1 rows; if we got more than limit,
                    # there is definitely a next page.
                    if len(items) > limit:
                        items = items[:limit]
                        _cached_has_more = True
                    else:
                        _cached_has_more = False

                    # Summary: count rig/animate items (per-item logging removed for production)
                    stage_counts = {}
                    for item in items:
                        stage = (item.get("stage") or "").lower()
                        if stage in ("rig", "animate", "animation"):
                            stage_counts[stage] = stage_counts.get(stage, 0) + 1
                    if stage_counts:
                        print(f"[History] Rig/animate items: {stage_counts}")

                    save_history_store(items)
                    # Cache the enriched items + has_more flag for short-TTL reuse
                    _history_cache[_hcache_key] = (items, _cached_has_more, _time.monotonic())
                except Exception as e:
                    if is_transient_db_error(e):
                        # Try stale cache before degrading to empty
                        if _hcached:
                            items = _hcached[0]
                            _cached_has_more = _hcached[1]
                            db_source = True
                            print(f"[History][STALE_OK] returning {len(items)} cached items: {type(e).__name__}")
                        else:
                            print(f"[History][DEGRADED] pool+direct both failed, returning empty: {type(e).__name__}: {e}")
                    else:
                        print(f"[History][mod] DB read failed (returning local/empty): {e}")
                        import traceback
                        traceback.print_exc()
                    # Fall through to return local history or empty array

            # If DB wasn't used or failed, try local history
            if not db_source:
                local_items = load_history_store()
                if isinstance(local_items, list):
                    items = local_items
                    print(f"[History][mod] GET: Returning {len(items)} items from local store")

            # Log total time including N+1 queries
            total_time = time.time() - start_time
            print(f"[History][mod] GET: Total response time {total_time:.3f}s for {len(items)} items")

            # Return paginated response.
            # has_more is determined by the fetch-limit+1 pattern (DB path)
            # or preserved from the process cache (cache-hit path).
            has_more = _cached_has_more

            # Build next_cursor from the last item in the page (for cursor-based pagination).
            next_cursor = None
            if has_more and items:
                last = items[-1]
                last_created_at = last.get("created_at")
                last_id = last.get("id")
                if last_created_at is not None and last_id:
                    # created_at is epoch ms (int) — convert to ISO for the cursor
                    from datetime import datetime, timezone as _tz
                    if isinstance(last_created_at, (int, float)):
                        iso_ts = datetime.fromtimestamp(last_created_at / 1000, tz=_tz.utc).isoformat()
                    else:
                        iso_ts = str(last_created_at)
                    cursor_payload = json.dumps({"created_at": iso_ts, "id": str(last_id)})
                    next_cursor = _b64.urlsafe_b64encode(cursor_payload.encode()).decode().rstrip("=")

            return jsonify({
                "ok": True,
                "items": items,
                "type": item_type_filter,
                "limit": limit,
                "offset": offset,
                "count": len(items),
                "has_more": has_more,
                "next_cursor": next_cursor,
                # Legacy field — kept for backward compatibility with offset-based consumers
                "next_offset": offset + len(items) if has_more else None,
            })

        try:
            payload = request.get_json(silent=True) or []
            if not isinstance(payload, list):
                return jsonify({"error": "Payload must be a list"}), 400

            db_ok = None
            db_errors: list[dict[str, str]] = []
            updated_ids: list[str] = []
            inserted_ids: list[str] = []
            skipped_items: list[dict[str, str]] = []

            if USE_DB:
                try:
                    # ── Pre-process: convert data-URIs to S3 URLs BEFORE
                    #    opening a DB connection, so S3 I/O never pins a
                    #    pooled connection. S3 keys are content-hash based
                    #    so the result is identical regardless of use_id. ──
                    for item in payload:
                        _item_type = item.get("type") or item.get("item_type") or "model"
                        _provider = "openai" if _item_type == "image" else "meshy"
                        _item_id = item.get("id") or item.get("job_id") or ""
                        _thumb = item.get("thumbnail_url")
                        _img = item.get("image_url")
                        if _thumb and isinstance(_thumb, str) and _thumb.startswith("data:"):
                            item["thumbnail_url"] = ensure_s3_url_for_data_uri(
                                _thumb, "thumbnails",
                                f"thumbnails/{identity_id}/{_item_id}",
                                user_id=identity_id, name="thumbnail",
                                provider=_provider,
                            )
                        if _img and isinstance(_img, str) and _img.startswith("data:"):
                            item["image_url"] = ensure_s3_url_for_data_uri(
                                _img, "images",
                                f"images/{identity_id}/{_item_id}",
                                user_id=identity_id, name="image",
                                provider=_provider,
                            )

                    # ── DB phase: atomic SELECT + INSERT/UPDATE (no S3 held) ──
                    with transaction("history_bulk_sync") as cur:
                        for item in payload:
                            item_id = item.get("id") or item.get("job_id")
                            if not item_id:
                                continue

                            cur.execute(
                                f"""
                                SELECT id FROM {Tables.HISTORY_ITEMS}
                                WHERE (id::text = %s
                                   OR payload->>'original_job_id' = %s
                                   OR payload->>'original_id' = %s
                                   OR payload->>'job_id' = %s)
                                  AND identity_id = %s
                                LIMIT 1
                                """,
                                (str(item_id), str(item_id), str(item_id), str(item_id), identity_id),
                            )
                            existing = cur.fetchone()
                            # Note: cursor returns dict due to connection's default row_factory=dict_row
                            existing_id = existing["id"] if existing else None

                            item_type = item.get("type") or item.get("item_type") or "model"
                            status = item.get("status") or "pending"
                            stage = item.get("stage")
                            title = item.get("title")
                            prompt = item.get("prompt")
                            root_prompt = item.get("root_prompt")
                            if not title:
                                title = derive_display_title(prompt, None, root_prompt=root_prompt)
                            # Nullify generic titles so SQL COALESCE preserves existing good titles
                            if is_generic_title(title):
                                title = None
                            thumbnail_url = item.get("thumbnail_url")
                            glb_url = item.get("glb_url")
                            image_url = item.get("image_url")
                            lineage_origin_id = item.get("lineage_origin_id") or item.get("lineage_root_id")
                            provider = "openai" if item_type == "image" else "meshy"

                            if existing_id:
                                use_id = str(existing_id)
                            else:
                                try:
                                    uuid.UUID(str(item_id))
                                    use_id = str(item_id)
                                except (ValueError, TypeError, AttributeError):
                                    use_id = str(uuid.uuid4())
                                    item["original_id"] = item_id

                            item["id"] = use_id

                            if existing_id:
                                cur.execute(
                                    f"""UPDATE {Tables.HISTORY_ITEMS}
                                       SET item_type = %s,
                                           status = COALESCE(%s, status),
                                           stage = COALESCE(%s, stage),
                                           title = CASE
                                               WHEN %s::text IS NOT NULL
                                                AND %s::text <> ''
                                                AND %s::text NOT IN ('3D Model', 'Untitled', '(untitled)', 'Textured Model', 'Remeshed Model', 'Refined Model', 'Rigged Model', 'Image to 3D Model', 'Generated Model', 'Model', 'Image', 'Video')
                                               THEN %s::text
                                               ELSE title
                                           END,
                                           prompt = COALESCE(%s, prompt),
                                           root_prompt = COALESCE(%s, root_prompt),
                                           identity_id = COALESCE(%s, identity_id),
                                           thumbnail_url = COALESCE(%s, thumbnail_url),
                                           glb_url = COALESCE(%s, glb_url),
                                           image_url = COALESCE(%s, image_url),
                                           lineage_origin_id = COALESCE(%s::uuid, lineage_origin_id),
                                           payload = %s,
                                           updated_at = NOW()
                                       WHERE id = %s;""",
                                    (
                                        item_type,
                                        status,
                                        stage,
                                        title,
                                        title,
                                        title,
                                        title,
                                        prompt,
                                        root_prompt,
                                        identity_id,
                                        thumbnail_url,
                                        glb_url,
                                        image_url,
                                        lineage_origin_id,
                                        json.dumps(item),
                                        use_id,
                                    ),
                                )
                                updated_ids.append(use_id)
                            else:
                                model_id = item.get("model_id")
                                image_id = item.get("image_id")
                                lookup_reason = None

                                # Video items use video_id, not model_id/image_id
                                video_id = None
                                if item_type == "video":
                                    model_id = None
                                    image_id = None
                                    video_id = item.get("video_id")
                                    if not video_id:
                                        # Failed videos have no video record — skip to avoid XOR constraint violation
                                        # Skipped: video item without video_id (counted in summary)
                                        skipped_items.append({"client_id": str(item_id), "reason": "no_video_id"})
                                        continue

                                    # Validate video_id references videos table (not jobs table).
                                    # Frontend may send job_id as video_id — resolve to real videos.id.
                                    cur.execute(
                                        f"SELECT id FROM {Tables.VIDEOS} WHERE id::text = %s LIMIT 1",
                                        (str(video_id),),
                                    )
                                    if not cur.fetchone():
                                        # video_id is not in videos table — try resolving via jobs.meta
                                        from backend.services.history_service import resolve_video_uuid
                                        resolved = resolve_video_uuid(str(video_id), identity_id)
                                        if resolved:
                                            # Resolved job_id → video_uuid (counted in summary)
                                            video_id = resolved
                                        else:
                                            # Skipped: video_id not in videos table (counted in summary)
                                            skipped_items.append({"client_id": str(item_id), "reason": "video_id_not_in_videos"})
                                            continue
                                else:
                                    if not model_id and not image_id:
                                        model_id, image_id, lookup_reason = _lookup_asset_id_for_history(
                                            cur, item_type, item_id, glb_url, image_url, identity_id, provider
                                        )
                                    if not _validate_history_item_asset_ids(model_id, image_id, f"bulk_sync:{item_id}"):
                                        if lookup_reason:
                                            skip_reason = lookup_reason
                                        elif model_id and image_id:
                                            skip_reason = "xor_violation"
                                        else:
                                            skip_reason = "missing_asset_reference"
                                        # Skipped: asset validation (counted in summary)
                                        skipped_items.append({"client_id": str(item_id), "reason": skip_reason})
                                        continue

                                cur.execute(
                                    f"""INSERT INTO {Tables.HISTORY_ITEMS} (id, identity_id, item_type, status, stage, title, prompt,
                                           root_prompt, thumbnail_url, glb_url, image_url, model_id, image_id, video_id,
                                           lineage_origin_id, payload)
                                       VALUES (%s, %s, %s, %s, %s, %s, %s,
                                           %s, %s, %s, %s, %s, %s, %s,
                                           %s::uuid, %s)
                                       ON CONFLICT (id) DO UPDATE
                                       SET item_type = EXCLUDED.item_type,
                                           status = COALESCE(EXCLUDED.status, {Tables.HISTORY_ITEMS}.status),
                                           stage = COALESCE(EXCLUDED.stage, {Tables.HISTORY_ITEMS}.stage),
                                           title = CASE
                                               WHEN EXCLUDED.title IS NOT NULL
                                                AND EXCLUDED.title <> ''
                                                AND EXCLUDED.title NOT IN ('3D Model', 'Untitled', '(untitled)', 'Textured Model', 'Remeshed Model', 'Refined Model', 'Rigged Model', 'Image to 3D Model', 'Generated Model', 'Model', 'Image', 'Video')
                                               THEN EXCLUDED.title
                                               ELSE {Tables.HISTORY_ITEMS}.title
                                           END,
                                           prompt = COALESCE(EXCLUDED.prompt, {Tables.HISTORY_ITEMS}.prompt),
                                           root_prompt = COALESCE(EXCLUDED.root_prompt, {Tables.HISTORY_ITEMS}.root_prompt),
                                           identity_id = COALESCE(EXCLUDED.identity_id, {Tables.HISTORY_ITEMS}.identity_id),
                                           thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {Tables.HISTORY_ITEMS}.thumbnail_url),
                                           glb_url = COALESCE(EXCLUDED.glb_url, {Tables.HISTORY_ITEMS}.glb_url),
                                           image_url = COALESCE(EXCLUDED.image_url, {Tables.HISTORY_ITEMS}.image_url),
                                           model_id = COALESCE(EXCLUDED.model_id, {Tables.HISTORY_ITEMS}.model_id),
                                           image_id = COALESCE(EXCLUDED.image_id, {Tables.HISTORY_ITEMS}.image_id),
                                           video_id = COALESCE(EXCLUDED.video_id, {Tables.HISTORY_ITEMS}.video_id),
                                           lineage_origin_id = COALESCE(EXCLUDED.lineage_origin_id, {Tables.HISTORY_ITEMS}.lineage_origin_id),
                                           payload = EXCLUDED.payload,
                                           updated_at = NOW();""",
                                    (
                                        use_id,
                                        identity_id,
                                        item_type,
                                        status,
                                        stage,
                                        title,
                                        prompt,
                                        root_prompt,
                                        thumbnail_url,
                                        glb_url,
                                        image_url,
                                        model_id,
                                        image_id,
                                        video_id,
                                        lineage_origin_id,
                                        json.dumps(item),
                                    ),
                                )
                                inserted_ids.append(use_id)
                    # transaction() auto-commits on success.
                    print(
                        f"[History][mod] Bulk sync: updated={len(updated_ids)}, inserted={len(inserted_ids)}, skipped={len(skipped_items)}"
                    )
                    db_ok = True
                except Exception as e:
                    log_db_continue("history_bulk_write", e)
                    db_errors.append({"op": "history_bulk_write", "error": str(e)})
                    import traceback
                    traceback.print_exc()
                    db_ok = False

            # Invalidate history cache after writes so new items appear immediately
            invalidate_history_cache(identity_id)
            save_history_store(payload)
            return jsonify(
                {
                    "ok": True,
                    "count": len(payload),
                    "updated": updated_ids,
                    "inserted": inserted_ids,
                    "skipped": skipped_items if skipped_items else None,
                    "db": db_ok,
                    "db_errors": db_errors or None,
                    "source": "modular",
                }
            )
        except Exception as e:
            print(f"[INTERNAL_ERROR] context=history_sync error={e}")
            return jsonify({"error": "SERVER_ERROR", "message": "Something went wrong. Please try again."}), 500

    return _inner()


@bp.route("/history/item", methods=["POST", "OPTIONS"])
def history_item_add_mod():
    @with_session
    def _inner():
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id = getattr(g, "identity_id", None)
        if not identity_id:
            return jsonify(
                {
                    "ok": False,
                    "error": {"code": "NO_SESSION", "message": "A valid session is required to add history items."},
                }
            ), 401

        try:
            item = request.get_json(silent=True) or {}
            item_id = item.get("id") or item.get("job_id")
            if not item_id:
                return jsonify({"error": "Item ID required"}), 400

            item["id"] = item_id
            db_ok = False
            db_errors: list[dict[str, str]] = []

            if USE_DB:
                try:
                    # ── Extract fields and convert data-URIs to S3 BEFORE
                    #    opening a DB connection, so S3 I/O never pins a
                    #    pooled connection. ──
                    item_type = item.get("type") or item.get("item_type") or "model"
                    status = item.get("status") or "pending"
                    stage = item.get("stage")
                    title = item.get("title")
                    prompt = item.get("prompt")
                    root_prompt = item.get("root_prompt")
                    if not title:
                        title = derive_display_title(prompt, None, root_prompt=root_prompt)
                    # Nullify generic titles so SQL COALESCE preserves existing good titles
                    if is_generic_title(title):
                        title = None
                    thumbnail_url = item.get("thumbnail_url")
                    glb_url = item.get("glb_url")
                    image_url = item.get("image_url")
                    lineage_origin_id = item.get("lineage_origin_id") or item.get("lineage_root_id")

                    provider = "openai" if item_type == "image" else "meshy"
                    s3_user_id = identity_id
                    if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
                        thumbnail_url = ensure_s3_url_for_data_uri(
                            thumbnail_url,
                            "thumbnails",
                            f"thumbnails/{s3_user_id}/{item_id}",
                            user_id=identity_id,
                            name="thumbnail",
                            provider=provider,
                        )
                    if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
                        image_url = ensure_s3_url_for_data_uri(
                            image_url,
                            "images",
                            f"images/{s3_user_id}/{item_id}",
                            user_id=identity_id,
                            name="image",
                            provider=provider,
                        )
                    item["thumbnail_url"] = thumbnail_url
                    item["image_url"] = image_url

                    # ── DB phase: atomic SELECT + INSERT/UPDATE (no S3 held) ──
                    with transaction("history_item_add") as cur:
                        cur.execute(
                            f"""
                            SELECT id FROM {Tables.HISTORY_ITEMS}
                            WHERE (id::text = %s
                               OR payload->>'original_job_id' = %s
                               OR payload->>'original_id' = %s
                               OR payload->>'job_id' = %s)
                              AND identity_id = %s
                            LIMIT 1
                            """,
                            (str(item_id), str(item_id), str(item_id), str(item_id), identity_id),
                        )
                        existing = cur.fetchone()
                        existing_id = existing["id"] if existing else None

                        if existing_id:
                            use_id = str(existing_id)
                        else:
                            try:
                                uuid.UUID(str(item_id))
                                use_id = str(item_id)
                            except (ValueError, TypeError, AttributeError):
                                use_id = str(uuid.uuid4())
                                item["original_id"] = item_id

                        item["id"] = use_id

                        if existing_id:
                            cur.execute(
                                f"""UPDATE {Tables.HISTORY_ITEMS}
                                   SET item_type = %s,
                                       status = COALESCE(%s, status),
                                       stage = COALESCE(%s, stage),
                                       title = CASE
                                           WHEN %s::text IS NOT NULL
                                            AND %s::text <> ''
                                            AND %s::text NOT IN ('3D Model', 'Untitled', '(untitled)', 'Textured Model', 'Remeshed Model', 'Refined Model', 'Rigged Model', 'Image to 3D Model', 'Generated Model', 'Model', 'Image', 'Video')
                                           THEN %s::text
                                           ELSE title
                                       END,
                                       prompt = COALESCE(%s, prompt),
                                       root_prompt = COALESCE(%s, root_prompt),
                                       identity_id = COALESCE(%s, identity_id),
                                       thumbnail_url = COALESCE(%s, thumbnail_url),
                                       glb_url = COALESCE(%s, glb_url),
                                       image_url = COALESCE(%s, image_url),
                                       lineage_origin_id = COALESCE(%s::uuid, lineage_origin_id),
                                       payload = %s,
                                       updated_at = NOW()
                                   WHERE id = %s;""",
                                (
                                    item_type, status, stage,
                                    title, title, title, title,
                                    prompt, root_prompt, identity_id,
                                    thumbnail_url, glb_url, image_url,
                                    lineage_origin_id,
                                    json.dumps(item), use_id,
                                ),
                            )
                            db_ok = True
                            item_id = use_id
                        else:
                            model_id = item.get("model_id")
                            image_id = item.get("image_id")
                            lookup_reason = None

                            # Video items use video_id, not model_id/image_id — skip XOR check
                            if item_type == "video":
                                model_id = None
                                image_id = None
                            elif not model_id and not image_id:
                                model_id, image_id, lookup_reason = _lookup_asset_id_for_history(
                                    cur, item_type, item_id, glb_url, image_url, identity_id, provider
                                )
                            if item_type != "video" and not _validate_history_item_asset_ids(model_id, image_id, f"item_add:{item_id}"):
                                if lookup_reason:
                                    skip_reason = lookup_reason
                                elif model_id and image_id:
                                    skip_reason = "xor_violation"
                                else:
                                    skip_reason = "missing_asset_reference"

                                if skip_reason == "xor_violation":
                                    print(f"[History][mod] Rejecting item {item_id} - reason: {skip_reason}")
                                    return jsonify(
                                        {
                                            "ok": False,
                                            "error": {
                                                "code": "INVALID_ASSET_REFERENCE",
                                                "message": "History item cannot have both model_id and image_id set",
                                                "reason": skip_reason,
                                                "client_id": str(item_id),
                                            },
                                        }
                                    ), 400

                                print(
                                    f"[History][mod] Skipped early insert for {item_id} - backend will create on job completion"
                                )
                                return jsonify(
                                    {
                                        "ok": True,
                                        "id": str(item_id),
                                        "db": False,
                                        "backend_handles": True,
                                        "message": "History will be created automatically when job completes and asset is saved",
                                        "source": "modular",
                                    }
                                )

                            cur.execute(
                                f"""INSERT INTO {Tables.HISTORY_ITEMS} (id, identity_id, item_type, status, stage, title, prompt,
                                       root_prompt, thumbnail_url, glb_url, image_url, model_id, image_id,
                                       lineage_origin_id, payload)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s,
                                       %s, %s, %s, %s, %s, %s,
                                       %s::uuid, %s)
                                   ON CONFLICT (id) DO UPDATE
                                   SET item_type = EXCLUDED.item_type,
                                       status = COALESCE(EXCLUDED.status, {Tables.HISTORY_ITEMS}.status),
                                       stage = COALESCE(EXCLUDED.stage, {Tables.HISTORY_ITEMS}.stage),
                                       title = CASE
                                           WHEN EXCLUDED.title IS NOT NULL
                                            AND EXCLUDED.title <> ''
                                            AND EXCLUDED.title NOT IN ('3D Model', 'Untitled', '(untitled)', 'Textured Model', 'Remeshed Model', 'Refined Model', 'Rigged Model', 'Image to 3D Model', 'Generated Model', 'Model', 'Image', 'Video')
                                           THEN EXCLUDED.title
                                           ELSE {Tables.HISTORY_ITEMS}.title
                                       END,
                                       prompt = COALESCE(EXCLUDED.prompt, {Tables.HISTORY_ITEMS}.prompt),
                                       root_prompt = COALESCE(EXCLUDED.root_prompt, {Tables.HISTORY_ITEMS}.root_prompt),
                                       identity_id = COALESCE(EXCLUDED.identity_id, {Tables.HISTORY_ITEMS}.identity_id),
                                       thumbnail_url = COALESCE(EXCLUDED.thumbnail_url, {Tables.HISTORY_ITEMS}.thumbnail_url),
                                       glb_url = COALESCE(EXCLUDED.glb_url, {Tables.HISTORY_ITEMS}.glb_url),
                                       image_url = COALESCE(EXCLUDED.image_url, {Tables.HISTORY_ITEMS}.image_url),
                                       model_id = COALESCE(EXCLUDED.model_id, {Tables.HISTORY_ITEMS}.model_id),
                                       image_id = COALESCE(EXCLUDED.image_id, {Tables.HISTORY_ITEMS}.image_id),
                                       lineage_origin_id = COALESCE(EXCLUDED.lineage_origin_id, {Tables.HISTORY_ITEMS}.lineage_origin_id),
                                       payload = EXCLUDED.payload,
                                       updated_at = NOW();""",
                                (
                                    use_id, identity_id, item_type, status, stage,
                                    title, prompt, root_prompt,
                                    thumbnail_url, glb_url, image_url,
                                    model_id, image_id,
                                    lineage_origin_id, json.dumps(item),
                                ),
                            )
                            db_ok = True
                            item_id = use_id
                    # transaction() auto-commits on success.
                except Exception as e:
                    log_db_continue("history_item_add", e)
                    db_errors.append({"op": "history_item_add", "error": str(e)})

            local_ok = upsert_history_local(item, merge=False)
            return jsonify(
                {
                    "ok": db_ok or False,
                    "id": item_id,
                    "db": db_ok,
                    "db_errors": db_errors or None,
                    "local": local_ok,
                    "source": "modular",
                }
            )
        except Exception as e:
            print(f"[INTERNAL_ERROR] context=history_item_add error={e}")
            return jsonify({"error": "SERVER_ERROR", "message": "Something went wrong. Please try again."}), 500

    return _inner()


# ─── Debug: inspect lineage family ────────────────────────────────────────
@bp.route("/history/lineage/<item_id>", methods=["GET", "OPTIONS"])
@with_session
def history_lineage_debug(item_id: str):
    """Debug endpoint: shows the full lineage family for a given item."""
    if request.method == "OPTIONS":
        return ("", 204)
    identity_id = g.identity_id
    if not USE_DB:
        return jsonify({"error": "DB not available"}), 503
    try:
        with get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Find the item's lineage_origin_id
                cur.execute(
                    f"""
                    SELECT id, lineage_origin_id, stage, title, status,
                           payload->>'original_job_id' as original_job_id,
                           payload->>'source_task_id' as source_task_id,
                           payload->>'preview_task_id' as preview_task_id,
                           payload->>'rig_task_id' as rig_task_id
                    FROM {Tables.HISTORY_ITEMS}
                    WHERE id::text = %s AND identity_id = %s
                    """,
                    (str(item_id), identity_id),
                )
                item = cur.fetchone()
                if not item:
                    return jsonify({"error": "Item not found"}), 404

                root_id = item.get("lineage_origin_id") or item["id"]

                # Find all items in the same family
                cur.execute(
                    f"""
                    SELECT id, lineage_origin_id, stage, title, status, created_at,
                           payload->>'original_job_id' as original_job_id,
                           payload->>'source_task_id' as source_task_id,
                           payload->>'preview_task_id' as preview_task_id,
                           payload->>'rig_task_id' as rig_task_id,
                           thumbnail_url IS NOT NULL as has_thumbnail
                    FROM {Tables.HISTORY_ITEMS}
                    WHERE identity_id = %s AND lineage_origin_id = %s
                    ORDER BY created_at ASC
                    """,
                    (identity_id, root_id),
                )
                family = cur.fetchall()

                return jsonify({
                    "ok": True,
                    "queried_item": str(item_id),
                    "lineage_root": str(root_id),
                    "family_size": len(family),
                    "members": [
                        {
                            "id": str(r["id"]),
                            "stage": r["stage"],
                            "title": (r["title"] or "")[:50],
                            "status": r["status"],
                            "lineage_origin_id": str(r["lineage_origin_id"]) if r["lineage_origin_id"] else None,
                            "original_job_id": r["original_job_id"],
                            "source_task_id": r["source_task_id"],
                            "preview_task_id": r["preview_task_id"],
                            "rig_task_id": r["rig_task_id"],
                            "has_thumbnail": r["has_thumbnail"],
                        }
                        for r in family
                    ],
                })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/history/item/<item_id>", methods=["GET", "PATCH", "DELETE", "OPTIONS"])
def history_item_update_mod(item_id: str):
    @with_session
    def _inner(item_id: str):
        if request.method == "OPTIONS":
            return ("", 204)

        identity_id = getattr(g, "identity_id", None)
        if not identity_id:
            return jsonify(
                {
                    "ok": False,
                    "error": {"code": "NO_SESSION", "message": "A valid session is required to modify history items."},
                }
            ), 401

        try:
            uuid.UUID(str(item_id))
        except (ValueError, TypeError):
            pass

        # ── GET: Return single history item ──────────────────
        if request.method == "GET":
            if not USE_DB:
                return jsonify({"ok": False, "error": "not_available"}), 503
            try:
                with get_conn() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        # Single optimized query with LEFT JOINs to hydrate all asset data
                        cur.execute(
                            f"""
                            SELECT
                                h.id, h.item_type, h.status, h.stage, h.title, h.prompt,
                                h.thumbnail_url, h.glb_url, h.image_url, h.video_url,
                                h.payload, h.created_at,
                                h.model_id, h.image_id, h.video_id,
                                -- Model data from joined models table
                                m.id AS m_id, m.title AS m_title, m.glb_url AS m_glb_url,
                                m.thumbnail_url AS m_thumbnail_url, m.meta AS m_meta,
                                m.prompt AS m_prompt, m.status AS m_status,
                                -- Image data from joined images table
                                i.id AS i_id, i.title AS i_title, i.image_url AS i_image_url,
                                i.thumbnail_url AS i_thumbnail_url,
                                -- Video data from joined videos table
                                v.id AS v_id, v.title AS v_title, v.video_url AS v_video_url,
                                v.thumbnail_url AS v_thumbnail_url, v.duration_seconds AS v_duration_seconds,
                                v.resolution AS v_resolution, v.aspect_ratio AS v_aspect_ratio
                            FROM {Tables.HISTORY_ITEMS} h
                            LEFT JOIN {Tables.MODELS} m ON h.model_id = m.id
                            LEFT JOIN {Tables.IMAGES} i ON h.image_id = i.id
                            LEFT JOIN {Tables.VIDEOS} v ON h.video_id = v.id
                            WHERE h.id::text = %s AND h.identity_id = %s
                            LIMIT 1
                            """,
                            (str(item_id), identity_id),
                        )
                        row = cur.fetchone()
                if not row:
                    return jsonify({"ok": False, "error": "not_found"}), 404

                item = {
                    "id": str(row["id"]),
                    "type": row["item_type"],
                    "status": row["status"],
                    "stage": row.get("stage"),
                    "title": row.get("title"),
                    "prompt": row.get("prompt"),
                    "thumbnail_url": row.get("thumbnail_url"),
                    "glb_url": row.get("glb_url"),
                    "image_url": row.get("image_url"),
                    "video_url": row.get("video_url"),
                    "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
                    "model_id": str(row["model_id"]) if row.get("model_id") else None,
                    "image_id": str(row["image_id"]) if row.get("image_id") else None,
                    "video_id": str(row["video_id"]) if row.get("video_id") else None,
                }

                # Helper to extract model_urls from meta JSONB
                def _extract_model_urls(meta):
                    if not meta:
                        return {}, {}
                    if isinstance(meta, str):
                        try:
                            meta = json.loads(meta)
                        except Exception:
                            return {}, {}
                    if isinstance(meta, dict):
                        return meta.get("model_urls", {}), meta.get("textured_model_urls", {})
                    return {}, {}

                # Enrich with model data (from LEFT JOIN)
                if row.get("m_id"):
                    if row.get("m_glb_url"):
                        item["glb_url"] = row["m_glb_url"]
                    if row.get("m_thumbnail_url"):
                        item["thumbnail_url"] = row["m_thumbnail_url"]
                    if row.get("m_title") and not item.get("title"):
                        item["title"] = row["m_title"]
                    if row.get("m_prompt") and not item.get("prompt"):
                        item["prompt"] = row["m_prompt"]
                    # Extract model_urls and textured_model_urls from meta
                    model_urls, textured_model_urls = _extract_model_urls(row.get("m_meta"))
                    if model_urls:
                        item["model_urls"] = model_urls
                    if textured_model_urls:
                        item["textured_model_urls"] = textured_model_urls

                # Enrich with image data (from LEFT JOIN)
                if row.get("i_id"):
                    if row.get("i_image_url"):
                        item["image_url"] = row["i_image_url"]
                    if row.get("i_thumbnail_url"):
                        item["thumbnail_url"] = row["i_thumbnail_url"]
                    if row.get("i_title") and not item.get("title"):
                        item["title"] = row["i_title"]

                # Enrich with video data (from LEFT JOIN)
                if row.get("v_id"):
                    if row.get("v_video_url"):
                        item["video_url"] = row["v_video_url"]
                    if row.get("v_thumbnail_url"):
                        item["thumbnail_url"] = row["v_thumbnail_url"]
                    if row.get("v_title") and not item.get("title"):
                        item["title"] = row["v_title"]
                    if row.get("v_duration_seconds"):
                        item["duration_seconds"] = row["v_duration_seconds"]
                    if row.get("v_resolution"):
                        item["resolution"] = row["v_resolution"]
                    if row.get("v_aspect_ratio"):
                        item["aspect_ratio"] = row["v_aspect_ratio"]

                # Include payload fields
                payload = row.get("payload") or {}
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}
                if isinstance(payload, dict):
                    item["payload"] = payload

                # Derive title if still missing
                if not item.get("title"):
                    item["title"] = derive_display_title(
                        item.get("prompt"),
                        None,
                        root_prompt=item.get("root_prompt"),
                    )

                return jsonify({"ok": True, "item": item})
            except Exception as e:
                log_db_continue("history_item_get", e)
                return jsonify({"ok": False, "error": "SERVER_ERROR", "message": "Something went wrong. Please try again."}), 500

        if request.method == "DELETE":
            db_ok = False
            db_errors: list[dict[str, str]] = []
            s3_cleanup_result = None

            if USE_DB:
                try:
                    # ── Read phase: fetch row + collect S3 keys (short get_conn) ──
                    with get_conn() as conn:
                        with conn.cursor(row_factory=dict_row) as cur:
                            cur.execute(
                                f"""
                                SELECT id, item_type, model_id, image_id, video_id, thumbnail_url, glb_url, image_url, video_url, payload
                                FROM {Tables.HISTORY_ITEMS}
                                WHERE id::text = %s AND identity_id = %s
                                LIMIT 1
                                """,
                                (str(item_id), identity_id),
                            )
                            row = cur.fetchone()
                    # Connection released here.

                    if not row:
                        return jsonify({"ok": False, "error": "not_found"}), 404

                    model_id = row["model_id"]
                    image_id = row["image_id"]
                    video_id = row["video_id"]
                    payload = row["payload"] if row["payload"] else {}
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except Exception:
                            payload = {}

                    row["payload"] = payload

                    # Collect ALL S3 keys BEFORE deleting DB rows.
                    # This calls get_conn() internally — safe now that
                    # the read-phase connection was already released.
                    s3_keys = collect_all_s3_keys_for_history_item(
                        history_row=row,
                        model_id=model_id,
                        image_id=image_id,
                        video_id=video_id,
                    )

                    print(f"[DELETE] history_item={item_id} model_id={model_id} image_id={image_id} video_id={video_id} s3_keys={len(s3_keys)}")

                    # ── Write phase: atomic multi-table cascade (transaction) ──
                    orphaned_jobs = []
                    with transaction("history_item_delete") as cur:
                        # Remove community posts referencing this history item,
                        # its sibling history items, or its model/image assets.
                        # Must happen BEFORE deleting history items to avoid
                        # ON DELETE SET NULL violating ck_community_one_ref.
                        cur.execute(
                            """
                            DELETE FROM timrx_app.community_posts
                            WHERE identity_id = %s AND (
                                history_item_id::text = %s
                                OR (model_id IS NOT NULL AND model_id = %s)
                                OR (image_id IS NOT NULL AND image_id = %s)
                                OR history_item_id IN (
                                    SELECT id FROM timrx_app.history_items
                                    WHERE identity_id = %s AND (
                                        (model_id IS NOT NULL AND model_id = %s)
                                        OR (image_id IS NOT NULL AND image_id = %s)
                                        OR (video_id IS NOT NULL AND video_id = %s)
                                    )
                                )
                            )
                            """,
                            (identity_id, str(item_id), model_id, image_id,
                             identity_id, model_id, image_id, video_id),
                        )
                        community_deleted = cur.rowcount
                        if community_deleted:
                            print(f"[DELETE] removed {community_deleted} community post(s) for history_item={item_id}")

                        # Delete the requested history item
                        cur.execute(
                            f"""
                            DELETE FROM {Tables.HISTORY_ITEMS}
                            WHERE id::text = %s AND identity_id = %s
                            """,
                            (str(item_id), identity_id),
                        )

                        # Delete any OTHER history items referencing the same asset
                        # to avoid ON DELETE SET NULL violating the asset_xor constraint
                        if model_id:
                            cur.execute(f"DELETE FROM {Tables.HISTORY_ITEMS} WHERE model_id = %s AND identity_id = %s", (model_id, identity_id))
                        if image_id:
                            cur.execute(f"DELETE FROM {Tables.HISTORY_ITEMS} WHERE image_id = %s AND identity_id = %s", (image_id, identity_id))
                        if video_id:
                            cur.execute(f"DELETE FROM {Tables.HISTORY_ITEMS} WHERE video_id = %s AND identity_id = %s", (video_id, identity_id))

                        # Mark linked jobs as deleted_by_user so recovery never revives them
                        if model_id:
                            # Find jobs via model's upstream_job_id
                            cur.execute(
                                f"""
                                SELECT j.id::text AS job_id, j.reservation_id::text AS reservation_id
                                FROM {Tables.JOBS} j
                                INNER JOIN {Tables.MODELS} m ON j.upstream_job_id = m.upstream_job_id
                                WHERE m.id = %s AND j.identity_id = %s
                                  AND j.status NOT IN ('ready', 'succeeded', 'failed', 'refunded', 'ready_unbilled', 'deleted_by_user')
                                """,
                                (model_id, identity_id),
                            )
                            model_orphaned_jobs = cur.fetchall() or []
                            if model_orphaned_jobs:
                                job_ids = [oj["job_id"] for oj in model_orphaned_jobs]
                                placeholders = ",".join(["%s"] * len(job_ids))
                                cur.execute(
                                    f"""
                                    UPDATE {Tables.JOBS}
                                    SET status = 'deleted_by_user',
                                        completed_at = COALESCE(completed_at, NOW()),
                                        meta = COALESCE(meta, '{{}}'::jsonb) || '{{"deleted_by_user": true}}'::jsonb,
                                        updated_at = NOW()
                                    WHERE id::text IN ({placeholders})
                                    """,
                                    tuple(job_ids),
                                )
                                print(f"[DELETE] marked {len(model_orphaned_jobs)} linked job(s) as deleted_by_user for model_id={model_id}")
                                orphaned_jobs.extend(model_orphaned_jobs)

                        if model_id:
                            cur.execute(f"DELETE FROM {Tables.MODELS} WHERE id = %s AND identity_id = %s", (model_id, identity_id))
                        if image_id:
                            cur.execute(f"DELETE FROM {Tables.IMAGES} WHERE id = %s AND identity_id = %s", (image_id, identity_id))
                        if video_id:
                            # Collect reservation_ids from linked jobs before marking them terminal
                            cur.execute(
                                f"""
                                SELECT id::text AS job_id, reservation_id::text AS reservation_id
                                FROM {Tables.JOBS}
                                WHERE meta->>'video_uuid' = %s
                                  AND status NOT IN ('ready', 'succeeded', 'refunded', 'ready_unbilled', 'deleted_by_user')
                                """,
                                (str(video_id),),
                            )
                            video_orphaned_jobs = cur.fetchall() or []

                            # Mark linked jobs as deleted_by_user so rescue never revives them
                            if video_orphaned_jobs:
                                cur.execute(
                                    f"""
                                    UPDATE {Tables.JOBS}
                                    SET status = 'deleted_by_user',
                                        completed_at = COALESCE(completed_at, NOW()),
                                        meta = COALESCE(meta, '{{}}'::jsonb) || '{{"deleted_by_user": true}}'::jsonb,
                                        updated_at = NOW()
                                    WHERE meta->>'video_uuid' = %s
                                      AND status NOT IN ('ready', 'succeeded', 'refunded', 'ready_unbilled', 'deleted_by_user')
                                    """,
                                    (str(video_id),),
                                )
                                print(f"[DELETE] marked {len(video_orphaned_jobs)} linked job(s) as deleted_by_user for video_id={video_id}")
                                orphaned_jobs.extend(video_orphaned_jobs)

                            cur.execute(f"DELETE FROM {Tables.VIDEOS} WHERE id = %s AND identity_id = %s", (video_id, identity_id))
                    # transaction() auto-commits on success, rolls back on exception.
                    db_ok = True

                    # Release held credits for orphaned jobs (after commit, best-effort)
                    if orphaned_jobs:
                        from backend.services.credits_helper import release_job_credits
                        from backend.services.video_errors import ErrorCategory
                        for oj in orphaned_jobs:
                            res_id = oj.get("reservation_id")
                            if res_id:
                                try:
                                    release_job_credits(res_id, ErrorCategory.INTERNAL, oj["job_id"])
                                    print(f"[DELETE] released credits reservation={res_id} job={oj['job_id']}")
                                except Exception as cre:
                                    print(f"[DELETE] WARNING: credit release failed reservation={res_id}: {cre}")

                    # Delete S3 objects (idempotent - safe to retry, logs errors)
                    if s3_keys:
                        s3_cleanup_result = delete_s3_objects_safe(
                            keys=s3_keys,
                            source=f"history_item_delete:{item_id}",
                        )
                        if s3_cleanup_result.get("errors"):
                            db_errors.append({
                                "op": "history_item_delete_s3",
                                "error": f"partial_failure: {len(s3_cleanup_result['errors'])} errors",
                            })

                except Exception as e:
                    log_db_continue("history_item_delete", e)
                    db_errors.append({"op": "history_item_delete", "error": str(e)})
                    delete_history_local(item_id)
                    return jsonify({"ok": False, "error": "delete_failed"}), 500

            delete_history_local(item_id)

            response = {"ok": True, "source": "modular"}
            if s3_cleanup_result:
                response["s3_cleanup"] = {
                    "deleted": s3_cleanup_result.get("deleted", 0),
                    "already_missing": s3_cleanup_result.get("already_missing", 0),
                    "errors": len(s3_cleanup_result.get("errors", [])),
                }
            return jsonify(response)

        if request.method == "PATCH":
            try:
                updates = request.get_json(silent=True) or {}
                db_ok = False
                db_errors: list[dict[str, str]] = []
                if USE_DB:
                    try:
                        # ── Phase 1: short DB read (fetch existing row) ──
                        with get_conn() as conn:
                            with conn.cursor(row_factory=dict_row) as cur:
                                cur.execute(
                                    f"""SELECT id, payload FROM {Tables.HISTORY_ITEMS}
                                           WHERE (id::text = %s
                                              OR payload->>'original_id' = %s
                                              OR payload->>'job_id' = %s)
                                             AND identity_id = %s
                                           LIMIT 1;""",
                                    (str(item_id), str(item_id), str(item_id), identity_id),
                                )
                                row = cur.fetchone()
                        # Connection returned to pool here — not held during S3 I/O.

                        if not row:
                            return jsonify({"error": "Item not found or access denied"}), 404

                        existing = row["payload"] if row["payload"] else {}
                        existing.update(updates)
                        actual_id = row["id"]

                        item_type = updates.get("type") or updates.get("item_type")
                        status = updates.get("status")
                        stage = updates.get("stage")
                        title = updates.get("title")
                        if isinstance(title, str):
                            title_norm = title.strip().lower()
                            if title_norm in ("", "untitled", "(untitled)"):
                                title = None
                        prompt = updates.get("prompt")
                        thumbnail_url = updates.get("thumbnail_url")
                        glb_url = updates.get("glb_url")
                        image_url = updates.get("image_url")
                        video_url = updates.get("video_url")

                        # ── Phase 2: S3 uploads (NO DB connection held) ──
                        provider = "google" if item_type == "video" else ("openai" if item_type == "image" else "meshy")
                        s3_user_id = identity_id
                        if thumbnail_url and isinstance(thumbnail_url, str) and thumbnail_url.startswith("data:"):
                            thumbnail_url = ensure_s3_url_for_data_uri(
                                thumbnail_url,
                                "thumbnails",
                                f"thumbnails/{s3_user_id}/{actual_id}",
                                user_id=identity_id,
                                name="thumbnail",
                                provider=provider,
                            )
                            updates["thumbnail_url"] = thumbnail_url
                        if image_url and isinstance(image_url, str) and image_url.startswith("data:"):
                            image_url = ensure_s3_url_for_data_uri(
                                image_url,
                                "images",
                                f"images/{s3_user_id}/{actual_id}",
                                user_id=identity_id,
                                name="image",
                                provider=provider,
                            )
                            updates["image_url"] = image_url
                        existing.update({k: v for k, v in updates.items() if k in {"thumbnail_url", "image_url", "video_url"}})

                        # ── Phase 3: short DB write (UPDATE + COMMIT) ──
                        title_to_set = title
                        with get_conn() as conn:
                            with conn.cursor(row_factory=dict_row) as cur:
                                cur.execute(
                                    f"""UPDATE {Tables.HISTORY_ITEMS}
                                       SET item_type = COALESCE(%s, item_type),
                                           status = COALESCE(%s, status),
                                           stage = COALESCE(%s, stage),
                                           title = COALESCE(%s::text, title),
                                           prompt = COALESCE(%s, prompt),
                                           thumbnail_url = COALESCE(%s, thumbnail_url),
                                           glb_url = COALESCE(%s, glb_url),
                                           image_url = COALESCE(%s, image_url),
                                           video_url = COALESCE(%s, video_url),
                                           payload = %s,
                                           updated_at = NOW()
                                       WHERE id = %s AND identity_id = %s;""",
                                    (
                                        item_type,
                                        status,
                                        stage,
                                        title_to_set,
                                        prompt,
                                        thumbnail_url,
                                        glb_url,
                                        image_url,
                                        video_url,
                                        json.dumps(existing),
                                        actual_id,
                                        identity_id,
                                    ),
                                )
                            conn.commit()
                        db_ok = True
                    except Exception as e:
                        log_db_continue("history_item_update", e)
                        db_errors.append({"op": "history_item_update", "error": str(e)})

                existing_local = None
                arr = load_history_store()
                if isinstance(arr, list):
                    for entry in arr:
                        if isinstance(entry, dict) and _local_history_id(entry) == item_id:
                            existing_local = entry
                            break
                merged_local = {**(existing_local or {}), **updates, "id": item_id}
                local_ok = upsert_history_local(merged_local, merge=True)

                return jsonify(
                    {
                        "ok": True,
                        "id": item_id,
                        "db": db_ok,
                        "db_errors": db_errors or None,
                        "local": local_ok,
                        "source": "modular",
                    }
                )
            except Exception as e:
                print(f"[INTERNAL_ERROR] context=history_item_update error={e}")
                return jsonify({"error": "SERVER_ERROR", "message": "Something went wrong. Please try again."}), 500

        return jsonify({"error": "Method not allowed"}), 405

    return _inner(item_id)
